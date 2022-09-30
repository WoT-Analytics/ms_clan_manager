"""This file contains the clan manager microservice based on fast api."""
import os
from collections.abc import AsyncGenerator

import fastapi
import nats
import requests
from pydantic import BaseModel

NATS_HOST = os.getenv("NATS_HOST")
NATS_PORT = os.getenv("NATS_PORT")

API_SERVICE_HOST = os.getenv("API_HOST")
API_SERVICE_PORT = os.getenv("API_PORT")

STORE_SERVICE_HOST = os.getenv("STORE_HOST")
STORE_SERVICE_PORT = os.getenv("STORE_PORT")

TIMEOUT = 5

app = fastapi.FastAPI()


class ClanModel(BaseModel):
    """Base Pydantic Model to represent the most import data for a single clan."""

    clan_id: int
    clan_tag: str


async def get_nats_session() -> AsyncGenerator:
    """
    yields a NATS connection
    :return: nats.Nats
    """
    nats_client = await nats.connect(servers=f"nats://{NATS_HOST}:{NATS_PORT}")
    try:
        yield nats_client
    finally:
        await nats_client.close()


def get_stored_clan_data(clan_tag: str) -> ClanModel | None:
    """
    Gets the data for a clan tag stored in the clan store.
    :param clan_tag: tag of the clan to be looked up
    :return: None if the clan cannot be found, otherwise ClanModel with collected data
    """
    response = requests.get(f"http://{STORE_SERVICE_HOST}:{STORE_SERVICE_PORT}/clans/{clan_tag}", timeout=TIMEOUT)
    if response.status_code == fastapi.status.HTTP_404_NOT_FOUND:
        return None
    response.raise_for_status()
    response_data = response.json()
    return ClanModel(**response_data)


def get_api_clan_data(clan_tag: str) -> ClanModel | None:
    """
    Gets the data for a clan tag from the api service.
    :param clan_tag: tag of the clan to be looked up
    :return: None if the clan cannot be found, otherwise ClanModel with collected data
    """
    response = requests.get(f"http://{API_SERVICE_HOST}:{API_SERVICE_PORT}/clans/tag/{clan_tag}", timeout=TIMEOUT)
    if response.status_code == fastapi.status.HTTP_404_NOT_FOUND:
        return None
    response.raise_for_status()
    response_data = response.json()
    return ClanModel(**response_data)


def save_clan(clan_data: ClanModel) -> bool:
    """
    Requests the creation of a clan in the clan store.
    :param clan_data: data of the clan to be created
    :return: True if store returns a CREATED-Status else False
    """
    response = requests.put(
        f"http://{STORE_SERVICE_HOST}:{STORE_SERVICE_PORT}/clans",
        json={"clan_id": clan_data.clan_id, "clan_tag": clan_data.clan_tag},
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    return response.status_code == fastapi.status.HTTP_201_CREATED


def delete_clan(clan_data: ClanModel) -> None:
    """
    Requests the deletion of a clan from the clan store.
    :param clan_data: data of the clan to be deleted
    :return: None
    """
    response = requests.delete(
        f"http://{STORE_SERVICE_HOST}:{STORE_SERVICE_PORT}/clans",
        json={"clan_id": clan_data.clan_id, "clan_tag": clan_data.clan_tag},
        timeout=TIMEOUT,
    )
    response.raise_for_status()


@app.put(
    "/clans/{clan_tag}",
    response_class=fastapi.Response,
    responses={
        201: {"description": "Clan added to the system."},
        200: {"description": "Clan exists in the system."},
        404: {"description": "Requested clan does not exist."},
    },
)
async def add_clan(clan_tag: str, nats_con: nats.NATS = fastapi.Depends(get_nats_session)) -> fastapi.Response:
    """
    Requests the addition of the clan defined by clan tag to the system.
    :param clan_tag: clan to be added from the system
    :param nats_con: connection to the NATS server
    :return: HTTP Response
    """
    existing_clan_data = get_stored_clan_data(clan_tag)
    if existing_clan_data:
        return fastapi.Response(status_code=fastapi.status.HTTP_200_OK)

    api_clan_data = get_api_clan_data(clan_tag)
    if not api_clan_data:
        raise fastapi.HTTPException(
            status_code=fastapi.status.HTTP_404_NOT_FOUND,
            detail=f"Clan [{clan_tag}] could not be added. " f"Unable to find this clan in the API.",
        )

    created = save_clan(api_clan_data)
    if not created:
        return fastapi.Response(status_code=fastapi.status.HTTP_200_OK)
    await nats_con.publish("clans.add", str(api_clan_data.clan_id).encode("utf-8"))
    return fastapi.Response(status_code=fastapi.status.HTTP_201_CREATED)


@app.delete(
    "/clans/{clan_tag}",
    response_class=fastapi.Response,
    responses={
        200: {"description": "Clan removed from the system."},
        404: {"description": "Requested clan does not exist."},
    },
)
async def remove_clan(clan_tag: str, nats_con: nats.NATS = fastapi.Depends(get_nats_session)) -> fastapi.Response:
    """
    Requests the removal of the clan defined by clan tag from the system.
    :param clan_tag: clan to be removed from the system
    :param nats_con: connection to the NATS server
    :return: HTTP Response
    """
    clan_data = get_stored_clan_data(clan_tag)
    if not clan_data:
        clan_data = get_api_clan_data(clan_tag)

    if not clan_data:
        raise fastapi.HTTPException(
            status_code=fastapi.status.HTTP_404_NOT_FOUND,
            detail=f"Clan [{clan_tag}] could not be added. " f"Unable to find this clan in the API.",
        )
    delete_clan(clan_data)
    await nats_con.publish("clans.delete", str(clan_data.clan_id).encode("utf-8"))
    return fastapi.Response(status_code=fastapi.status.HTTP_200_OK)
