import responses
from fastapi.testclient import TestClient

import service.main
from service.main import app, get_nats_session


class NatsMock:

    def __init__(self):
        self.messages = []

    async def publish(self, subject: str, payload: bytes):
        self.messages.append((subject, payload))


async def get_nats_mock():
    return nats_mock

service.main.API_SERVICE_HOST = "api.host"
service.main.API_SERVICE_PORT = "8080"

service.main.STORE_SERVICE_HOST = "store.host"
service.main.STORE_SERVICE_PORT = "8080"

nats_mock = NatsMock()
client = TestClient(app)
app.dependency_overrides[get_nats_session] = get_nats_mock


@responses.activate
def test_add_clan_existing():
    nats_mock.messages = []
    responses.add(
        responses.GET, "http://store.host:8080/clans/TEST", json={"clan_id": 1, "clan_tag": "TEST"}, status=200,
    )

    response = client.put("/clans/TEST")
    assert response.status_code == 200
    assert nats_mock.messages == []


@responses.activate
def test_add_clan_error():
    nats_mock.messages = []
    responses.add(
        responses.GET, "http://store.host:8080/clans/TEST", json={"clan_id": 1, "clan_tag": "TEST"}, status=404,
    )
    responses.add(responses.GET, "http://api.host:8080/clans/tag/TEST", status=404)

    response = client.put("/clans/TEST")
    assert response.status_code == 404
    assert nats_mock.messages == []


@responses.activate
def test_add_clan_success_new():
    nats_mock.messages = []
    responses.add(
        responses.GET, "http://store.host:8080/clans/TEST", json={"clan_id": 1, "clan_tag": "TEST"}, status=404,
    )
    responses.add(responses.GET, "http://api.host:8080/clans/tag/TEST",
                  json={"clan_id": 1, "clan_tag": "TEST"}, status=200)
    responses.add(responses.PUT, "http://store.host:8080/clans", status=201)

    response = client.put("/clans/TEST")
    assert response.status_code == 201
    assert nats_mock.messages == [("clans.add", b"1")]


@responses.activate
def test_add_clan_success_new():
    nats_mock.messages = []
    responses.add(
        responses.GET, "http://store.host:8080/clans/TEST", json={"clan_id": 1, "clan_tag": "TEST"}, status=404,
    )
    responses.add(responses.GET, "http://api.host:8080/clans/tag/TEST",
                  json={"clan_id": 1, "clan_tag": "TEST"}, status=200)
    responses.add(responses.PUT, "http://store.host:8080/clans", status=200)

    response = client.put("/clans/TEST")
    assert response.status_code == 200
    assert nats_mock.messages == []
