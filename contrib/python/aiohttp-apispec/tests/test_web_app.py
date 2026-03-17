async def test_response_200_get(aiohttp_app):
    res = await aiohttp_app.get("/v1/test", params={"id": 1, "name": "max"})
    assert res.status == 200


async def test_response_400_get(aiohttp_app):
    res = await aiohttp_app.get("/v1/test", params={"id": "string", "name": "max"})
    assert res.status == 400
    assert await res.json() == {
        'errors': {'id': ['Not a valid integer.']},
        'text': 'Oops',
    }


async def test_response_200_post(aiohttp_app):
    res = await aiohttp_app.post("/v1/test", json={"id": 1, "name": "max"})
    assert res.status == 200


async def test_response_200_post_callable_schema(aiohttp_app):
    res = await aiohttp_app.post("/v1/test_call", json={"id": 1, "name": "max"})
    assert res.status == 200


async def test_response_400_post(aiohttp_app):
    res = await aiohttp_app.post("/v1/test", json={"id": "string", "name": "max"})
    assert res.status == 400
    assert await res.json() == {
        'errors': {'id': ['Not a valid integer.']},
        'text': 'Oops',
    }


async def test_response_not_docked(aiohttp_app):
    res = await aiohttp_app.get("/v1/other", params={"id": 1, "name": "max"})
    assert res.status == 200


async def test_response_data_post(aiohttp_app):
    res = await aiohttp_app.post(
        "/v1/echo", json={"id": 1, "name": "max", "list_field": [1, 2, 3, 4]}
    )
    assert (await res.json()) == {"id": 1, "name": "max", "list_field": [1, 2, 3, 4]}


async def test_response_data_get(aiohttp_app):
    res = await aiohttp_app.get(
        "/v1/echo",
        params=[
            ("id", "1"),
            ("name", "max"),
            ("bool_field", "0"),
            ("list_field", "1"),
            ("list_field", "2"),
            ("list_field", "3"),
            ("list_field", "4"),
        ],
    )
    assert (await res.json()) == {
        "id": 1,
        "name": "max",
        "bool_field": False,
        "list_field": [1, 2, 3, 4],
    }


async def test_response_data_class_get(aiohttp_app):
    res = await aiohttp_app.get(
        "/v1/class_echo",
        params=[
            ("id", "1"),
            ("name", "max"),
            ("bool_field", "0"),
            ("list_field", "1"),
            ("list_field", "2"),
            ("list_field", "3"),
            ("list_field", "4"),
        ],
    )
    assert (await res.json()) == {
        "id": 1,
        "name": "max",
        "bool_field": False,
        "list_field": [1, 2, 3, 4],
    }


async def test_response_data_class_post(aiohttp_app):
    res = await aiohttp_app.post("/v1/class_echo")
    assert res.status == 405


async def test_path_variable_described_correctly(aiohttp_app):
    if aiohttp_app.app._subapps:
        swag = aiohttp_app.app._subapps[0]["swagger_dict"]["paths"][
            "/v1/variable/{var}"
        ]
    else:
        swag = aiohttp_app.app["swagger_dict"]["paths"]["/v1/variable/{var}"]
    assert len(swag["get"]["parameters"]) == 1, "There should only be one"
    assert swag["get"]["parameters"][0]["name"] == "var"
    assert swag["get"]["parameters"][0]["schema"]["format"] == "uuid"


async def test_response_data_class_without_spec(aiohttp_app):
    res = await aiohttp_app.delete("/v1/class_echo")
    assert (await res.json()) == {"hello": "world"}


async def test_swagger_handler_200(aiohttp_app):
    res = await aiohttp_app.get("/v1/api/docs/api-docs")
    assert res.status == 200


async def test_match_info(aiohttp_app):
    res = await aiohttp_app.get("/v1/variable/hello")
    assert res.status == 200
    assert await res.json() == {}


async def test_validators(aiohttp_app):
    res = await aiohttp_app.post(
        "/v1/validate/123456",
        json={"id": 1, "name": "max", "bool_field": False, "list_field": [1, 2, 3, 4]},
        params=[
            ("id", "1"),
            ("name", "max"),
            ("bool_field", "0"),
            ("list_field", "1"),
            ("list_field", "2"),
            ("list_field", "3"),
            ("list_field", "4"),
        ],
        cookies={"some_cookie": "test-cookie-value"},
        headers={"some_header": "test-header-value"},
    )
    assert res.status == 200
    assert await res.json() == {
        "json": {
            "id": 1,
            "name": "max",
            "bool_field": False,
            "list_field": [1, 2, 3, 4],
        },
        "querystring": {
            "id": 1,
            "name": "max",
            "bool_field": False,
            "list_field": [1, 2, 3, 4],
        },
        "cookies": {"some_cookie": "test-cookie-value"},
        "headers": {"some_header": "test-header-value"},
        "match_info": {"uuid": 123456},
    }


async def test_swagger_path(aiohttp_app):
    res = await aiohttp_app.get("/v1/api/docs")
    assert res.status == 200


async def test_swagger_static(aiohttp_app):
    assert (await aiohttp_app.get("/static/swagger/swagger-ui.css")).status == 200 \
        or (await aiohttp_app.get("/v1/static/swagger/swagger-ui.css")).status == 200
