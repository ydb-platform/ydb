# mongomock-motor

[![PyPI version](https://badge.fury.io/py/mongomock-motor.svg)](https://badge.fury.io/py/mongomock-motor)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fmichaelkryukov%2Fmongomock_motor.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fmichaelkryukov%2Fmongomock_motor?ref=badge_shield)

Best effort mock for [AsyncIOMotorClient](https://motor.readthedocs.io/en/stable/api-asyncio/asyncio_motor_client.html)
(Database, Collection, e.t.c) built on top of [mongomock](https://github.com/mongomock/mongomock) library.

## Example / Showcase

```py
from mongomock_motor import AsyncMongoMockClient


async def test_mock_client():
    collection = AsyncMongoMockClient()['tests']['test-1']

    assert await collection.find({}).to_list(None) == []

    result = await collection.insert_one({'a': 1})
    assert result.inserted_id

    assert len(await collection.find({}).to_list(None)) == 1
```

## License

[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fmichaelkryukov%2Fmongomock_motor.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Fmichaelkryukov%2Fmongomock_motor?ref=badge_large)
