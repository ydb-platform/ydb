fakeredis: A fake version of a redis-py
=======================================

[![badge](https://img.shields.io/pypi/v/fakeredis)](https://pypi.org/project/fakeredis/)
[![CI](https://github.com/cunla/fakeredis-py/actions/workflows/test.yml/badge.svg)](https://github.com/cunla/fakeredis-py/actions/workflows/test.yml)
[![badge](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/cunla/b756396efb895f0e34558c980f1ca0c7/raw/fakeredis-py.json)](https://github.com/cunla/fakeredis-py/actions/workflows/test.yml)
[![badge](https://img.shields.io/pypi/dm/fakeredis)](https://pypi.org/project/fakeredis/)
[![badge](https://img.shields.io/pypi/l/fakeredis)](./LICENSE)
[![Open Source Helpers](https://www.codetriage.com/cunla/fakeredis-py/badges/users.svg)](https://www.codetriage.com/cunla/fakeredis-py)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
--------------------


Documentation is hosted in https://fakeredis.readthedocs.io/

# Intro

FakeRedis is a pure-Python implementation of the Redis protocol API. It provides enhanced versions of the redis-py Python bindings for Redis.

It enables running tests requiring [Redis](https://redis.io/)/[ValKey](https://github.com/valkey-io/valkey)/[DragonflyDB](https://www.dragonflydb.io/) server without an actual server.

It also enables testing compatibility of different key-value datastores.

That provides the following added functionality: A built-in Redis server that is automatically installed, configured and managed when the Redis bindings are used. A single server shared by multiple programs or multiple independent servers. All the servers provided by FakeRedis support all Redis functionality including advanced features such as RedisJson, RedisBloom, GeoCommands.


See [official documentation](https://fakeredis.readthedocs.io/) for list of supported commands.

# Sponsor

fakeredis-py is developed for free.

You can support this project by becoming a sponsor using [this link](https://github.com/sponsors/cunla).
