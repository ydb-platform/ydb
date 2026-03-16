# Asyncio zookeeper client (aiozk)

[![PyPi version](https://img.shields.io/pypi/v/aiozk.svg)](https://pypi.python.org/pypi/aiozk)

[![Build Status](https://img.shields.io/github/workflow/status/micro-fan/aiozk/master)](https://github.com/micro-fan/aiozk/actions)


<!-- markdown-toc start - Don't edit this section. Run M-x markdown-toc-refresh-toc -->
**Table of Contents**

- [Asyncio zookeeper client (aiozk)](#asyncio-zookeeper-client-aiozk)
    - [Status](#status)
    - [Installation](#installation)
    - [Quick Example](#quick-example)
    - [Recipes](#recipes)
        - [Caution](#caution)
    - [Testing](#testing)
        - [Run tests](#run-tests)
        - [Testing approach](#testing-approach)
        - [Recipes testing](#recipes-testing)
        - [Run some tests directly](#run-some-tests-directly)
    - [References](#references)

<!-- markdown-toc end -->


## Status

Have no major bugs in client/session/connection, but recipes need more test
code to become more robust.

Any help and interest are welcome 😀

## Installation

```bash
$ pip install aiozk
```


## Quick Example

```python
import asyncio
from aiozk import ZKClient


async def main():
    zk = ZKClient('localhost')
    await zk.start()
    await zk.create('/foo', data=b'bazz', ephemeral=True)
    assert b'bazz' == await zk.get_data('/foo')
    await zk.close()

asyncio.run(main())
```

## Recipes

You may use recipes, similar to zoonado, kazoo, and other libs:

```python
# assuming zk is aiozk.ZKClient

# Lock
async with await zk.recipes.Lock('/path/to/lock').acquire():
    # ... Do some stuff ...
    pass

# Barrier
barrier = zk.recipes.Barrier('/path/to/barrier)
await barrier.create()
await barrier.lift()
await barrier.wait()

# DoubleBarrier
double_barrier = zk.recipes.DoubleBarrier('/path/to/double/barrier', min_participants=4)
await double_barrier.enter(timeout=0.5)
# ...  Do some stuff ...
await double_barrier.leave(timeout=0.5)
```

You can find full list of recipes provided by aiozk here:
[aiozk recipes](https://github.com/micro-fan/aiozk/tree/master/aiozk/recipes)

To understand ideas behind recipes [please read
this](https://zookeeper.apache.org/doc/current/recipes.html) and [even more
recipes here](http://curator.apache.org/curator-recipes/index.html). Make sure
you're familiar with all recipes before doing something new by yourself,
especially when it involves more than few zookeeper calls.

### Caution
Don't mix different type of recipes at the same znode path. For example,
creating a Lock and a DoubleBarrier object at the same path. It may cause
undefined behavior 😓

## Testing

**NB**: please ensure that you're using recent `docker-compose` version. You can get it by running

```
pip install --user -U docker-compose
```


### Run tests

```
# you should have access to docker

docker-compose build
./test-runner.sh
```

Or you can run tests with tox

```
pip install --user tox tox-docker
tox
```

### Testing approach

Most of tests are integration tests and running on real zookeeper instances.
We've chosen `zookeeper 3.5` version since it has an ability to dynamic reconfiguration and we're going to do all connecting/reconnecting/watches tests on zk docker cluster as this gives us the ability to restart any server and see what happens.

```sh
# first terminal: launch zookeeper cluster
docker-compose rm -fv && docker-compose build zk && docker-compose up --scale zk=7 zk_seed zk

# it will launch cluster in this terminal and remain. last lines should be like this:

zk_6       | Servers: 'server.1=172.23.0.9:2888:3888:participant;0.0.0.0:2181\nserver.2=172.23.0.2:2888:3888:participant;0.0.0.0:2181\nserver.3=172.23.0.3:2888:3888:participant;0.0.0.0:2181\nserver.4=172.23.0.4:2888:3888:participant;0.0.0.0:2181\nserver.5=172.23.0.5:2888:3888:participant;0.0.0.0:2181\nserver.6=172.23.0.7:2888:3888:participant;0.0.0.0:2181'
zk_6       | CONFIG: server.1=172.23.0.9:2888:3888:participant;0.0.0.0:2181
zk_6       | server.2=172.23.0.2:2888:3888:participant;0.0.0.0:2181
zk_6       | server.3=172.23.0.3:2888:3888:participant;0.0.0.0:2181
zk_6       | server.4=172.23.0.4:2888:3888:participant;0.0.0.0:2181
zk_6       | server.5=172.23.0.5:2888:3888:participant;0.0.0.0:2181
zk_6       | server.6=172.23.0.7:2888:3888:participant;0.0.0.0:2181
zk_6       | server.7=172.23.0.6:2888:3888:observer;0.0.0.0:2181
zk_6       |
zk_6       |
zk_6       | Reconfiguring...
zk_6       | ethernal loop
zk_7       | Servers: 'server.1=172.23.0.9:2888:3888:participant;0.0.0.0:2181\nserver.2=172.23.0.2:2888:3888:participant;0.0.0.0:2181\nserver.3=172.23.0.3:2888:3888:participant;0.0.0.0:2181\nserver.4=172.23.0.4:2888:3888:participant;0.0.0.0:2181\nserver.5=172.23.0.5:2888:3888:participant;0.0.0.0:2181\nserver.6=172.23.0.7:2888:3888:participant;0.0.0.0:2181\nserver.7=172.23.0.6:2888:3888:participant;0.0.0.0:2181'
zk_7       | CONFIG: server.1=172.23.0.9:2888:3888:participant;0.0.0.0:2181
zk_7       | server.2=172.23.0.2:2888:3888:participant;0.0.0.0:2181
zk_7       | server.3=172.23.0.3:2888:3888:participant;0.0.0.0:2181
zk_7       | server.4=172.23.0.4:2888:3888:participant;0.0.0.0:2181
zk_7       | server.5=172.23.0.5:2888:3888:participant;0.0.0.0:2181
zk_7       | server.6=172.23.0.7:2888:3888:participant;0.0.0.0:2181
zk_7       | server.7=172.23.0.6:2888:3888:participant;0.0.0.0:2181
zk_7       | server.8=172.23.0.8:2888:3888:observer;0.0.0.0:2181
zk_7       |
zk_7       |
zk_7       | Reconfiguring...
zk_7       | ethernal loop
```

Run tests in docker:

```sh
docker-compose run --no-deps aiozk
# last lines will be about testing results

............lot of lines ommited........
.
----------------------------------------------------------------------
Ran 3 tests in 1.059s

OK

```

Run tests locally:
```sh
# ZK_IP can be something from logs above, like: ZK_HOST=172.21.0.6:2181
ZK_HOST=<ZK_IP> ./venv/bin/pytest
```

### Recipes testing

It seems that usually recipes require several things to be tested:

* That recipe flow is working as expected
* Timeouts: reproduce every timeout with meaningful values (timeout 0.5s and block for 0.6s)


### Run some tests directly

Another way to run tests only which you are interested in quickly. Or this is
useful when you run tests under the other version of python.

```sh
# Run zookeeper container
docker run -p 2181:2181 zookeeper

# Run pytest directly at the development source tree
export ZK_HOST=localhost
pytest -s --log-cli-level=DEBUG aiozk/test/test_barrier.py
```

## References
* It is based on [wglass/zoonado](https://github.com/wglass/zoonado/tree/master/zoonado) implementation
