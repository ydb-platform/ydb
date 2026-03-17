## aiohttp-socks

[![CI](https://github.com/romis2012/aiohttp-socks/actions/workflows/ci.yml/badge.svg)](https://github.com/romis2012/aiohttp-socks/actions/workflows/ci.yml)
[![Coverage Status](https://codecov.io/gh/romis2012/aiohttp-socks/branch/master/graph/badge.svg)](https://codecov.io/gh/romis2012/aiohttp-socks)
[![PyPI version](https://badge.fury.io/py/aiohttp-socks.svg)](https://pypi.python.org/pypi/aiohttp-socks)
[![versions](https://img.shields.io/pypi/pyversions/aiohttp-socks.svg)](https://github.com/romis2012/aiohttp-socks)
<!--
[![Downloads](https://pepy.tech/badge/aiohttp-socks/month)](https://pepy.tech/project/aiohttp-socks)
-->
The `aiohttp-socks` package provides a proxy connector for [aiohttp](https://github.com/aio-libs/aiohttp). 
Supports SOCKS4(a), SOCKS5(h), HTTP (CONNECT) as well as Proxy chains.
It uses [python-socks](https://github.com/romis2012/python-socks) for core proxy functionality.


## Requirements
- Python >= 3.8
- aiohttp >= 3.10.0
- python-socks[asyncio] >= 2.4.3

## Installation
```
pip install aiohttp_socks
```

## Usage

#### Simple usage
```python
import aiohttp
from aiohttp_socks import ProxyType, ProxyConnector, ChainProxyConnector


async def fetch(url):
    connector = ProxyConnector.from_url('socks5://user:password@127.0.0.1:1080')
    
    ### or use ProxyConnector constructor
    # connector = ProxyConnector(
    #     proxy_type=ProxyType.SOCKS5,
    #     host='127.0.0.1',
    #     port=1080,
    #     username='user',
    #     password='password',
    #     rdns=True # default is True for socks5
    # )
    
    ### proxy chaining (since ver 0.3.3)
    # connector = ChainProxyConnector.from_urls([
    #     'socks5://user:password@127.0.0.1:1080',
    #     'socks4://127.0.0.1:1081',
    #     'http://user:password@127.0.0.1:3128',
    # ])
    async with aiohttp.ClientSession(connector=connector) as session:
        async with session.get(url) as response:
            return await response.text()
```

## Why yet another SOCKS connector for aiohttp

Unlike [aiosocksy](https://github.com/romis2012/aiosocksy), aiohttp_socks has only single point of integration with aiohttp. 
This makes it easier to maintain compatibility with new aiohttp versions.


