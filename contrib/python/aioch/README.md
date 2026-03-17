# aioch
**aioch** is a library for accessing a ClickHouse database over native interface from the asyncio.
It wraps features of [clickhouse-driver](https://github.com/mymarilyn/clickhouse-driver) for asynchronous usage.

[![Coverage Status](https://coveralls.io/repos/github/mymarilyn/aioch/badge.svg?branch=master)](https://coveralls.io/github/mymarilyn/aioch?branch=master)
[![Build Status](https://travis-ci.org/mymarilyn/aioch.svg?branch=master)](https://travis-ci.org/mymarilyn/aioch)


## Installation

The package can be installed using `pip`:

```bash
pip install aioch
```

To install from source:

```bash
git clone https://github.com/mymarilyn/aioch
cd aioch
python setup.py install
```

## Usage
```python
from datetime import datetime

import asyncio
from aioch import Client


async def exec_progress():
    client = Client('localhost')

    progress = await client.execute_with_progress('LONG AND COMPLICATED QUERY')
    timeout = 20
    started_at = datetime.now()

    async for num_rows, total_rows in progress:
        done = num_rows / total_rows if total_rows else total_rows
        now = datetime.now()
        # Cancel query if it takes more than 20 seconds to process 50% of rows.
        if (now - started_at).total_seconds() > timeout and done < 0.5:
            await client.cancel()
            break
    else:
        rv = await progress.get_result()
        print(rv)


async def exec_no_progress():
    client = Client('localhost')
    rv = await client.execute('LONG AND COMPLICATED QUERY')
    print(rv)


loop = asyncio.get_event_loop()
loop.run_until_complete(asyncio.wait([exec_progress(), exec_no_progress()]))
```

For more information see **clickhouse-driver** usage examples.

## Parameters

* `executor` - instance of custom Executor, if not supplied default executor will be used
* `loop` - asyncio compatible event loop

Other parameters are passing to wrapped clickhouse-driver's Client.

## License

aioch is distributed under the [MIT license](http://www.opensource.org/licenses/mit-license.php).
