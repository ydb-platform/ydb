# mujson

`mujson` lets python libraries make use of the most performant JSON functions available at import time. It is small, and does not itself implement any encoding or decoding functionality.

## installation

Install with:

``` shell
$ pip install --upgrade mujson
```

## rationale

JSON decoding and encoding is a common application bottleneck, and a variety of "fast" substitutes for the standard library's `json` exist, typically implemented in C. This is great for projects which can fine tune their dependency trees, but third party libraries are often forced to rely on the standard library so as to avoid superfluous or expensive requirements.

It is common for libraries to use guarded imports (i.e. `try... except` logic), hoping to find some better JSON implementation available. But this approach is sub-optimal. There are many python JSON libraries, and the relative performance of these varies between encoding and decoding, as well as between Python 2 and 3.

`mujson` just uses the most performant JSON functions available, with the option to specify what JSON implementations are best for your project. It may also be of use to developers who don't always want to worry about compiling C extensions, but still want performance in production.

## usage

For simple usage:

```python
import mujson as json

json.loads(json.dumps({'hello': 'world!'}))
```

To customize the ranked preference of JSON libraries, including libraries not contemplated by `mujson`:

``` python
from mujson import mujson_function

FAST_JSON_LIBS = ['newjsonlib', 'orjson', 'ujson', 'rapidjson', 'yajl']

fast_dumps = mujson_function('fast_dumps', ranking=FAST_JSON_LIBS)
```

`mujson` implements one additional set of custom mujson functions, called "compliant". These functions use a ranking that excludes JSON libraries that do not support the function signatures supported by corresponding functions in the standard library.

```python
import logging

from mujson import compliant_dumps
from pythonjsonlogger import jsonlogger

logger = logging.getLogger()

logHandler = logging.StreamHandler()
# NOTE: we use `compliant_dumps` because the `JsonFormmatter` makes use of
# kwargs like `cls` and `default` which not all json libraries support. (This
# would not strictly be a concern if this was the only use of mujson in a given
# application, but better safe than sorry.)
formatter = jsonlogger.JsonFormatter(json_serializer=compliant_dumps)
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
```

## default rankings

`mujson`'s default rankings are scoped to function and python version. The default rankings are based on the benchmarked performance of common JSON libraries encoding and decoding the JSON data in [bench/json](bench/json). [`bench/json/tweet.json`](bench/json/tweet.json) was given the most weight, under the assumption that it most closely resembles common application data.

### python 3

| library                                                           | dumps | dump | loads | load | compliant |
|:------------------------------------------------------------------|:-----:|:----:|:-----:|:----:|:---------:|
| [orjson](https://github.com/ijl/orjson)                           |  1st  |      |  1st  |      |    no     |
| [metamagic.json](https://github.com/sprymix/metamagic.json)       |  2nd  |      |       |      |    no     |
| [ujson](https://github.com/esnme/ultrajson)                       |  4th  | 2nd  |  2nd  | 1st  |    no     |
| [rapidjson](https://github.com/python-rapidjson/python-rapidjson) |  3rd  | 1st  |  4th  | 3rd  |    yes    |
| [simplejson](https://github.com/simplejson/simplejson)            |  8th  | 6th  |  3rd  | 2nd  |    yes    |
| [json](https://docs.python.org/3.6/library/json.html)             |  6th  | 4th  |  5th  | 4th  |    yes    |
| [yajl](https://github.com/rtyler/py-yajl)                         |  5th  | 3rd  |  7th  | 6th  |    yes    |
| [nssjson](https://github.com/lelit/nssjson)                       |  7th  | 5th  |  6th  | 5th  |    yes    |

### python 2

| library                                                | dumps | dump | loads | load | compliant |
|:-------------------------------------------------------|:-----:|:----:|:-----:|:----:|:---------:|
| [ujson](https://github.com/esnme/ultrajson)            |  1st  | 1st  |  2nd  | 1st  |    no     |
| [cjson](https://github.com/AGProjects/python-cjson)    |  4th  |      |  1st  |      |    no     |
| [yajl](https://github.com/rtyler/py-yajl)              |  2nd  | 2nd  |  5th  | 4th  |    yes    |
| [simplejson](https://github.com/simplejson/simplejson) |  6th  | 5th  |  3rd  | 2nd  |    yes    |
| [nssjson](https://github.com/lelit/nssjson)            |  5th  | 4th  |  4th  | 3rd  |    yes    |
| [json](https://docs.python.org/2/library/json.html)    |  3rd  | 3rd  |  6th  | 5th  |    yes    |

### PyPy

When [PyPy](https://pypy.org/) is used, `mujson` simply falls back to the standard library's `json`, as it currently outperforms all third party libaries.

## running benchmarks

You can build the python 3 benchmarking environment from within the bench directory with something like:

``` shell
$ docker build -t mujson-bench:py3 -f py3.Dockerfile .
```

And you can run the benchmark against any of the provided json files:

``` text
$ docker run -it mujson-bench:py3 1000 apache.json

***************************************************************************

rapidjson       decoded apache.json 1000 times in 1602.057653999509 milliseconds
simplejson      decoded apache.json 1000 times in 1034.323225998378 milliseconds
nssjson         decoded apache.json 1000 times in 1100.1701329987554 milliseconds
json            decoded apache.json 1000 times in 1170.220017000247 milliseconds
yajl            decoded apache.json 1000 times in 1224.6836369995435 milliseconds
ujson           decoded apache.json 1000 times in 971.0670500026026 milliseconds
mujson          decoded apache.json 1000 times in 966.8092329993669 milliseconds

***************************************************************************

simplejson      encoded apache.json 1000 times in 2175.9825850022025 milliseconds
nssjson         encoded apache.json 1000 times in 2175.597892000951 milliseconds
json            encoded apache.json 1000 times in 1711.0415339993779 milliseconds
yajl            encoded apache.json 1000 times in 1038.154541998665 milliseconds
ujson           encoded apache.json 1000 times in 789.5985149989428 milliseconds
rapidjson       encoded apache.json 1000 times in 616.3629779985058 milliseconds
metamagic.json  encoded apache.json 1000 times in 357.27883399886196 milliseconds
mujson          encoded apache.json 1000 times in 364.98578699684003 milliseconds

***************************************************************************

nssjson         de/encoded apache.json 1000 times in 3245.4301819998363 milliseconds
simplejson      de/encoded apache.json 1000 times in 3285.083388000203 milliseconds
json            de/encoded apache.json 1000 times in 2727.172070000961 milliseconds
yajl            de/encoded apache.json 1000 times in 2573.481614999764 milliseconds
rapidjson       de/encoded apache.json 1000 times in 2262.237699000252 milliseconds
ujson           de/encoded apache.json 1000 times in 1749.4632090019877 milliseconds
mujson          de/encoded apache.json 1000 times in 1608.914870001172 milliseconds

***************************************************************************
```

---

_In computability theory, the **μ** operator, minimization operator, or unbounded search operator searches for the least natural number with a given property._