# Validr

[![github-action](https://github.com/guyskk/validr/actions/workflows/build-test.yml/badge.svg?branch=main)](https://github.com/guyskk/validr/actions/workflows/build-test.yml) [![codecov](https://codecov.io/gh/guyskk/validr/branch/master/graph/badge.svg)](https://codecov.io/gh/guyskk/validr)

A simple, fast, extensible python library for data validation.

- Simple and readable schema
- 10X faster than [jsonschema](https://github.com/Julian/jsonschema),
  40X faster than [schematics](https://github.com/schematics/schematics)
- Can validate and serialize any object
- Easy to create custom validators
- Accurate and friendly error messages

简单，快速，可拓展的数据校验库。

- 简洁，易读的 Schema
- 比 [jsonschema](https://github.com/Julian/jsonschema) 快 10 倍，比 [schematics](https://github.com/schematics/schematics) 快 40 倍
- 能够校验&序列化任意类型对象
- 易于拓展自定义校验器
- 准确友好的错误提示

## Overview

```python
from validr import T, modelclass, asdict

@modelclass
class Model:
    """Base Model"""

class Person(Model):
    name=T.str.maxlen(16).desc('at most 16 chars')
    website=T.url.optional.desc('website is optional')

guyskk = Person(name='guyskk', website='https://github.com/guyskk')
print(asdict(guyskk))
```

## Install

Note: Only support Python 3.5+

    pip install validr

When you have c compiler in your system, validr will be c speedup mode.
Otherwise validr will fallback to pure python mode.

To force c speedup mode:

    VALIDR_SETUP_MODE=c pip install validr

To force pure python mode:

    VALIDR_SETUP_MODE=py pip install validr

## Document

https://github.com/guyskk/validr/wiki

## Performance

benchmark result in Travis-CI:

```
--------------------------timeits---------------------------
  voluptuous:default             10000 loops cost 0.368s
      schema:default              1000 loops cost 0.318s
        json:loads-dumps        100000 loops cost 1.380s
      validr:default            100000 loops cost 0.719s
      validr:model              100000 loops cost 1.676s
  jsonschema:draft3              10000 loops cost 0.822s
  jsonschema:draft4              10000 loops cost 0.785s
  schematics:default              1000 loops cost 0.792s
---------------------------scores---------------------------
  voluptuous:default               375
      schema:default                43
        json:loads-dumps          1000
      validr:default              1918
      validr:model                 823
  jsonschema:draft3                168
  jsonschema:draft4                176
  schematics:default                17
```

## Develop

Validr is implemented by [Cython](http://cython.org/) since v0.14.0, it's 5X
faster than pure Python implemented.

**setup**:

It's better to use [virtualenv](https://virtualenv.pypa.io/en/stable/) or
similar tools to create isolated Python environment for develop.

After that, install dependencys:

```
./bootstrap.sh
```

**build, test and benchmark**:

```
inv build
inv test
inv benchmark
```

## License

The project is open source under [Anti-996 License](/LICENSE) and [GNU GPL License](/LICENSE-GPL), you can choose one of them.
