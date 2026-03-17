# panamap-proto

[![PyPI version](https://badge.fury.io/py/panamap-proto.svg)](https://badge.fury.io/py/panamap-proto)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/panamap-proto)](https://pypi.org/project/panamap-proto/)
[![Build Status](https://travis-ci.com/panamap-object-mapper/panamap-proto.svg?branch=master)](https://travis-ci.com/panamap-object-mapper/panamap-proto)
[![Coveralls github](https://img.shields.io/coveralls/github/panamap-object-mapper/panamap-proto)](https://coveralls.io/github/panamap-object-mapper/panamap-proto?branch=master)

Panamap-proto adds protobuf support to [panamap object mapper](https://github.com/panamap-object-mapper/panamap).

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install panamap-proto.

```bash
pip install panamap
```

## Usage

### Initialize mapper

To enabe protobuf support you must pass `ProtoMappingDescriptor` in list to `custom_descriptors` parameter to `Mapper` constructor: 

```python
from panamap import Mapper
from panamap_proto import ProtoMappingDescriptor

mapper = Mapper(custom_descriptors=[ProtoMappingDescriptor])
```

After that you can set up mapping for protobuf generated classes.

### Map classes

To map message `Simple`:

```proto
// messages.proto
syntax = "proto3";

message Simple {
    string value = 1;
}
```

You should use folowing configuration:

```python
from panamap import Mapper
from panamap_proto import ProtoMappingDescriptor

from messages_pb2 import Simple

mapper = Mapper(custom_descriptors=[ProtoMappingDescriptor])

mapper.mapping(Simple, SimpleData).map_matching().register()

s = mapper.map(SimpleData("abc"), Simple)

print(s.value)
# 'abc'
```

### Map enums

There is many ways to map python enums to protobuf generated enums, but the easiest way is to use `values_map` utility method:

```proto
// messages.proto
enum Lang {
    PYTHON = 0;
    CPP = 1;
    JAVA = 2;
}
```

```python
from enum import Enum

from panamap import Mapper, values_map
from panamap_proto import ProtoMappingDescriptor

from messages_pb2 import Lang

class PyLang(Enum):
    PYTHON = 1
    CPP = 2
    JAVA = 3

mapper = Mapper(custom_descriptors=[ProtoMappingDescriptor])

pairs = [
    (PyLang.PYTHON, Lang.Value("PYTHON")),
    (PyLang.JAVA, Lang.Value("JAVA")),
    (PyLang.CPP, Lang.Value("CPP")),
]

mapper.mapping(PyLang, Lang) \
    .l_to_r_converter(values_map({py: proto for py, proto in pairs})) \
    .r_to_l_converter(values_map({proto: py for py, proto in pairs})) \
    .register()
```

## Contributing

Contributing described in [separate document](docs/contributing.md).
