# `pure-protobuf`

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/eigenein/protobuf/check.yml?label=checks&logo=github)](https://github.com/eigenein/protobuf/actions/workflows/check.yml)
[![Code coverage](https://codecov.io/gh/eigenein/protobuf/branch/master/graph/badge.svg?token=bJarwbLlY7)](https://codecov.io/gh/eigenein/protobuf)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/pure-protobuf.svg)](https://pypistats.org/packages/pure-protobuf)
[![PyPI – Version](https://img.shields.io/pypi/v/pure-protobuf.svg)](https://pypi.org/project/pure-protobuf/#history)
[![PyPI – Python](https://img.shields.io/pypi/pyversions/pure-protobuf.svg?logo=python&logoColor=yellow)](https://pypi.org/project/pure-protobuf/#files)
[![License](https://img.shields.io/pypi/l/pure-protobuf.svg)](https://github.com/eigenein/protobuf/blob/master/LICENSE)

<small><strong>Wow! Such annotated! Very buffers!</strong></small>

## Documentation

<a href="https://eigenein.github.io/protobuf/">
    <img alt="Documentation" height="30em" src="https://img.shields.io/github/actions/workflow/status/eigenein/protobuf/docs.yml?label=documentation&logo=github">
</a>

## Quick examples

### `.proto` definition

It's not needed for `pure-protobuf`, but for the sake of an example, let's consider the following definition:

```protobuf
syntax = "proto3";

message SearchRequest {
  string query = 1;
  int32 page_number = 2;
  int32 result_per_page = 3;
}
```

And here's the same via `pure-protobuf`:

### With [dataclasses](https://docs.python.org/3/library/dataclasses.html)

```python title="dataclass_example.py"
from dataclasses import dataclass
from io import BytesIO

from pure_protobuf.annotations import Field
from pure_protobuf.message import BaseMessage
from typing_extensions import Annotated


@dataclass
class SearchRequest(BaseMessage):
    query: Annotated[str, Field(1)] = ""
    page_number: Annotated[int, Field(2)] = 0
    result_per_page: Annotated[int, Field(3)] = 0


request = SearchRequest(query="hello", page_number=1, result_per_page=10)
buffer = bytes(request)
assert buffer == b"\x0A\x05hello\x10\x01\x18\x0A"
assert SearchRequest.read_from(BytesIO(buffer)) == request
```

### With [`pydantic`](https://docs.pydantic.dev/)

```python title="pydantic_example.py"
from io import BytesIO

from pure_protobuf.annotations import Field
from pure_protobuf.message import BaseMessage
from pydantic import BaseModel
from typing_extensions import Annotated


class SearchRequest(BaseMessage, BaseModel):
    query: Annotated[str, Field(1)] = ""
    page_number: Annotated[int, Field(2)] = 0
    result_per_page: Annotated[int, Field(3)] = 0


request = SearchRequest(query="hello", page_number=1, result_per_page=10)
buffer = bytes(request)
assert buffer == b"\x0A\x05hello\x10\x01\x18\x0A"
assert SearchRequest.read_from(BytesIO(buffer)) == request
```
