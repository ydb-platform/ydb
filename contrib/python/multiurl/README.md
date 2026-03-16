# multiurl

A package to download several URL as one, as well as supporting multi-part URLs

## Simple example

```python
from multiurl import download

download(url="http://example.com/test.data",
         target="data.file")
```

## Download from two URLs into one file

```python
from multiurl import download

download(url=["http://example.com/test1.data",
              "http://example.com/test2.data"],
         target="data.file")
```

URLs types can be mixed:
```python
from multiurl import download

download(url=["http://example.com/test1.data",
              "ftp://example.com/test2.data"],
         target="data.file")
```

## Download parts of URLs

Provide parts of URLs as a list of `(offset, length)` tuples, expressed in bytes.

```python
from multiurl import download

download(url="http://example.com/test.data",
         parts = [(0, 10), (40, 10), (60, 10)],
         target="data.file")
```

## Download parts of URLs form several URLs

```python
from multiurl import download

download(url=[("http://example.com/test1.data", [(0, 10), (40, 10), (60, 10)]),
              ("http://example.com/test2.data", [(0, 10), (40, 10), (60, 10)])],
         target="data.file")
```

### License
[Apache License 2.0](LICENSE) In applying this licence, ECMWF does not waive the privileges and immunities
granted to it by virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
