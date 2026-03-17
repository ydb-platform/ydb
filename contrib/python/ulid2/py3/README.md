ulid2
=====

[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Build Status](https://travis-ci.org/valohai/ulid2.svg?branch=master)](https://travis-ci.org/valohai/ulid2)
[![codecov](https://codecov.io/gh/valohai/ulid2/branch/master/graph/badge.svg)](https://codecov.io/gh/valohai/ulid2)

[ULID (Universally Unique Lexicographically Sortable Identifier)][ulid] encoding and
decoding for Python.

* Python 2.7 and 3.x compatible
* Supports binary ULIDs
* Supports bidirectional conversion of base32 ULIDs
* Supports expressing ULIDs as UUIDs

Usage
-----

### Generating ULIDs

* Use `ulid2.generate_binary_ulid()` to generate a raw binary ULID
* Use `ulid2.generate_ulid_as_uuid()` to generate an ULID as an `uuid.UUID`
* Use `ulid2.generate_ulid_as_base32()` to generate an ULID as ASCII

These functions accept optional arguments:

* `timestamp`: a `datetime.datetime` or integer UNIX timestamp to base the ULID on.
* `monotonic`: boolean; whether to attempt to ensure ULIDs are monotonically increasing.  Monotonic behavior is not guaranteed when used from multiple threads.

### Parsing ULIDs

* Use `ulid2.get_ulid_time(ulid)` to get the time from an ULID (in any format)

### Converting ULIDs

* Use `ulid2.ulid_to_base32(ulid)` to convert an ULID to its ASCII representation
* Use `ulid2.ulid_to_uuid(ulid)` to convert an ULID to its UUID representation
* Use `ulid2.ulid_to_binary(ulid)` to convert an ULID to its binary representation

### Base32

* Use `ulid2.encode_ulid_base32(binary)` to convert 16 bytes to 26 ASCII characters
* Use `ulid2.decode_ulid_base32(ascii)` to convert 26 ASCII characters to 16 bytes

Django compatibility
--------------------

As `ulid2` is capable of expressing ULIDs as Python UUIDs, it's
directly compatible with Django's UUIDFields.  For instance, to ULID-ify a model's
primary key, simply

```python
from django.db import models
from ulid2 import generate_ulid_as_uuid

class MyModel(models.Model):
    id = models.UUIDField(default=generate_ulid_as_uuid, primary_key=True)
```

and you're done!


Why the 2 in the name?
----------------------

`ulid` is already taken by [mdipierro's implementation][mdi]. :)

Prior Art
---------

* [NUlid](https://github.com/RobThree/NUlid) (MIT License)
* [oklog/ulid](https://github.com/oklog/ulid)

[ulid]: https://github.com/alizain/ulid
[mdi]: https://github.com/mdipierro/ulid
