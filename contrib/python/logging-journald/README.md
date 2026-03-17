logging-journald
================

[![PyPI - License](https://img.shields.io/pypi/l/logging-journald)](https://pypi.org/project/logging-journald) [![Wheel](https://img.shields.io/pypi/wheel/logging-journald)](https://pypi.org/project/logging-journald) [![PyPI](https://img.shields.io/pypi/v/logging-journald)](https://pypi.org/project/logging-journald) [![PyPI](https://img.shields.io/pypi/pyversions/logging-journald)](https://pypi.org/project/logging-journald) [![Coverage Status](https://coveralls.io/repos/github/mosquito/logging-journald/badge.svg?branch=master)](https://coveralls.io/github/mosquito/logging-journald?branch=master) ![tests](https://github.com/mosquito/logging-journald/workflows/tests/badge.svg?branch=master)

Pure python logging handler for writing logs to the journald using
native protocol.

```python
import logging
from logging_journald import JournaldLogHandler, check_journal_stream

# Use python default handler
LOG_HANDLERS = None


if (
    # Check if program running as systemd service
    check_journal_stream() or
    # Check if journald socket is available
    JournaldLogHandler.SOCKET_PATH.exists()
):
    LOG_HANDLERS = [JournaldLogHandler()]

logging.basicConfig(level=logging.INFO, handlers=LOG_HANDLERS)
logging.info("Hello logging world.")
```

`MESSAGE_ID` field
------------------

As defined in [catalog documentation](https://www.freedesktop.org/wiki/Software/systemd/catalog/):

> A `128-bit` message identifier ID for recognizing certain message types, if this is desirable. This should contain a
128-bit ID formatted as a lower-case hexadecimal string, without any separating dashes or suchlike. This is recommended
to be a UUID-compatible ID, but this is not enforced, and formatted differently. Developers can generate a new ID for
this purpose with systemd-id128 new.

So you're free to choose how you want to act. By default, `MESSAGE_ID` is generated as a hash of the message and some
static fields. But you can disable this by passing `use_message_id=False` to the class constructor.


```python
from logging_journald import JournaldLogHandler

...

handler = JournaldLogHandler(use_message_id=False)

...
```