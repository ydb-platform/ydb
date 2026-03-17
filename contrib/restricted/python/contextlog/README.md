contextlog
==========
[![Build Status](https://travis-ci.org/mdevaev/contextlog.svg?branch=master)](https://travis-ci.org/mdevaev/contextlog)
[![Coverage Status](https://coveralls.io/repos/mdevaev/contextlog/badge.png?branch=master)](https://coveralls.io/r/mdevaev/contextlog?branch=master)
[![Latest Version](https://pypip.in/v/contextlog/badge.png)](https://pypi.python.org/pypi/contextlog/)


##Context-based logger and formatters collection##


###Example###
```python
# pip install pyyaml

import logging
import logging.config
import contextlog
import yaml

logging.config.dictConfig(yaml.load("""
version: 1
disable_existing_loggers: false
loggers:
    test:
        level: DEBUG
        handlers: [default]
formatters:
    default:
        (): contextlog.SmartFormatter
        style: "{"
        format: "{yellow}{asctime} {log_color}{levelname:>7} {purple}{name:20.20}{reset} CTX={ctx} CTX_INT={ctx_internal} {message}"
handlers:
    default:
        level: DEBUG
        class: logging.StreamHandler
        formatter: default
root:
    level: DEBUG
    handlers: [default]
"""))

log = contextlog.get_logger(__name__, ctx="test")
log.info("Message #1")

def method():
    bar = 1
    log = contextlog.get_logger(__name__, ctx_internal="method")
    log.debug("Message #2")
    try:
        raise RuntimeError
    except:
        log.exception("Exception")
method()

log = contextlog.get_logger(__name__)
log.info("Message #3")
```
Results:
```
2014-07-25 16:24:49,422    INFO __main__             CTX=test CTX_INT= Message #1
2014-07-25 16:24:49,423   DEBUG __main__             CTX=test CTX_INT=method Message #2
2014-07-25 16:24:49,423   ERROR __main__             CTX=test CTX_INT=method Exception
Traceback (most recent call last):
  File "foo.py", line 43, in method
    raise RuntimeError
RuntimeError

Locals at innermost frame:

{'__logger_context': {'ctx': 'test', 'ctx_internal': 'method'},
 'bar': 1,
 'log': <contextlog._ContextLogger object at 0x7f8a29c20c50>}
2014-07-25 16:24:49,423    INFO __main__             CTX=test CTX_INT= Message #3
```
