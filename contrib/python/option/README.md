# Option
[![CircleCI](https://circleci.com/gh/MaT1g3R/option/tree/master.svg?style=svg)](https://circleci.com/gh/MaT1g3R/option/tree/master)

Rust-like [Option](https://doc.rust-lang.org/std/option/enum.Option.html) and [Result](https://doc.rust-lang.org/std/result/enum.Result.html) types in Python, slotted and fully typed.

An `Option` type represents an optional value, every `Option` is either `Some` and contains Some value, or `NONE`

A `Result` type represents a value that might be an error. Every `Result` is either `Ok` and contains a success value, or `Err` and contains an error value.

Using an `Option` type forces you to deal with `None` values in your code and increase type safety.

Using a `Result` type simplifies error handling and reduces `try` `except` blocks.

## Quick Start
```Python
from option import Result, Option, Ok, Err
from requests import get


def call_api(url, params) -> Result[dict, int]:
    result = get(url, params)
    code = result.status_code
    if code == 200:
        return Ok(result.json())
    return Err(code)


def calculate(url, params) -> Option[int]:
    return call_api(url, params).ok().map(len)


dict_len = calculate('https://example.com', {})
```

## Install
Option can be installed from PyPi:
```bash
pip install option
```

## Documentation
The documentation lives at https://mat1g3r.github.io/option/

## License
MIT
