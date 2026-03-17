### Attention: This repo is unmaintained and has been outside of minor changes for a long time. Please consider [playwright-python](https://github.com/microsoft/playwright-python) as an alternative. 
If you would like to overhaul this code to bring it up to date, please contact [me](https://github.com/Mattwmaster58)

pyppeteer
==========

[![PyPI](https://img.shields.io/pypi/v/pyppeteer.svg)](https://pypi.python.org/pypi/pyppeteer)
[![PyPI version](https://img.shields.io/pypi/pyversions/pyppeteer.svg)](https://pypi.python.org/pypi/pyppeteer)
[![Documentation](https://img.shields.io/badge/docs-latest-brightgreen.svg)](https://pyppeteer.github.io/pyppeteer/)
[![CircleCI](https://circleci.com/gh/pyppeteer/pyppeteer.svg?style=shield)](https://circleci.com/gh/pyppeteer/pyppeteer)
[![codecov](https://codecov.io/gh/pyppeteer/pyppeteer/branch/dev/graph/badge.svg)](https://codecov.io/gh/pyppeteer/pyppeteer)

_Note: this is a continuation of the [pyppeteer project](https://github.com/miyakogi/pyppeteer)_

Unofficial Python port of [puppeteer](https://github.com/GoogleChrome/puppeteer) JavaScript (headless) chrome/chromium browser automation library.

* Free software: MIT license (including the work distributed under the Apache 2.0 license)
* Documentation: https://pyppeteer.github.io/pyppeteer/

## Installation

pyppeteer requires Python >= 3.8

Install with `pip` from PyPI:

```
pip install pyppeteer
```

Or install the latest version from [this github repo](https://github.com/pyppeteer/pyppeteer/):

```
pip install -U git+https://github.com/pyppeteer/pyppeteer@dev
```

## Usage

> **Note**: When you run pyppeteer for the first time, it downloads the latest version of Chromium (~150MB) if it is not found on your system. If you don't prefer this behavior, ensure that a suitable Chrome binary is installed. One way to do this is to run `pyppeteer-install` command before prior to using this library.

Full documentation can be found [here](https://pyppeteer.github.io/pyppeteer/reference.html). [Puppeteer's documentation](https://github.com/GoogleChrome/puppeteer/blob/master/docs/api.md#) and [its troubleshooting guide](https://github.com/GoogleChrome/puppeteer/blob/master/docs/troubleshooting.md) are also great resources for pyppeteer users.

### Examples

Open web page and take a screenshot:
```py
import asyncio
from pyppeteer import launch

async def main():
    browser = await launch()
    page = await browser.newPage()
    await page.goto('https://example.com')
    await page.screenshot({'path': 'example.png'})
    await browser.close()

asyncio.get_event_loop().run_until_complete(main())
```

Evaluate javascript on a page:
```py
import asyncio
from pyppeteer import launch

async def main():
    browser = await launch()
    page = await browser.newPage()
    await page.goto('https://example.com')
    await page.screenshot({'path': 'example.png'})

    dimensions = await page.evaluate('''() => {
        return {
            width: document.documentElement.clientWidth,
            height: document.documentElement.clientHeight,
            deviceScaleFactor: window.devicePixelRatio,
        }
    }''')

    print(dimensions)
    # >>> {'width': 800, 'height': 600, 'deviceScaleFactor': 1}
    await browser.close()

asyncio.get_event_loop().run_until_complete(main())
```

## Differences between puppeteer and pyppeteer

pyppeteer strives to replicate the puppeteer API as close as possible, however, fundamental differences between Javascript and Python make this difficult to do precisely. More information on specifics can be found in the [documentation](https://pyppeteer.github.io/pyppeteer/reference.html).

### Keyword arguments for options

puppeteer uses an object for passing options to functions/methods. pyppeteer methods/functions accept both dictionary (python equivalent to JavaScript's objects) and keyword arguments for options.

Dictionary style options (similar to puppeteer):

```python
browser = await launch({'headless': True})
```

Keyword argument style options (more pythonic, isn't it?):

```python
browser = await launch(headless=True)
```

### Element selector method names

In python, `$` is not a valid identifier. The equivalent methods to Puppeteer's `$`, `$$`, and `$x` methods are listed below, along with some shorthand methods for your convenience:

| puppeteer | pyppeteer              | pyppeteer shorthand |
|-----------|-------------------------|----------------------|
| Page.$()  | Page.querySelector()    | Page.J()             |
| Page.$$() | Page.querySelectorAll() | Page.JJ()            |
| Page.$x() | Page.xpath()            | Page.Jx()            |

### Arguments of `Page.evaluate()` and `Page.querySelectorEval()`

puppeteer's version of `evaluate()` takes a JavaScript function or a string representation of a JavaScript expression. pyppeteer takes string representation of JavaScript expression or function. pyppeteer will try to automatically detect if the string is function or expression, but it will fail sometimes. If an expression is erroneously treated as function and an error is raised, try setting `force_expr` to `True`, to force pyppeteer to treat the string as expression.

### Examples:

Get a page's `textContent`:

```python
content = await page.evaluate('document.body.textContent', force_expr=True)
```

Get an element's `textContent`:

```python
element = await page.querySelector('h1')
title = await page.evaluate('(element) => element.textContent', element)
```

## Roadmap

See [projects](https://github.com/pyppeteer/pyppeteer/projects)

## Credits

###### This package was created with [Cookiecutter](https://github.com/audreyr/cookiecutter) and the [audreyr/cookiecutter-pypackage](https://github.com/audreyr/cookiecutter-pypackage) project template.
