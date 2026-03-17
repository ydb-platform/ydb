# validators - Python Data Validation for Humans™

[![PyCQA][pycqa-badge]][pycqa-link] [![SAST][sast-badge]][sast-link] [![Docs][docs-badge]][docs-link] [![Version][vs-badge]][vs-link] [![Downloads][dw-badge]][dw-link]

<!-- [![Package][package-badge]][package-link] -->

Python has all kinds of data validation tools, but every one of them seems to
require defining a schema or form. I wanted to create a simple validation
library where validating a simple value does not require defining a form or a
schema.

```shell
pip install validators
```

Then,

```python
>>> import validators
>>>
>>> validators.email('someone@example.com')
True
```

## Resources

<!-- Backup documentation URL :  https://yozachar.github.io/pyvalidators/ -->
<!-- Original documentation URL :  https://python-validators.github.io/validators/ -->

- [Documentation](https://yozachar.github.io/pyvalidators)
- [Bugtracker](https://github.com/python-validators/validators/issues)
- [Security](https://github.com/python-validators/validators/blob/master/SECURITY.md)
- [Code](https://github.com/python-validators/validators/)

<!-- Original docs URL will be restored, once properly versioned docs are ready. -->

---

> **_Python 3.9 [reaches EOL in](https://endoflife.date/python) October 2025._**

<!-- Links -->
[sast-badge]: https://github.com/python-validators/validators/actions/workflows/sast.yaml/badge.svg
[sast-link]: https://github.com/python-validators/validators/actions/workflows/sast.yaml
[pycqa-badge]: https://github.com/python-validators/validators/actions/workflows/pycqa.yaml/badge.svg
[pycqa-link]: https://github.com/python-validators/validators/actions/workflows/pycqa.yaml
[docs-badge]: https://github.com/yozachar/pyvalidators/actions/workflows/pages/pages-build-deployment/badge.svg
[docs-link]: https://github.com/yozachar/pyvalidators/actions/workflows/pages/pages-build-deployment
[vs-badge]: https://img.shields.io/pypi/v/validators?logo=pypi&logoColor=white&label=version&color=blue
[vs-link]: https://pypi.python.org/pypi/validators/
[dw-badge]: https://img.shields.io/pypi/dm/validators?logo=pypi&logoColor=white&color=blue
[dw-link]: https://pypi.python.org/pypi/validators/

<!-- [package-badge]: https://github.com/python-validators/validators/actions/workflows/package.yaml/badge.svg
[package-link]: https://github.com/python-validators/validators/actions/workflows/package.yaml -->
