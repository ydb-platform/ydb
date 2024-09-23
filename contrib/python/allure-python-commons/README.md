## Allure Common API

[![Release Status](https://img.shields.io/pypi/v/allure-python-commons)](https://pypi.python.org/pypi/allure-python-commons)
[![Downloads](https://img.shields.io/pypi/dm/allure-python-commons)](https://pypi.python.org/pypi/allure-python-commons)

> The package contains classes and functions for users of Allure Report. It can
> be used to enhance reports using an existing Allure adapter or to create new
> adapters.

[<img src="https://allurereport.org/public/img/allure-report.svg" height="85px" alt="Allure Report logo" align="right" />](https://allurereport.org "Allure Report")

- Learn more about Allure Report at [https://allurereport.org](https://allurereport.org)
- 📚 [Documentation](https://allurereport.org/docs/) – discover official documentation for Allure Report
- ❓ [Questions and Support](https://github.com/orgs/allure-framework/discussions/categories/questions-support) – get help from the team and community
- 📢 [Official announcements](https://github.com/orgs/allure-framework/discussions/categories/announcements) –  stay updated with our latest news and updates
- 💬 [General Discussion](https://github.com/orgs/allure-framework/discussions/categories/general-discussion) – engage in casual conversations, share insights and ideas with the community
- 🖥️ [Live Demo](https://demo.allurereport.org/) — explore a live example of Allure Report in action

---

## User's API

Install an adapter that suits your test framework. You can then add more
information to the report by using functions from the `allure` module.

### Decorators API

Use these functions as decorators of your own functions, e.g.:

```python
import allure

@allure.title("My test")
def test_fn():
    pass
```

The full list of decorators:

  - `allure.title`
  - `allure.description`
  - `allure.description_html`
  - `allure.label`
  - `allure.severity`
  - `allure.epic`
  - `allure.feature`
  - `allure.story`
  - `allure.suite`
  - `allure.parent_suite`
  - `allure.sub_suite`
  - `allure.tag`
  - `allure.id`
  - `allure.manual`
  - `allure.link`
  - `allure.issue`
  - `allure.testcase`
  - `allure.step`

Refer to the adapter's documentation for the information about what decorators
are supported and what functions they can be applied to.

### Runtime API

Most of the functions of Runtime API can be accessed via `allure.dynamic.*`.
Call them at runtime from your code.

The full list includes:

  - `allure.dynamic.title`
  - `allure.dynamic.description`
  - `allure.dynamic.description_html`
  - `allure.dynamic.label`
  - `allure.dynamic.severity`
  - `allure.dynamic.epic`
  - `allure.dynamic.feature`
  - `allure.dynamic.story`
  - `allure.dynamic.suite`
  - `allure.dynamic.parent_suite`
  - `allure.dynamic.sub_suite`
  - `allure.dynamic.tag`
  - `allure.dynamic.id`
  - `allure.dynamic.manual`
  - `allure.dynamic.link`
  - `allure.dynamic.issue`
  - `allure.dynamic.testcase`
  - `allure.dynamic.parameter`
  - `allure.attach`
  - `allure.attach.file`
  - `allure.step`

Refer to the adapter's documentation for the information about what functions
are supported and where you can use them.

## Adapter API

You may use `allure-pytest-commons` to build your own Allure adapter. The key
elements of the corresponding API are:

  - `allure_python_commons.model2`: the object model of Allure Report.
  - `allure_python_commons.logger`: classes that are used to emit Allure Report objects (tests, containers, attachments):
      - `AllureFileLogger`: emits to the file system.
      - `AllureMemoryLogger`: collects the objects in memory. Useful for
        testing.
  - `allure_python_commons.lifecycle.AllureLifecycle`: an implementation of
    Allure lifecycle that doesn't isolate the state between threads.
  - `allure_python_commons.reporter.AllureReporter`: an implementation of
    Allure lifecycle that supports some multithreaded scenarios.

A new version of the API is likely to be released in the future as we need
a decent support for multithreaded and async-based concurrency (see
[here](https://github.com/allure-framework/allure-python/issues/697) and
[here](https://github.com/allure-framework/allure-python/issues/720)).
