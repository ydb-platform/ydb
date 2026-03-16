# pyjson5

A Python implementation of the JSON5 data format.

[JSON5](https://json5.org) extends the
[JSON](http://www.json.org) data interchange format to make it
slightly more usable as a configuration language:

* JavaScript-style comments (both single and multi-line) are legal.

* Object keys may be unquoted if they are legal ECMAScript identifiers

* Objects and arrays may end with trailing commas.

* Strings can be single-quoted, and multi-line string literals are allowed.

There are a few other more minor extensions to JSON; see the above page for
the full details.

This project implements a reader and writer implementation for Python;
where possible, it mirrors the
[standard Python JSON API](https://docs.python.org/library/json.html)
package for ease of use.

There is one notable difference from the JSON api: the `load()` and
`loads()` methods support optionally checking for (and rejecting) duplicate
object keys; pass `allow_duplicate_keys=False` to do so (duplicates are
allowed by default).

This is an early release. It has been reasonably well-tested, but it is
**SLOW**. It can be 1000-6000x slower than the C-optimized JSON module,
and is 200x slower (or more) than the pure Python JSON module.

**Please Note:** This library only handles JSON5 documents, it does not
allow you to read arbitrary JavaScript. For example, bare integers can
be legal object keys in JavaScript, but they aren't in JSON5.

## Known issues

* Did I mention that it is **SLOW**?

* The implementation follows Python3's `json` implementation where
  possible. This means that the `encoding` method to `dump()` is
  ignored, and unicode strings are always returned.

* The `cls` keyword argument that `json.load()`/`json.loads()` accepts
  to specify a custom subclass of ``JSONDecoder`` is not and will not be
  supported, because this implementation uses a completely different
  approach to parsing strings and doesn't have anything like the
  `JSONDecoder` class.

* The `cls` keyword argument that `json.dump()`/`json.dumps()` accepts
  is also not supported, for consistency with `json5.load()`. The `default`
  keyword *is* supported, though, and might be able to serve as a
  workaround.

## Contributing

`json5` have no runtime dependencies and it is supported on Python version 3.8
or later. However, in order to develop and build the package you need a
bunch of extra tools and the latest versions of those tools may require 3.9
or later. You can install the extra environment on 3.8 (and get older versions
of the tools), but they may not run completely cleanly.

#### On Mac

The easiest thing to do is to install [`uv`](https://docs.astral.sh/uv) and
use `uv` and the `//run` script to develop things. See `./run --help` for
the various commands that are supported. `glop` is the parser generator
tool used to generate a parser from the grammar in `json5/json5.g`.

```
$ brew install uv
$ git clone https://github.com/dpranke/pyjson5
$ git clone https://github.com/dpranke/glop
$ cd pyjson5
$ source $(./run devenv)  # To activate a venv w/ all the needed dev tools.
```

#### On other platforms

Install `uv` via whatever mechanism is appropriate.

### Create the venv

```
$ ./run devenv
```

(This calls `uv sync --extra dev`.)

### Running the tests

```
$ ./run tests
```

### Updating the packages

```
# Update the version in json5/version.py to $VERSION, which should be of
# the form X.Y.Z where X, Y, and Z are numbers.
$ ./run regen
$ ./run presubmit
$ git commit -a -m "Bump the version to $VERSION"
$ git tag "v$VERSION"
$ ./run build
$ ./run publish --prod
$ git push origin
$ git push --tags origin
```

(Assuming you have upload privileges to PyPI and the GitHub repo, of course.)

## Version History / Release Notes

* v0.13.0 (2026-01-01)
    * No code changes.
    * Add Python 3.14 to supported version, project config, dependencies
    * Update dependencies to latest stuff < 2025-12-01
      * This relaxes the versions specified in pyproject.toml to just
        use 'newer than' rather than exact matches.
      * Sets 'uv.tool.exclude-newer' in pyproject.toml to tell uv not
        to look at packages published within the past 30 days; this will
        hopefully help prevent dependencies on compromised projects.
    * Switch to using dependency-groups for 'dev' group.

* v0.12.1 (2025-08-12)
    * Fix [#94](https://github.com/dpranke/pyjson5/issues/94), where objects
      returned from a custom encoder were not being indented properly.

* v0.12.0 (2025-04-03)
    * Roll back pyproject.toml change for licensing so that we can still
      build the package on 3.8.
    * Upgrade devenv package dependencies to latest versions; they now need
      Python 3.9 or newer, though json5 itself still supports 3.8.

* v0.11.0 (2025-04-01)
    * Add a couple examples to the documentation and run doctest over
      them.
    * Fix a typing issue in dump and dumps with the `cls` argument; turns
      out mypy was right and I was wrong and I didn't realize it :).
    * Introduce a new `parse` method that can be used to iterate through
      a string, extracting multiple values.
    * Add a new `consume_trailing` parameter to `load`/`loads`/`parse`
      that specifies whether to keep parsing after a valid object is
      reached. By default, this is True and the string must only contain
      trailing whitespace. If set to False, parsing will stop when a
      valid object is reached.
    * Add a new `start` parameter to `load`/`loads`/`parse` to specify
      the zero-based offset to start parsing the string or file from.
    * [GitHub issue #60](https://github.com/dpranke/pyjson5/issues/60).
      Fix a bug where we were attempting to allow '--4' as a valid number.

* v0.10.0 (2024-11-25)
    * [GitHub issue #57](https://github.com/dpranke/pyjson5/issues/57).
      Added a `JSON5Encoder` class that can be overridden to do custom
      encoding of values. This class is vaguely similar to the `JSONEncoder`
      class in the standard `json` library, except that it has an
      `encode()` method that can be overridden to customize *any*
      value, not just ones the standard encoder doesn't know how to handle.
      It does also support a `default()` method that can be used to
      encode things not normally encodable, like the JSONEncoder class.
      It does not support an `iterencode` method. One could probably
      be added in the future, although exactly how that would work and
      interact with `encode` is a little unclear.
    * Restructured the code to use the new encoder class; doing so actually
      allowed me to delete a bunch of tediously duplicative code.
    * Added a new `quote_style` argument to `dump()`/`dumps()` to control
      how strings are encoded by default. For compatibility with older
      versions of the json5 library and the standard json library, it
      uses `QuoteStyle.ALWAYS_DOUBLE` which encodes all strings with double
      quotes all the time. You can also configure it to use single quotes
      all the time (`ALWAYS_SINGLE`), and to switch between single and double
      when doing so eliminates a need to escape quotes (`PREFER_SINGLE` and
      `PREFER_DOUBLE`). This also adds a `--quote-style` argument to
      `python -m json5`.
    * This release has a fair number of changes, but is intended to be
      completely backwards-compatible. Code without changes should run exactly
      as it did before.
* v0.9.28 (2024-11-11)
    * Fix GitHub CI to install `uv` so `./run tests` works properly.
    * Mark Python3.13 as supported in package metadata.
    * Update dev package dependencies (note that the latest versions
      of coverage and pylint no longer work w/ Python3.8)
* v0.9.27 (2024-11-10)
    * Fix typo in //README.md
* v0.9.26 (2024-11-10)
    * [GitHub issue #82](https://github.com/dpranke/pyjson5/issues/82)
      Add support for the `strict` parameter to `load()`/`loads()`.
    * Significantly rework the infra and the `run` script to be
      contemporary.
* v0.9.25 (2024-04-12)
    * [GitHub issue #81](https://github.com/dpranke/pyjson5/issues/81)
      Explicitly specify the directory to use for the package in
      pyproject.toml.
* v0.9.24 (2024-03-16)
    * Update GitHub workflow config to remove unnecessary steps and
      run on pull requests as well as commits.
    * Added note about removing `hypothesize` in v0.9.23.
    * No code changes.
* v0.9.23 (2024-03-16)
    * Lots of cleanup:
      * Removed old code needed for Python2 compatibility.
      * Removed tests using `hypothesize`. This ran model-based checks
        and didn't really add anything useful in terms of coverage to
        the test suite, and it introduced dependencies and slowed down
        the tests significantly. It was a good experiment but I think
        we're better off without it.
      * Got everything linting cleanly with pylint 3.1 and `ruff check`
        using ruff 0.3.3 (Note that commit message in 00d73a3 says pylint
        3.11, which is a typo).
      * Code reformatted with `ruff format`
      * Added missing tests to bring coverage up to 100%.
      * Lots of minor code changes as the result of linting and coverage
        testing, but no intentional functional differences.
* v0.9.22 (2024-03-06)
    * Attempt to fix the GitHub CI configuration now that setup.py
      is gone. Also, test on 3.12 instead of 3.11.
    * No code changes.
* v0.9.21 (2024-03-06)
    * Moved the benchmarks/*.json data files' license information
      to //LICENSE to (hopefully) make the Google linter happy.
* v0.9.20 (2024-03-03)
    * Added `json5.__version__` in addition to `json5.VERSION`.
    * More packaging modernization (no more setup.{cfg,py} files).
    * Mark Python3.12 as supported in project.classifiers.
    * Updated the `//run` script to use python3.
* v0.9.19 (2024-03-03)
    * Replaced the benchmarking data files that came from chromium.org with
      three files obtained from other datasets on GitHub. Since this repo
      is vendored into the chromium/src repo it was occasionally confusing
      people who thought the data was actually used for non-benchmarking
      purposes and thus updating it for whatever reason.
    * No code changes.
* v0.9.18 (2024-02-29)
    * Add typing information to the module. This is kind of a big change,
      but there should be no functional differences.
* v0.9.17 (2024-02-19)
    * Move from `setup.py` to `pyproject.toml`.
    * No code changes (other than the version increasing).
* v0.9.16 (2024-02-19)
    * Drop Python2 from `setup.py`
    * Add minimal packaging instructions to `//README.md`.
* v0.9.15 (2024-02-19)
    * Merge in [Pull request #66](https://github.com/dpranke/pyjson5/pull/66)
      to include the tests and sample file in a source distribution.
* v0.9.14 (2023-05-14)
    * [GitHub issue #63](https://github.com/dpranke/pyjson5/issues/63)
      Handle `+Infinity` as well as `-Infinity` and `Infinity`.
* v0.9.13 (2023-03-16)
    * [GitHub PR #64](https://github.com/dpranke/pyjson5/pull/64)
      Remove a field from one of the JSON benchmark files to
      reduce confusion in Chromium.
    * No code changes.
* v0.9.12 (2023-01-02)
    * Fix GitHub Actions config file to no longer test against
      Python 3.6 or 3.7. For now we will only test against an
      "oldest" release (3.8 in this case) and a "current"
      release (3.11 in this case).
* v0.9.11 (2023-01-02)
    * [GitHub issue #60](https://github.com/dpranke/pyjson5/issues/60)
      Fixed minor Python2 compatibility issue by referring to
      `float("inf")` instead of `math.inf`.
* v0.9.10 (2022-08-18)
    * [GitHub issue #58](https://github.com/dpranke/pyjson5/issues/58)
      Updated the //README.md to be clear that parsing arbitrary JS
      code may not work.
    * Otherwise, no code changes.
* v0.9.9 (2022-08-01)
    * [GitHub issue #57](https://github.com/dpranke/pyjson5/issues/57)
      Fixed serialization for objects that subclass `int` or `float`:
      Previously we would use the objects __str__ implementation, but
      that might result in an illegal JSON5 value if the object had
      customized __str__ to return something illegal. Instead,
      we follow the lead of the `JSON` module and call `int.__repr__`
      or `float.__repr__` directly.
    * While I was at it, I added tests for dumps(-inf) and dumps(nan)
      when those were supposed to be disallowed by `allow_nan=False`.
* v0.9.8 (2022-05-08)
    * [GitHub issue #47](https://github.com/dpranke/pyjson5/issues/47)
      Fixed error reporting in some cases due to how parsing was handling
      nested rules in the grammar - previously the reported location for
      the error could be far away from the point where it actually happened.

* v0.9.7 (2022-05-06)
    * [GitHub issue #52](https://github.com/dpranke/pyjson5/issues/52)
      Fixed behavior of `default` fn in `dump` and `dumps`. Previously
      we didn't require the function to return a string, and so we could
      end up returning something that wasn't actually valid. This change
      now matches the behavior in the `json` module. *Note: This is a
      potentially breaking change.*
* v0.9.6 (2021-06-21)
    * Bump development status classifier to 5 - Production/Stable, which
      the library feels like it is at this point. If I do end up significantly
      reworking things to speed it up and/or to add round-trip editing,
      that'll likely be a 2.0. If this version has no reported issues,
      I'll likely promote it to 1.0.
    * Also bump the tested Python versions to 2.7, 3.8 and 3.9, though
      earlier Python3 versions will likely continue to work as well.
    * [GitHub issue #46](https://github.com/dpranke/pyjson5/issues/36)
      Fix incorrect serialization of custom subtypes
    * Make it possible to run the tests if `hypothesis` isn't installed.

* v0.9.5 (2020-05-26)
    * Miscellaneous non-source cleanups in the repo, including setting
      up GitHub Actions for a CI system. No changes to the library from
      v0.9.4, other than updating the version.

* v0.9.4 (2020-03-26)
    * [GitHub pull #38](https://github.com/dpranke/pyjson5/pull/38)
      Fix from fredrik@fornwall.net for dumps() crashing when passed
      an empty string as a key in an object.

* v0.9.3 (2020-03-17)
    * [GitHub pull #35](https://github.com/dpranke/pyjson5/pull/35)
      Fix from pastelmind@ for dump() not passing the right args to dumps().
    * Fix from p.skouzos@novafutur.com to remove the tests directory from
      the setup call, making the package a bit smaller.

* v0.9.2 (2020-03-02)
    * [GitHub pull #34](https://github.com/dpranke/pyjson5/pull/34)
      Fix from roosephu@ for a badly formatted nested list.

* v0.9.1 (2020-02-09)
    * [GitHub issue #33](https://github.com/dpranke/pyjson5/issues/33):
       Fix stray trailing comma when dumping an object with an invalid key.

* v0.9.0 (2020-01-30)
    * [GitHub issue #29](https://github.com/dpranke/pyjson5/issues/29):
       Fix an issue where objects keys that started with a reserved
       word were incorrectly quoted.
    * [GitHub issue #30](https://github.com/dpranke/pyjson5/issues/30):
       Fix an issue where dumps() incorrectly thought a data structure
       was cyclic in some cases.
    * [GitHub issue #32](https://github.com/dpranke/pyjson5/issues/32):
       Allow for non-string keys in dicts passed to ``dump()``/``dumps()``.
       Add an ``allow_duplicate_keys=False`` to prevent possible
       ill-formed JSON that might result.

* v0.8.5 (2019-07-04)
    * [GitHub issue #25](https://github.com/dpranke/pyjson5/issues/25):
      Add LICENSE and README.md to the dist.
    * [GitHub issue #26](https://github.com/dpranke/pyjson5/issues/26):
      Fix printing of empty arrays and objects with indentation, fix
      misreporting of the position on parse failures in some cases.

* v0.8.4 (2019-06-11)
    * Updated the version history, too.

* v0.8.3 (2019-06-11)
    * Tweaked the README, bumped the version, forgot to update the version
      history :).

* v0.8.2 (2019-06-11)
    * Actually bump the version properly, to 0.8.2.

* v0.8.1 (2019-06-11)
    * Fix bug in setup.py that messed up the description. Unfortunately,
      I forgot to bump the version for this, so this also identifies as 0.8.0.

* v0.8.0 (2019-06-11)
    * Add `allow_duplicate_keys=True` as a default argument to
      `json5.load()`/`json5.loads()`. If you set the key to `False`, duplicate
      keys in a single dict will be rejected. The default is set to `True`
      for compatibility with `json.load()`, earlier versions of json5, and
      because it's simply not clear if people would want duplicate checking
      enabled by default.

* v0.7 (2019-03-31)
    * Changes dump()/dumps() to not quote object keys by default if they are
      legal identifiers. Passing `quote_keys=True` will turn that off
      and always quote object keys.
    * Changes dump()/dumps() to insert trailing commas after the last item
      in an array or an object if the object is printed across multiple lines
      (i.e., if `indent` is not None). Passing `trailing_commas=False` will
      turn that off.
    * The `json5.tool` command line tool now supports the `--indent`,
      `--[no-]quote-keys`, and `--[no-]trailing-commas` flags to allow
      for more control over the output, in addition to the existing
      `--as-json` flag.
    * The `json5.tool` command line tool no longer supports reading from
      multiple files, you can now only read from a single file or
      from standard input.
    * The implementation no longer relies on the standard `json` module
      for anything. The output should still match the json module (except
      as noted above) and discrepancies should be reported as bugs.

* v0.6.2 (2019-03-08)
    * Fix [GitHub issue #23](https://github.com/dpranke/pyjson5/issues/23) and
      pass through unrecognized escape sequences.

* v0.6.1 (2018-05-22)
    * Cleaned up a couple minor nits in the package.

* v0.6.0 (2017-11-28)
    * First implementation that attempted to implement 100% of the spec.

* v0.5.0 (2017-09-04)
    * First implementation that supported the full set of kwargs that
      the `json` module supports.
