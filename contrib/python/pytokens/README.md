# pytokens

A Fast, spec compliant Python 3.14+ tokenizer that runs on older Pythons.

## Installation

```bash
pip install pytokens
```

## Usage

```bash
python -m pytokens path/to/file.py
```

## Local Development / Testing

- Create and activate a virtual environment
- Run `PYTOKENS_USE_MYPYC=0 pip install -e '.[dev]'` to do an editable install
- Run `pytest` to run tests

## Type Checking

Run `mypy .`

## Compilation with mypyc

By default, we compile with mypyc.
Use the `PYTOKENS_USE_MYPYC` environment variable to control this.

To check if you are using a compiled version, see whether the output of this is `.py` or `.so`:
```bash
python -c "import pytokens; print(pytokens.__file__)"
```

## Create and upload a package to PyPI

1. Make sure to bump the version in `pyproject.toml`.
2. Push to Github so CI can build the wheels and sdist.
3. Download the artifacts from the CI run.
  - Find the "Build wheels" job, click "Summary", scroll down to the bottom to see "Artifacts"
    and download the `cibw-wheels` artifact.
4. Unzip the artifact.
5. Upload the contents of the artifact to PyPI
  - e.g. via `twine upload cibw-wheels/*`
