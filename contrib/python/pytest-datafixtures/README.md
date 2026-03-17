# pytest-datafixtures

https://github.com/idlesign/pytest-datafixtures

[![PyPI - Version](https://img.shields.io/pypi/v/pytest-datafixtures)](https://pypi.python.org/pypi/pytest-datafixtures)
[![License](https://img.shields.io/pypi/l/pytest-datafixtures)](https://pypi.python.org/pypi/pytest-datafixtures)
[![Coverage](https://img.shields.io/coverallsCoverage/github/idlesign/pytest-datafixtures)](https://coveralls.io/r/idlesign/pytest-datafixtures)

## Description

*Data fixtures for pytest made simple*

Offers fixtures for your tests to simplify data fixtures access.
Makes use of Python's native `Path` objects.

Data fixtures (files) expected to be stored in `datafixtures` directory next to your test modules:

```
tests
|-- datafixtures
|   |-- mydatafixture.xml
|   |-- mydata.bin   
|-- test_basic.py
|
|-- subdirectory
|---- datafixtures
|     |-- payload.json
|     |-- cert.cer
|---- test_other.py
```


## Requirements

* Python 3.10+


## Fixtures

* `datafix_dir` - Path object for data fixtures directory from the current test module's directory.
* `datafix` - Path object for a file in data fixtures directory with the same name as the current test function.
* `datafix_read` - Returns text contents of a data fixture by name.
* `datafix_readbin` - Returns binary contents of a data fixture by name.
* `datafix_dump` - Allows data dumping for further usage.


#### datafix_dir

Access data fixtures directory:

```python
def test_me(datafix_dir):

    # datafix_dir returns a Path object.
    assert datafix_dir.exists()

    # Gather data fixtures filenames.
    files = list(f'{file.name}' for file in datafix_dir.iterdir())

    # Read some fixture as text.
    # The same as using `datafix_read` fixture (see below).
    filecontent = (datafix_dir / 'expected.html').read_text()

    # Or read binary.
    filecontent = (datafix_dir / 'dumped.bin').read_bytes()
```

#### datafix

Access a data fixture with test name:

```python
def test_me(datafix):
    # Read datafixtures/test_me.txt file
    filecontents = datafix.with_suffix('.txt').read_text()
```

#### datafix_read

Access text contents of a data fixture by name:

```python
def test_datafix_read(datafix_read):
    # Read datafixtures/expected.html file
    filecontents = datafix_read('expected.html')

    # Read encoded and represent as an StringIO object.
    encoded_io = datafix_read('test_datafix.txt', encoding='cp1251', io=True)
```

#### datafix_readbin

Access binary contents of a data fixture by name:

```python
def test_datafix_read(datafix_readbin):
    # Read datafixtures/dumped.bin file
    binary = datafix_readbin('dumped.bin')

    # Read binary and represent as an BytesIO object.
    bin_io = datafix_readbin('dumped.bin', io=True)
```

#### datafix_dump

Dump data for later usage as a datafixture:

```python
def test_datafix_dump(datafix_dump):
    # Dump text
    dumped_filepath = datafix_dump('sometext', encoding='cp1251')
    
    # Dump binary
    dumped_filepath = datafix_dump(b'\x12')
```
