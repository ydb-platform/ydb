# argclass

[![Coverage](https://coveralls.io/repos/github/mosquito/argclass/badge.svg?branch=master)](https://coveralls.io/github/mosquito/argclass?branch=master) [![Actions](https://github.com/mosquito/argclass/workflows/tests/badge.svg)](https://github.com/mosquito/argclass/actions?query=workflow%3Atests) [![Latest Version](https://img.shields.io/pypi/v/argclass.svg)](https://pypi.python.org/pypi/argclass/) [![Python Versions](https://img.shields.io/pypi/pyversions/argclass.svg)](https://pypi.python.org/pypi/argclass/) [![License](https://img.shields.io/pypi/l/argclass.svg)](https://pypi.python.org/pypi/argclass/)

**Declarative CLI parser with type hints, config files, and environment variables.**

Build type-safe command-line interfaces using Python classes. Zero dependencies.

**[Documentation](https://docs.argclass.com)** | **[PyPI](https://pypi.org/project/argclass/)**

## Installation

```bash
pip install argclass
```

## Quick Start

<!--- name: test_hero_example --->
```python
import argclass

class Server(argclass.Parser):
    host: str = "127.0.0.1"
    port: int = 8080
    debug: bool = False

server = Server()
server.parse_args(["--host", "0.0.0.0", "--port", "9000", "--debug"])
assert server.host == "0.0.0.0"
assert server.port == 9000
assert server.debug is True
```

```bash
$ python server.py --host 0.0.0.0 --port 9000 --debug
```

## Features

| Feature | argclass | argparse | click/typer |
|---------|----------|----------|-------------|
| Type hints | Yes | No | Yes |
| IDE autocompletion | Yes | No | Yes |
| Config files | Built-in | No | No |
| Environment variables | Built-in | No | Plugin |
| Secret masking | Built-in | No | No |
| Dependencies | stdlib | stdlib | Many |

## Examples

### Type Annotations

<!--- name: test_type_annotations --->
```python
import argclass
from pathlib import Path

class Parser(argclass.Parser):
    name: str                    # required
    count: int = 10              # optional with default
    config: Path | None = None   # optional path
    files: list[str]             # list of values

parser = Parser()
parser.parse_args(["--name", "test", "--files", "a.txt", "b.txt"])
assert parser.name == "test"
assert parser.count == 10
assert parser.files == ["a.txt", "b.txt"]
```

### Argument Groups

<!--- name: test_groups_example --->
```python
import argclass

class DatabaseGroup(argclass.Group):
    host: str = "localhost"
    port: int = 5432

class Parser(argclass.Parser):
    debug: bool = False
    db = DatabaseGroup()

parser = Parser()
parser.parse_args(["--db-host", "db.example.com", "--db-port", "3306"])
assert parser.db.host == "db.example.com"
assert parser.db.port == 3306
```

### Configuration Files

Load default values from configuration files. INI by default, JSON/TOML via `config_parser_class`.
See [Config Files](https://docs.argclass.com/config-files.html) for details.

<!--- name: test_config_example --->
```python
import argclass
from pathlib import Path
from tempfile import NamedTemporaryFile

class Parser(argclass.Parser):
    host: str = "localhost"
    port: int = 8080

# Config file content
CONFIG_CONTENT = """
[DEFAULT]
host = example.com
port = 9000
"""

with NamedTemporaryFile(mode="w", suffix=".ini", delete=False) as f:
    f.write(CONFIG_CONTENT)
    config_path = f.name

parser = Parser(config_files=[config_path])
parser.parse_args([])
assert parser.host == "example.com"
assert parser.port == 9000

Path(config_path).unlink()
```

**Tip:** Use `os.getenv()` for dynamic config paths. Multiple files are merged
(later overrides earlier), enabling global defaults with user overrides:

```python
import os
import argclass

class Parser(argclass.Parser):
    host: str = "localhost"

parser = Parser(config_files=[
    os.getenv("MYAPP_CONFIG", "/etc/myapp/config.ini"),  # Global defaults
    "~/.config/myapp.ini",  # User overrides (partial config OK)
])
```

### Environment Variables

<!--- name: test_env_example --->
```python
import os
import argclass

os.environ["APP_HOST"] = "env.example.com"
os.environ["APP_DEBUG"] = "true"

class Parser(argclass.Parser):
    host: str = "localhost"
    debug: bool = False

parser = Parser(auto_env_var_prefix="APP_")
parser.parse_args([])
assert parser.host == "env.example.com"
assert parser.debug is True

del os.environ["APP_HOST"]
del os.environ["APP_DEBUG"]
```

### Subcommands

```python
import argclass

class ServeCommand(argclass.Parser):
    """Start the server."""
    host: str = "0.0.0.0"
    port: int = 8080

    def __call__(self) -> int:
        print(f"Serving on {self.host}:{self.port}")
        return 0

class CLI(argclass.Parser):
    verbose: bool = False
    serve = ServeCommand()

if __name__ == "__main__":
    cli = CLI()
    cli.parse_args()
    exit(cli())
```

```bash
$ python app.py serve --host 127.0.0.1 --port 9000
Serving on 127.0.0.1:9000
```

### Secrets

<!--- name: test_secrets_example --->
```python
import argclass

class Parser(argclass.Parser):
    api_key: str = argclass.Secret(env_var="API_KEY")

# SecretString prevents accidental logging
# repr() returns '******', str() returns actual value
```

## Documentation

Full documentation at **[docs.argclass.com](https://docs.argclass.com)**:

- [Quick Start](https://docs.argclass.com/quickstart.html)
- [Tutorial](https://docs.argclass.com/tutorial.html)
- [Arguments](https://docs.argclass.com/arguments.html)
- [Groups](https://docs.argclass.com/groups.html)
- [Subparsers](https://docs.argclass.com/subparsers.html)
- [Config Files](https://docs.argclass.com/config-files.html)
- [Environment Variables](https://docs.argclass.com/environment.html)
- [Secrets](https://docs.argclass.com/secrets.html)
- [API Reference](https://docs.argclass.com/api.html)
