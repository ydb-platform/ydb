<a href="https://explosion.ai"><img src="https://explosion.ai/assets/img/logo.svg" width="125" height="125" align="right" /></a>

# Confection: The sweetest config system for Python

`confection` :candy: is a lightweight library that offers a **configuration
system** letting you conveniently describe arbitrary trees of objects.

Configuration is a huge challenge for machine-learning code because you may want
to expose almost any detail of any function as a hyperparameter. The setting you
want to expose might be arbitrarily far down in your call stack, so it might
need to pass all the way through the CLI or REST API, through any number of
intermediate functions, affecting the interface of everything along the way. And
then once those settings are added, they become hard to remove later. Default
values also become hard to change without breaking backwards compatibility.

To solve this problem, `confection` offers a config system that lets you easily
describe arbitrary trees of objects. The objects can be created via function
calls you register using a simple decorator syntax. You can even version the
functions you create, allowing you to make improvements without breaking
backwards compatibility. The most similar config system we’re aware of is
[Gin](https://github.com/google/gin-config), which uses a similar syntax, and
also allows you to link the configuration system to functions in your code using
a decorator. `confection`'s config system is simpler and emphasizes a different
workflow via a subset of Gin’s functionality.

[![tests](https://github.com/explosion/confection/actions/workflows/tests.yml/badge.svg)](https://github.com/explosion/confection/actions/workflows/tests.yml)
[![Current Release Version](https://img.shields.io/github/v/release/explosion/confection.svg?style=flat-square&include_prereleases&logo=github)](https://github.com/explosion/confection/releases)
[![pypi Version](https://img.shields.io/pypi/v/confection.svg?style=flat-square&logo=pypi&logoColor=white)](https://pypi.org/project/confection/)
[![conda Version](https://img.shields.io/conda/vn/conda-forge/confection.svg?style=flat-square&logo=conda-forge&logoColor=white)](https://anaconda.org/conda-forge/confection)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg?style=flat-square)](https://github.com/ambv/black)

## ⏳ Installation

```bash
pip install confection
```

```bash
conda install -c conda-forge confection
```

## 👩‍💻 Usage

The configuration system parses a `.cfg` file like

```ini
[training]
patience = 10
dropout = 0.2
use_vectors = false

[training.logging]
level = "INFO"

[nlp]
# This uses the value of training.use_vectors
use_vectors = ${training.use_vectors}
lang = "en"
```

and resolves it to a `Dict`:

```json
{
  "training": {
    "patience": 10,
    "dropout": 0.2,
    "use_vectors": false,
    "logging": {
      "level": "INFO"
    }
  },
  "nlp": {
    "use_vectors": false,
    "lang": "en"
  }
}
```

The config is divided into sections, with the section name in square brackets –
for example, `[training]`. Within the sections, config values can be assigned to
keys using `=`. Values can also be referenced from other sections using the dot
notation and placeholders indicated by the dollar sign and curly braces. For
example, `${training.use_vectors}` will receive the value of use_vectors in the
training block. This is useful for settings that are shared across components.

The config format has three main differences from Python’s built-in
`configparser`:

1. JSON-formatted values. `confection` passes all values through `json.loads` to
   interpret them. You can use atomic values like strings, floats, integers or
   booleans, or you can use complex objects such as lists or maps.
2. Structured sections. `confection` uses a dot notation to build nested
   sections. If you have a section named `[section.subsection]`, `confection`
   will parse that into a nested structure, placing subsection within section.
3. References to registry functions. If a key starts with `@`, `confection` will
   interpret its value as the name of a function registry, load the function
   registered for that name and pass in the rest of the block as arguments. If
   type hints are available on the function, the argument values (and return
   value of the function) will be validated against them. This lets you express
   complex configurations, like a training pipeline where `batch_size` is
   populated by a function that yields floats.

There’s no pre-defined scheme you have to follow; how you set up the top-level
sections is up to you. At the end of it, you’ll receive a dictionary with the
values that you can use in your script – whether it’s complete initialized
functions, or just basic settings.

For instance, let’s say you want to define a new optimizer. You'd define its
arguments in `config.cfg` like so:

```ini
[optimizer]
@optimizers = "my_cool_optimizer.v1"
learn_rate = 0.001
gamma = 1e-8
```

To load and parse this configuration using a `catalogue` registry (install
[`catalogue`](https://github.com/explosion/catalogue) separately):

```python
import dataclasses
from typing import Union, Iterable
import catalogue
from confection import registry, Config

# Create a new registry.
registry.optimizers = catalogue.create("confection", "optimizers", entry_points=False)


# Define a dummy optimizer class.
@dataclasses.dataclass
class MyCoolOptimizer:
    learn_rate: float
    gamma: float


@registry.optimizers.register("my_cool_optimizer.v1")
def make_my_optimizer(learn_rate: Union[float, Iterable[float]], gamma: float):
    return MyCoolOptimizer(learn_rate, gamma)


# Load the config file from disk, resolve it and fetch the instantiated optimizer object.
config = Config().from_disk("./config.cfg")
resolved = registry.resolve(config)
optimizer = resolved["optimizer"]  # MyCoolOptimizer(learn_rate=0.001, gamma=1e-08)
```

> ⚠️ Caution: Type-checkers such as `mypy` will mark adding new attributes to `registry` this way - i. e. 
> `registry.new_attr = ...` - as errors. This is because a new attribute is added to the class after initialization. If 
> you are using typecheckers, you can either ignore this (e. g. with `# type: ignore` for `mypy`) or use a typesafe 
> alternative: instead of `registry.new_attr = ...`, use `setattr(registry, "new_attr", ...)`. 

Under the hood, `confection` will look up the `"my_cool_optimizer.v1"` function
in the "optimizers" registry and then call it with the arguments `learn_rate`
and `gamma`. If the function has type annotations, it will also validate the
input. For instance, if `learn_rate` is annotated as a float and the config
defines a string, `confection` will raise an error.

The Thinc documentation offers further information on the configuration system:

- [recursive blocks](https://thinc.ai/docs/usage-config#registry-recursive)
- [defining variable positional arguments](https://thinc.ai/docs/usage-config#registries-args)
- [using interpolation](https://thinc.ai/docs/usage-config#config-interpolation)
- [using custom registries](https://thinc.ai/docs/usage-config#registries-custom)
- [advanced type annotations with Pydantic](https://thinc.ai/docs/usage-config#advanced-types)
- [using base schemas](https://thinc.ai/docs/usage-config#advanced-types-base-schema)
- [filling a configuration with defaults](https://thinc.ai/docs/usage-config#advanced-types-fill-defaults)

## 🎛 API

### <kbd>class</kbd> `Config`

This class holds the model and training
[configuration](https://thinc.ai/docs/usage-config) and can load and save the
INI-style configuration format from/to a string, file or bytes. The `Config`
class is a subclass of `dict` and uses Python’s `ConfigParser` under the hood.

#### <sup><kbd>method</kbd> `Config.__init__`</sup>

Initialize a new `Config` object with optional data.

```python
from confection import Config
config = Config({"training": {"patience": 10, "dropout": 0.2}})
```

| Argument          | Type                                      | Description                                                                                                                                                 |
| ----------------- | ----------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `data`            | `Optional[Union[Dict[str, Any], Config]]` | Optional data to initialize the config with.                                                                                                                |
| `section_order`   | `Optional[List[str]]`                     | Top-level section names, in order, used to sort the saved and loaded config. All other sections will be sorted alphabetically.                              |
| `is_interpolated` | `Optional[bool]`                          | Whether the config is interpolated or whether it contains variables. Read from the `data` if it’s an instance of `Config` and otherwise defaults to `True`. |

#### <sup><kbd>method</kbd> `Config.from_str`</sup>

Load the config from a string.

```python
from confection import Config

config_str = """
[training]
patience = 10
dropout = 0.2
"""
config = Config().from_str(config_str)
print(config["training"])  # {'patience': 10, 'dropout': 0.2}}
```

| Argument      | Type             | Description                                                                                                          |
| ------------- | ---------------- | -------------------------------------------------------------------------------------------------------------------- |
| `text`        | `str`            | The string config to load.                                                                                           |
| `interpolate` | `bool`           | Whether to interpolate variables like `${section.key}`. Defaults to `True`.                                          |
| `overrides`   | `Dict[str, Any]` | Overrides for values and sections. Keys are provided in dot notation, e.g. `"training.dropout"` mapped to the value. |
| **RETURNS**   | `Config`         | The loaded config.                                                                                                   |

#### <sup><kbd>method</kbd> `Config.to_str`</sup>

Load the config from a string.

```python
from confection import Config

config = Config({"training": {"patience": 10, "dropout": 0.2}})
print(config.to_str()) # '[training]\npatience = 10\n\ndropout = 0.2'
```

| Argument      | Type   | Description                                                                 |
| ------------- | ------ | --------------------------------------------------------------------------- |
| `interpolate` | `bool` | Whether to interpolate variables like `${section.key}`. Defaults to `True`. |
| **RETURNS**   | `str`  | The string config.                                                          |

#### <sup><kbd>method</kbd> `Config.to_bytes`</sup>

Serialize the config to a byte string.

```python
from confection import Config

config = Config({"training": {"patience": 10, "dropout": 0.2}})
config_bytes = config.to_bytes()
print(config_bytes)  # b'[training]\npatience = 10\n\ndropout = 0.2'
```

| Argument      | Type             | Description                                                                                                          |
| ------------- | ---------------- | -------------------------------------------------------------------------------------------------------------------- |
| `interpolate` | `bool`           | Whether to interpolate variables like `${section.key}`. Defaults to `True`.                                          |
| `overrides`   | `Dict[str, Any]` | Overrides for values and sections. Keys are provided in dot notation, e.g. `"training.dropout"` mapped to the value. |
| **RETURNS**   | `str`            | The serialized config.                                                                                               |

#### <sup><kbd>method</kbd> `Config.from_bytes`</sup>

Load the config from a byte string.

```python
from confection import Config

config = Config({"training": {"patience": 10, "dropout": 0.2}})
config_bytes = config.to_bytes()
new_config = Config().from_bytes(config_bytes)
```

| Argument      | Type     | Description                                                                 |
| ------------- | -------- | --------------------------------------------------------------------------- |
| `bytes_data`  | `bool`   | The data to load.                                                           |
| `interpolate` | `bool`   | Whether to interpolate variables like `${section.key}`. Defaults to `True`. |
| **RETURNS**   | `Config` | The loaded config.                                                          |

#### <sup><kbd>method</kbd> `Config.to_disk`</sup>

Serialize the config to a file.

```python
from confection import Config

config = Config({"training": {"patience": 10, "dropout": 0.2}})
config.to_disk("./config.cfg")
```

| Argument      | Type               | Description                                                                 |
| ------------- | ------------------ | --------------------------------------------------------------------------- |
| `path`        | `Union[Path, str]` | The file path.                                                              |
| `interpolate` | `bool`             | Whether to interpolate variables like `${section.key}`. Defaults to `True`. |

#### <sup><kbd>method</kbd> `Config.from_disk`</sup>

Load the config from a file.

```python
from confection import Config

config = Config({"training": {"patience": 10, "dropout": 0.2}})
config.to_disk("./config.cfg")
new_config = Config().from_disk("./config.cfg")
```

| Argument      | Type               | Description                                                                                                          |
| ------------- | ------------------ | -------------------------------------------------------------------------------------------------------------------- |
| `path`        | `Union[Path, str]` | The file path.                                                                                                       |
| `interpolate` | `bool`             | Whether to interpolate variables like `${section.key}`. Defaults to `True`.                                          |
| `overrides`   | `Dict[str, Any]`   | Overrides for values and sections. Keys are provided in dot notation, e.g. `"training.dropout"` mapped to the value. |
| **RETURNS**   | `Config`           | The loaded config.                                                                                                   |

#### <sup><kbd>method</kbd> `Config.copy`</sup>

Deep-copy the config.

| Argument    | Type     | Description        |
| ----------- | -------- | ------------------ |
| **RETURNS** | `Config` | The copied config. |

#### <sup><kbd>method</kbd> `Config.interpolate`</sup>

Interpolate variables like `${section.value}` or `${section.subsection}` and
return a copy of the config with interpolated values. Can be used if a config is
loaded with `interpolate=False`, e.g. via `Config.from_str`.

```python
from confection import Config

config_str = """
[hyper_params]
dropout = 0.2

[training]
dropout = ${hyper_params.dropout}
"""
config = Config().from_str(config_str, interpolate=False)
print(config["training"])  # {'dropout': '${hyper_params.dropout}'}}
config = config.interpolate()
print(config["training"])  # {'dropout': 0.2}}
```

| Argument    | Type     | Description                                    |
| ----------- | -------- | ---------------------------------------------- |
| **RETURNS** | `Config` | A copy of the config with interpolated values. |

##### <sup><kbd>method</kbd> `Config.merge`</sup>

Deep-merge two config objects, using the current config as the default. Only
merges sections and dictionaries and not other values like lists. Values that
are provided in the updates are overwritten in the base config, and any new
values or sections are added. If a config value is a variable like
`${section.key}` (e.g. if the config was loaded with `interpolate=False)`, **the
variable is preferred**, even if the updates provide a different value. This
ensures that variable references aren’t destroyed by a merge.

> :warning: Note that blocks that refer to registered functions using the `@`
> syntax are only merged if they are referring to the same functions. Otherwise,
> merging could easily produce invalid configs, since different functions can
> take different arguments. If a block refers to a different function, it’s
> overwritten.

```python
from confection import Config

base_config_str = """
[training]
patience = 10
dropout = 0.2
"""
update_config_str = """
[training]
dropout = 0.1
max_epochs = 2000
"""

base_config = Config().from_str(base_config_str)
update_config = Config().from_str(update_config_str)
merged = Config(base_config).merge(update_config)
print(merged["training"])  # {'patience': 10, 'dropout': 0.1, 'max_epochs': 2000}
```

| Argument    | Type                            | Description                                         |
| ----------- | ------------------------------- | --------------------------------------------------- |
| `overrides` | `Union[Dict[str, Any], Config]` | The updates to merge into the config.               |
| **RETURNS** | `Config`                        | A new config instance containing the merged config. |

### Config Attributes

| Argument          | Type   | Description                                                                                                                                                              |
| ----------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `is_interpolated` | `bool` | Whether the config values have been interpolated. Defaults to `True` and is set to `False` if a config is loaded with `interpolate=False`, e.g. using `Config.from_str`. |
