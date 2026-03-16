# complexipy

<p align="center">
    <a href="https://rohaquinlop.github.io/complexipy/"><img src="https://raw.githubusercontent.com/rohaquinlop/complexipy/main/docs/img/logo-vector.svg" alt="complexipy"></a>
</p>

<p align="center">
    <em>An extremely fast Python library to calculate the cognitive complexity of Python files, written in Rust.</em>
</p>

<p align="center">
    <a href="https://sonarcloud.io/summary/new_code?id=rohaquinlop_complexipy" target="_blank">
        <img src="https://sonarcloud.io/api/project_badges/measure?project=rohaquinlop_complexipy&metric=alert_status" alt="Quality Gate">
    </a>
    <a href="https://pypi.org/project/complexipy" target="_blank">
    <img src="https://img.shields.io/pypi/v/complexipy?color=%2334D058&label=pypi%20package" alt="Package version">
</a>
</p>


Cognitive Complexity breaks from using mathematical models to assess software
maintainability by combining Cyclomatic Complexity precedents with human
assessment. It yields method complexity scores that align well with how
developers perceive maintainability. Read the white paper here: [Cognitive Complexity, a new way of measuring understandability](https://www.sonarsource.com/resources/cognitive-complexity/)


---

**Documentation**: <a href="https://rohaquinlop.github.io/complexipy/" target="_blank">https://rohaquinlop.github.io/complexipy/</a>

**Source Code**: <a href="https://github.com/rohaquinlop/complexipy" target="_blank">https://github.com/rohaquinlop/complexipy</a>

**PyPI**: <a href="https://pypi.org/project/complexipy/" target="_blank">https://pypi.org/project/complexipy/</a>

---


## Contributors

<p align="center">
    <a href = "https://github.com/rohaquinlop/complexipy/graphs/contributors">
    <img src = "https://contrib.rocks/image?repo=rohaquinlop/complexipy"/>
    </a>
</p>

Made with [contributors-img](https://contrib.rocks)

## Requirements

- Python >= 3.8
- You also need to install `git` in your computer if you want to analyze a git repository.

## Installation

```bash
pip install complexipy
```

## Usage

To run **complexipy** you can use the following command:

```shell
complexipy .                            # Use complexipy to analyze the current directory and any subdirectories
complexipy path/to/directory            # Use complexipy to analyze a specific directory and any subdirectories
complexipy git_repository_url           # Use complexipy to analyze a git repository
complexipy path/to/file.py              # Use complexipy to analyze a specific file
complexipy path/to/file.py -c 20        # Use the -c option to set the maximum congnitive complexity, default is 15
complexipy path/to/directory -c 0       # Set the maximum cognitive complexity to 0 to disable the exit with error
complexipy path/to/directory -o         # Use the -o option to output the results to a CSV file, default is False
complexipy path/to/directory -d low     # Use the -d option to set detail level, default is "normal". If set to "low" will show only files with complexity greater than the maximum complexity
complexipy path/to/directory -l file    # Use the -l option to set the level of measurement, default is "function". If set to "file" will measure the complexity of the file and will validate the maximum complexity according to the file complexity.
complexipy path/to/directory -q         # Use the -q option to disable the output to the console, default is False.
complexipy path/to/directory -s desc    # Use the -s option to set the sort order, default is "asc". If set to "desc" will sort the results in descending order. If set to "asc" will sort the results in ascending order. If set to "name" will sort the results by name.
```

### Options

- `-c` or `--max-complexity`: Set the maximum cognitive complexity, default is 15.
  If the cognitive complexity of a file is greater than the maximum cognitive,
  then the return code will be 1 and exit with error, otherwise it will be 0.
  If set to 0, the exit with error will be disabled.
- `-o` or `--output`: Output the results to a CSV file, default is False. The
  filename will be `complexipy.csv` and will be saved in the invocation directory.
- `-d` or `--details`: Set the detail level, default is "normal". If set to "low"
  will show only files or functions with complexity greater than the maximum
  complexity.
- `-l` or `--level` Set the level of measurement, default is "function". If set
  to "file" will measure the complexity of the file and will validate the maximum
  complexity according to the file complexity. If set to "function" will measure
  the complexity of the functions and will validate the maximum complexity
  according to the function complexity. This option is useful if you want to set
  a maximum complexity according for each file or for each function in the file
  (or files).
- `-q` or `--quiet`: Disable the output to the console, default is False.
- `-s` or `--sort`: Set the sort order, default is "asc". If set to "desc" will
  sort the results in descending order. If set to "asc" will sort the results in
  ascending order. If set to "name" will sort the results by name. This option will
  affect the output to the console and the output to the CSV file.

If the cognitive complexity of a file or a function is greater than the maximum
cognitive cognitive complexity, then the return code will be 1 and exit with
error, otherwise it will be 0.

## Example

### Analyzing a file

For example, given the following file:

```python
def a_decorator(a, b):
    def inner(func):
        return func
    return inner

def b_decorator(a, b):
    def inner(func):
        if func:
            return None
        return func
    return inner
```

The cognitive complexity of the file is 1, and the output of the command
`complexipy path/to/file.py` will be:

```txt
───────────────────────────── 🐙 complexipy 0.4.0 ──────────────────────────────
                                    Summary
      ┏━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━┓
      ┃ Path              ┃ File              ┃ Function    ┃ Complexity ┃
      ┡━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━┩
      │ test_decorator.py │ test_decorator.py │ a_decorator │ 0          │
      ├───────────────────┼───────────────────┼─────────────┼────────────┤
      │ test_decorator.py │ test_decorator.py │ b_decorator │ 1          │
      └───────────────────┴───────────────────┴─────────────┴────────────┘
🧠 Total Cognitive Complexity in ./tests/src/test_decorator.py: 1
1 file analyzed in 0.0032 seconds
────────────────────────── 🎉 Analysis completed! 🎉 ───────────────────────────
```

#### Explaining the results of the analysis

```python
def a_decorator(a, b): # 0
    def inner(func): # 0
        return func # 0
    return inner # 0

def b_decorator(a, b): # 0
    def inner(func): # 0
        if func: # 1 (nested = 0), total 1
            return None # 0
        return func # 0
    return inner # 0
```

The cognitive complexity of the file is 1, and the cognitive complexity of the
function `b_decorator` is 1. This example is simple, but it shows how
**complexipy** calculates the cognitive complexity according to the specifications
of the paper "Cognitive Complexity a new way to measure understandability",
considering the decorators and the if statement.

#### Output to a CSV file

If you want to output the results to a CSV file, you can use the `-o` option,
this is really useful if you want to integrate **complexipy** with other tools,
for example, a CI/CD pipeline. You will get the output in the console and will
create a CSV file with the results of the analysis.

The filename will be `complexipy.csv` and will be saved in the current directory.

```bash
$ complexipy path/to/file.py -o
```

The output will be:

```csv
Path,File Name,Function Name,Cognitive Complexity
test_decorator.py,test_decorator.py,a_decorator,0
test_decorator.py,test_decorator.py,b_decorator,1
```

### Analyzing a directory

You can also analyze a directory, for example:

```bash
$ complexipy .
```

And **complexipy** will analyze all the files in the current directory and any
subdirectories.

### Analyzing a git repository

You can also analyze a git repository, for example:

```bash
$ complexipy https://github.com/rohaquinlop/complexipy
```

And to generate the output to a CSV file:

```bash
$ complexipy https://github.com/rohaquinlop/complexipy -o
```

## License

This project is licensed under the MIT License - see the [LICENSE](https://github.com/rohaquinlop/complexipy/blob/main/LICENSE) file
for details.

## Acknowledgments

- Thanks to G. Ann Campbell for publishing the paper "Cognitive Complexity a new
way to measure understandability".
- This project is inspired by the Sonar way to calculate the cognitive
complexity.

## References

- [Cognitive Complexity](https://www.sonarsource.com/resources/cognitive-complexity/)
