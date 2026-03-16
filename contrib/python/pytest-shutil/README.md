# pytest-shutil


This library is a goodie-bag of Unix shell and environment management tools for automated tests.
A summary of the available functions is below, look at the source for the full listing.
               
## Installation
                  
Install using your favourite package manager::

```bash
    pip install pytest-shutil
    #  or..
    easy_install pytest-shutil
```               

## Workspace Fixture

The workspace fixture is simply a temporary directory at function-scope with a few bells and whistles::

```python
    # Enable the fixture explicitly in your tests or conftest.py (not required when using setuptools entry points)
    pytest_plugins = ['pytest_shutil']
    
    def test_something(workspace):
        # Workspaces contain a handle to the path `path` object (see https://path.readthedocs.io/)
        path = workspace.workspace         
        script = path / 'hello.sh'
        script.write_text('#!/bin/sh\n echo hello world!')
        
        # There is a 'run' method to execute things relative to the workspace root
        workspace.run('hello.sh')
```

## ``pytest_shutil.env``: Shell helpers

| function  | description
| --------- | -----------
| set_env   | contextmanager to set env vars 
| unset_env | contextmanager to unset env vars 
| no_env    | contextmanager to unset a single env var 
| no_cov    | contextmanager to disable coverage in subprocesses 

## ``pytest_shutil.cmdline``: Command-line helpers

| function  | description
| --------- | -----------
| umask                      | contextmanager to set the umask
| chdir                      | contextmanager to change to a directory
| TempDir                    | contextmanager for a temporary directory
| PrettyFormatter            | simple text formatter for drawing title, paragrahs, hrs. 
| copy_files                 | copy all files from one directory to another
| getch                      | cross-platform read of a single character from the screen
| which                      | analoge of unix ``which``
| get_real_python_executable | find our system Python, useful when running under virtualenv

## ``pytest_shutil.run``: Running things in subprocesses

| function  | description
| --------- | -----------
| run                | run a command, with options for capturing output, checking return codes.
| run_as_main        | run a function as if it was the system entry point
| run_module_as_main | run a module as if it was the system entry point
| run_in_subprocess  | run a function in a subprocess
| run_with_coverage  | run a command with coverage enabled
