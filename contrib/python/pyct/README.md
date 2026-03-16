# pyct

A utility package that includes:

  1. **pyct.cmd**: Makes various commands available to other
     packages. (Currently no sophisticated plugin system, just a try
     import/except in the other packages.) The same commands are
     available from within python. Can either add new subcommands to
     an existing argparse based command if the module has an existing
     command, or create the entire command if the module has no
     existing command. Currently, there are commands for copying
     examples and fetching data. See

  2. **pyct.build**: Provides various commands to help package
     building, primarily as a convenience for project maintainers.

## pyct.cmd

To install pyct with the dependencies required for pyct.cmd: `pip
install pyct[cmd]` or `conda install -c pyviz pyct`.

An example of how to use in a project:
https://github.com/holoviz/geoviews/blob/main/geoviews/__main__.py

Once added, users can copy the examples of a package and download the
required data with the `examples` command:

```
$ datashader examples --help
usage: datashader examples [-h] [--path PATH] [-v] [--force] [--use-test-data]

optional arguments:
  -h, --help       show this help message and exit
  --path PATH      location to place examples and data
  -v, --verbose
  --force          if PATH already exists, force overwrite existing examples
                   if older than source examples. ALSO force any existing data
                   files to be replaced
  --use-test-data  Use data's test files, if any, instead of fetching full
                   data. If test file not in '.data_stubs', fall back to
                   fetching full data.
```

To copy the examples of e.g. datashader but not download the data,
there's a `copy-examples` command:

```
usage: datashader copy-examples [-h] [--path PATH] [-v] [--force]

optional arguments:
  -h, --help     show this help message and exit
  --path PATH    where to copy examples
  -v, --verbose
  --force        if PATH already exists, force overwrite existing files if
                 older than source files
```

And to download the data only, the `fetch-data` command:

```
usage: datashader fetch-data [-h] [--path PATH] [--datasets DATASETS] [-v]
                        [--force] [--use-test-data]

optional arguments:
  -h, --help           show this help message and exit
  --path PATH          where to put data
  --datasets DATASETS  *name* of datasets file; must exist either in path
                       specified by --path or in package/examples/
  -v, --verbose
  --force              Force any existing data files to be replaced
  --use-test-data      Use data's test files, if any, instead of fetching full
                       data. If test file not in '.data_stubs', fall back to
                       fetching full data.
```

Can specify different 'datasets' file:

```
$ cat earthsim-examples/test.yml
---

data:

  - url: http://s3.amazonaws.com/datashader-data/Chesapeake_and_Delaware_Bays.zip
    title: 'Depth data for the Chesapeake and Delaware Bay region of the USA'
    files:
      - Chesapeake_and_Delaware_Bays.3dm

$ earthsim fetch-data --path earthsim-examples --datasets-filename test.yml
Downloading data defined in /tmp/earthsim-examples/test.yml to /tmp/earthsim-examples/data
Skipping Depth data for the Chesapeake and Delaware Bay region of the USA
```

Can use smaller files instead of large ones by using the `--use-test-data` flag
and placing a small file with the same name in `examples/data/.data_stubs`:

```
$ tree examples/data -a
examples/data
├── .data_stubs
│   └── nyc_taxi_wide.parq
└── diamonds.csv

$ cat examples/dataset.yml
data:

  - url: http://s3.amazonaws.com/datashader-data/nyc_taxi_wide.parq
    title: 'NYC Taxi Data'
    files:
      - nyc_taxi_wide.parq

  - url: http://s3.amazonaws.com/datashader-data/maccdc2012_graph.zip
    title: 'National CyberWatch Mid-Atlantic Collegiate Cyber Defense Competition'
    files:
      - maccdc2012_nodes.parq
      - maccdc2012_edges.parq
      - maccdc2012_full_nodes.parq
      - maccdc2012_full_edges.parq

$ pyviz fetch-data --path=examples --use-test-data
Fetching data defined in /tmp/pyviz/examples/datasets.yml and placing in /tmp/pyviz/examples/data
Copying test data file '/tmp/pyviz/examples/data/.data_stubs/nyc_taxi_wide.parq' to '/tmp/pyviz/examples/data/nyc_taxi_wide.parq'
No test file found for: /tmp/pyviz/examples/data/.data_stubs/maccdc2012_nodes.parq. Using regular file instead
Downloading National CyberWatch Mid-Atlantic Collegiate Cyber Defense Competition 1 of 1
[################################] 59/59 - 00:00:00
```

To clean up any potential test files masquerading as real data use `clean-data`:

```
usage: pyviz clean-data [-h] [--path PATH]

optional arguments:
  -h, --help   show this help message and exit
  --path PATH  where to clean data
```

## pyct.build

Currently provides a way to package examples with a project, by
copying an examples folder into the package directory whenever
setup.py is run. The way this works is likely to change in the near
future, but is provided here as the first step towards
unifying/simplifying the maintenance of a number of pyviz projects.

## pyct report

Provides a way to check the package versions in the current environment using:
  1. A console script (entry point): `pyct report [packages]`, or
  2. A python function: `import pyct; pyct.report(packages)`

The python function can be particularly useful for e.g. jupyter notebook users, since it is the packages in the current kernel that we usually care about (not those in the environment from which jupyter notebook server/lab was launched).

Note that `packages` above can include the name of any Python package (returning the `__version__`), along with the special cases `python` or `conda` (returning the version of the command-line tool) or `system` (returning the OS version).
