![Crossplane Logo](https://raw.githubusercontent.com/nginxinc/crossplane/master/ext/crossplane-logo.png)
<h1 align="center">crossplane</h1>
<h3 align="center">Reliable and fast NGINX configuration file parser and builder</h3>

<p align="center">
<a href="https://github.com/nginxinc/crossplane/actions/workflows/crossplane-ci.yml"><img src="https://github.com/nginxinc/crossplane/actions/workflows/crossplane-ci.yml/badge.svg"></a>
<a href="https://github.com/nginxinc/crossplane/releases"><img src="https://img.shields.io/github/release/nginxinc/crossplane.svg"></a>
<a href="https://pypi.python.org/pypi/crossplane"><img src="https://img.shields.io/pypi/l/crossplane.svg"></a>
<a href="https://pypi.python.org/pypi/crossplane"><img src="https://img.shields.io/pypi/pyversions/crossplane.svg"></a>
</p>

  - [Install](#install)
  - [Command Line Interface](#command-line-interface)
      - [crossplane parse](#crossplane-parse)
      - [crossplane build](#crossplane-build)
      - [crossplane lex](#crossplane-lex)
      - [crossplane format](#crossplane-format)
      - [crossplane minify](#crossplane-minify)
  - [Python Module](#python-module)
      - [crossplane.parse()](#crossplaneparse)
      - [crossplane.build()](#crossplanebuild)
      - [crossplane.lex()](#crossplanelex)
  - [Other Languages](#other-languages)

## Install

You can install both the [Command Line
Interface](#command-line-interface) and [Python Module](#python-module)
via:

    pip install crossplane

## Command Line Interface

```
usage: crossplane <command> [options]

various operations for nginx config files

optional arguments:
  -h, --help            show this help message and exit
  -V, --version         show program's version number and exit

commands:
  parse                 parses a json payload for an nginx config
  build                 builds an nginx config from a json payload
  lex                   lexes tokens from an nginx config file
  minify                removes all whitespace from an nginx config
  format                formats an nginx config file
  help                  show help for commands
```

### crossplane parse

This command will take a path to a main NGINX config file as input, then
parse the entire config into the schema defined below, and dumps the
entire thing as a JSON payload.

```
usage: crossplane parse [-h] [-o OUT] [-i NUM] [--ignore DIRECTIVES]
                        [--no-catch] [--tb-onerror] [--single-file]
                        [--include-comments] [--strict]
                        filename

parses a json payload for an nginx config

positional arguments:
  filename              the nginx config file

optional arguments:
  -h, --help            show this help message and exit
  -o OUT, --out OUT     write output to a file
  -i NUM, --indent NUM  number of spaces to indent output
  --ignore DIRECTIVES   ignore directives (comma-separated)
  --no-catch            only collect first error in file
  --tb-onerror          include tracebacks in config errors
  --combine             use includes to create one single file
  --single-file         do not include other config files
  --include-comments    include comments in json
  --strict              raise errors for unknown directives
```

**Privacy and Security**

Since `crossplane` is usually used to create payloads that are sent to
different servers, it's important to keep security in mind. For that
reason, the `--ignore` option was added. It can be used to keep certain
sensitive directives out of the payload output entirely.

For example, we always use the equivalent of this flag in the [NGINX Amplify
Agent](https://github.com/nginxinc/nginx-amplify-agent/) out of respect
for our users'
    privacy:

    --ignore=auth_basic_user_file,secure_link_secret,ssl_certificate_key,ssl_client_certificate,ssl_password_file,ssl_stapling_file,ssl_trusted_certificate

#### Schema

**Response Object**

```js
{
    "status": String, // "ok" or "failed" if "errors" is not empty
    "errors": Array,  // aggregation of "errors" from Config objects
    "config": Array   // Array of Config objects
}
```

**Config Object**

```js
{
    "file": String,   // the full path of the config file
    "status": String, // "ok" or "failed" if errors is not empty array
    "errors": Array,  // Array of Error objects
    "parsed": Array   // Array of Directive objects
}
```

**Directive Object**

```js
{
    "directive": String, // the name of the directive
    "line": Number,      // integer line number the directive started on
    "args": Array,       // Array of String arguments
    "includes": Array,   // Array of integers (included iff this is an include directive)
    "block": Array       // Array of Directive Objects (included iff this is a block)
}
```

<div class="note">

<div class="admonition-title">

Note

</div>

If this is an `include` directive and the `--single-file` flag was not
used, an `"includes"` value will be used that holds an Array of indices
of the configs that are included by this directive.

If this is a block directive, a `"block"` value will be used that holds
an Array of more Directive Objects that define the block context.

</div>

**Error Object**

```js
{
    "file": String,     // the full path of the config file
    "line": Number,     // integer line number the directive that caused the error
    "error": String,    // the error message
    "callback": Object  // only included iff an "onerror" function was passed to parse()
}
```

<div class="note">

<div class="admonition-title">

Note

</div>

If the `--tb-onerror` flag was used by crossplane parse, `"callback"`
will contain a string that represents the traceback that the error
caused.

</div>

#### Example

The main NGINX config file is at `/etc/nginx/nginx.conf`:

```nginx
events {
    worker_connections 1024;
}

http {
    include conf.d/*.conf;
}
```

And this config file is at `/etc/nginx/conf.d/servers.conf`:

```nginx
server {
    listen 8080;
    location / {
        try_files 'foo bar' baz;
    }
}

server {
    listen 8081;
    location / {
        return 200 'success!';
    }
}
```

So then if you run this:

    crossplane parse --indent=4 /etc/nginx/nginx.conf

The prettified JSON output would look like this:

```js
{
    "status": "ok",
    "errors": [],
    "config": [
        {
            "file": "/etc/nginx/nginx.conf",
            "status": "ok",
            "errors": [],
            "parsed": [
                {
                    "directive": "events",
                    "line": 1,
                    "args": [],
                    "block": [
                        {
                            "directive": "worker_connections",
                            "line": 2,
                            "args": [
                                "1024"
                            ]
                        }
                    ]
                },
                {
                    "directive": "http",
                    "line": 5,
                    "args": [],
                    "block": [
                        {
                            "directive": "include",
                            "line": 6,
                            "args": [
                                "conf.d/*.conf"
                            ],
                            "includes": [
                                1
                            ]
                        }
                    ]
                }
            ]
        },
        {
            "file": "/etc/nginx/conf.d/servers.conf",
            "status": "ok",
            "errors": [],
            "parsed": [
                {
                    "directive": "server",
                    "line": 1,
                    "args": [],
                    "block": [
                        {
                            "directive": "listen",
                            "line": 2,
                            "args": [
                                "8080"
                            ]
                        },
                        {
                            "directive": "location",
                            "line": 3,
                            "args": [
                                "/"
                            ],
                            "block": [
                                {
                                    "directive": "try_files",
                                    "line": 4,
                                    "args": [
                                        "foo bar",
                                        "baz"
                                    ]
                                }
                            ]
                        }
                    ]
                },
                {
                    "directive": "server",
                    "line": 8,
                    "args": [],
                    "block": [
                        {
                            "directive": "listen",
                            "line": 9,
                            "args": [
                                "8081"
                            ]
                        },
                        {
                            "directive": "location",
                            "line": 10,
                            "args": [
                                "/"
                            ],
                            "block": [
                                {
                                    "directive": "return",
                                    "line": 11,
                                    "args": [
                                        "200",
                                        "success!"
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    ]
}
```

#### crossplane parse (advanced)

This tool uses two flags that can change how `crossplane` handles
errors.

The first, `--no-catch`, can be used if you'd prefer that crossplane
quit parsing after the first error it finds.

The second, `--tb-onerror`, will add a `"callback"` key to all error
objects in the JSON output, each containing a string representation of
the traceback that would have been raised by the parser if the exception
had not been caught. This can be useful for logging purposes.

### crossplane build

This command will take a path to a file as input. The file should
contain a JSON representation of an NGINX config that has the structure
defined above. Saving and using the output from `crossplane parse` to
rebuild your config files should not cause any differences in content
except for the formatting.

```
usage: crossplane build [-h] [-d PATH] [-f] [-i NUM | -t] [--no-headers]
                        [--stdout] [-v]
                        filename

builds an nginx config from a json payload

positional arguments:
  filename              the file with the config payload

optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose         verbose output
  -d PATH, --dir PATH   the base directory to build in
  -f, --force           overwrite existing files
  -i NUM, --indent NUM  number of spaces to indent output
  -t, --tabs            indent with tabs instead of spaces
  --no-headers          do not write header to configs
  --stdout              write configs to stdout instead
```

### crossplane lex

This command takes an NGINX config file, splits it into tokens by
removing whitespace and comments, and dumps the list of tokens as a JSON
array.

```
usage: crossplane lex [-h] [-o OUT] [-i NUM] [-n] filename

lexes tokens from an nginx config file

positional arguments:
  filename              the nginx config file

optional arguments:
  -h, --help            show this help message and exit
  -o OUT, --out OUT     write output to a file
  -i NUM, --indent NUM  number of spaces to indent output
  -n, --line-numbers    include line numbers in json payload
```

#### Example

Passing in this NGINX config file at `/etc/nginx/nginx.conf`:

```nginx
events {
    worker_connections 1024;
}

http {
    include conf.d/*.conf;
}
```

By running:

    crossplane lex /etc/nginx/nginx.conf

Will result in this JSON
output:

```js
["events","{","worker_connections","1024",";","}","http","{","include","conf.d/*.conf",";","}"]
```

However, if you decide to use the `--line-numbers` flag, your output
will look
like:

```js
[["events",1],["{",1],["worker_connections",2],["1024",2],[";",2],["}",3],["http",5],["{",5],["include",6],["conf.d/*.conf",6],[";",6],["}",7]]
```

### crossplane format

This is a quick and dirty tool that uses [crossplane
parse](#crossplane-parse) internally to format an NGINX config file.
It serves the purpose of demonstrating what you can do with `crossplane`'s
parsing abilities. It is not meant to be a fully fleshed out, feature-rich
formatting tool. If that is what you are looking for, then you may want to
look writing your own using crossplane's Python API.

```
usage: crossplane format [-h] [-o OUT] [-i NUM | -t] filename

formats an nginx config file

positional arguments:
  filename              the nginx config file

optional arguments:
  -h, --help            show this help message and exit
  -o OUT, --out OUT     write output to a file
  -i NUM, --indent NUM  number of spaces to indent output
  -t, --tabs            indent with tabs instead of spaces
```

### crossplane minify

This is a simple and fun little tool that uses [crossplane
lex](#crossplane-lex) internally to remove as much whitespace from an
NGINX config file as possible without affecting what it does. It can't
imagine it will have much of a use to most people, but it demonstrates
the kinds of things you can do with `crossplane`'s lexing abilities.

```
usage: crossplane minify [-h] [-o OUT] filename

removes all whitespace from an nginx config

positional arguments:
  filename           the nginx config file

optional arguments:
  -h, --help         show this help message and exit
  -o OUT, --out OUT  write output to a file
```

## Python Module

In addition to the command line tool, you can import `crossplane` as a
python module. There are two basic functions that the module will
provide you: `parse` and `lex`.

### crossplane.parse()

```python
import crossplane
payload = crossplane.parse('/etc/nginx/nginx.conf')
```

This will return the same payload as described in the [crossplane
parse](#crossplane-parse) section, except it will be Python dicts and
not one giant JSON string.

### crossplane.build()

```python
import crossplane
config = crossplane.build(
    [{
        "directive": "events",
        "args": [],
        "block": [{
            "directive": "worker_connections",
            "args": ["1024"]
        }]
    }]
)
```

This will return a single string that contains an entire NGINX config
file.

### crossplane.lex()

```python
import crossplane
tokens = crossplane.lex('/etc/nginx/nginx.conf')
```

`crossplane.lex` generates 2-tuples. Inserting these pairs into a list
will result in a long list similar to what you can see in the
[crossplane lex](#crossplane-lex) section when the `--line-numbers` flag
is used, except it will obviously be a Python list of tuples and not one
giant JSON string.

## Other Languages

- Go port by [@aluttik](https://github.com/aluttik):
    <https://github.com/aluttik/go-crossplane>
- Ruby port by [@gdanko](https://github.com/gdanko):
    <https://github.com/gdanko/crossplane>
