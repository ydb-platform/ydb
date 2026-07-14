# Getting an authentication token

Using the `auth get-token` subcommand, you can get an authentication token based on the authentication parameters specified in the profile, environment variables, or command-line parameters.

General command format:


```bash
{{ ydb-cli }} [global options...] auth get-token [options...]
```


* `global options` — [global parameters](commands/global-options.md).
* `options` — [subcommand parameters](#options).

See the description of the token retrieval command:


```bash
{{ ydb-cli }} auth get-token --help
```


## Subcommand parameters {#options}

| Parameter | Description |
| --- | --- |
| `-f, --force` | Output the token without a confirmation prompt. |
| `--timeout` | Client response timeout in milliseconds. After this time expires, there is no point in waiting for the result. |

## Examples {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

### Getting a token with confirmation {#with-prompt}

By default, the command asks for confirmation before outputting the token, because the token will be displayed in the console:


```bash
{{ ydb-cli }} -p quickstart auth get-token
```


Result:


```text
Caution: Your auth token will be printed to console. Use "--force" ("-f") option to print without prompting.
Do you want to proceed? (y/N): y
t1.eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...
```


### Getting a token without confirmation {#without-prompt}

For automation or use in scripts, use the `--force` option to output the token without a confirmation prompt:


```bash
{{ ydb-cli }} -p quickstart auth get-token --force
```


Result:


```text
t1.eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...
```


### Use in scripts {#in-scripts}

The command can be used to get a token in scripts:


```bash
TOKEN=$({{ ydb-cli }} -p quickstart auth get-token --force)
echo "Token: $TOKEN"
```
