# Displaying connection parameters

`config info` is a service command for debugging various issues with [connection and authentication](../connect.md). The command displays the final connection parameters, such as the [endpoint](../../../concepts/connect.md#endpoint) and [database path](../../../concepts/connect.md#database), obtained by considering parameters from all possible sources. With these parameters, the CLI would connect when executing a command that implies a connection to the database. When specifying the [global option](global-options.md) `-v`, in addition to the final connection parameters, values of all connection parameters from all sources that the CLI managed to detect will be displayed, along with the names of those sources in order of priority

General format of the command:

```bash
{{ ydb-cli }} [global options...] config info
```

* `global options` — [global parameters](global-options.md).

## Connection parameter list {#parameters-list}

* [endpoint](../../../concepts/connect.md#endpoint) — URL of database cluster.
* [database](../../../concepts/connect.md#database) — Database path.
* **Authentication parameters:**
  * [token](../../../concepts/auth.md#iam) — Access Token.
  * [yc-token](../../../concepts/auth.md#iam) — Refresh Token.
  * [sa-key-file](../../../concepts/auth.md#iam) — Service Account Key.
  * [use-metadata-credentials](../../../concepts/auth.md#iam) — Metadata.
  * [user](../../../concepts/auth.md#static-credentials)
  * [password](../../../concepts/auth.md#static-credentials)
* [ca-file](../../../concepts/connect.md#tls-cert) — Root certificate.
* [iam-endpoint](../../../concepts/auth.md#iam) — URL of IAM service.

## Examples {#examples}

### Display final connection parameters {#basic-example}

```bash
$ ydb -e grpcs://another.endpoint:2135 --ca-file some_certs.crt -p db123 config info
endpoint: another.endpoint:2135
yc-token: SOME_A12****************21_TOKEN
iam-endpoint: iam.api.cloud.yandex.net
ca-file: some_certs.crt
```

### Display all connection parameters along with their sources {#verbose-example}

```bash
$ ydb -e grpcs://another.endpoint:2135 --ca-file some_certs.crt -p db123 -v config info
Using Yandex.Cloud Passport token from YC_TOKEN env variable

endpoint: another.endpoint:2135
yc-token: SOME_A12****************21_TOKEN
iam-endpoint: iam.api.cloud.yandex.net
ca-file: some_certs.crt
current auth method: yc-token

"ca-file" sources:
  1. Value: some_certs.crt. Got from: explicit --ca-file option

"database" sources:
  1. Value: /some/path. Got from: active profile "test_config_info"

"endpoint" sources:
  1. Value: another.endpoint:2135. Got from: explicit --endpoint option
  2. Value: db123.endpoint:2135. Got from: profile "db123" from explicit --profile option
  3. Value: some.endpoint:2135. Got from: active profile "test_config_info"

"iam-endpoint" sources:
  1. Value: iam.api.cloud.yandex.net. Got from: default value

"sa-key-file" sources:
  1. Value: /Users/username/some-sa-key-file. Got from: active profile "test_config_info"

"yc-token" sources:
  1. Value: SOME_A12****************21_TOKEN. Got from: YC_TOKEN enviroment variable
```
