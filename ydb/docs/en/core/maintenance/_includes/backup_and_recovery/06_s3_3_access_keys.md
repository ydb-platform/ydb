### Creating access keys {#s3_create_access_keys}

Access keys are used for authentication and authorization in S3-compatible storage. The [{{ ydb-short-name }} console client](../../../reference/ydb-cli/index.md) provides three ways to pass keys:

* Through the `--access-key` and `--secret-key` command line options.
* Using the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables.
* Through the `~/.aws/credentials` file created and used by the [AWS CLI]{% if lang == "en" %}(https://aws.amazon.com/cli/){% endif %}{% if lang == "ru" %}(https://aws.amazon.com/ru/cli/){% endif %}.

The settings apply in the order described above. For example, if you use all three ways to pass the access_key or secret_key value at the same time, the values passed through the command-line options will be used.
