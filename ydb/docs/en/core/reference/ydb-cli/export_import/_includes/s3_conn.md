# Connecting to S3-compatible object storage

The `export s3` and `import s3` commands for exporting data to and importing data from S3-compatible storage, respectively, use the same S3 connection and authentication parameters. For information about how to find out these parameters for some cloud providers, see the [Getting S3 connection parameters](#procure) section below.

## Connection {#conn}

To connect to S3, make sure to specify the endpoint and bucket:

`--s3-endpoint HOST`: S3 endpoint. `HOST`: Valid hostname such as `storage.yandexcloud.net`

`--bucket STR`: S3 bucket. `STR`: String with the bucket name.

## Authentication {#auth}

To establish a connection, except when importing data from a public bucket, you'll need to authenticate under an account with write (for import) or read (for export) permission granted for this bucket.

To authenticate in S3, the following two parameters are required:

- Access key ID (access_key_id)
- Secret access key (secret_access_key)

The YDB CLI takes these parameter values from the following sources (in order of priority):

1. The command line.
2. Environment variables.
3. The `~/.aws/credentials` file.

### Command line parameters

`--access-key STR`: Access key ID `--secret-key STR`: Secret access key

### Environment variables

If any authentication parameter is not specified in the command line, the YDB CLI tries to get it from the following environment variables:

`AWS_ACCESS_KEY_ID`: Access key ID `AWS_SECRET_ACCESS_KEY`: Secret access key

### AWS authentication file

If any authentication parameter is not specified in the command line and the YDB CLI couldn't fetch it from the environment variable, it tries to get it from the `~/.aws/credentials` file that is used for authentication in the [AWS CLI](https://aws.amazon.com/ru/cli/). You can create this file with the AWS CLI `aws configure` command.

## Getting S3 connection parameters {#procure}

### {{ yandex-cloud }}

Follow the instructions below to get [{{ yandex-cloud }} Object Storage](https://cloud.yandex.com/en-ru/docs/storage/) access keys using the {{ yandex-cloud }} CLI.

1. [Install and configure]{% if lang == "ru" %}(https://cloud.yandex.ru/docs/cli/quickstart){% endif %}{% if lang == "en" %}(https://cloud.yandex.com/docs/cli/quickstart){% endif %} the {{ yandex-cloud }} CLI.

2. Run the following command to get the ID of your folder in the cloud (you'll need to specify it in the commands below):

   ```bash
   yc config list
   ```

   In the command output, the cloud folder ID is in the `folder-id:` line:

   ```yaml
   folder-id: b2ge70qdcff4bo9q6t19
   ```

3. [Run the following command to create a service account](https://cloud.yandex.com/en-ru/docs/iam/operations/sa/create):

   ```bash
   yc iam service-account create --name s3account
   ```

   You can specify any account name except `s3account` or use an existing one. In this case, you'll also need to replace it when copying commands below via the clipboard.

3. [Run the following command to assign roles for the service account]{% if lang == "ru" %}(https://cloud.yandex.ru/docs/iam/operations/sa/assign-role-for-sa){% endif %}{% if lang == "en" %}(https://cloud.yandex.com/docs/iam/operations/sa/assign-role-for-sa){% endif %} roles according to the required S3 access level by running the command:

   {% list tabs %}

   - Read (to import data to the YDB database)

     ```bash
     yc resource-manager folder add-access-binding <folder-id> \
       --role storage.viewer --subject serviceAccount:s3account
     ```

   - Write (to export data from the YDB database)

     ```bash
     yc resource-manager folder add-access-binding <folder-id> \
       --role storage.editor --subject serviceAccount:s3account
     ```

   {% endlist %}

   , where `<folder-id>` is the cloud folder ID obtained in step 2.

   You can also view a [full list]{% if lang == "ru" %}(https://cloud.yandex.ru/docs/iam/concepts/access-control/roles#object-storage){% endif %}{% if lang == "en" %}(https://cloud.yandex.com/docs/iam/concepts/access-control/roles#object-storage){% endif %} {{ yandex-cloud }} roles.

4. Get [static access keys]{% if lang == "ru" %}(https://cloud.yandex.ru/docs/iam/operations/sa/create-access-key){% endif %}{% if lang == "en" %}(https://cloud.yandex.com/docs/iam/operations/sa/create-access-key){% endif %} by running the following command:

   ```bash
   yc iam access-key create --service-account-name s3account
   ```

   If successful, the command returns access_key attributes and the secret value:

   ```yaml
   access_key:
     id: aje6t3vsbj8lp9r4vk2u
     service_account_id: ajepg0mjt06siuj65usm
     created_at: "2018-11-22T14:37:51Z"
     key_id: 0n8X6WY6S24N7OjXQ0YQ
   secret: JyTRFdqw8t1kh2-OJNz4JX5ZTz9Dj1rI9hxtzMP1
   ```

   In this output:
   - `access_key.key_id` is the access key ID.
   - `secret` is the secret access key.

{% include [s3_conn_procure_overlay.md](s3_conn_procure_overlay.md) %}

