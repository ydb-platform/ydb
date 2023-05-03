# Connecting to S3-compatible object storages

The commands used to export data from S3-compatible storages, `export s3` and `import s3`, use the same parameters for S3 connection and authentication. To learn how to get these parameters for certain cloud providers, see [Getting S3 connection parameters](#procure) below.

## Connecting {#conn}

To connect with S3, you need to specify an endpoint and bucket:

`--s3-endpoint HOST`: An S3 endpoint. `HOST`: A valid host name, such as: `storage.yandexcloud.net`

`--bucket STR`: An S3 bucket. `STR`: A string containing the bucket name

## Authentication {#auth}

Except when you import data from a public bucket, to connect, log in with an account that has write access to the bucket (for export to it) and read access to the bucket (for import from it).

You need two parameters to authenticate with S3:
- ID of the access key (access_key_id).
- Secret access key (secret_access_key).

The YDB CLI takes values of these parameters from the following sources (listed in descending priority):
1. The command line.
2. Environment variables.
3. The `~/.aws/credentials` file.

### Command line parameters

* `--access-key`: Access key ID.
* `--secret-key`: Secret access key.
* `--aws-profile`: Profile name from the `~/.aws/credentials` file. The default value is `default`.

### Environment variables

If a certain authentication parameter is omitted in the command line, the YDB CLI tries to retrieve it from the following environment variables:

* `AWS_ACCESS_KEY_ID`: Access key ID.
* `AWS_SECRET_ACCESS_KEY`: Secret access key.
* `AWS_PROFILE`: Profile name from the `~/.aws/credentials` file.

### AWS authentication file

If a certain authentication parameter is omitted in the command line and cannot be retrieved from an environment variable, the YDB CLI tries to get it from the specified profile or the default profile in the `~/.aws/credentials` file used for authenticating the [AWS CLI](https://aws.amazon.com/ru/cli/). You can create this file with the `aws configure` AWS CLI command.

## Getting the S3 connection parameters {#procure}

### {{ yandex-cloud }}

Below is an example of getting access keys for the [{{ yandex-cloud }} Object Storage]{% if lang == "ru" %}(https://cloud.yandex.ru/docs/storage/){% endif %}{% if lang == "en" %}(https://cloud.yandex.com/docs/storage/){% endif %} using the {{ yandex-cloud }} CLI.

1. [Install and set up]{% if lang == "ru" %}(https://cloud.yandex.ru/docs/cli/quickstart){% endif %}{% if lang == "en" %}(https://cloud.yandex.com/docs/cli/quickstart){% endif %} the {{ yandex-cloud }} CLI.

2. Use the following command to get the ID of your cloud folder (you'll need to add it to the below commands):

   ```bash
   yc config list
   ```

   The ID of your cloud folder is in the `folder-id:` line in the result:

   ```yaml
   folder-id: b2ge70qdcff4bo9q6t19
   ```


3. To [create a service account]{% if lang == "ru" %}(https://cloud.yandex.ru/docs/iam/operations/sa/create){% endif %}{% if lang == "en" %}(https://cloud.yandex.com/docs/iam/operations/sa/create){% endif %}, run the command:

   ```bash
   yc iam service-account create --name s3account
   ```

   You can indicate any account name except `s3account`, or use your existing account name (be sure to replace it when copying the commands below).

3. [Grant roles to your service account]{% if lang == "ru" %}(https://cloud.yandex.ru/docs/iam/operations/sa/assign-role-for-sa){% endif %}{% if lang == "en" %}(https://cloud.yandex.com/docs/iam/operations/sa/assign-role-for-sa){% endif %} according to your intended S3 access level by running the command:

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

   Where `<folder-id>` is the cloud folder ID that you retrieved at step 2.

   You can also read a [full list]{% if lang == "ru" %}(https://cloud.yandex.ru/docs/iam/concepts/access-control/roles#object-storage){% endif %}{% if lang == "en" %}(https://cloud.yandex.com/docs/iam/concepts/access-control/roles#object-storage){% endif %} of {{ yandex-cloud }} roles.

4. Get [static access keys]{% if lang == "ru" %}(https://cloud.yandex.ru/docs/iam/operations/sa/create-access-key){% endif %}{% if lang == "en" %}(https://cloud.yandex.com/docs/iam/operations/sa/create-access-key){% endif %} by running the command:

   ```bash
   yc iam access-key create --service-account-name s3account
   ```

   If successful, the command will return the access_key attributes and the secret value:

   ```yaml
   access_key:
     id: aje6t3vsbj8lp9r4vk2u
     service_account_id: ajepg0mjt06siuj65usm
     created_at: "2018-11-22T14:37:51Z"
     key_id: 0n8X6WY6S24N7OjXQ0YQ
   secret: JyTRFdqw8t1kh2-OJNz4JX5ZTz9Dj1rI9hxtzMP1
   ```

   In this result:
   - `access_key.key_id` is the access key ID
   - `secret` is the secret access key

{% include [s3_conn_procure_overlay.md](s3_conn_procure_overlay.md) %}
