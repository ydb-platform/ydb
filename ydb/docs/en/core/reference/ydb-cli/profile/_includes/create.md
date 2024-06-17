# Creating and updating profiles

You can set connection parameter values for the profile being created or updated through the [command line](#cmdline) or request them [in interactive mode](#interactive) in the console.

## Command line {#cmdline}

To create or update a profile, the command line uses the `profile create`, `profile update`, and `profile replace` commands.

They only use the values entered directly on the command line without accessing environment variables or the activated profile.

### Profile create {#create}

`Profile create` creates a new profile with the specified parameter values:

```bash
{{ ydb-cli }} config profile create <profile_name> <connection_options>
```

Where:
- `<profile_name>` is the required profile name.
- `<connection options>` are [connection parameters](../../connect.md#command-line-pars) to be written to the profile. You need to specify at least one connection parameter; otherwise the command will run [in interactive mode](#interactive).

If a profile with the specified name exists, the command will return an error.

### Profile replace {#replace}

`Profile replace` creates or replaces a profile with the specified parameter values:

```bash
{{ ydb-cli }} config profile replace <profile_name> [connection_options]
```

Where:
- `<profile_name>` is the required profile name.
- `<connection options>` are optional [connection parameters](../../connect.md#command-line-pars) to be written to the profile.

If a profile with the specified name already exists, it will be overwritten with a new one with the passed-in parameters. If you specify no connection parameters, the profile will be empty once the command completes.

### Profile update {#update}

`Profile update` modifies the properties of an existing profile:

```bash
{{ ydb-cli }} config profile update <profile_name> [connection_options] [reset-options]
```

Where:
- `<profile_name>` is the required profile name.
- `<connection options>` are optional [connection parameters](../../connect.md#command-line-pars) to be written to the profile.
- `<reset options>` are optional settings for deleting parameters from an existing profile. Possible values:

   `--no-endpoint`: Delete an endpoint from the profile
   `--no-database`: Delete the database path from the profile
   `--no-auth`: Delete authentication information from the profile
   `--no-iam-endpoint`: Delete the IAM server URL

The profile will update with the parameters entered on the command line. Any properties not listed on the command line will remain unchanged.

### Examples {#create-cmdline-examples}

#### Creating a profile to connect to a test database {#quickstart}

To connect to a DB in a single-node {{ ydb-short-name }} cluster, you can use the `quickstart` profile:

```bash
ydb config profile create quickstart --endpoint grpc://localhost:2136 --database <path_database>
```

* `path_database`: Database path. Specify one of these values:

   * `/Root/test`: If you used an executable to deploy your cluster.
   * `/local`: If you deployed your cluster from a Docker image.

#### Creating a profile from previous connection settings {#cmdline-example-from-explicit}

Any command with explicit connection settings performing a YDB database transaction can be converted to a profile create command by moving connection properties from global options to options specific to the `config profile create` command.

For instance, if you successfully ran the `scheme Is` command with the following properties:

```bash
{{ydb-cli}} \
  -e grpcs://example.com:2135 -d /Root/somedatabase --sa-key-file ~/sa_key.json \
  scheme ls
```

You can create a profile to connect to the accessed database using the following command:

```bash
{{ydb-cli}} \
  config profile create db1 \
  -e grpcs://example.com:2135 -d /Root/somedatabase --sa-key-file ~/sa_key.json
```

You can now use much shorter syntax to re-write the original command:

```bash
{{ydb-cli}} -p db1 scheme ls
```

#### Profile to connect to a local database {#cmdline-example-local}

Creating/replacing a `local` profile to connect to a local {{ ydb-short-name }} database deployed using [quick start](../../../../quickstart.md):

```bash
{{ydb-cli}} config profile replace local --endpoint grpc://localhost:2136 --database /Root/local
```

Defining the login and password authentication method in the `local` profile:

```bash
{{ydb-cli}} config profile update local --user user1 --password-file ~/pwd.txt
```

## Interactive mode {#interactive}

You can use the commands below to create and update profiles in interactive mode:

```bash
{{ ydb-cli }} init
```

or

```bash
{{ ydb-cli }} config profile create [profile_name] [connection_options]
```

Where:
- `[profile_name]` is an optional name of the profile to create or update.
- `[connection_options]` are optional [connection settings](../../connect.md#command-line-pars) to write to the profile.

The `init` command always runs in interactive mode while `config profile create` launches in interactive mode unless you specify a profile name or none of the connection settings on the command line.

The interactive scenario starts differently for the `init` and the `profile create` commands:

{% list tabs %}

- Init

   1. Prints a list of existing profiles (if any) and prompts you to make a choice: Create a new or update the configuration of an existing profile:

      ```text
      Please choose profile to configure:
      [1] Create a new profile
      [2] test
      [3] local
      ```

   2. If no profiles exist or you select option `1` in the previous step, the name of a profile to create is requested:

      ```text
      Please enter name for a new profile:
      ```

   3. If you enter the name of an existing profile at this point, the {{ ydb-short-name }} CLI proceeds to updating its parameters as if an option with the name of this profile was selected at once.

- Profile Create

   If no profile name is specified on the command line, it is requested:
   ```text
   Please enter configuration profile name to create or re-configure:
   ```

{% endlist %}

Next, you'll be prompted to sequentially perform the following actions with each connection parameter that can be saved in the profile:

- Don't save
- Set a new value or Use <value>
- Use current value (this option is available when updating an existing profile)

### Example {#interactive-example}

Creating a new `mydb1` profile:

1. Run this command:

   ```bash
   {{ ydb-cli }} config profile create mydb1
   ```

1. Enter the [endpoint](../../../../concepts/connect.md#endpoint) or don't save this parameter for the profile:

   ```text
   Pick desired action to configure endpoint:
    [1] Set a new endpoint value
    [2] Don't save endpoint for profile "mydb1"
   Please enter your numeric choice:
   ```

1. Enter the [database name](../../../../concepts/connect.md#database) or don't save this parameter for the profile:

   ```text
   Pick desired action to configure database:
    [1] Set a new database value
    [2] Don't save database for profile "mydb1"
   Please enter your numeric choice:
   ```

1. Select the authentication mode or don't save this parameter for the profile:

   ```text
   Pick desired action to configure authentication method:
     [1] Use static credentials (user & password)
     [2] Use IAM token (iam-token) cloud.yandex.com/docs/iam/concepts/authorization/iam-token
     [3] Use OAuth token of a Yandex Passport user (yc-token). Doesn't work with federative accounts. cloud.yandex.com/docs/iam/concepts/authorization/oauth-token
     [4] Use metadata service on a virtual machine (use-metadata-credentials) cloud.yandex.com/docs/compute/operations/vm-connect/auth-inside-vm
     [5] Use service account key file (sa-key-file) cloud.yandex.com/docs/iam/operations/iam-token/create-for-sa
     [6] Set new access token (ydb-token)
     [7] Don't save authentication data for profile "mydb1"
   Please enter your numeric choice:
   ```

   All the available authentication methods are described in [{#T}](../../../../concepts/auth.md). The set of methods and text of the hints may differ from those given in this example.

   If the method you choose involves specifying an additional parameter, you'll be prompted to enter it. For example, if you select `4` (Use service account key file):

   ```text
   Please enter Path to service account key file (sa-key-file):
   ```

1. In the last step, you'll be prompted to activate the created profile to be used by default. Choose 'n' (No) until you read the article about [Activating a profile and using the activated profile](../activate.md):

   ```text
   Activate profile "mydb1" to use by default? (current active profile is not set) y/n: n
   ```
