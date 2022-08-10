# Creating and updating profiles

Currently you can only create and update profiles interactively using the following commands:

```bash
{{ ydb-cli }} init
```

or

```bash
{{ ydb-cli }} config profile create [profile_name]
```

where `[profile_name]` is an optional name of the profile being created or updated.


The first step of the interactive scenario is different in the `init` and `profile create` commands:

{% list tabs %}

- Init

   Prints a list of existing profiles (if any) and prompts you to make a choice: Create a new or update the configuration of an existing profile:

   ```text
   Please choose profile to configure:
   [1] Create a new profile
   [2] test
   [3] local
   ```

   If no profiles exist or you select option `1` in the previous step, the name of a profile to create is requested:

   ```text
   Please enter name for a new profile:
   ```

   If you enter the name of an existing profile at this point, the {{ ydb-short-name }} CLI proceeds to updating its parameters as if an option with the name of this profile was selected at once.

- Profile Create

   If no profile name is specified in the command line, it is requested:
   ```text
   Please enter configuration profile name to create or re-configure:
   ```

{% endlist %}

Next, you'll be prompted to sequentially perform the following actions with each connection parameter that can be saved in the profile:

- Don't save
- Set a new value or Use <value>
- Use current value (this option is available when updating an existing profile)

## Example

Creating a new `mydb1` profile:

1. Run the command:

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
    [1] Use IAM token (iam-token) cloud.yandex.com/docs/iam/concepts/authorization/iam-token
    [2] Use OAuth token of a Yandex Passport user (yc-token) cloud.yandex.com/docs/iam/concepts/authorization/oauth-token
    [3] Use metadata service on a virtual machine (use-metadata-credentials) cloud.yandex.com/docs/compute/operations/vm-connect/auth-inside-vm
    [4] Use security account key file (sa-key-file) cloud.yandex.com/docs/iam/operations/iam-token/create-for-sa
    [5] Don't save authentication data for profile "mydb1"
   Please enter your numeric choice:
   ```

   If you are not sure what authentication mode to choose, use the recipe from [Authentication](../../../../getting_started/auth.md) under "Getting started".

   All the available authentication methods are described in [{#T}](../../../../concepts/auth.md). The set of methods and text of the hints may differ from those given in this example.

   If the method you choose involves specifying an additional parameter, you'll be prompted to enter it. For example, if you select `4` (Use service account key file):

   ```text
   Please enter Path to service account key file (sa-key-file):
   ```

1. In the last step, you'll be prompted to activate the created profile to be used by default. Choose 'n' (No) until you read the article about [Activating a profile and using the activated profile](../activate.md):

   ```text
   Activate profile "mydb1" to use by default? (current active profile is not set) y/n: n
   ```
