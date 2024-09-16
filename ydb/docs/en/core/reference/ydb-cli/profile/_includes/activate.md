# Activated profile

Executing {{ ydb-short-name }} CLI commands on a database require establishing a connection to the database. If the {{ ydb-short-name }} CLI couldn't identify a certain connection parameter by [command-line parameters and environment variables](../../connect.md), it's taken from the activated profile.

Profile activation is an easy way to get started with the {{ ydb-short-name }} CLI, since the connection parameters that are set once will be applied automatically to any command, without the need to specify any connection parameters in the command line.

However, this simplicity may lead to undesirable behavior in further operation, as soon as you need to work with multiple databases:

- The activated profile is applied implicitly, meaning that it can be applied by mistake when a certain connection parameter is missing in the command line.
- The activated profile is applied implicitly, meaning that it can be applied by mistake when a typo is made in the name of an environment variable.
- The activated profile cannot be used in scripts, since it is saved in a file and its change in one terminal window will affect all other windows, possibly leading to an unexpected change of the DB in the middle of the loop being executed in the script.

When you need to connect to any new database other than the initial one for the first time, we recommend that you deactivate the profile and always select it explicitly using the `--profile` option.

## Activating a profile with a command {#activate}

Profile activation is performed by running the command

```bash
{{ ydb-cli }} config profile activate [profile_name]
```

where `[profile_name]` is an optional profile name.

If the profile name is specified, it is activated. If a profile with the specified name does not exist, an error is returned prompting you to view the list of available profiles:

```text
No existing profile "<profile_name>". Run "ydb config profile list" without arguments to see existing profiles
```

If the profile name is not specified, you'll be asked to choose between the following options in interactive mode:

```text
Please choose profile to activate:
 [1] Don't do anything, just exit
 [2] Deactivate current active profile (if any)
 [3] <profile_name_1> (active)
 [4] <profile_name_2>
 ...
Please enter your numeric choice:
```

- `1` terminates the command execution and keeps the currently activated profile activated. It's marked as `(active)` in the list of existing profiles starting from item 3.
- `2` deactivates the currently activated profile. If no profile has been activated before, nothing changes.
- `3` and so on activates the selected profile. The currently activated profile is marked as `(active)`.

If the profile is successfully activated, the execution ends with a message saying

```text
Profile "<profile_name>" was activated.
```

### Example

Activating a profile named `mydb1`:

```bash
$ {{ ydb-cli }} config profile activate mydb1
Profile "mydb1" was activated.
```

## Activating a profile during its initialization {#init}

As the last step during the interactive execution of the [command to create or update](../create.md) the profile `{{ ydb-cli }} config profile create`, you're prompted to activate the created (or updated) profile:

```text
Activate profile "<profile_name>" to use by default? (current active profile is not set) y/n:
```

Choose `y` (Yes) to activate the profile.

## Deactivating a profile {#deactivate}

Currently, the {{ ydb-short-name }} CLI only supports profile deactivation in interactive mode, when calling the activation command without specifying the profile (choosing item `2` in the above [activation command](#activate)).

If necessary, you can use input redirection in your OS to automatically select option `2` in interactive input:

```bash
echo 2 | {{ ydb-cli }} config profile activate
```

The efficiency of this method is not guaranteed in any way.

