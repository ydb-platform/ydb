1. [Role `packages`](https://github.com/ydb-platform/ydb-ansible/blob/main/roles/packages/tasks/main.yaml). Tasks:

* `check dpkg audit` – Verifies the [dpkg](https://en.wikipedia.org/wiki/Dpkg) state using the `dpkg --audit` command and saves the command results in the `dpkg_audit_result` variable. The task will terminate with an error if the `dpkg_audit_result.rc` command returns a value other than 0 or 1.
* `run the equivalent of "apt-get clean" as a separate step` – Cleans the apt cache, similarly to the `apt-get clean` command.
* `run the equivalent of "apt-get update" as a separate step` – Updates the apt cache, akin to the `apt-get update` command.
* `fix unconfigured packages` – Fixes packages that are not configured using the `dpkg --configure --pending` command.
* `set vars_for_distribution_version variables` – Sets variables for a specific Linux distribution version.
* `setup apt repositories` – Configures apt repositories from a specified list.
* `setup apt preferences` – Configures apt preferences (variable contents are specified in `roles/packages/vars/distributions/<distributive name>/<version>/main.yaml`).
* `setup apt configs`– Configures apt settings.
* `flush handlers` – Forcibly runs all accumulated handlers. In this context, it triggers a handler that updates the apt cache.
* `install packages` – Installs apt packages considering specified parameters and cache validity.

Links to the lists of packages that will be installed for Ubuntu 22.04 or Astra Linux 1.7:

* [List](https://github.com/ydb-platform/ydb-ansible/blob/main/roles/packages/vars/distributions/Ubuntu/22.04/main.yaml) of packages for Ubuntu 22.04;
* [List](https://github.com/ydb-platform/ydb-ansible/blob/main/roles/packages/vars/distributions/Astra%20Linux/1.7_x86-64/main.yaml) of packages for Astra Linux 1.7.

1. [Role `system`](https://github.com/ydb-platform/ydb-ansible/blob/main/roles/system/tasks/main.yaml). Tasks:

* `configure clock` – A block of tasks for setting up system clocks:

  + `assert required variables are defined` – Checks for the existence of the `system_timezone` variable. This check ensures that the necessary variable is available for the next task in the block.
  + `set system timezone` – Sets the system timezone. The timezone is determined by the value of the `system_timezone` variable, and the hardware clock (`hwclock`) is set to UTC. After completing the task, a notification is sent to restart the `cron` service.
  + `flush handlers` – Forces the execution of accumulated handlers using the `meta` directive. This will restart the following processes: `timesyncd`, `journald`, `cron`, `cpufrequtils`, and execute the `sysctl -p` command.
* `configure systemd-timesyncd` – A task block for configuring `systemd-timesyncd`:
  + `assert required variables are defined` asserts that the number of NTP servers (`system_ntp_servers`) is more than one if the variable `system_ntp_servers` is defined. If the variable `system_ntp_servers` is not defined, the execution of the `configure systemd-timesyncd` task block will be skipped, including the check for the number of NTP servers and the configuration of `systemd-timesyncd`.
  + `create conf.d directory for timesyncd` - Creates the `/etc/systemd/timesyncd.conf.d` directory if the `system_ntp_servers` variable is defined.
  + `configure systemd-timesyncd` - Creates a configuration file `/etc/systemd/timesyncd.conf.d/ydb.conf` for the `systemd-timesyncd` service with primary and backup NTP servers. The task is executed if the `system_ntp_servers` variable is defined. After completing the task, a notification is sent to restart the `timesyncd` service.
  + `flush handlers` - Calls accumulated handlers. Executes the handler `restart timesyncd`, which restarts the `systemd-timesyncd.service`.
  + `start timesyncd` - Starts and enables the `systemd-timesyncd.service`. Subsequently, the service will start automatically at system boot.

* `configure systemd-journald` – A block of tasks for configuring the `systemd-journald` service:

  + `create conf.d directory for journald` - Creates the `/etc/systemd/journald.conf.d` directory for storing `systemd-journald` configuration files.
  + `configure systemd-journald` - Creates a configuration file `/etc/systemd/journald.conf.d/ydb.conf` for `systemd-journald`, specifying a `Journal` section with the option `ForwardToWall=no`. The `ForwardToWall=no` setting in the `systemd-journald` configuration means that system log messages will not be forwarded as "wall" messages to all logged-in users. After completing the task, a notification is sent to restart the `journald` service.
  + `flush handlers` - Calls accumulated handlers. Executes the handler `restart journald`, which restarts the `systemd-journald` service.
  + `start journald` - Starts and enables the `systemd-journald.service`. Subsequently, the service will start automatically at system boot.

* `configure kernel` – A block of tasks for kernel configuration:

  + `configure /etc/modules-load.d dir` - Creates the `/etc/modules-load.d` directory with owner and group permissions for the root user and `0755` permissions.
  + `setup conntrack module` - Copies the `nf_conntrack` line into the file `/etc/modules-load.d/conntrack.conf` to load the `nf_conntrack` module at system start.
  + `load conntrack module` - Loads the `nf_conntrack` module in the current session.
  + `setup sysctl files` - Applies templates to create configuration files in `/etc/sysctl.d/` for various system settings (such as security, network, and filesystem settings). The list of files includes `10-console-messages.conf`, `10-link-restrictions.conf`, and others. After completing this task, a notification is sent to apply the kernel settings changes.
  + `flush handlers` - Calls accumulated handlers. Executes the handler `apply kernel settings`, which runs the `sysctl -p` command to apply the kernel parameters specified in `/etc/sysctl.conf` or in other files in the `/etc/sysctl.d/` directory.

* `configure cpu governor` – A block of tasks for configuring the CPU frequency management mode:

  + `install cpufrequtils` - Installs the `cpufrequtils` package from apt. The task is set with cache check parameters and a task timeout of 300 seconds to expedite task execution and avoid an infinite loop waiting for apt package updates.
  + `use performance cpu governor` - Creates the file `/etc/default/cpufrequtils` with content "GOVERNOR=performance", which sets the CPU governor mode to "performance" (disabling power-saving mode when CPU cores are idle). After completing the task, a notification is sent to restart the `cpufrequtils` service.
  + `disable ondemand.service` - Disables the `ondemand.service` if it is present in the system. The service is stopped, its automatic start is disabled, and it is masked (preventing its start). After completing the task, a notification is sent to restart cpufrequtils.
  + `flush handlers` - Calls accumulated handlers. Executes the handler `restart cpufrequtils`, which restarts the `cpufrequtils` service.
  + `start cpufrequtils` - Starts and enables the `cpufrequtils.service`. Subsequently, the service will start automatically at system boot.

1. [Role](https://github.com/ydb-platform/ydb-ansible/blob/main/roles/ydbd/tasks/main.yaml) `ydbd`. Tasks:

* `check if required variables are defined` – Checks that the variables `ydb_archive`, `ydb_config`, `ydb_tls_dir` are defined. If any of these are undefined, Ansible will display an appropriate error message and stop the playbook execution.
* `set vars_for_distribution variables` – Sets variables from the specified file in the `vars_for_distribution_file` variable during playbook execution. This task manages a set of variables dependent on the specific Linux distribution.
* `ensure libaio is installed` – Ensures that the `libaio` package is installed.
* `install custom libidn from archive` – Installs a custom version of the `libidn` library from an archive.
* `create certs group` – Creates a system group `certs`.
* `create ydb group` – Creates a system group `ydb`.
* `create ydb user` – Creates a system user `ydb` with a home directory.
* `install YDB server binary package from archive` – Installs {{ ydb-short-name }} from a downloaded archive.
* `create YDB audit directory` – Creates an `audit` subdirectory in the {{ ydb-short-name }} installation directory.
* `setup certificates` – A block of tasks for setting up security certificates:

  + `create YDB certs directory` – Creates a `certs` subdirectory in the {{ ydb-short-name }} installation directory.
  + `copy the TLS ca.crt` – Copies the root certificate `ca.crt` to the server.
  + `copy the TLS node.crt` – Copies the TLS certificate `node.crt` from the generated certificates directory.
  + `copy the TLS node.key` – Copies the TLS certificate `node.key` from the generated certificates directory.
  + `copy the TLS web.pem` – Copies the TLS pem key `web.pem` from the generated certificates directory.

* `copy configuration file` – Copies the configuration file `config.yaml` to the server.
* `add configuration file updater script` – Copies the `update_config_file.sh` script to the server.

1. [Role `ydbd_static`](https://github.com/ydb-platform/ydb-ansible/blob/main/roles/ydbd_static/tasks/main.yaml). Tasks:

* `check if required variables are defined` – Checks that the variables `ydb_cores_static`, `ydb_disks`, `ydb_domain`, `ydb_user` are defined. If any of these variables are undefined, the task will fail and an appropriate error message will be displayed for each undefined variable.
* `check if required secrets are defined` – Verifies that the secret variable `ydb_password` is defined. If this variable is undefined, the task will fail and an error message will be displayed.
* `create static node configuration file` – Creates a static node configuration file by running the copied `update_config_file.sh` script with `ydbd-config.yaml` and `ydbd-config-static.yaml` configurations.
* `create static node systemd unit` – Creates a `ydbd-storage.service` file for the static node based on a template. After completing the task, a notification is sent to restart the `systemd` service.
* `flush handlers` – Executes accumulated handlers. Restarts all `systemd` services.
* `format drives confirmation block` – A block of tasks for formatting disks and interrupting playbook execution in case the user declines confirmation. A confirmation request to format the connected disk will be displayed in the terminal. Response options: `yes` – to continue executing the playbook with disk formatting. Any other value will be interpreted as a refusal to format. By default, disks are formatted automatically without asking the user for permission, as the variables `ydb_allow_format_drives` and `ydb_skip_data_loss_confirmation_prompt` are set to `true`. If user confirmation is required, the value of the `ydb_skip_data_loss_confirmation_prompt` variable should be changed to `false` in the inventory file `50-inventory.yaml`.
* `prepare drives` – A task for formatting connected disks. Calls the `drive_prepare` plugin – a specially developed Ansible module for {{ ydb-short-name }} installation, which is part of the {{ ydb-short-name }} collection and is located in the directory `.../.ansible/collections/ansible_collections/ydb_platform/ydb/plugins/action/drive_prepare.py`. The module will format the connected disk specified in the `ydb_disks` variable if the `ydb_allow_format_drives` variable is set to `true`.
* `start storage node` – Starts the storage node process using `systemd`. If any errors occur during service startup, playbook execution will be interrupted.
* `get ydb token` – Requests a YDB token to perform the storage initialization command. The token is stored in the `ydb_credentials` variable. The task calls the `get_token` module from the directory `.../.ansible/collections/ansible_collections/ydb_platform/ydb/plugins/modules`. If any errors occur at this step, playbook execution will be interrupted.
* `wait for ydb discovery to start working locally` – Calls the `wait_discovery` module, which performs a `ListEndpoints` request to YDB to check the operability of the cluster's basic subsystems. If the subsystems are working properly, storage initialization commands and database creation can be executed.
* `init YDB storage if not initialized` – Initializes the storage if it has not already been created. The task calls the `init_storage` plugin, which performs the storage initialization command using a grpcs request to the static node on port 2135. The command result is stored in the `init_storage` variable.
* `wait for ydb healthcheck switch to "GOOD" status` – Waits for the YDB healthcheck system to switch to a `GOOD` status. The task calls the `wait_healthcheck` plugin, which performs a health check command on YDB.
* `set cluster root password` – Sets the password for the YDB root user. The task is executed by the `set_user_password` plugin, which performs a grpcs request to YDB and sets a pre-defined password for the YDB root user. The password is specified in the `ydb_password` variable in the inventory file `/examples/9-nodes-mirror-3-dc/inventory/99-inventory-vault.yaml` in an encrypted form.


1. [Role `ydbd_dynamic`](https://github.com/ydb-platform/ydb-ansible/blob/main/roles/ydbd_dynamic/tasks/main.yaml). Tasks:

* `check if required variables are defined` – Verifies the presence of required variables (`ydb_domain`, `ydb_pool_kind`, `ydb_cores_dynamic`, `ydb_brokers`, `ydb_dbname`, `ydb_dynnodes`) and displays an error if any variable is missing.
* `create dynamic node configuration file` – Creates a configuration file for dynamic nodes.
* `create dynamic node systemd unit` – Creates a systemd service for dynamic nodes. After completing the task, a notification is sent to restart the `systemd` service.
* `flush handlers` – Executes accumulated handlers. This will restart `systemd`.
* `start dynamic nodes` – Starts the process of dynamic nodes using `systemd`.
* `get ydb token` – Obtains a token for creating a database.
* `create YDB database` – Creates a database. The task is executed by the `create_database` plugin, which performs a request to 99-inventory-vault.yaml to create the database.
* `wait for ydb discovery to start working locally` – Calls the `wait_discovery` module again to check the operability of the cluster's basic subsystems.