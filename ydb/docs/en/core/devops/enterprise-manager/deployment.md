# Running the Deployment {#deployment}

After completing all the preparatory steps, you can proceed with deploying {{ ydb-short-name }} Enterprise Manager.

## Automatic Deployment {#automatic}

Navigate to the `examples` directory and run the installation script:

```bash
cd examples
./install.sh
```

The `install.sh` script will perform all the necessary steps to deploy {{ ydb-short-name }} Enterprise Manager on the hosts specified in the inventory.

<!-- TODO: Describe what install.sh does specifically and what steps it performs -->
<!-- TODO: Describe the expected results of running the script -->

## Manual Deployment {#manual}

If you need more control over the installation process, you can run the Ansible playbook manually:

```bash
ansible-playbook ydb_platform.ydb_em.initial_setup
```

<!-- TODO: Describe available playbook parameters (extra vars, tags, etc.) -->
<!-- TODO: Describe the playbook execution stages -->
<!-- TODO: Describe how to verify a successful deployment -->

## Troubleshooting {#troubleshooting}

<!-- TODO: Describe common installation issues and their solutions -->
<!-- TODO: Describe logs and their locations -->

{% note info %}

If you encounter connection errors, check the following:
* The {{ ydb-short-name }} database endpoint is specified correctly.
* There is network connectivity between the control machine and the cluster hosts.
* The TLS certificates are valid.
* SSH access is available to all hosts in the inventory.

{% endnote %}
