```text
├── 3-nodes-mirror-3-dc / 9-nodes-mirror-3-dc / 8-nodes-block-4-2
│   ├── ansible.cfg # An Ansible configuration file containing settings for connecting to servers and project structure options. It is essential for customizing Ansible's behavior and specifying default settings.
│   ├── ansible_vault_password_file # A file containing the password for decrypting encrypted data with Ansible Vault, such as sensitive variables or configuration details. This is crucial for securely managing secrets like the root user password.
│   ├── creds # A directory for environment variables that specify the username and password for YDB, facilitating secure access to the database.
│   ├── files
│   │   ├── config.yaml # A YDB configuration file, which contains settings for the database instances.
│   ├── inventory # A directory containing inventory files, which list and organize the servers Ansible will manage.
│   │   ├── 50-inventory.yaml # The main inventory file, specifying the hosts and groups for Ansible tasks.
│   │   └── 99-inventory-vault.yaml #  An encrypted inventory file storing sensitive information, such as the root user's password for YDB, using Ansible Vault.
├── README.md # A markdown file providing a description of the repository, including how to use it, prerequisites, and any other relevant information.
├── requirements.txt # A file listing Python package dependencies required for the virtual environment, ensuring all necessary tools and libraries are installed.
├── requirements.yaml # Specifies the Ansible collections needed, pointing to the latest versions or specific versions required for the project.
├── TLS #A directory intended for storing TLS (Transport Layer Security) certificates and keys for secure communication.
│   ├── ydb-ca-nodes.txt # Contains a list of Fully Qualified Domain Names (FQDNs) of the servers for which TLS certificates will be generated, ensuring secure connections to each node.
│   └── ydb-ca-update.sh # A script for generating TLS certificates from the ydb-ca-nodes.txt list, automating the process of securing communication within the cluster.
```
