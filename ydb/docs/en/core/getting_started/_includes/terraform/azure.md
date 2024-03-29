Create an [account](https://portal.azure.com/#home) in Azure and fund your [account balance](https://portal.azure.com/#view/Microsoft_Azure_GTM/ModernBillingMenuBlade/~/BillingAccounts) with an amount sufficient for the operation of nine VMs. You can calculate the approximate cost of maintaining the infrastructure depending on the region and other circumstances using the [calculator](https://azure.com/e/26977c150e854617a888fb3a7d1a399d).

Authentication in the Azure provider for Terraform is done through the CLI. Download, install, and configure the Azure CLI by following [these instructions](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli). You can authenticate in Azure using the Azure CLI in interactive mode with the `az login` command, and generating a pair of SSH keys (Linux, macOS) can be most easily done with the `ssh-keygen` command.

After authenticating in Azure and generating SSH keys, you need to change the default value of the following variables in the root file `variables.tf`:
* `auth_location` – the name of the region where the infrastructure will be deployed. You can get the list of available regions depending on the subscription with the command `az account list-locations | grep "displayName"`.
* `ssh_key_path` – the path to the public part of the generated SSH key.

Now, being in the `azure` subdirectory, you can perform the following sequence of commands to install the provider, initialize modules, and create infrastructure:
* `terraform init` – installing the provider and initializing modules.
* `terraform plan` – creating a future infrastructure plan.
* `terraform init` (repeat execution) – creating resources in the cloud.

The commands `terraform plan`, `terraform init`, and `terraform destroy` (for destroying the created infrastructure) are used thereafter.
