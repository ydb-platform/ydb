Create an [account](https://portal.azure.com/#home) in Azure and top up your [account](https://portal.azure.com/#view/Microsoft_Azure_GTM/ModernBillingMenuBlade/~/BillingAccounts) account with the amount , sufficient to operate nine VMs. You can calculate the approximate cost of maintaining infrastructure depending on the region and other circumstances using [calculator](https://azure.com/e/26977c150e854617a888fb3a7d1a399d).

Authentication to the Azure Provider for Terraform is done through the CLI. You can download, install and configure the Azure CLI by following [this](https://learn.microsoft.com/ru-ru/cli/azure/install-azure-cli) instructions. You can log in to Azure using the Azure CLI interactively with the `az login` command, and the easiest way to create a pair of SSH keys (Linux, macOS) is to use the `ssh-keygen` command.

After logging into Azure and generating SSH keys, you need to change the default value of the following variables in the root file `variables.tf`:

* `auth_location` – name of the region where the infrastructure will be deployed. A list of available regions depending on the subscription can be obtained with the command `az account list-locations | grep "displayName"`.
* `ssh_key_path` – path to the public part of the generated SSH key.

The values of the remaining variables can be left unchanged. While in the root directory of the Terraform script, run the `terraform init` command - the Terraform provider will be installed and the modules will be initialized. Then run the `terraform plan` command to create an infrastructure plan and then run the `terraform apply` command to create the infrastructure in the Azure cloud.