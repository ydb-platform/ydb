Create an [account](https://portal.azure.com/#home) in Azure and top up your [account](https://portal.azure.com/#view/Microsoft_Azure_GTM/ModernBillingMenuBlade/~/BillingAccounts) account with the amount, sufficient to operate 9 VMs. You can estimate the approximate cost of maintaining infrastructure depending on the region and other circumstances using [calculator](https://azure.com/e/26977c150e854617a888fb3a7d1a399d).

Authentication to the Azure Provider for Terraform goes through the CLI:
1. You can download, install, and configure the Azure CLI by following [these instructions](https://learn.microsoft.com/ru-ru/cli/azure/install-azure-cli).
2. Log in using the Azure CLI interactively with the `az login` command.
3. The easiest way to create a pair of SSH keys (Linux, macOS) is to use the `ssh-keygen` command.

After logging into Azure and generating SSH keys, you need to change the default value of the following variables in the root file `variables.tf`:

1. `auth_location`—the name of the region where the infrastructure will be deployed. The command `az account list-locations | grep "displayName"` can obtain a list of available regions depending on the subscription.
2. `ssh_key_path` – path to the public part of the generated SSH key.

Now, being in the `azure` subdirectory, you can run the following sequence of commands to install the provider, initialize modules, and create the infrastructure:

1. `terraform init` – installing the provider and initializing modules.
2. `terraform plan` – creating a plan for future infrastructure.
3. `terraform init` – create resources in the cloud.

Next, use the commands `terraform plan`, `terraform init`, and `terraform destroy` (destruction of the created infrastructure) to apply further changes as necessary.