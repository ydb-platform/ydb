To create infrastructure in Yandex Cloud using Terraform, you need:

1. Prepare the cloud for work:

     * [Register](https://console.yandex.cloud) in Yandex Cloud.
     * [Connect]({{ yandex_docs }}/billing/concepts/billing-account) payment account.
     * [Make sure](https://billing.yandex.cloud) that there are enough funds to create nine VMs.

2. Install and configure Yandex Cloud CLI:

     * [Download]({{ yandex_docs }}/cli/quickstart) Yandex Cloud CLI.
     * [Create]({{ yandex_docs }}/cli/quickstart#initialize) profile

3. [Create]({{ yandex_docs }}/tutorials/infrastructure-management/terraform-quickstart#get-credentials) service account using the CLI.
4. [Generate]({{ yandex_docs }}/cli/operations/authentication/service-account#auth-as-sa) Authorized key in JSON format for connecting Terraform to the cloud using the CLI: `yc iam key create --service-account-name <acc name> --output <file name> --folder-id <cloud folder id>`. Information about the created key will be displayed in the terminal:

     ```text
    id: ajenap572v8e1l...
    service_account_id: aje90em65r69...
    created_at: "2024-09-03T15:34:57.495126296Z"
    key_algorithm: RSA_2048
    ```

    The authorized key will be created in the directory where the command was executed.

5. [Configure]({{ yandex_docs }}/tutorials/infrastructure-management/terraform-quickstart#configure-provider) Yandex Cloud Terraform provider.
6. Download this repository with the command `git clone https://github.com/ydb-platform/ydb-terraform.git`.
7. Go to the `yandex_cloud` directory in the downloaded repository and make changes to the following variables in the `variables.tf` file:

     * `key_path` – path to the SA key generated using the CLI.
     * `cloud_id` – cloud ID. You can get a list of available clouds with the command `yc resource-manager cloud list`.
     * `folder_id` – Cloud folder ID. Can be obtained with the command `yc resource-manager folder list`.

Now, being in the `yandex_cloud` subdirectory, you can run the following sequence of commands to install the provider, initialize modules, and create the infrastructure:

1. `terraform init` – installing the provider and initializing modules.
1. `terraform plan` – creating a plan for future infrastructure.
1. `terraform init` – create resources in the cloud.

Next, use the commands `terraform plan`, `terraform init`, and `terraform destroy` (destruction of the created infrastructure) to apply further changes as necessary.
