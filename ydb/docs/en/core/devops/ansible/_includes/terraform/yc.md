To create infrastructure in Yandex Cloud using Terraform, you need:

1. Prepare the cloud for work:
     * [Register](https://console.cloud.yandex.ru/) in Yandex Cloud.
     * [Connect](https://cloud.yandex.com/ru/docs/billing/concepts/billing-account) payment account.
     * [Make sure](https://console.cloud.yandex.ru/billing) that there are enough funds to create nine VMs.
2. Install and configure Yandex Cloud CLI:
     * [Download](https://cloud.yandex.ru/ru/docs/cli/quickstart) Yandex Cloud CLI.
     * [Create](https://cloud.yandex.ru/ru/docs/cli/quickstart#initialize) profile
3. [Create](https://cloud.yandex.com/ru/docs/tutorials/infrastructure-management/terraform-quickstart#get-credentials) service account using the CLI.
4. [Generate](https://cloud.yandex.ru/ru/docs/cli/operations/authentication/service-account#auth-as-sa) SA key in JSON format for connecting Terraform to the cloud using the CLI: `yc iam key create --service-account-name <acc name> --output <file name> --folder-id <cloud folder id>`. An SA key will be generated, and secret information will be output to the terminal:
     ```
     access_key:
         id: ajenhnhaqgd3vp...
         service_account_id: aje90em65r6922...
         created_at: "2024-03-05T20:10:50.0150..."
         key_id: YCAJElaLsa0z3snzH4E...
     secret: YCPKNJDVhRZgyywl4hQwVdcSRC...
     ```
     Copy `access_key.id` and `secret`. The values of these fields will be needed later when working with AWS CLI.
5. [Download](https://aws.amazon.com/ru/cli/) AWS CLI.
6. Set up the AWS CLI environment:
     * Run the `aws configure` command and sequentially enter the previously saved `access_key.id` and `secret`. For the region value, use `ru-central1`:
     ```
     aws configure
     AWS Access Key ID [None]: AKIAIOSFODNN********
     AWS Secret Access Key [None]: wJalr********/*******/bPxRfiCYEX********
     Default region name [None]: ru-central1
     Default output format [None]:
     ```
     The files `~/.aws/credentials` and `~/.aws/config` will be created.
7. Edit `~/.aws/credentials` and `~/.aws/config` as follows:
     * Add `[Ya_def_reg]` to `~/.aws/config` before `region = ru-central1-a`.
     * Add `[Yandex]` before the secret information about connection keys.
8. [Configure](https://cloud.yandex.com/ru/docs/tutorials/infrastructure-management/terraform-quickstart#configure-provider) Yandex Cloud Terraform provider.
9. Download this repository with the command `git clone https://github.com/ydb-platform/ydb-terraform.git`.
10. Go to the `yandex_cloud` directory in the downloaded repository and make changes to the following variables in the `variables.tf` file:
     * `key_path` – path to the SA key generated using the CLI.
     * `cloud_id` – cloud ID. You can get a list of available clouds with the command `yc resource-manager cloud list`.
     * `profile` – profile name from the file `~/.aws/config`.
     * `folder_id` – Cloud folder ID. Can be obtained with the command `yc resource-manager folder list`.

Now, being in the `yandex_cloud` subdirectory, you can run the following sequence of commands to install the provider, initialize modules, and create the infrastructure:

1. `terraform init` – installing the provider and initializing modules.
2. `terraform plan` – creating a plan for future infrastructure.
3. `terraform init` – create resources in the cloud.

Next, use the commands `terraform plan`, `terraform init`, and `terraform destroy` (destruction of the created infrastructure) to apply further changes as necessary.