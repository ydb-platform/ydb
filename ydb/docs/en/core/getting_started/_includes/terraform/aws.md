Create an [account](https://console.aws.amazon.com) in AWS and fund your balance with an amount sufficient for the operation of 9 VMs. You can calculate the approximate cost of maintaining the infrastructure depending on the region and other circumstances using the [calculator](https://calculator.aws/#/createCalculator/ec2-enhancement).

Create a user and a connection key in AWS Cloud for AWS CLI operations:
* A user is created in the section Security credentials → Access management → [Users](https://console.aws.amazon.com/iam/home#/users) → Create User.
* On the next step, assign rights to the user. Choose `AmazonEC2FullAccess`.
* After creating the user – go to it, open the `Security credentials` tab and press the **Create access key** button in the **Access keys** section.
* From the presented options of sets, choose `Command Line Interface`.
* Next, come up with a tag for marking the key and press the **Create access key** button.
* Copy the values of `Access key` and `Secret access key`.

Install [AWS CLI](https://aws.amazon.com/cli/) and execute the `aws configure` command. Enter the previously saved `Access key` and `Secret access key` values. Edit the files `~/.aws/credentials` and `~/.aws/config` as follows:
* Add `[AWS_def_reg]` in `~/.aws/config` before `region = ...`.
* Add `[AWS]` before the secret information about the connection keys.

Navigate to the `aws` directory in the downloaded repository and edit the following variables in the `variable.tf` file:
* `aws_region` – the region where the infrastructure will be deployed.
* `aws_profile` – the name of the security profile from the `~/.aws/credentials` file.
* `availability_zones` – the list of availability zones for the region. It is formed from the name of the region and the sequential letter. For example, for the `us-west-2` region, the list of availability zones would look like this: `["us-west-2a", "us-west-2b", "us-west-2c"]`.

Now, being in the `aws` subdirectory, you can perform the following sequence of commands to install the provider, initialize modules, and create infrastructure:
* `terraform init` – installing the provider and initializing modules.
* `terraform plan` – creating a future infrastructure plan.
* `terraform init` (repeat execution) – creating resources in the cloud.

The commands `terraform plan`, `terraform init`, and `terraform destroy` (for destroying the created infrastructure) are used thereafter.