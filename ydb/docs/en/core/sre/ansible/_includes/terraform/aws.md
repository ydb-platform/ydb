Create an [account](https://console.aws.amazon.com) in AWS and add enough balance to run 9 VMs. You can calculate the approximate cost of maintaining infrastructure depending on the region and other circumstances using the [AWS calculator](https://calculator.aws/#/createCalculator/ec2-enhancement).

Create a user and connection key in AWS Cloud to run the AWS CLI:

* The user is created in the Security credentials → Access management → [Users](https://console.aws.amazon.com/iam/home#/users) → Create User section.
* The next step is to assign rights to the user. Select `AmazonEC2FullAccess`.
* After creating a user, go to it, open the `Security credentials` tab and click the **Create access key** button in the **Access keys** section.
* From the proposed options for sets of options, select `Command Line Interface`.
* Next, come up with a tag for marking the key and click the **Create access key** button.
* Copy the values of the `Access key` and `Secret access key` fields.

Install [AWS CLI](https://aws.amazon.com/cli/) and run the `aws configure` command. Enter the values of the `Access key` and `Secret access key` fields saved earlier. Edit the `~/.aws/credentials` and `~/.aws/config` files as follows:
* Add `[AWS_def_reg]` to `~/.aws/config` before `region = ...`.
* Add `[AWS]` before the connection key secret information.

Go to the `aws` directory in the downloaded repository and edit the following variables in the `variable.tf` file:
* `aws_region` – the region in which the infrastructure will be deployed.
* `aws_profile` – security profile name from the file `~/.aws/credentials`.
* `availability_zones` – list of region availability zones. It is formed from the name of the region and the serial letter. For example, for the `us-west-2` region the list of availability zones will look like this: `["us-west-2a", "us-west-2b", "us-west-2c"]`.

To create the infrastructure for the first time, you need to run the following commands:
* `terraform init` – installing the provider and initializing modules.
* `terraform plan` – creating a plan for future infrastructure.
* `terraform apply` (re-execute) - Create resources in the cloud.

Next, the commands `terraform plan`, `terraform init` and `terraform destroy` (destruction of the created infrastructure) are used.