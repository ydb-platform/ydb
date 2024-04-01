1. Register in the Google Cloud console and [create](https://console.cloud.google.com/projectselector2/home) a project.
3. Activate your [payment account](https://console.cloud.google.com/billing/manage) and top it up with funds to launch nine VMs. You can calculate the cost in [calculator](https://cloud.google.com/products/calculator).
4. Activate [Compute Engine API](https://console.cloud.google.com/apis/api/compute.googleapis.com/metrics) and [Cloud DNS API](https://console.cloud.google. com/apis/api/dns.googleapis.com/metrics).
5. Download and install GCP CLI by following [this](https://cloud.google.com/sdk/docs/install) instructions.
6. Go to the `.../google-cloud-sdk/bin` subdirectory and run the `./gcloud compute regions list` command to get a list of available regions.
7. Run the command `./gcloud auth application-default login` to configure the connection profile.
8. Download the repository using the `git clone https://github.com/ydb-platform/ydb-terraform` command.
9. Go to the `gcp` subdirectory (located in the downloaded repository) and in the `variables.tf` file set the current values for the following variables:
     * `project` – the name of the project that was set in the Google Cloud cloud console.
     * `region` – the region where the infrastructure will be deployed.
     * `zones` – list of availability zones in which subnets and VMs will be created.

Now, being in the `gcp` subdirectory, you can run the following sequence of commands to install the provider, initialize modules and create the infrastructure:
* `terraform init` – installing the provider and initializing modules.
* `terraform plan` – creating a plan for future infrastructure.
* `terraform init` (re-execute) – create resources in the cloud.

Next, the commands `terraform plan`, `terraform init` and `terraform destroy` (destruction of the created infrastructure) are used.