1. Register in the Google Cloud console and [create](https://console.cloud.google.com/projectselector2/home) a project.
2. Activate your [billing account](https://console.cloud.google.com/billing/manage) and fund it with enough money to launch nine VMs. You can estimate the cost with the [calculator](https://cloud.google.com/products/calculator).
3. Activate the [Compute Engine API](https://console.cloud.google.com/apis/api/compute.googleapis.com/metrics) and [Cloud DNS API](https://console.cloud.google.com/apis/api/dns.googleapis.com/metrics).
4. Download and install the GCP CLI by following [these instructions](https://cloud.google.com/sdk/docs/install).
5. Go to the subdirectory `.../google-cloud-sdk/bin` and execute the command `./gcloud compute regions list` to get a list of available regions.
6. Perform the command `./gcloud auth application-default login` to set up the connection profile.
7. Download the repository using the command `git clone https://github.com/ydb-platform/ydb-terraform`.
8. Navigate to the `gcp` subdirectory (located in the downloaded repository) and in the `variables.tf` file, set the actual values to the following variables:
    * `project` – the name of the project that was assigned in the Google Cloud console.
    * `region` – the region where the infrastructure will be deployed.
    * `zones` – the list of availability zones where subnets and VMs will be created.

Now, being in the `gcp` subdirectory, you can execute the following sequence of commands to install the provider, initialize modules, and create infrastructure:
* `terraform init` – installing the provider and initializing modules.
* `terraform plan` – creating a plan for the future infrastructure.
* `terraform init` (repeat execution) – creating resources in the cloud.

Subsequently, the commands `terraform plan`, `terraform init`, and `terraform destroy` (for destroying the created infrastructure) are used.
