 Register in the Google Cloud console and [create](https://console.cloud.google.com/projectselector2/home) a project. Activate your [payment account](https://console.cloud.google.com/billing/manage) and top it up with funds to launch nine VMs. You can estimate the approximate cost in [calculator](https://cloud.google.com/products/calculator).

Set up GCP CLI:

1. Activate [Compute Engine API](https://console.cloud.google.com/apis/api/compute.googleapis.com/metrics) and [Cloud DNS API](https://console.cloud.google. com/apis/api/dns.googleapis.com/metrics).
2. Download and install GCP CLI by following [these instructions](https://cloud.google.com/sdk/docs/install).
3. Go to the `.../google-cloud-sdk/bin` subdirectory and run the `./gcloud compute regions list` command to get a list of available regions.
4. Run the command `./gcloud auth application-default login` to configure the connection profile.

Go to the `gcp` subdirectory (located in the downloaded repository), and in the `variables.tf` file set the current values for the following variables:

1. `project` – the project's name that was set in the Google Cloud cloud console.
2. `region` – the region where the infrastructure will be deployed.
3. `zones` – list of availability zones in which subnets and VMs will be created.

Now, being in the `gcp` subdirectory, you can run the following sequence of commands to install the provider, initialize modules, and create the infrastructure:

1. `terraform init` – installing the provider and initializing modules.
2. `terraform plan` – creating a plan for future infrastructure.
3. `terraform init` – create resources in the cloud.

Next, use the commands `terraform plan`, `terraform init`, and `terraform destroy` (destruction of the created infrastructure) to apply further changes as necessary.