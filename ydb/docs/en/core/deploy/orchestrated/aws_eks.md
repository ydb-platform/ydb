# Deploying {{ ydb-short-name }} in AWS EKS

To use [{{ k8s }}](https://kubernetes.io/) to create a cluster [{{ ydb-short-name }}]{% if lang == "en" %}(https://cloud.yandex.com/en/docs/ydb/){% endif %}{% if lang == "ru" %}(https://cloud.yandex.ru/docs/ydb/){% endif %}, follow the steps below.

## Before you start {#before-begin}

1. Configure `awscli` and `eksctl` to work with AWS resources according to the [documentation](https://docs.aws.amazon.com/eks/latest/userguide/getting-started-eksctl.html).

1. Install the {{ k8s }} CLI [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl).

1. Create a {{ k8s }} cluster.

    You can use an existing {{ k8s }} cluster or create a new one.

    {% note warning %}

    Make sure that you're using {{ k8s }} version 1.20 or higher.

    {% endnote %}

    {% cut "How to create {{ k8s }} cluster" %}

    {% list tabs %}

    - CLI

      ```bash
        eksctl create cluster \
          --name yandex-database \
          --nodegroup-name standard-workers \
          --node-type c5a.2xlarge \
          --nodes 3 \
          --nodes-min 1 \
          --nodes-max 4
      ```

    {% endlist %}

    A {{ k8s }} cluster named `yandex-database` is created. The `--node-type` flag indicates that the cluster is deployed using `c5a.2xlarge` (8vCPUs, 16 GiB RAM) instances. This meets our guidelines for running {{ ydb-short-name }}.

    It takes 10 to 15 minutes on average to create a cluster. Wait for the process to complete before proceeding to the next step of {{ ydb-short-name }} deployment. The kubectl configuration will be automatically updated to work with the cluster after it is created.

    {% endcut %}

1. Install the {{ k8s }} [Helm 3](https://helm.sh/docs/intro/install/) package manager.

1. Add a repository for {{ yandex-cloud }} to Helm:

    {% list tabs %}

    - CLI

      Run the command:

      ```bash
      helm repo add ydb https://charts.ydb.tech/
      ```
      * `ydb`: The repository alias.
      * `https://charts.ydb.tech/`: The repository URL.

      Output:

      ```text
      "ydb" has been added to your repositories
      ```

    {% endlist %}

1. Update the Helm chart index:

    {% list tabs %}

    - CLI

      Run the command:

      ```bash
      helm repo update
      ```

      Output:

      ```text
      Hang tight while we grab the latest from your chart repositories...
      ...Successfully got an update from the "ydb" chart repository
      Update Complete. ⎈Happy Helming!⎈
      ```

    {% endlist %}

{% include notitle [ydb-kubernetes-operator](_includes/ydb-kubernetes-operator.md) %}

