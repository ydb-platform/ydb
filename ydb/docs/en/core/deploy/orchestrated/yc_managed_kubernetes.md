# Deploying {{ ydb-short-name }} in {{ managed-k8s-name }}

To create a [{{ ydb-short-name }}](https://cloud.yandex.com/en/docs/ydb/) cluster using [{{ k8s }}](https://kubernetes.io/), follow these steps.

## Before you start {#before-begin}

1. Create a {{ k8s }} cluster.

    You can use an existing {{ k8s }} cluster or [create](https://cloud.yandex.com/en/docs/managed-kubernetes/operations/kubernetes-cluster/kubernetes-cluster-create) a new one.

    {% note warning %}

    Make sure that you're using {{ k8s }} version 1.20 or higher.

    {% endnote %}

1. Install the {{ k8s }} CLI [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl).

1. [Define](https://cloud.yandex.com/en/docs/managed-kubernetes/operations/kubernetes-cluster/kubernetes-cluster-get-credetials) a kubectl configuration.

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

