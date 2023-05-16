# Overview

Deploying {{ ydb-short-name }} in {{ k8s }} is a simple and easy way to install {{ ydb-short-name }}. With {{ k8s }}, you can use a universal approach to managing your application in any cloud provider. This guide provides instructions on how to deploy {{ ydb-short-name }} in [{{ managed-k8s-full-name}}](yc_managed_kubernetes.md) and [AWS EKS](aws_eks.md).

{{ ydb-short-name }} is delivered as a Helm chart that is a package with templates of {{ k8s }} structures. You can deploy charts in the following environment:

1. The Helm package manager of version higher than 3.1.0 is installed.
1. {{ k8s }} version 1.20 or higher is used.
1. The kubectl command line tool is installed and cluster access is configured.
1. [Dynamic Volume Provisioning](https://kubernetes.io/docs/concepts/storage/dynamic-provisioning/) is supported in clusters.

For more information about Helm, see the [documentation]{% if lang == "en" %}(https://helm.sh/docs/){% endif %}{% if lang == "ru" %}(https://helm.sh/ru/docs/){% endif %}.

A Helm chart installs a controller in the {{ k8s }} cluster. It implements the logic required for deploying {{ ydb-short-name }} components. The controller is based on the [Operator](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/) pattern.

{{ydb-short-name}} consists of two components:

* _Storage nodes_: Provide the data storage layer.
* _Dynamic nodes_: Implement data access and processing.

To deploy each of the components, just create an appropriate resource: [Storage](https://github.com/ydb-platform/ydb-kubernetes-operator/tree/master/samples/storage-block-4-2.yaml) or [Database](https://github.com/ydb-platform/ydb-kubernetes-operator/tree/master/samples/database.yaml) with the desired parameters. To learn more about the resource schema, follow [this link](https://github.com/ydb-platform/ydb-kubernetes-operator/tree/master/deploy/ydb-operator/crds).

After the chart data is processed by the controller, the following resources are created:

* [StatefulSet](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/): A workload controller that assigns stable network IDs and disk resources to each container.
* [Service](https://kubernetes.io/docs/concepts/services-networking/service/): An object that is used to access the created databases from applications.
* [ConfigMap](https://kubernetes.io/docs/concepts/configuration/configmap/): An object that is used to store the cluster configuration.

See the controller's source code [here](https://github.com/ydb-platform/ydb-kubernetes-operator). The Helm chart is in the `deploy` directory.
{{ydb-short-name}} containers are deployed using `cr.yandex/yc/ydb` images. Currently, they are only available as prebuilt artifacts.

