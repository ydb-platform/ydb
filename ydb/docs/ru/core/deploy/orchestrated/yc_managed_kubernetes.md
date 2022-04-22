# Развертывание {{ ydb-short-name }} в {{ managed-k8s-name }}

Чтобы с помощью [{{ k8s }}](https://kubernetes.io/) создать кластер [{{ ydb-short-name }}]{% if lang == "en" %}(https://cloud.yandex.com/en/docs/ydb/){% endif %}{% if lang == "ru" %}(https://cloud.yandex.ru/docs/ydb/){% endif %}, выполните следующие действия.

## Перед началом работы {#before-begin}

1. Создайте кластер {{ k8s }}.

    Вы можете использовать уже работающий кластер {{ k8s }} или [создать]{% if lang == "en" %}(https://cloud.yandex.com/en/docs/managed-kubernetes/operations/kubernetes-cluster/kubernetes-cluster-create){% endif %}{% if lang == "ru" %}(https://cloud.yandex.ru/docs/managed-kubernetes/operations/kubernetes-cluster/kubernetes-cluster-create){% endif %} новый.

    {% note warning %}

    Убедитесь, что используется версия {{ k8s }} 1.20 или выше.

    {% endnote %}

1. Установите {{ k8s }} CLI [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl).
1. [Настройте]{% if lang == "en" %}(https://cloud.yandex.com/en/docs/managed-kubernetes/operations/kubernetes-cluster/kubernetes-cluster-get-credetials){% endif %}{% if lang == "ru" %}(https://cloud.yandex.ru/docs/managed-kubernetes/operations/kubernetes-cluster/kubernetes-cluster-get-credetials){% endif %} конфигурацию kubectl.
1. Установите менеджер пакетов {{ k8s }} [Нelm 3](https://helm.sh/docs/intro/install/).
1. Склонируйте репозиторий с [ydb-kubernetes-operator](https://github.com/ydb-platform/ydb-kubernetes-operator)

      ```bash
      git clone https://github.com/ydb-platform/ydb-kubernetes-operator && cd ydb-kubernetes-operator
      ```
1. Добавьте в Helm репозиторий для {{ yandex-cloud }}:

    {% list tabs %}

    - CLI

      Выполните команду:

      ```bash
      helm repo add ydb https://charts.ydb.tech/
      ```

      * `ydb` — алиас репозитория
      * `https://charts.ydb.tech/` — URL репозитория.

      Результат выполнения:

      ```text
      "ydb" has been added to your repositories
      ```

    {% endlist %}

1. Обновите индекс чартов Helm:

    {% list tabs %}

    - CLI

      Выполните команду:

      ```bash
      helm repo update
      ```
    
      Результат выполнения:
    
      ```text
      Hang tight while we grab the latest from your chart repositories...
      ...Successfully got an update from the "ydb" chart repository
      Update Complete. ⎈Happy Helming!⎈
      ```

    {% endlist %}

{% include notitle [ydb-kubernetes-operator](_includes/ydb-kubernetes-operator.md) %}
