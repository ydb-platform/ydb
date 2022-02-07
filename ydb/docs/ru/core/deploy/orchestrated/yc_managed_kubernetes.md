# Развертывание {{ ydb-name }} в {{ managed-k8s-name }}

Чтобы с помощью [{{ k8s }}](https://kubernetes.io/) создать кластер [{{ ydb-name }}](https://cloud.yandex.ru/docs/ydb/), выполните следующие действия.

## Перед началом работы {#before-begin}

1. Создайте кластер {{ k8s }}.

    Вы можете использовать уже работающий кластер {{ k8s }} или [создать](https://cloud.yandex.ru/docs/managed-kubernetes/operations/kubernetes-cluster/kubernetes-cluster-create) новый.

    {% note warning %}

    Убедитесь, что используется версия {{ k8s }} 1.20 или выше.

    {% endnote %}

1. Установите {{ k8s }} CLI [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl).
1. [Настройте](https://cloud.yandex.ru/docs/managed-kubernetes/operations/kubernetes-cluster/kubernetes-cluster-get-credetials) конфигурацию kubectl.
1. Установите менеджер пакетов {{ k8s }} [Нelm 3](https://helm.sh/docs/intro/install/).
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
