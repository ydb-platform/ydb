# Развертывание {{ ydb-short-name }} в AWS EKS

Чтобы с помощью [{{ k8s }}](https://kubernetes.io/) создать кластер [{{ ydb-short-name }}]{% if lang == "en" %}(https://cloud.yandex.com/en/docs/ydb/){% endif %}{% if lang == "ru" %}(https://cloud.yandex.ru/docs/ydb/){% endif %}, выполните следующие действия.

## Перед началом работы {#before-begin}

1. Настройте утилиты `awscli` и `eksctl` для работы с ресурсами AWS по [документации](https://docs.aws.amazon.com/eks/latest/userguide/getting-started-eksctl.html).
1. Установите {{ k8s }} CLI [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl).
1. Создайте кластер {{ k8s }}.

    Вы можете использовать уже работающий кластер {{ k8s }} или создать новый.

    {% note warning %}

    Убедитесь, что используется версия {{ k8s }} 1.20 или выше.

    {% endnote %}

    {% cut "Как создать кластер {{ k8s }}" %}

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

    Будет создан кластер {{ k8s }} с именем `yandex-database`. Флаг `--node-type` указывает, что кластер будет развернут с использованием инстансов `c5a.2xlarge` (8vCPUs, 16 GiB RAM), что соответствует нашим рекомендациям по запуску {{ ydb-short-name }}.

    Создание кластера занимает в среднем от 10 до 15 минут. Дождитесь завершения процесса перед переходом к следующим шагам развертывания {{ ydb-short-name }}. Конфигурация kubectl будет автоматически обновлена для работы с кластером после его создания.

    {% endcut %}

1. Установите менеджер пакетов {{ k8s }} [Нelm 3](https://helm.sh/docs/intro/install/).

1. Добавьте в Helm репозиторий для {{ yandex-cloud }}:

    {% list tabs %}

    - CLI

      Выполните команду:

      ```bash
      helm repo add ydb https://charts.ydb.tech/
      ```

      * `ydb` — алиас репозитория;
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
