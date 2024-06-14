# Начало работы с {{ ydb-short-name }} в {{ k8s }}

Развертывание {{ ydb-short-name }} в {{ k8s }} — это простой способ установки и эксплуатации {{ ydb-short-name }} кластера. С {{ k8s }} вы можете использовать универсальный подход к управлению приложением в любом облачном провайдере. Это руководство содержит инструкции для развертывания {{ ydb-short-name }} в [AWS EKS](https://aws.amazon.com/ru/eks/) or [{{ managed-k8s-full-name}}](https://cloud.yandex.ru/services/managed-kubernetes).

## Пререквизиты

{{ ydb-short-name }} поставляется в виде [Helm](https://helm.sh/)-чарта — пакета, который содержит шаблоны структур {{ k8s }}. Подробнее о Helm читайте в [документации](https://helm.sh/docs/). Чарт {{ ydb-short-name }} может быть развернут в следующем окружении:

1. Кластер {{ k8s }} версии 1.20 и старше. Он должен поддерживать динамическое предоставление томов ([Dynamic Volume Provisioning](https://kubernetes.io/docs/concepts/storage/dynamic-provisioning/)). Следуйте иструкциям ниже, если у вас ещё не настроен подходящий кластер.
2. [Установлена](https://kubernetes.io/docs/tasks/tools/install-kubectl) утилита kubectl и настроен доступ к кластеру.
3. [Установлен](https://helm.sh/docs/intro/install/) менеджер пакетов Helm версии старше 3.1.0.

{% include [_includes/storage-device-requirements.md](../../_includes/storage-device-requirements.md) %}

### Создание {{ k8s }} кластера

Пропустите этот раздел, если у вас уже имеется подходящий {{ k8s }} 

{% list tabs %}

- AWS EKS

  1. Настройте утилиты `awscli` и `eksctl` для работы с ресурсами AWS по [документации](https://docs.aws.amazon.com/eks/latest/userguide/getting-started-eksctl.html).
  
  2. Настройте `kubectl` для работы с кластером {{ k8s }}.

  3. Выполните следующую команду:

      ```bash
        eksctl create cluster \
          --name ydb \
          --nodegroup-name standard-workers \
          --node-type c5a.2xlarge \
          --nodes 3 \
          --nodes-min 1 \
          --nodes-max 4
      ```

      Это команда создаст {{ k8s }} кластер с именем `ydb`. Флаг `--node-type` указывает, что кластер будет развернут с использованием инстансов `c5a.2xlarge` (8vCPUs, 16 GiB RAM), что соответствует рекомендациям по запуску {{ ydb-short-name }}.

      Создание {{ k8s }} кластера занимает в среднем от 10 до 15 минут. Дождитесь завершения процесса перед переходом к следующим шагам развертывания {{ ydb-short-name }}. Конфигурация `kubectl` будет автоматически обновлена для работы с кластером после его создания.


- {{ managed-k8s-full-name }}

  1. Создайте кластер {{ k8s }}.

      Вы можете использовать уже работающий кластер {{ k8s }} или создать новый.

      {% cut "Как создать кластер {{ k8s }} в {{ managed-k8s-full-name }}" %}

      Следуйте инструкциям по [началу работы с {{ managed-k8s-full-name }}](https://cloud.yandex.ru/docs/managed-kubernetes/quickstart).

      {% endcut %}

{% endlist %}

## Обзор {{ ydb-short-name }} Helm-чарта

Helm-чарт устанавливает [YDB Kubernetes Operator](https://github.com/ydb-platform/ydb-kubernetes-operator) в {{ k8s }} кластер. Он представляет собой контроллер, построенный по паттерну [Оператор](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/). Он реализует необходимую логику для развертывания и управления компонентами {{ ydb-short-name }}.

Кластер {{ ydb-short-name }} состоит из двух видов узлов:

* **Storage nodes** (ресурс [Storage](https://github.com/ydb-platform/ydb-kubernetes-operator/tree/master/samples/storage-block-4-2.yaml)) — обеспечивают слой хранения данных;
* **Dynamic nodes** (ресурс [Database](https://github.com/ydb-platform/ydb-kubernetes-operator/tree/master/samples/database.yaml)) — реализуют доступ к данным и их обработку.

Для развертывания кластера {{ ydb-short-name }} в {{ k8s }} необходимо создать оба эти ресурса. Этот процесс будет рассмотрен подробнее ниже. Схема этих ресурсов [располагается на GitHub](https://github.com/ydb-platform/ydb-kubernetes-operator/tree/master/deploy/ydb-operator/crds).

После обработки чарта контроллером будут созданы следующие ресурсы:

* [StatefulSet](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/) — контроллер рабочей нагрузки, который предоставляет предсказуемые сетевые имена и дисковые ресурсы для каждого контейнера.
* [Service](https://kubernetes.io/docs/concepts/services-networking/service/) для доступа к созданным базам данных из приложений.
* [ConfigMap](https://kubernetes.io/docs/concepts/configuration/configmap/) для хранения конфигурации кластера.

Ознакомиться с исходным кодом оператора можно [на GitHub](https://github.com/ydb-platform/ydb-kubernetes-operator), Helm-чарт расположен в папке [deploy](https://github.com/ydb-platform/ydb-kubernetes-operator/tree/master/deploy).
При разворачивании контейнеров {{ ydb-short-name }} используются образы `cr.yandex/yc/ydb`, на данный момент доступные только как предсобранные артефакты.

## Подготовка рабочего окружения

1. Склонируйте репозиторий с [ydb-kubernetes-operator](https://github.com/ydb-platform/ydb-kubernetes-operator):

    ```bash
    git clone https://github.com/ydb-platform/ydb-kubernetes-operator && cd ydb-kubernetes-operator
    ```

2. Добавьте в Helm репозиторий для {{ ydb-short-name }}:

    Выполните команду:

    ```bash
    helm repo add ydb https://charts.ydb.tech/
    ```

    * `ydb` — алиас репозитория;
    * `https://charts.ydb.tech/` — URL репозитория {{ ydb-short-name }}.

    Результат выполнения:

    ```text
    "ydb" has been added to your repositories
    ```

3. Обновите индекс чартов Helm:

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


## Развертывание кластера {{ ydb-short-name }}

### Установите {{ ydb-short-name }} {{ k8s }} оператор

Разверните {{ ydb-short-name }} {{ k8s }} оператор на кластере с помощью `helm`:

  Выполните команду:

  ```bash
  helm install ydb-operator ydb/ydb-operator
  ```

  * `ydb-operator` — имя установки;
  * `ydb/ydb-operator` — название чарта в добавленном ранее репозитории.

  Результат выполнения:

  ```text
  NAME: ydb-operator
  LAST DEPLOYED: Thu Aug 12 19:32:28 2021
  NAMESPACE: default
  STATUS: deployed
  REVISION: 1
  TEST SUITE: None
  ```


### Разверните узлы хранения

{{ ydb-short-name }} поддерживает различные [топологии хранения данных](../../concepts/topology.md). {{ ydb-short-name }} {{ k8s }} оператор имеет несколько примеров конфигураций для типовых топологий. В данной инструкции они используются без изменений, но их можно подстраивать под свои нужды или написать новый конфигурационный файл с нуля.

Примените манифест для создания узлов хранения:

{% list tabs %}

- block-4-2

  ```bash
  kubectl apply -f samples/storage-block-4-2.yaml
  ```

  Это создаст 8 узлов хранения {{ ydb-short-name }}, сохраняющих данные с использованием erasure coding. В этом случае используется лишь +50% дополнительного дискового пространства для обеспечения отказоустойчивости.

- mirror-3-dc

  ```bash
  kubectl apply -f samples/storage-mirror-3-dc.yaml
  ```

  Это создаст 9 узлов хранения {{ ydb-short-name }}, которые сохраняют данные с фактором репликации 3.

{% endlist %}

Эта команда создаст объект `StatefulSet`, который описывает набор контейнеров с предсказуемыми сетевыми именами и закрепленными за ними дисками, а также необходимые для работы кластера объекты `Service` и `ConfigMap`.

Узлам хранения {{ ydb-short-name }} требуется время для инициализации. За прогрессом инициализации можно следить с помощью команд `kubectl get storages.ydb.tech` или `kubectl describe storages.ydb.tech`. Дождитесь пока статус ресурса `Storage` станет `Ready`.

{% note warning %}

Конфигурация кластера статична, контроллер не будет обрабатывать изменения при повторном применении манифеста. Изменение таких параметров кластера, как версия или размер дисков, возможно только через пересоздание кластера.

{% endnote %}

### Создайте базу данных и динамические узлы {#create-database}

{{ ydb-short-name }} база данных представляет собой логическую сущность, обслуживаемую динамическими узлами. Пример конфигурационного файла, который поставляется с {{ ydb-short-name }} {{ k8s }} оператором, создаёт базу даннух с именем `database-sample` и 3 динамическими узлами. Как и в случае с узлами хранения, конфигурацию можно изменять под свои нужды.

Примените манифест для создания базы данных и динамических узлов:

```bash
kubectl apply -f samples/database.yaml
```

{% note info %}

Значение по ключу `.spec.storageClusterRef.name` должно совпадать с именем ресурса `Storage` с узлами хранения.

{% endnote %}

После обработки манифеста будет создан объект `StatefulSet`, который описывает набор динамических нод. Созданная база данных будет доступна изнутри {{ k8s }} кластера по имени `database-sample`, или по FQDN-имени `database-sample.<namespace>.svc.cluster.local`, где `namespace` — название пространства имен, использовавшегося при установке. Подключиться к базе данных можно по порту 2135.

Посмотрите статус созданного ресурса:

```bash
kubectl describe database.ydb.tech
```

Результат выполнения команды:

```text
Name:         database-sample
Namespace:    default
Labels:       <none>
Annotations:  <none>
API Version:  ydb.tech/v1alpha1
Kind:         Database
...
Status:
  State:  Ready
Events:
  Type     Reason              Age    From          Message
  ----     ------              ----   ----          -------
  Normal   Provisioning        8m10s  ydb-operator  Resource sync is in progress
  Normal   Provisioning        8m9s   ydb-operator  Resource sync complete
  Normal   TenantInitialized   8m9s   ydb-operator  Tenant /root/database-sample created
```

`State: Ready` означает, что база данных готова к работе.


### Проверьте работу кластера

Проверьте работоспособность {{ ydb-short-name }}:

1. Проверьте, что все узлы в состоянии `Running`:

    ```bash
    kubectl get pods
    ```

    Результат:

    ```
    NAME                READY   STATUS    RESTARTS   AGE
    database-sample-0   1/1     Running   0          1m
    database-sample-1   1/1     Running   0          1m
    database-sample-2   1/1     Running   0          1m
    database-sample-3   1/1     Running   0          1m
    database-sample-4   1/1     Running   0          1m
    database-sample-5   1/1     Running   0          1m
    storage-sample-0    1/1     Running   0          1m
    storage-sample-1    1/1     Running   0          1m
    storage-sample-2    1/1     Running   0          1m
    storage-sample-3    1/1     Running   0          1m
    storage-sample-4    1/1     Running   0          1m
    storage-sample-5    1/1     Running   0          1m
    storage-sample-6    1/1     Running   0          1m
    storage-sample-7    1/1     Running   0          1m
    storage-sample-8    1/1     Running   0          1m
    ```

2. Запустите новый под с [{{ ydb-short-name }} CLI](../../reference/ydb-cli/index.md):

    ```bash
    kubectl run -it --image=cr.yandex/crptqonuodf51kdj7a7d/ydb:22.4.44 --rm ydb-cli bash
    ```

3. Выполните запрос к базе данных {{ ydb-short-name }}:

    ```bash
    ydb \
      --endpoint grpc://database-sample-grpc:2135 \
      --database /root/database-sample \
      table query execute --query 'SELECT 2 + 2;'
    ```

    * `--endpoint` — эндпоинт базы данных;
    * `--database` — имя созданной базы данных;
    * `--query` — текст запроса.

    Результат:

    ```text
    ┌─────────┐
    | column0 |
    ├─────────┤
    | 4       |
    └─────────┘
    ```


## Дальнейшие шаги

После проверки, что созданный кластер {{ ydb-short-name }} работает нормально, его можно использовать в соответствии с вашими задачами. Например, если вы хотите просто продолжить экспериментировать, можете использовать этот кластер, чтобы пройти [YQL туториал](../../dev/yql-tutorial/index.md).

Также, ниже описаны ещё несколько аспектов, которые можно учесть далее.

### Мониторинг

{{ ydb-short-name }} предоставляет стандартные механизмы сбора логов и метрик. Логирование осуществляется в стандартные каналы `stdout` и `stderr` и может быть перенаправлено при помощи популярных решений. Например, можно использовать комбинацию из [Fluentd](https://www.fluentd.org/) и [Elastic Stack](https://www.elastic.co/elastic-stack/).

Для сбора метрик `ydb-controller` предоставляет ресурсы типа `ServiceMonitor`, которые могут быть обработаны с помощью [kube-prometheus-stack](https://github.com/prometheus-community/helm-charts/tree/main/charts/kube-prometheus-stack).

### Выделение ресурсов {#resource-allocation}

Каждый под {{ ydb-short-name }} может быть ограничен в потреблении ресурсов. Если оставить значения ограничений пустыми, поду будет доступно все процессорное время и вся память виртуальной машины, что может привести к нежелательным последствиям. Мы рекомендуем всегда явно указывать лимиты ресурсов.

Более детально ознакомиться с принципами распределения и ограничения ресурсов можно в [документации {{ k8s }}](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/).

### Освобождение неиспользуемых ресурсов {#cleanup}

Если созданный кластер {{ ydb-short-name }} больше не нужен, удалите его с помощью следующих шагов:

  1. Чтобы удалить базу данных {{ ydb-short-name }} и её динамические узлы, достаточно удалить сопоставленный с ней ресурс `Database`:

      ```bash
      kubectl delete database.ydb.tech database-sample
      ```

  2. Чтобы удалить узлы хранения {{ ydb-short-name }}, выполните следующие команды:

      ```bash
      kubectl delete storage.ydb.tech storage-sample
      kubectl delete pvc -l app.kubernetes.io/name=ydb
      ```

  3. Чтобы удалить {{ ydb-short-name }} {{ k8s }} оператор с кластера, используйте Helm:

      ```bash
      helm delete ydb-operator
      ```
