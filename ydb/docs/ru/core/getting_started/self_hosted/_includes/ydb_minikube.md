# Запуск {{ ydb-short-name }} в Minikube


Чтобы с помощью [Minikube](https://kubernetes.io/ru/docs/tasks/tools/install-minikube/) создать кластер {{ ydb-short-name }}, выполните следующие действия.

## Перед началом работы {#before-begin}

1. Установите {{ k8s }} CLI [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl).
1. Установите и запустите [Minikube](https://kubernetes.io/ru/docs/tasks/tools/install-minikube/).
1. Установите менеджер пакетов {{ k8s }} [Нelm 3](https://helm.sh/docs/intro/install/).
1. Склонируйте репозиторий с [ydb-kubernetes-operator](https://github.com/ydb-platform/ydb-kubernetes-operator).

      ```bash
      git clone https://github.com/ydb-platform/ydb-kubernetes-operator && cd ydb-kubernetes-operator
      ```

# Установка кластера

## Установите контроллер {{ ydb-short-name }} в кластер {#install-ydb-controller}

Установите {{ ydb-short-name }} в стандартной конфигурации:

{% list tabs %}

- CLI

  Выполните команду:

  ```bash
  helm upgrade --install ydb-operator deploy/ydb-operator --set metrics.enabled=false
  ```

  * `ydb-operator` — имя релиза;
  * `ydb/ydb-operator` — название чарта в добавленном ранее репозитории.

  Результат выполнения:

  ```text
  Release "ydb-operator" does not exist. Installing it now.
  NAME: ydb-operator
  LAST DEPLOYED: Tue Mar 22 08:54:08 2022
  NAMESPACE: default
  STATUS: deployed
  REVISION: 1
  TEST SUITE: None
  ```

{% endlist %}

## Создайте кластер {{ ydb-short-name }} {#create-cluster}

Примените манифест для создания кластера {{ ydb-short-name }}:

{% list tabs %}

- CLI

  Выполните команду:

  ```bash
  kubectl apply -f samples/minikube/storage.yaml
  ```

  Эта команда создаст объект StatefulSet, который описывает набор контейнеров с предсказуемыми сетевыми именами и закрепленными за ними дисками, а также необходимые для работы кластера объекты Service и ConfigMap.

  Прогресс создания кластера {{ ydb-short-name }} можно посмотреть с помощью следующих команд:

  ```bash
  kubectl get storages.ydb.tech
  kubectl describe storages.ydb.tech
  ```

  Дождитесь, когда ресурс типа Storage перейдет в состояние `Ready`.

{% endlist %}

{% note warning %}

Конфигурация кластера статична, контроллер не будет обрабатывать изменения при повторном применении манифеста. Изменение таких параметров кластера, как версия или размер дисков, возможно только через пересоздание кластера.

{% endnote %}

## Создайте базу данных {#create-database}

Примените манифест для создания базы данных:

{% list tabs %}

- CLI

  Выполните команду:

  ```bash
  kubectl apply -f samples/minikube/database.yaml
  ```

  После обработки манифеста будет создан объект StatefulSet, который описывает набор динамических нод. Созданная БД будет доступна изнутри {{ k8s }} кластера по DNS-имени `database-minikube-sample`. Порт для подключения к БД — `2135`.

  Посмотрите статус созданного ресурса:

  ```bash
  kubectl describe database.ydb.tech

  Name:         database-sample
  Namespace:    default
  Labels:       <none>
  Annotations:  <none>
  API Version:  ydb.tech/v1alpha1
  Kind:         Database
  ...
  Status:
    ...
    State:  Ready
  Events:
    Type    Reason          Age    From          Message
    ----    ------          ----   ----          -------
    Normal  Provisioning    6m32s  ydb-operator  Resource: *v1.ConfigMap, Namespace: default, Name: database-minikube-sample, changed, result: created
    Normal  Provisioning    6m32s  ydb-operator  Resource: *v1.Service, Namespace: default, Name: database-minikube-sample-grpc, changed, result: created
    Normal  Provisioning    6m32s  ydb-operator  Resource: *v1.Service, Namespace: default, Name: database-minikube-sample-interconnect, changed, result: created
    Normal  Provisioning    6m32s  ydb-operator  Resource: *v1.Service, Namespace: default, Name: database-minikube-sample-status, changed, result: created
    Normal  Provisioning    6m32s  ydb-operator  Resource: *v1.StatefulSet, Namespace: default, Name: database-minikube-sample, changed, result: created
    Normal  Initialized     6m31s  ydb-operator  Tenant /Root/database-minikube-sample created
    Normal  ResourcesReady  6m30s  ydb-operator  Resource are ready and DB is initialized
  ```

  База данных готова к работе.

{% endlist %}

## Проверьте работу {#test-ydb}

Проверьте работоспособность {{ ydb-short-name }}:

{% list tabs %}

- CLI

  1. Проверьте, что все узлы в состоянии `Ready`:

      ```bash
      kubectl get pods
      NAME                            READY   STATUS    RESTARTS   AGE
      database-minikube-sample-0      1/1     Running   0          9m33s
      storage-minikube-sample-0       1/1     Running   0          11m
      ydb-operator-6fc76b5b68-q269l   1/1     Running   0          12m      
      ```

  1. Перенаправьте порт 2135:

      ```bash
      kubectl port-forward database-minikube-sample-0 2135
      ```

  1. Установите {{ ydb-short-name }} CLI, как описано в статье [Установка {{ ydb-short-name }} CLI](../../../reference/ydb-cli/install.md).


  1. Выполните запрос к базе данных {{ ydb-short-name }}:

      ```bash
      ydb \
        --endpoint grpc://localhost:2135 \
        --database /Root/database-minikube-sample \
        table query execute --query 'select 1;'
      ```

      * `--endpoint` — эндпоинт базы данных;
      * `--database` — имя созданной базы данных;
      * `--query` — текст запроса.

      Результат выполнения:

      ```text
      ┌─────────┐
      | column0 |
      ├─────────┤
      | 1       |
      └─────────┘
      ```

      Подробнее о командах {{ ydb-short-name }} CLI читайте в [документации](../../../reference/ydb-cli/index.md).

{% endlist %}

## Освободите неиспользуемые ресурсы {#cleanup}

Если созданные ресурсы больше не нужны, удалите их:

{% list tabs %}

- CLI

  1. Чтобы удалить базу данных {{ ydb-short-name }}, достаточно удалить сопоставленный с ней ресурс `Database`:

      ```bash
      kubectl delete database.ydb.tech database-minikube-sample
      ```

  1. Чтобы удалить кластер {{ ydb-short-name }}, выполните следующие команды:

      ```bash
      kubectl delete storage.ydb.tech storage-minikube-sample
      ```

  1. Чтобы удалить контроллер {{ ydb-short-name }} из кластера {{ k8s }}, удалите релиз, созданный Helm:

      ```bash
      helm delete ydb-operator
      ```

      * `ydb-operator` — имя релиза, с которым был установлен контроллер.

{% endlist %}

