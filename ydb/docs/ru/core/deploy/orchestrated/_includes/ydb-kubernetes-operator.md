# Установка кластера

## Установите контроллер {{ ydb-short-name }} в кластер {#install-ydb-controller}

Установите {{ ydb-short-name }} в стандартной конфигурации:

{% list tabs %}

- CLI

  Выполните команду:

  ```bash
  helm install ydb-operator ydb/operator
  ```

  * `ydb-operator` — имя релиза;
  * `ydb/operator` — название чарта в добавленном ранее репозитории.

  Результат выполнения:

  ```text
  NAME: ydb-operator
  LAST DEPLOYED: Thu Aug 12 19:32:28 2021
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
  kubectl apply -f samples/storage.yaml
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

Стандартная конфигурация включает минимально необходимые 9 нод хранения, каждая с диском размером 80 ГБ.

{% include [_includes/storage-device-requirements.md](../../../_includes/storage-device-requirements.md) %}

## Создайте базу данных {#create-database}

Примените манифест для создания базы данных:

{% list tabs %}

- CLI

  Выполните команду:

  ```bash
  kubectl apply -f samples/database.yaml
  ```


  {% note info %}

  Значение ключа `.spec.storageClusterRef.name` должно совпадать с именем ресурса storage части кластера.

  {% endnote %}

  После обработки манифеста будет создан объект StatefulSet, который описывает набор динамических нод. Созданная БД будет доступна изнутри {{ k8s }} кластера по DNS-имени `database-sample`, или по FQDN-имени `database-sample.<namespace>.svc.cluster.local`, где `namespace` — название пространства имен, в котором был установлен релиз. Порт для подключения к БД — `2135`.

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
    State:  Ready
  Events:
    Type     Reason              Age    From          Message
    ----     ------              ----   ----          -------
    Normal   Provisioning        8m10s  ydb-operator  Resource sync is in progress
    Normal   Provisioning        8m9s   ydb-operator  Resource sync complete
    Normal   TenantInitialized   8m9s   ydb-operator  Tenant /root/database-sample created
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

  1. Запустите новый под с {{ ydb-short-name }} CLI:

      ```bash
      kubectl run -it --image=cr.yandex/yc/ydb:21.4.30 --rm ydb-cli bash
      ```

  1. Выполните запрос к базе данных {{ ydb-short-name }}:

      ```bash
      ydb \
        --endpoint grpc://database-sample-grpc:2135 \
        --database /root/database-sample \
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
      kubectl delete database.ydb.tech database-sample
      ```

  1. Чтобы удалить кластер {{ ydb-short-name }}, выполните следующие команды:

      ```bash
      kubectl delete storage.ydb.tech storage-sample
      kubectl delete pvc -l app.kubernetes.io/name=ydb
      ```

  1. Чтобы удалить контроллер {{ ydb-short-name }} из кластера {{ k8s }}, удалите релиз, созданный Helm:

      ```bash
      helm delete ydb-operator
      ```

      * `ydb-operator` — имя релиза, с которым был установлен контроллер.

{% endlist %}
