# Замена FQDN статического узла

{% include [_](../_includes/experimental_v2.md) %}

Полное доменное имя (Fully Qualified Domain Name, FQDN) [статического узла](../../../concepts/glossary.md#static-node) задаётся параметром `host` в секции [`hosts`](../../../reference/configuration/hosts.md). Чтобы изменить FQDN без замены сервера и дисков, выполните следующие действия.

Создайте DNS-запись для нового FQDN, указывающую на тот же сервер. Новый FQDN должен разрешаться со всех узлов кластера. Не удаляйте старую DNS-запись до окончания процедуры. Если кластер использует TLS, подготовьте [сертификат узла](../../deployment-options/manual/initial-deployment/deployment-preparation.md#tls-certificates), содержащий новый FQDN.

1. Получите текущую конфигурацию кластера с помощью команды [ydb admin cluster config fetch](../../../reference/ydb-cli/commands/configuration/cluster/fetch.md):

    ```bash
    ydb [global options...] admin cluster config fetch > config.yaml
    ```

1. В файле `config.yaml` найдите существующий элемент списка `hosts` для заменяемого узла и измените только его значение `host`. Не удаляйте запись узла и не создавайте новую в другом месте — это изменит позицию узла в конфигурации и нарушит обновление.

    Например, для узла с идентификатором `1` замените `node-1.example.com` на `node-1-new.example.com`:

    ```yaml
    config:
      hosts:
      - host: node-1-new.example.com
        node_id: 1
    ```

1. Примените новую конфигурацию с помощью команды [ydb admin cluster config replace](../../../reference/ydb-cli/commands/configuration/cluster/replace.md):

    ```bash
    ydb [global options...] admin cluster config replace -f config.yaml
    ```

1. Дождитесь, пока в локальной копии файла `config.yaml` на заменяемом узле появится новый FQDN.

1. Подготовьте заменяемый узел к перезапуску:

    - Если кластер использует TLS, установите подготовленный сертификат.
    - Если узел запускается с параметром `--node static`, убедитесь, что новое значение `host` совпадает с выводом команды `hostname` или `hostname -f`. Если значения не совпадают, перед перезапуском настройте операционную систему так, чтобы команда `hostname -f` возвращала новое значение `host`.

1. Получите [разрешение на остановку процесса](../../../maintenance/manual/node_restarting.md#restart_process) и перезапустите процесс статического узла. Например, при использовании `systemd`:

    ```bash
    sudo systemctl restart ydbd-storage
    ```

    Имя сервиса может отличаться в зависимости от способа развёртывания.

1. На вкладке **Nodes** [страницы мониторинга кластера](../../../reference/ydb-ui/ydb-monitoring.md#node_list_page) убедитесь, что узел подключился с прежним идентификатором и новым FQDN. После этого можно удалить старую DNS-запись.
