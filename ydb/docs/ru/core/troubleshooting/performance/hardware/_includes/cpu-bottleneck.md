1. Используйте вкладку **Diagnostics** во [встроенном UI](../../../../reference/embedded-ui/index.md) для анализа загрузки процессора во всех пулах ресурсов:

    1. Откройте [встроенный UI](../../../../reference/embedded-ui/index.md), перейдите на вкладку **Databases** и нажмите на требуемую базу данных.

    1. На вкладке **Navigation** убедитесь, что требуемая база данных выбрана.

    1. Откройте вкладку **Diagnostics**.

    1. На вкладке **Info** нажмите на кнопку **CPU** и проверьте уровни загрузки процессора во всех пулах ресурсов.

        ![](../_assets/embedded-ui-cpu-system-pool.png)
1. Проанализируйте загрузку процессора во всех пулах ресурсов на графиках Grafana:

    1. Откройте панель мониторинга **[CPU](../../../../reference/observability/metrics/grafana-dashboards.md#cpu)** в Grafana.

    1. Проверьте наличие скачков на следующих графиках:

        - **CPU by execution pool**

            ![](../_assets/cpu-by-pool.png)

        - **User pool - CPU by host**

            ![](../_assets/cpu-user-pool.png)

        - **System pool - CPU by host**

            ![](../_assets/cpu-system-pool.png)

        - **Batch pool - CPU by host**

            ![](../_assets/cpu-batch-pool.png)

        - **IC pool - CPU by host**

            ![](../_assets/cpu-ic-pool.png)

        - **IO pool - CPU by host**

            ![](../_assets/cpu-io-pool.png)

1. Если скачки потребления ресурсов процессора обнаружены в пользовательском пуле ресурсов (user pool), проанализируйте изменения пользовательской нагрузки, которые могли бы вызвать недостаток ресурсов процессора. Проверьте следующие графики на панели мониторинга **DB overview** в Grafana:

    - **Requests**

        ![](../_assets/requests.png)

    - **Request size**

        ![](../_assets/request-size.png)

    - **Response size**

        ![](../_assets/response-size.png)

    Также проверьте все графики в секции **Operations** на панели мониторинга **DataShard**.

1. Если скачки потребления ресурсов процессора обнаружены в пакетном пуле ресурсов (batch pool), проверьте, не запущены ли процессы резервного копирования (backups).
