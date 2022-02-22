# Мониторинг YDB С++ SDK

Имеется возможность подключать расширение для C++ SDK, позволяющее мониторить работу последнего. В частности возможен мониторинг транспортных ошибок, infligh для каждого хоста, работа discovery, размер пула сессий, количество сессий на host, количество endpoint'ов, размеры, latency запросов и другие. 

Есть возможность как использовать http сервер создаваемый в sdk, так и использовать свой MetricRegistry (что часто удобнее).

## Настройка Solomon

При использовании выделенного http сервера для sdk требуется подготовить проект в Solomon, для чего:

Нужно [создать проект, кластер и сервис, и связать в шард в Solomon.](https://wiki.yandex-team.ru/solomon/howtostart/)
При создании кластера в **Cluster hosts** добавить host опрашиваемого клиента (без http://). Поле **Port** будет использоваться для всех host'ов, добавленных в данный кластер, вне зависимости от настроек сервиса.
При создании сервиса указать следующие настройки:
- **Monitoring model** : **PULL**;
- **URL Path** : **/stats**;
- **Interval** : **10**;
- **Port** : порт http сервера.

## Настройка мониторинга в SDK при использовании http сервера в sdk

После создания драйвера нужно добавить расширение для мониторинга. При указании неверного host или открытого порта, генерируется исключение **TSystemError**.

Внимание: подключение мониторинга должно происходить до создания клиента.

```cl
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/extensions/solomon_stats/pull_client.h>

...

{
    auto config = NYdb::TDriverConfig();
    NYdb::TDriver driver(config);
    try {

        const TString host = ... // без http://
        const ui64 port = ...
        const TString project = ... // поле id проекта в Solomon
        const TString service = ... // поле id сервиса в Solomon
        const TString cluster = ... // поле id кластера в Solomon 
        NSolomonStatExtension::TSolomonStatPullExtension::TParams params(host, port, project, service, cluster);
        driver.AddExtension<NSolomonStatExtension::TSolomonStatPullExtension>(params);

    } catch (TSystemError& error) {
        ...
    }
}

```

## Настройка мониторинга в SDK при использовании своего SensorsRegestry

В данном случае подключение еще проще. Где то в вашем коде есть экземпляр IMetricRegistry. В SDK есть перегрузки для хелпера AddMetricRegistry

Внимание: подключение мониторинга должно происходить до создания клиента.

```cl
#include <ydb/public/sdk/cpp/client/extensions/solomon_stats/pull_connector.h>

...

void AddMetricRegistry(NYdb::TDriver& driver, NMonitoring::IMetricRegistry* ptr);
void AddMetricRegistry(NYdb::TDriver& driver, std::shared_ptr<NMonitoring::IMetricRegistry> ptr);
void AddMetricRegistry(NYdb::TDriver& driver, TAtomicSharedPtr<NMonitoring::IMetricRegistry> ptr);
```

которые позволяют подключить IMetricRegistry к sdk. При передаче по сырому указателю обязанность по контролю времени жизни лежит на пользовательском коде, sdk должен быть остановлен до разрушения MetricRegistry.

## Описание графиков

Наиболле интересные сенсоры:

- Grpc/InFlightByYdbHost - запросы в полете для заданного ydb хоста
- Request/Latency - Гистограмма времени выполнения запроса с клиентской стороны, ms (без учета времени в RetryOperation)
- Request/ParamsSize - Гистограмма размера параметров используемых в запросах, в байтах
- Request/QuerySize - Гистограмма размера текстов программ, в байтах
- Request/ResultSize - Гистограмма размера ответов, в байтах
- SessionBalancer/Variation - Коэффициент вариации распределения сессий по хостам базы данных
- Sessions/InPool - количество сессий в пуле
- Sessions/SessionsLimitExceeded - Рейт событий превышения лимита на количество сессий на клиентской стороне
- SessionsByYdbHost - количество сессий по хостам базы данных

