#pragma once

#include "request.h"

#include <util/generic/ptr.h>
#include <util/generic/queue.h>
#include <util/generic/strbuf.h>
#include <util/system/mutex.h>

namespace NHttp {
    using namespace NHttpFetcher;

    // Асинхронный механизм скачивания.
    // Несколько документов одновременно.
    //  - несколько потоков
    //  - контроль нагрузки на хост => один объект на приложение.
    // Редиректы
    // Отмена запроса по таймеру.

    class IHostsPolicy {
    public:
        virtual ~IHostsPolicy() = default;

        //! Максимальное количество одновременных соединений к хосту.
        virtual size_t GetMaxHostConnections(const TStringBuf& host) const = 0;
    };

    //! Управляет процессом скачивания документа по заданному урлу.
    class TScheduler {
        // host loading
        // redirects
    public:
        TScheduler();

        //! Получить запрос на скачивание.
        TFetchRequestRef Extract();

        //! Поместить запрос в очередь на скачивание.
        void Schedule(TFetchRequestRef req);

    private:
        THolder<IHostsPolicy> HostsPolicy_;
        TMutex Lock_;
        TQueue<TFetchRequestRef> RequestQueue_;
    };

}
