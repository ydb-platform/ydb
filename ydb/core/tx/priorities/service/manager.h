#pragma once
#include "counters.h"

#include <ydb/core/tx/priorities/usage/abstract.h>
#include <ydb/core/tx/priorities/usage/config.h>

#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NPrioritiesQueue {

class TManager {
private:
    std::shared_ptr<TCounters> Counters;
    const TConfig Config;

    class TPriority {
    private:
        ui64 ExternalPriority;
        static inline TAtomicCounter Counter = 0;
        ui64 Sequence = Counter.Inc();

    public:
        TPriority(const ui64 priority)
            : ExternalPriority(priority) {
        }

        bool operator<(const TPriority& item) const {
            if (item.ExternalPriority < ExternalPriority) {
                return true;
            } else if (ExternalPriority < item.ExternalPriority) {
                return false;
            } else {
                return item.Sequence < Sequence;
            }
        }
    };

    class TClientStatus {
    private:
        YDB_READONLY(ui64, ClientId, 0);
        YDB_ACCESSOR(ui32, Count, 0);
        YDB_ACCESSOR_DEF(std::optional<TPriority>, LastPriority);

    public:
        TClientStatus(const ui64 clientId)
            : ClientId(clientId) {
        }
    };

    THashMap<ui64, TClientStatus> Clients;

    class TAskRequest {
    private:
        YDB_READONLY(ui64, ClientId, 0);
        YDB_READONLY_DEF(std::shared_ptr<IRequest>, Request);
        YDB_READONLY(ui32, Size, 0);

    public:
        TAskRequest(const ui64 clientId, const std::shared_ptr<IRequest>& request, const ui32 size)
            : ClientId(clientId)
            , Request(request)
            , Size(size) {
        }
    };

    ui32 UsedCount = 0;
    std::map<TPriority, TAskRequest> WaitingQueue;

    void AllocateNext();

    void RemoveFromQueue(const TClientStatus& client);

public:
    TManager(const std::shared_ptr<TCounters>& counters, const TConfig& config);

    void Ask(const ui64 client, const ui32 count, const std::shared_ptr<IRequest>& request, const ui64 extPriority);
    void Free(const ui64 client, const ui32 count);

    void RegisterClient(const ui64 clientId);
    void UnregisterClient(const ui64 clientId);
};

}   // namespace NKikimr::NPrioritiesQueue
