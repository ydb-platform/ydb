#pragma once

#include "flat_part_iface.h"

#include <util/generic/function_ref.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>

#include <memory>

namespace NKikimr::NTable {
class TDatabase;
}

namespace NKikimr::NTabletFlatExecutor {

class ITransaction;
struct TSeat;
struct TTransactionEnv;

struct TPageReadStats {
    size_t NewlyPinnedPages = 0;
    ui64 NewlyPinnedBytes = 0;
    size_t ToLoadPages = 0;
    ui64 ToLoadBytes = 0;
};

using TPageRequests = THashMap<TLogoBlobID, TVector<NTable::NPage::TPageLocation>>;

class ITransactionPages : public NTable::IPages {
public:
    virtual const TPageReadStats& GetStats() const = 0;
    virtual TPageRequests ObtainToLoad() = 0;
    virtual void EnableReadMissingReferences() = 0;
    virtual void DisableReadMissingReferences() = 0;
    virtual ui64 MissingReferencesSize() const = 0;
};

// Backend operations shared by the executor; concrete pin owners stay private
class IExecutorPageCache {
public:
    virtual ~IExecutorPageCache() = default;

    virtual std::unique_ptr<TSeat> MakeSeat(ui64 uniqueId, TAutoPtr<ITransaction> transaction) = 0;

    // Calls execute once synchronously; frame references must not escape
    virtual void WithTransactionPages(
        NTable::TDatabase& database, TSeat& seat, TFunctionRef<void(ITransactionPages&, TTransactionEnv&)> execute) = 0;

    virtual void ReleasePins(TSeat& seat) = 0;
};

} // namespace NKikimr::NTabletFlatExecutor
