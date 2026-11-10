#pragma once

#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tx/cdc_aggregator/stream/stream_repository.h>

namespace NKikimr::NCdcAggregator {

// Stream state in the tablet's LocalDB, bound to one transaction.
class TStreamRepositoryOverLocalDb : public IStreamRepository {
public:
    explicit TStreamRepositoryOverLocalDb(NTable::TDatabase& database);

    // Returns false when data pages are not loaded yet; the transaction is
    // restarted then and stream is left untouched.
    bool Load(TStream& stream);

    void Save(const TStream& stream, const TStreamChanges& changes) override;

private:
    NIceDb::TNiceDb Db;
};

} // namespace NKikimr::NCdcAggregator
