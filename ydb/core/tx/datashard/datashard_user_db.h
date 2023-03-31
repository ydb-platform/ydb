#pragma once
#include "datashard_impl.h"

#include <util/generic/maybe.h>

namespace NKikimr::NDataShard {

class IDataShardUserDb {
protected:
    ~IDataShardUserDb() = default;

public:
    virtual NTable::EReady SelectRow(
            const TTableId& tableId,
            TArrayRef<const TRawTypeValue> key,
            TArrayRef<const NTable::TTag> tags,
            NTable::TRowState& row,
            NTable::TSelectStats& stats,
            const TMaybe<TRowVersion>& readVersion = {}) = 0;

    virtual NTable::EReady SelectRow(
            const TTableId& tableId,
            TArrayRef<const TRawTypeValue> key,
            TArrayRef<const NTable::TTag> tags,
            NTable::TRowState& row,
            const TMaybe<TRowVersion>& readVersion = {}) = 0;
};

class TDataShardUserDb final : public IDataShardUserDb {
public:
    TDataShardUserDb(TDataShard& self, NTable::TDatabase& db, const TRowVersion& readVersion)
        : Self(self)
        , Db(db)
        , ReadVersion(readVersion)
    { }

    NTable::EReady SelectRow(
            const TTableId& tableId,
            TArrayRef<const TRawTypeValue> key,
            TArrayRef<const NTable::TTag> tags,
            NTable::TRowState& row,
            NTable::TSelectStats& stats,
            const TMaybe<TRowVersion>& readVersion = {}) override;

    NTable::EReady SelectRow(
            const TTableId& tableId,
            TArrayRef<const TRawTypeValue> key,
            TArrayRef<const NTable::TTag> tags,
            NTable::TRowState& row,
            const TMaybe<TRowVersion>& readVersion = {}) override;

private:
    TDataShard& Self;
    NTable::TDatabase& Db;
    TRowVersion ReadVersion;
};

} // namespace NKikimr::NDataShard
