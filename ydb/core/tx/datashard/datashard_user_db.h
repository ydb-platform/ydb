#pragma once
#include "datashard_impl.h"

namespace NKikimr::NDataShard {

class IDataShardUserDb {
protected:
    ~IDataShardUserDb() = default;

public:
    virtual NTable::EReady SelectRow(
            const TTableId& tableId,
            TArrayRef<const TRawTypeValue> key,
            TArrayRef<const NTable::TTag> tags,
            NTable::TRowState& row) = 0;
};

class TDataShardUserDb final : public IDataShardUserDb {
public:
    TDataShardUserDb(TDataShard& self, NTable::TDatabase& db, const TRowVersion& readVersion = TRowVersion::Min())
        : Self(self)
        , Db(db)
        , ReadVersion(readVersion)
    { }

    NTable::EReady SelectRow(
            const TTableId& tableId,
            TArrayRef<const TRawTypeValue> key,
            TArrayRef<const NTable::TTag> tags,
            NTable::TRowState& row) override;

private:
    TDataShard& Self;
    NTable::TDatabase& Db;
    TRowVersion ReadVersion;
};

} // namespace NKikimr::NDataShard
