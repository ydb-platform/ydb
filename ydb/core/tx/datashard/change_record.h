#pragma once

#include "datashard_user_table.h"

#include <ydb/core/change_exchange/change_record.h>
#include <ydb/core/scheme/scheme_pathid.h>

namespace NKikimr::NDataShard {

class TChangeRecordBuilder;

class TChangeRecord: public NChangeExchange::TChangeRecord {
    friend class TChangeRecordBuilder;

public:
    ui64 GetLockId() const { return LockId; }
    ui64 GetLockOffset() const { return LockOffset; }
    const TPathId& GetPathId() const { return PathId; }

    const TPathId& GetTableId() const { return TableId; }
    ui64 GetSchemaVersion() const { return SchemaVersion; }
    TUserTable::TCPtr GetSchema() const { return Schema; }

    void Serialize(NKikimrChangeExchange::TChangeRecord& record) const;

    TString GetPartitionKey() const;
    bool IsBroadcast() const;

    void Out(IOutputStream& out) const;

private:
    ui64 LockId = 0;
    ui64 LockOffset = 0;
    TPathId PathId;

    ui64 SchemaVersion;
    TPathId TableId;
    TUserTable::TCPtr Schema;

}; // TChangeRecord

class TChangeRecordBuilder: public NChangeExchange::TChangeRecordBuilder<TChangeRecord, TChangeRecordBuilder> {
public:
    using NChangeExchange::TChangeRecordBuilder<TChangeRecord, TChangeRecordBuilder>::TChangeRecordBuilder;

    TSelf& WithLockId(ui64 lockId) {
        Record.LockId = lockId;
        return static_cast<TSelf&>(*this);
    }

    TSelf& WithLockOffset(ui64 lockOffset) {
        Record.LockOffset = lockOffset;
        return static_cast<TSelf&>(*this);
    }

    TSelf& WithPathId(const TPathId& pathId) {
        Record.PathId = pathId;
        return static_cast<TSelf&>(*this);
    }

    TSelf& WithTableId(const TPathId& tableId) {
        Record.TableId = tableId;
        return static_cast<TSelf&>(*this);
    }

    TSelf& WithSchemaVersion(ui64 version) {
        Record.SchemaVersion = version;
        return static_cast<TSelf&>(*this);
    }

    TSelf& WithSchema(TUserTable::TCPtr schema) {
        Record.Schema = schema;
        return static_cast<TSelf&>(*this);
    }

}; // TChangeRecordBuilder

}

Y_DECLARE_OUT_SPEC(inline, NKikimr::NDataShard::TChangeRecord, out, value) {
    return value.Out(out);
}
