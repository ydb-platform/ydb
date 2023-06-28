#pragma once

#include "datashard_user_table.h"

#include <library/cpp/json/json_value.h>

#include <ydb/core/base/pathid.h>
#include <ydb/core/scheme/scheme_tablecell.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NKikimrChangeExchange {
    class TChangeRecord;
}

namespace NKikimr::NDataShard {

class TChangeRecordBuilder;

class TChangeRecord {
    friend class TChangeRecordBuilder;

public:
    enum class EKind: ui8 {
        AsyncIndex,
        CdcDataChange,
        CdcHeartbeat,
    };

    struct TAwsJsonOptions {
        TString AwsRegion;
        NKikimrSchemeOp::ECdcStreamMode StreamMode;
        ui64 ShardId;
    };

public:
    ui64 GetOrder() const { return Order; }
    ui64 GetGroup() const { return Group; }
    ui64 GetStep() const { return Step; }
    ui64 GetTxId() const { return TxId; }
    ui64 GetLockId() const { return LockId; }
    ui64 GetLockOffset() const { return LockOffset; }
    const TPathId& GetPathId() const { return PathId; }
    EKind GetKind() const { return Kind; }
    const TString& GetBody() const { return Body; }

    const TPathId& GetTableId() const { return TableId; }
    ui64 GetSchemaVersion() const { return SchemaVersion; }

    void SerializeToProto(NKikimrChangeExchange::TChangeRecord& record) const;
    void SerializeToYdbJson(NJson::TJsonValue& json, bool virtualTimestamps) const;
    void SerializeToDynamoDBStreamsJson(NJson::TJsonValue& json, const TAwsJsonOptions& opts) const;

    TConstArrayRef<TCell> GetKey() const;
    i64 GetSeqNo() const;
    TString GetPartitionKey() const;
    TInstant GetApproximateCreationDateTime() const;
    bool IsBroadcast() const;

    TString ToString() const;
    void Out(IOutputStream& out) const;

private:
    ui64 Order = Max<ui64>();
    ui64 Group = 0;
    ui64 Step = 0;
    ui64 TxId = 0;
    ui64 LockId = 0;
    ui64 LockOffset = 0;
    TPathId PathId;
    EKind Kind;
    TString Body;

    ui64 SchemaVersion;
    TPathId TableId;
    TUserTable::TCPtr Schema;

    mutable TMaybe<TOwnedCellVec> Key;
    mutable TMaybe<TString> PartitionKey;

}; // TChangeRecord

class TChangeRecordBuilder {
    using EKind = TChangeRecord::EKind;

public:
    explicit TChangeRecordBuilder(EKind kind);

    TChangeRecordBuilder& WithLockId(ui64 lockId);
    TChangeRecordBuilder& WithLockOffset(ui64 lockOffset);

    TChangeRecordBuilder& WithOrder(ui64 order);
    TChangeRecordBuilder& WithGroup(ui64 group);
    TChangeRecordBuilder& WithStep(ui64 step);
    TChangeRecordBuilder& WithTxId(ui64 txId);
    TChangeRecordBuilder& WithPathId(const TPathId& pathId);

    TChangeRecordBuilder& WithTableId(const TPathId& tableId);
    TChangeRecordBuilder& WithSchemaVersion(ui64 version);
    TChangeRecordBuilder& WithSchema(TUserTable::TCPtr schema);

    TChangeRecordBuilder& WithBody(const TString& body);
    TChangeRecordBuilder& WithBody(TString&& body);

    TChangeRecord&& Build();

private:
    TChangeRecord Record;

}; // TChangeRecordBuilder

}

Y_DECLARE_OUT_SPEC(inline, NKikimr::NDataShard::TChangeRecord, out, value) {
    return value.Out(out);
}
