#pragma once

#include <ydb/core/base/pathid.h>
#include <ydb/core/scheme/scheme_tablecell.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NKikimrChangeExchange {
    class TChangeRecord;
}

namespace NKikimr {
namespace NDataShard {

class TChangeRecordBuilder;

class TChangeRecord {
    friend class TChangeRecordBuilder;

public:
    enum class EKind: ui8 {
        AsyncIndex,
        CdcDataChange,
    };

public:
    ui64 GetOrder() const { return Order; }
    ui64 GetGroup() const { return Group; }
    ui64 GetStep() const { return Step; }
    ui64 GetTxId() const { return TxId; }
    const TPathId& GetPathId() const { return PathId; }
    ui64 GetSchemaVersion() const { return SchemaVersion; }
    EKind GetKind() const { return Kind; }
    const TString& GetBody() const { return Body; }
    i64 GetSeqNo() const;
    TConstArrayRef<TCell> GetKey() const;

    void SerializeTo(NKikimrChangeExchange::TChangeRecord& record) const;

    TString ToString() const;
    void Out(IOutputStream& out) const;

private:
    ui64 Order;
    ui64 Group;
    ui64 Step;
    ui64 TxId;
    TPathId PathId;
    ui64 SchemaVersion;
    EKind Kind;
    TString Body;

    mutable TMaybe<TOwnedCellVec> Key;

}; // TChangeRecord

class TChangeRecordBuilder {
    using EKind = TChangeRecord::EKind;

public:
    explicit TChangeRecordBuilder(EKind kind);

    TChangeRecordBuilder& WithOrder(ui64 order);
    TChangeRecordBuilder& WithGroup(ui64 group);
    TChangeRecordBuilder& WithStep(ui64 step);
    TChangeRecordBuilder& WithTxId(ui64 txId);
    TChangeRecordBuilder& WithPathId(const TPathId& pathId);
    TChangeRecordBuilder& WithSchemaVersion(ui64 version);

    TChangeRecordBuilder& WithBody(const TString& body);
    TChangeRecordBuilder& WithBody(TString&& body);

    TChangeRecord&& Build();

private:
    TChangeRecord Record;

}; // TChangeRecordBuilder

} // NDataShard
} // NKikimr

Y_DECLARE_OUT_SPEC(inline, NKikimr::NDataShard::TChangeRecord, out, value) {
    return value.Out(out);
}
