#pragma once

#include <ydb/core/scheme/scheme_tablecell.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NKikimrChangeExchange {
    class TChangeRecord;
}

namespace NKikimr::NChangeExchange {

template <typename T, typename TDerived> class TChangeRecordBuilder;

class TChangeRecord {
    template <typename T, typename TDerived> friend class TChangeRecordBuilder;

public:
    enum class ESource: ui8 {
        Unspecified = 0,
        InitialScan = 1,
    };

    enum class EKind: ui8 {
        AsyncIndex,
        CdcDataChange,
        CdcHeartbeat,
    };

public:
    ui64 GetOrder() const { return Order; }
    ui64 GetGroup() const { return Group; }
    ui64 GetStep() const { return Step; }
    ui64 GetTxId() const { return TxId; }
    EKind GetKind() const { return Kind; }
    const TString& GetBody() const { return Body; }
    ESource GetSource() const { return Source; }

    void Serialize(NKikimrChangeExchange::TChangeRecord& record) const;

    TConstArrayRef<TCell> GetKey() const;
    i64 GetSeqNo() const;
    TInstant GetApproximateCreationDateTime() const;

    TString ToString() const;
    void Out(IOutputStream& out) const;

protected:
    ui64 Order = Max<ui64>();
    ui64 Group = 0;
    ui64 Step = 0;
    ui64 TxId = 0;
    EKind Kind;
    TString Body;
    ESource Source = ESource::Unspecified;

    mutable TMaybe<TOwnedCellVec> Key;
    mutable TMaybe<TString> PartitionKey;

}; // TChangeRecord

template <typename T, typename TDerived>
class TChangeRecordBuilder {
protected:
    using TSelf = TDerived;
    using EKind = TChangeRecord::EKind;
    using ESource = TChangeRecord::ESource;

public:
    explicit TChangeRecordBuilder(EKind kind) {
        Record.Kind = kind;
    }

    explicit TChangeRecordBuilder(T&& record) {
        Record = std::move(record);
    }

    TSelf& WithOrder(ui64 order) {
        Record.Order = order;
        return static_cast<TSelf&>(*this);
    }

    TSelf& WithGroup(ui64 group) {
        Record.Group = group;
        return static_cast<TSelf&>(*this);
    }

    TSelf& WithStep(ui64 step) {
        Record.Step = step;
        return static_cast<TSelf&>(*this);
    }

    TSelf& WithTxId(ui64 txId) {
        Record.TxId = txId;
        return static_cast<TSelf&>(*this);
    }

    TSelf& WithBody(const TString& body) {
        Record.Body = body;
        return static_cast<TSelf&>(*this);
    }

    TSelf& WithBody(TString&& body) {
        Record.Body = std::move(body);
        return static_cast<TSelf&>(*this);
    }

    TSelf& WithSource(ESource source) {
        Record.Source = source;
        return static_cast<TSelf&>(*this);
    }

    T&& Build() {
        return std::move(Record);
    }

protected:
    T Record;

}; // TChangeRecordBuilder

}

Y_DECLARE_OUT_SPEC(inline, NKikimr::NChangeExchange::TChangeRecord, out, value) {
    return value.Out(out);
}
