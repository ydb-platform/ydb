#pragma once

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/stream/output.h>

namespace NKikimr::NChangeExchange {

class IChangeRecord: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IChangeRecord>;

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
    virtual ui64 GetOrder() const = 0;
    virtual ui64 GetGroup() const = 0;
    virtual ui64 GetStep() const = 0;
    virtual ui64 GetTxId() const = 0;
    virtual EKind GetKind() const = 0;
    virtual const TString& GetBody() const = 0;
    virtual ESource GetSource() const = 0;
    virtual bool IsBroadcast() const = 0;

    virtual TString ToString() const = 0;
    virtual void Out(IOutputStream& out) const = 0;

    template <typename T>
    T* Get() {
        return dynamic_cast<T*>(this);
    }

}; // IChangeRecord

template <typename T, typename TDerived> class TChangeRecordBuilder;

class TChangeRecordBase: public IChangeRecord {
    template <typename T, typename TDerived> friend class TChangeRecordBuilder;

public:
    ui64 GetOrder() const override { return Order; }
    ui64 GetGroup() const override { return Group; }
    ui64 GetStep() const override { return Step; }
    ui64 GetTxId() const override { return TxId; }
    EKind GetKind() const override { return Kind; }
    const TString& GetBody() const override { return Body; }
    ESource GetSource() const override { return Source; }

    TString ToString() const override;
    void Out(IOutputStream& out) const override;

protected:
    ui64 Order = Max<ui64>();
    ui64 Group = 0;
    ui64 Step = 0;
    ui64 TxId = 0;
    EKind Kind;
    TString Body;
    ESource Source = ESource::Unspecified;

}; // TChangeRecordBase

template <typename T, typename TDerived>
class TChangeRecordBuilder {
protected:
    using TSelf = TDerived;
    using EKind = IChangeRecord::EKind;
    using ESource = IChangeRecord::ESource;

    T* GetRecord() {
        return static_cast<T*>(Record.Get());
    }

public:
    explicit TChangeRecordBuilder(EKind kind)
        : Record(MakeIntrusive<T>())
    {
        GetRecord()->Kind = kind;
    }

    explicit TChangeRecordBuilder(IChangeRecord::TPtr record)
        : Record(std::move(record))
    {
    }

    TSelf& WithOrder(ui64 order) {
        GetRecord()->Order = order;
        return static_cast<TSelf&>(*this);
    }

    TSelf& WithGroup(ui64 group) {
        GetRecord()->Group = group;
        return static_cast<TSelf&>(*this);
    }

    TSelf& WithStep(ui64 step) {
        GetRecord()->Step = step;
        return static_cast<TSelf&>(*this);
    }

    TSelf& WithTxId(ui64 txId) {
        GetRecord()->TxId = txId;
        return static_cast<TSelf&>(*this);
    }

    TSelf& WithBody(const TString& body) {
        GetRecord()->Body = body;
        return static_cast<TSelf&>(*this);
    }

    TSelf& WithBody(TString&& body) {
        GetRecord()->Body = std::move(body);
        return static_cast<TSelf&>(*this);
    }

    TSelf& WithSource(ESource source) {
        GetRecord()->Source = source;
        return static_cast<TSelf&>(*this);
    }

    IChangeRecord::TPtr Build() {
        return Record;
    }

protected:
    IChangeRecord::TPtr Record;

}; // TChangeRecordBuilder

class TChangeRecord: public TChangeRecordBase {
public:
    bool IsBroadcast() const override { return false; }
};

}

Y_DECLARE_OUT_SPEC(inline, NKikimr::NChangeExchange::TChangeRecord, out, value) {
    return value.Out(out);
}

Y_DECLARE_OUT_SPEC(inline, NKikimr::NChangeExchange::IChangeRecord::TPtr, out, value) {
    return value->Out(out);
}
