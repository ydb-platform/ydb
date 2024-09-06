#pragma once

#include "lightweight_schema.h"

#include <ydb/core/change_exchange/change_exchange.h>
#include <ydb/core/change_exchange/change_record.h>
#include <ydb/core/change_exchange/change_sender_resolver.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/tablet_flat/flat_row_eggs.h>

#include <library/cpp/json/json_reader.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/memory/pool.h>
#include <util/string/join.h>

namespace NKikimrTxDataShard {
    class TEvApplyReplicationChanges_TChange;
}

namespace NKikimr::NReplication::NService {

class TChangeRecordBuilder;

class TChangeRecord: public NChangeExchange::TChangeRecordBase {
    friend class TChangeRecordBuilder;
    using TSerializationContext = TChangeRecordBuilderContextTrait<TChangeRecord>;

public:
    using TPtr = TIntrusivePtr<TChangeRecord>;

    const static NKikimrSchemeOp::ECdcStreamFormat StreamType = NKikimrSchemeOp::ECdcStreamFormatJson;

    ui64 GetGroup() const override;
    ui64 GetStep() const override;
    ui64 GetTxId() const override;
    EKind GetKind() const override;
    TString GetSourceId() const;

    void Serialize(NKikimrTxDataShard::TEvApplyReplicationChanges_TChange& record, TSerializationContext& ctx) const;

    TConstArrayRef<TCell> GetKey(TMemoryPool& pool) const;
    TConstArrayRef<TCell> GetKey() const;

private:
    TString SourceId;
    NJson::TJsonValue JsonBody;
    TLightweightSchema::TCPtr Schema;

    mutable TMaybe<TOwnedCellVec> Key;

}; // TChangeRecord

class TChangeRecordBuilder: public NChangeExchange::TChangeRecordBuilder<TChangeRecord, TChangeRecordBuilder> {
public:
    using TBase::TBase;

    TSelf& WithSourceId(const TString& sourceId) {
        GetRecord()->SourceId = sourceId;
        return static_cast<TSelf&>(*this);
    }

    template <typename T>
    TSelf& WithBody(T&& body) {
        auto res = NJson::ReadJsonTree(body, &GetRecord()->JsonBody);
        Y_ABORT_UNLESS(res);
        return static_cast<TBase*>(this)->WithBody(std::forward<T>(body));
    }

    TSelf& WithSchema(TLightweightSchema::TCPtr schema) {
        GetRecord()->Schema = schema;
        return static_cast<TSelf&>(*this);
    }

}; // TChangeRecordBuilder

}

namespace NKikimr {

template <>
struct TChangeRecordContainer<NReplication::NService::TChangeRecord>
    : public TBaseChangeRecordContainer<NReplication::NService::TChangeRecord>
{
    using TBaseChangeRecordContainer<NReplication::NService::TChangeRecord>::TBaseChangeRecordContainer;
};

template <>
struct TChangeRecordBuilderTrait<NReplication::NService::TChangeRecord>
    : public NReplication::NService::TChangeRecordBuilder
{};

template <>
struct TChangeRecordBuilderContextTrait<NReplication::NService::TChangeRecord> {
    TMemoryPool MemoryPool;

    TChangeRecordBuilderContextTrait()
        : MemoryPool(256)
    {}

    // do not preserve any state between writers, just construct new one.
    TChangeRecordBuilderContextTrait(const TChangeRecordBuilderContextTrait<NReplication::NService::TChangeRecord>&)
        : MemoryPool(256)
    {}
};

}

Y_DECLARE_OUT_SPEC(inline, NKikimr::NReplication::NService::TChangeRecord, out, value) {
    return value.Out(out);
}

Y_DECLARE_OUT_SPEC(inline, NKikimr::NReplication::NService::TChangeRecord::TPtr, out, value) {
    return value->Out(out);
}
