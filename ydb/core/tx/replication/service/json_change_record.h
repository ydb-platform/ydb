#pragma once

#include <ydb/core/change_exchange/change_record.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/tablet_flat/flat_row_eggs.h>

#include <library/cpp/json/json_reader.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/memory/pool.h>

namespace NKikimr::NReplication::NService {

struct TLightweightSchema: public TThrRefBase {
    using TPtr = TIntrusivePtr<TLightweightSchema>;
    using TCPtr = TIntrusiveConstPtr<TLightweightSchema>;

    struct TColumn {
        NTable::TTag Tag;
        NScheme::TTypeInfo Type;
    };

    TVector<NScheme::TTypeInfo> KeyColumns;
    THashMap<TString, TColumn> ValueColumns;
    ui64 Version = 0;
};

class TChangeRecordBuilder;

class TChangeRecord: public NChangeExchange::TChangeRecordBase {
    friend class TChangeRecordBuilder;

public:
    ui64 GetGroup() const override;
    ui64 GetStep() const override;
    ui64 GetTxId() const override;
    EKind GetKind() const override;
    TString GetSourceId() const;

    void Serialize(NKikimrTxDataShard::TEvApplyReplicationChanges::TChange& record, TMemoryPool& pool) const;
    void Serialize(NKikimrTxDataShard::TEvApplyReplicationChanges::TChange& record) const;

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

Y_DECLARE_OUT_SPEC(inline, NKikimr::NReplication::NService::TChangeRecord, out, value) {
    return value.Out(out);
}
