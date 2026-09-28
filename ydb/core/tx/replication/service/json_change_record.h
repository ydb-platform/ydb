#pragma once

#include "lightweight_schema.h"

#include <ydb/core/change_exchange/change_record.h>
#include <ydb/core/scheme/scheme_tablecell.h>

#include <library/cpp/json/json_reader.h>

#include <util/generic/maybe.h>
#include <util/memory/pool.h>

namespace NKikimrTxDataShard {
    class TEvApplyReplicationChanges_TChange;
}

namespace NKikimrReplication {
    class TSchemaChange;
}

namespace NKikimr::NReplication::NService {

class TChangeRecordBuilder;

class TChangeRecord: public NChangeExchange::TChangeRecordBase {
    friend class TChangeRecordBuilder;

public:
    ui64 GetGroup() const override;
    ui64 GetStep() const override;
    ui64 GetTxId() const override;
    EKind GetKind() const override;

    bool IsValidJson(TString& error) const;

    void Serialize(NKikimrTxDataShard::TEvApplyReplicationChanges_TChange& record, TMemoryPool& pool) const;

    TConstArrayRef<TCell> GetKey(TMemoryPool& pool) const;
    TConstArrayRef<TCell> GetKey() const;

    void Accept(NChangeExchange::IVisitor& visitor) const override;
    void RewriteTxId(ui64 value) override;

    // Parses the complete snapshot in a CDC schema record. Unlike the row-data
    // parser this is fallible: malformed control records must put replication
    // into an error state, never terminate an actor process.
    bool TryGetSchemaChange(NKikimrReplication::TSchemaChange& schema, TString& error) const;

private:
    NJson::TJsonValue JsonBody;
    bool JsonParsed = false;
    TString JsonError;
    TLightweightSchema::TCPtr Schema;
    ui64 WriteTxId = 0;

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
        auto* record = GetRecord();
        record->JsonParsed = NJson::ReadJsonTree(body, &record->JsonBody);
        if (!record->JsonParsed) {
            record->JsonError = "cannot parse JSON";
        }
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
