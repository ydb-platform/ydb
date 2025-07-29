#include "json_change_record.h"

#include <ydb/core/io_formats/cell_maker/cell_maker.h>
#include <ydb/core/protos/tx_datashard.pb.h>

#include <library/cpp/json/json_writer.h>

namespace NKikimr::NReplication::NService {

ui64 TChangeRecord::GetGroup() const {
    return 0;
}

ui64 GetVitualTsComponent(const NJson::TJsonValue& json, TStringBuf key, size_t index) {
    static constexpr TStringBuf paths[] = {"[0]", "[1]"};
    Y_ABORT_UNLESS(index < std::size(paths));

    if (!json.Has(key)) {
        return 0;
    }

    if (const auto* step = json[key].GetValueByPath(paths[index])) {
        return step->GetUIntegerRobust();
    }

    return 0;
}

ui64 GetStep(const NJson::TJsonValue& json, TStringBuf key) {
    return GetVitualTsComponent(json, key, 0);
}

ui64 GetTxId(const NJson::TJsonValue& json, TStringBuf key) {
    return GetVitualTsComponent(json, key, 1);
}

ui64 TChangeRecord::GetStep() const {
    switch (GetKind()) {
        case EKind::CdcDataChange: return NService::GetStep(JsonBody, "ts");
        case EKind::CdcHeartbeat: return NService::GetStep(JsonBody, "resolved");
        default: Y_ABORT("unreachable");
    }
}

ui64 TChangeRecord::GetTxId() const {
    switch (GetKind()) {
        case EKind::CdcDataChange: return NService::GetTxId(JsonBody, "ts");
        case EKind::CdcHeartbeat: return NService::GetTxId(JsonBody, "resolved");
        default: Y_ABORT("unreachable");
    }
}

NChangeExchange::IChangeRecord::EKind TChangeRecord::GetKind() const {
    return JsonBody.Has("resolved")
        ? EKind::CdcHeartbeat
        : EKind::CdcDataChange;
}

static bool ParseKey(TVector<TCell>& cells,
        const NJson::TJsonValue::TArray& key, TLightweightSchema::TCPtr schema, TMemoryPool& pool, TString& error)
{
    cells.resize(key.size());

    Y_ABORT_UNLESS(key.size() == schema->KeyColumns.size());
    for (ui32 i = 0; i < key.size(); ++i) {
        if (!NFormats::MakeCell(cells[i], key[i], schema->KeyColumns[i], pool, error)) {
            return false;
        }
    }

    return true;
}

template <typename T>
static bool TransformKey(T& to,
        const NJson::TJsonValue::TArray& from, TLightweightSchema::TCPtr schema, TMemoryPool& pool, TString& error)
{
    TVector<TCell> cells;
    if (!ParseKey(cells, from, schema, pool, error)) {
        return false;
    }

    if constexpr (std::is_same_v<T, NKikimrTxDataShard::TEvApplyReplicationChanges::TChange>) {
        to.SetKey(TSerializedCellVec::Serialize(cells));
    } else if constexpr (std::is_same_v<T, TMaybe<TOwnedCellVec>>) {
        to.ConstructInPlace(cells);
    } else {
        static_assert(false, "Unsupported type");
    }

    return true;
}

static bool ParseValue(TVector<NTable::TTag>& tags, TVector<TCell>& cells,
        const NJson::TJsonValue::TMapType& value, TLightweightSchema::TCPtr schema, TMemoryPool& pool, TString& error)
{
    tags.reserve(value.size());
    cells.reserve(value.size());

    for (const auto& [column, value] : value) {
        auto it = schema->ValueColumns.find(column);
        Y_ABORT_UNLESS(it != schema->ValueColumns.end());

        tags.push_back(it->second.Tag);
        if (!NFormats::MakeCell(cells.emplace_back(), value, it->second.Type, pool, error)) {
            return false;
        }
    }

    return true;
}

static bool TransformValue(NKikimrTxDataShard::TEvApplyReplicationChanges::TUpdates& to,
        const NJson::TJsonValue::TMapType& from, TLightweightSchema::TCPtr schema, TMemoryPool& pool, TString& error)
{
    TVector<NTable::TTag> tags;
    TVector<TCell> cells;
    if (!ParseValue(tags, cells, from, schema, pool, error)) {
        return false;
    }

    *to.MutableTags() = {tags.begin(), tags.end()};
    to.SetData(TSerializedCellVec::Serialize(cells));

    return true;
}

void TChangeRecord::Serialize(NKikimrTxDataShard::TEvApplyReplicationChanges_TChange& record, TMemoryPool& pool) const {
    pool.Clear();
    record.SetSourceOffset(GetOrder());
    if (WriteTxId) {
        record.SetWriteTxId(WriteTxId);
    }

    TString error;

    if (JsonBody.Has("key") && JsonBody["key"].IsArray()) {
        auto res = TransformKey(record, JsonBody["key"].GetArray(), Schema, pool, error);
        Y_ABORT_UNLESS(res, "Cannot transform key: %s", error.c_str());
    } else {
        Y_ABORT("Malformed json record: %s", NJson::WriteJson(JsonBody, false).c_str());
    }

    if (JsonBody.Has("update") && JsonBody["update"].IsMap()) {
        auto res = TransformValue(*record.MutableUpsert(), JsonBody["update"].GetMap(), Schema, pool, error);
        Y_ABORT_UNLESS(res, "Cannot transform value: %s", error.c_str());
    } else if (JsonBody.Has("reset") && JsonBody["reset"].IsMap()) {
        auto res = TransformValue(*record.MutableReset(), JsonBody["reset"].GetMap(), Schema, pool, error);
        Y_ABORT_UNLESS(res, "Cannot transform value: %s", error.c_str());
    } else if (JsonBody.Has("erase")) {
        record.MutableErase();
    } else {
        Y_ABORT("Malformed json record: %s", NJson::WriteJson(JsonBody, false).c_str());
    }
}

TConstArrayRef<TCell> TChangeRecord::GetKey(TMemoryPool& pool) const {
    if (!Key) {
        TString error;

        if (JsonBody.Has("key") && JsonBody["key"].IsArray()) {
            auto res = TransformKey(Key, JsonBody["key"].GetArray(), Schema, pool, error);
            Y_ABORT_UNLESS(res, "Cannot transform key: %s", error.c_str());
        } else {
            Y_ABORT("Malformed json record: %s", NJson::WriteJson(JsonBody, false).c_str());
        }
    }

    Y_ABORT_UNLESS(Key);
    return *Key;
}

TConstArrayRef<TCell> TChangeRecord::GetKey() const {
    TMemoryPool pool(256);
    return GetKey(pool);
}

void TChangeRecord::Accept(NChangeExchange::IVisitor& visitor) const {
    return visitor.Visit(*this);
}

void TChangeRecord::RewriteTxId(ui64 value) {
    WriteTxId = value;
}

}
