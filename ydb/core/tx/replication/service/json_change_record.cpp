#include "json_change_record.h"

#include <ydb/core/io_formats/cell_maker/cell_maker.h>
#include <ydb/core/protos/base.pb.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/tx/replication/common/family_settings.h>

#include <library/cpp/json/json_writer.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

namespace NKikimr::NReplication::NService {

namespace {

class TFamilyKeyParser: public NJson::TParserCallbacks {
public:
    explicit TFamilyKeyParser(NJson::TJsonValue& value)
        : TParserCallbacks(value)
    {
    }

    bool OnOpenMap() override {
        const bool isFamilyMap = CurrentState == AFTER_MAP_KEY && Key == "columnFamilies";
        if (!TParserCallbacks::OnOpenMap()) {
            return false;
        }

        if (isFamilyMap) {
            FamilyMaps.push_back(ValuesStack.back());
        }

        return true;
    }

    bool OnMapKey(const TStringBuf& key) override {
        if (!FamilyMaps.empty() && ValuesStack.back()->Has(key)) {
            if (FamilyMaps.back() == ValuesStack.back()) {
                DuplicateFamily = key;
            } else {
                DuplicateSetting = key;
            }
            return false;
        }

        return TParserCallbacks::OnMapKey(key);
    }

    bool OnCloseMap() override {
        if (!FamilyMaps.empty() && FamilyMaps.back() == ValuesStack.back()) {
            FamilyMaps.pop_back();
        }

        return TParserCallbacks::OnCloseMap();
    }

    TString DuplicateFamily;
    TString DuplicateSetting;

private:
    TVector<const NJson::TJsonValue*> FamilyMaps;
};

} // namespace

bool TChangeRecordBuilder::ParseJsonBody(TStringBuf body, NJson::TJsonValue& json, TString& error) {
    TMemoryInput input(body.data(), body.size());
    TFamilyKeyParser parser(json);
    if (NJson::ReadJson(&input, &parser)) {
        return true;
    }

    if (parser.DuplicateFamily) {
        error = TStringBuilder() << "duplicate column family: " << parser.DuplicateFamily;
    } else if (parser.DuplicateSetting) {
        error = TStringBuilder() << "duplicate column family setting: " << parser.DuplicateSetting;
    } else {
        error = "cannot parse JSON";
    }
    return false;
}

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
        case EKind::CdcSchemaChange: return NService::GetStep(JsonBody, "ts");
        default: Y_ABORT("unreachable");
    }
}

ui64 TChangeRecord::GetTxId() const {
    switch (GetKind()) {
        case EKind::CdcDataChange: return NService::GetTxId(JsonBody, "ts");
        case EKind::CdcHeartbeat: return NService::GetTxId(JsonBody, "resolved");
        case EKind::CdcSchemaChange: return NService::GetTxId(JsonBody, "ts");
        default: Y_ABORT("unreachable");
    }
}

bool TChangeRecord::IsValidJson(TString& error) const {
    if (JsonParsed) {
        return true;
    }
    error = JsonError;
    return false;
}

NChangeExchange::IChangeRecord::EKind TChangeRecord::GetKind() const {
    if (JsonBody.Has("resolved")) {
        return EKind::CdcHeartbeat;
    }
    if (JsonBody.Has("tableChanges")) {
        return EKind::CdcSchemaChange;
    }
    return EKind::CdcDataChange;
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

bool TChangeRecord::TryGetSchemaChange(NKikimrReplication::TSchemaChange& schema, TString& error) const {
    if (!IsValidJson(error)) {
        return false;
    }

    if (GetKind() != EKind::CdcSchemaChange) {
        error = "record is not a schema change";
        return false;
    }

    const auto& timestamp = JsonBody["ts"];
    if (!timestamp.IsArray() || timestamp.GetArray().size() != 2
        || !timestamp.GetArray()[0].IsUInteger() || !timestamp.GetArray()[1].IsUInteger()
        || (timestamp.GetArray()[0].GetUInteger() == 0 && timestamp.GetArray()[1].GetUInteger() == 0))
    {
        error = "schema record has an invalid timestamp";
        return false;
    }

    const auto& changes = JsonBody["tableChanges"];
    if (!changes.IsArray() || changes.GetArray().size() != 1) {
        error = "schema record must contain exactly one table change";
        return false;
    }

    const auto& change = changes.GetArray().front();
    if (!change.IsMap() || !change.Has("table") || !change["table"].IsMap()) {
        error = "schema record has no table snapshot";
        return false;
    }

    const auto& table = change["table"];
    if (!table.Has("schemaVersion") || !table["schemaVersion"].IsUInteger()
        || table["schemaVersion"].GetUInteger() == 0
        || !table.Has("columns") || !table["columns"].IsMap()
        || !table.Has("primaryKeyColumnNames") || !table["primaryKeyColumnNames"].IsArray())
    {
        error = "schema record has an invalid table snapshot";
        return false;
    }

    schema.Clear();
    schema.SetSourceSchemaVersion(table["schemaVersion"].GetUInteger());
    schema.MutableVersion()->SetStep(timestamp.GetArray()[0].GetUInteger());
    schema.MutableVersion()->SetTxId(timestamp.GetArray()[1].GetUInteger());

    const bool hasFamilies = table.Has("columnFamilies");
    THashSet<TString> families;
    if (hasFamilies) {
        if (!table["columnFamilies"].IsMap() || !table["columnFamilies"].GetMap().contains("default")) {
            error = "schema record has invalid column families: default family is required";
            return false;
        }

        TVector<TString> names;
        for (const auto& [name, definition] : table["columnFamilies"].GetMap()) {
            if (name.empty() || !definition.IsMap()
                || !definition.Has("compression") || !definition["compression"].IsString()
                || !definition.Has("cacheMode") || !definition["cacheMode"].IsString())
            {
                error = TStringBuilder() << "schema record has invalid column family: " << name;
                return false;
            }

            const auto compression = CompressionFromString(definition["compression"].GetString());
            const auto cacheMode = CacheModeFromString(definition["cacheMode"].GetString());
            if (!IsValidCompression(compression) || !IsValidCacheMode(cacheMode)) {
                error = TStringBuilder() << "schema record has invalid column family: " << name;
                return false;
            }

            for (const auto& [setting, value] : definition.GetMap()) {
                if (setting != "data" && setting != "compression" && setting != "cacheMode") {
                    error = TStringBuilder() << "unsupported setting '" << setting << "' in column family " << name;
                    return false;
                }
            }

            if (definition.Has("data")) {
                const auto& data = definition["data"];
                if (!data.IsMap() || data.GetMap().size() != 1 || !data.Has("media")
                    || !data["media"].IsString() || data["media"].GetString().empty())
                {
                    error = TStringBuilder() << "invalid data setting in column family " << name;
                    return false;
                }
            }

            names.push_back(name);
            families.insert(name);
        }

        Sort(names);
        for (const auto& name : names) {
            const auto& definition = table["columnFamilies"][name];
            auto* family = schema.AddFamilies();
            family->SetName(name);
            family->SetCompression(CompressionFromString(definition["compression"].GetString()));
            family->SetCacheMode(CacheModeFromString(definition["cacheMode"].GetString()));
            if (definition.Has("data")) {
                family->SetMedia(definition["data"]["media"].GetString());
            }
        }
    }

    THashSet<TString> columns;
    TVector<TString> orderedColumns;
    orderedColumns.reserve(table["columns"].GetMap().size());
    for (const auto& [name, description] : table["columns"].GetMap()) {
        if (name.empty() || !description.IsMap() || !description.Has("type")
            || !description["type"].IsString() || description["type"].GetString().empty())
        {
            error = "schema record has an invalid column";
            return false;
        }

        if (hasFamilies) {
            if (!description.Has("family") || !description["family"].IsString()
                || !families.contains(description["family"].GetString()))
            {
                error = TStringBuilder() << "schema record column '" << name << "' has an unknown or missing family";
                return false;
            }
        } else if (description.Has("family")) {
            error = TStringBuilder() << "schema record column '" << name << "' has a family without columnFamilies";
            return false;
        }

        columns.insert(name);
        orderedColumns.push_back(name);
    }

    Sort(orderedColumns);
    for (const auto& name : orderedColumns) {
        const auto& description = table["columns"][name];
        auto* column = schema.AddColumns();
        column->SetName(name);
        column->SetType(description["type"].GetString());
        if (hasFamilies) {
            column->SetFamily(description["family"].GetString());
        }
    }

    THashSet<TString> keys;
    for (const auto& key : table["primaryKeyColumnNames"].GetArray()) {
        if (!key.IsString() || !columns.contains(key.GetString()) || !keys.insert(key.GetString()).second) {
            error = "schema record has an invalid primary key";
            return false;
        }

        schema.AddPrimaryKeyColumnNames(key.GetString());
    }

    if (schema.PrimaryKeyColumnNamesSize() == 0) {
        error = "schema record has no primary key";
        return false;
    }

    return true;
}

}
