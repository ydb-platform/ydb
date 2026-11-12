#include "json_change_record.h"

#include <ydb/core/io_formats/cell_maker/cell_maker.h>
#include <ydb/core/protos/base.pb.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/tx/replication/common/family_settings.h>

#include <contrib/libs/rapidjson/include/rapidjson/reader.h>
#include <contrib/libs/zstd/include/zstd.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/rapidjson_helpers.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

namespace NKikimr::NReplication::NService {

namespace {

bool IsSupportedSetting(TStringBuf setting) {
    return setting == "data"
        || setting == "compression"
        || setting == "compressionLevel"
        || setting == "cacheMode";
}

// NJson::ReadJson does not expose kParseFullPrecisionFlag, and its RapidJSON
// callback adapter is private to the JSON library. This local bridge lets
// replication parse CDC doubles exactly while retaining the existing schema
// validation callbacks.
class TRapidJsonCallbacks {
public:
    explicit TRapidJsonCallbacks(NJson::TJsonCallbacks& callbacks)
        : Callbacks(callbacks)
    {
    }

    bool Null() {
        return Callbacks.OnNull();
    }

    bool Bool(bool value) {
        return Callbacks.OnBoolean(value);
    }

    bool Int(int value) {
        return Callbacks.OnInteger(value);
    }

    bool Uint(unsigned value) {
        return Uint64(value);
    }

    bool Int64(i64 value) {
        return Callbacks.OnInteger(value);
    }

    bool Uint64(ui64 value) {
        if (value <= ui64(Max<i64>())) {
            return Callbacks.OnInteger(i64(value));
        }
        return Callbacks.OnUInteger(value);
    }

    bool Double(double value) {
        return Callbacks.OnDouble(value);
    }

    bool RawNumber(const char*, rapidjson::SizeType, bool) {
        return false;
    }

    bool String(const char* value, rapidjson::SizeType length, bool) {
        return Callbacks.OnString(TStringBuf(value, length));
    }

    bool StartObject() {
        return Callbacks.OnOpenMap();
    }

    bool Key(const char* value, rapidjson::SizeType length, bool) {
        return Callbacks.OnMapKey(TStringBuf(value, length));
    }

    bool EndObject(rapidjson::SizeType) {
        return Callbacks.OnCloseMap();
    }

    bool StartArray() {
        return Callbacks.OnOpenArray();
    }

    bool EndArray(rapidjson::SizeType) {
        return Callbacks.OnCloseArray();
    }

private:
    NJson::TJsonCallbacks& Callbacks;
};

class TSchemaKeyParser: public NJson::TParserCallbacks {
public:
    explicit TSchemaKeyParser(NJson::TJsonValue& value)
        : TParserCallbacks(value)
    {
    }

    bool OnOpenMap() override {
        const bool isSchemaMap = CurrentState == AFTER_MAP_KEY && (Key == "columnFamilies" || Key == "indexes");
        const TString mapKind = isSchemaMap ? Key : TString();
        if (!TParserCallbacks::OnOpenMap()) {
            return false;
        }

        if (isSchemaMap) {
            SchemaMaps.push_back(ValuesStack.back());
            MapKind = mapKind;
        }

        return true;
    }

    bool OnMapKey(const TStringBuf& key) override {
        if (!SchemaMaps.empty() && ValuesStack.back()->Has(key)) {
            if (SchemaMaps.back() == ValuesStack.back()) {
                DuplicateName = key;
            } else {
                DuplicateSetting = key;
            }
            return false;
        }

        return TParserCallbacks::OnMapKey(key);
    }

    bool OnCloseMap() override {
        if (!SchemaMaps.empty() && SchemaMaps.back() == ValuesStack.back()) {
            SchemaMaps.pop_back();
        }

        return TParserCallbacks::OnCloseMap();
    }

    TString DuplicateName;
    TString DuplicateSetting;
    TString MapKind;

private:
    TVector<const NJson::TJsonValue*> SchemaMaps;
};

} // namespace

static bool ValidateYsonMetadata(const NJson::TJsonValue& json, TString& error) {
    if (!json.Has("yson")) {
        return true;
    }

    error = "invalid YSON metadata";
    if (!json["yson"].IsMap()) {
        return false;
    }

    for (const auto& [section, columns] : json["yson"].GetMap()) {
        const bool knownSection = section == "update" || section == "reset"
            || section == "oldImage" || section == "newImage";
        if (!knownSection || !columns.IsMap() || !json[section].IsMap()) {
            return false;
        }

        for (const auto& [column, value] : columns.GetMap()) {
            if (!json[section].Has(column) || !value.IsString()) {
                return false;
            }

            try {
                const auto bytes = Base64StrictDecode(value.GetString());
                const TCell cell(bytes.data(), bytes.size());
                if (bytes.empty() || !NFormats::CheckCellValue(cell, NScheme::TTypeInfo(NScheme::NTypeIds::Yson))) {
                    return false;
                }
            } catch (const yexception&) {
                return false;
            }
        }
    }

    error.clear();
    return true;
}

bool TChangeRecordBuilder::ParseJsonBody(TStringBuf body, NJson::TJsonValue& json, TString& error) {
    NJson::TStringBufStreamWrapper input(body);
    TSchemaKeyParser parser(json);
    TRapidJsonCallbacks callbacks(parser);
    rapidjson::Reader reader;
    // CDC emits enough digits to recover the original double exactly.
    constexpr auto flags = rapidjson::kParseValidateEncodingFlag | rapidjson::kParseFullPrecisionFlag;
    if (reader.Parse<flags>(input, callbacks) && parser.OnEnd()) {
        return ValidateYsonMetadata(json, error);
    }

    if (parser.DuplicateName) {
        error = TStringBuilder() << (parser.MapKind == "indexes" ? "duplicate index: " : "duplicate column family: ")
            << parser.DuplicateName;
    } else if (parser.DuplicateSetting) {
        error = TStringBuilder() << (parser.MapKind == "indexes" ? "duplicate index setting: " : "duplicate column family setting: ")
            << parser.DuplicateSetting;
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
        const NJson::TJsonValue::TMapType& value, const NJson::TJsonValue& ysonValues,
        TLightweightSchema::TCPtr schema, TMemoryPool& pool, TString& error)
{
    tags.reserve(value.size());
    cells.reserve(value.size());

    for (const auto& [column, value] : value) {
        auto it = schema->ValueColumns.find(column);
        Y_ABORT_UNLESS(it != schema->ValueColumns.end());

        tags.push_back(it->second.Tag);
        if (it->second.Type.GetTypeId() == NScheme::NTypeIds::Yson && ysonValues.Has(column)) {
            // ParseJsonBody validated the base64 and YSON before the writer
            // accepted this record. An absent entry uses the legacy format.
            const auto bytes = Base64StrictDecode(ysonValues[column].GetString());
            cells.emplace_back(pool.Append(bytes.data(), bytes.size()), bytes.size());
            continue;
        }

        if (!NFormats::MakeCell(cells.emplace_back(), value, it->second.Type, pool, error)) {
            return false;
        }
    }

    return true;
}

static bool TransformValue(NKikimrTxDataShard::TEvApplyReplicationChanges::TUpdates& to,
        const NJson::TJsonValue::TMapType& from, const NJson::TJsonValue& ysonValues,
        TLightweightSchema::TCPtr schema, TMemoryPool& pool, TString& error)
{
    TVector<NTable::TTag> tags;
    TVector<TCell> cells;
    if (!ParseValue(tags, cells, from, ysonValues, schema, pool, error)) {
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
        auto res = TransformValue(*record.MutableUpsert(), JsonBody["update"].GetMap(), JsonBody["yson"]["update"], Schema, pool, error);
        Y_ABORT_UNLESS(res, "Cannot transform value: %s", error.c_str());
    } else if (JsonBody.Has("reset") && JsonBody["reset"].IsMap()) {
        auto res = TransformValue(*record.MutableReset(), JsonBody["reset"].GetMap(), JsonBody["yson"]["reset"], Schema, pool, error);
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

            if (definition.Has("compressionLevel")) {
                if (compression != NKikimrReplication::TSchemaChange::TFamily::COMPRESSION_ZSTD) {
                    error = TStringBuilder() << "compression level in column family " << name
                        << " is only supported for ZSTD";
                    return false;
                }
                const auto& level = definition["compressionLevel"];
                if (!level.IsInteger() && !level.IsUInteger()) {
                    error = TStringBuilder() << "compression level in column family " << name << " must be an integer";
                    return false;
                }
                const auto maxLevel = static_cast<ui64>(ZSTD_maxCLevel());
                if (!level.IsUInteger() || level.GetUInteger() > maxLevel) {
                    error = TStringBuilder() << "compression level in column family " << name
                        << " must be between 0 and " << maxLevel << ": " << level.GetStringRobust();
                    return false;
                }
            }

            for (const auto& [setting, value] : definition.GetMap()) {
                if (!IsSupportedSetting(setting)) {
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
            if (definition.Has("compressionLevel")) {
                family->SetCompressionLevel(definition["compressionLevel"].GetUInteger());
            }
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

    if (table.Has("indexes")) {
        if (!table["indexes"].IsMap()) {
            error = "schema record has invalid indexes";
            return false;
        }

        auto* indexes = schema.MutableIndexes();
        TVector<TString> names;
        for (const auto& [name, definition] : table["indexes"].GetMap()) {
            if (name.empty() || !definition.IsMap() || !definition["type"].IsString()
                || definition["type"].GetString().empty()
                || !definition["indexColumns"].IsArray() || definition["indexColumns"].GetArray().empty()
                || !definition["dataColumns"].IsArray())
            {
                error = TStringBuilder() << "schema record has invalid index: " << name;
                return false;
            }

            THashSet<TString> indexColumns;
            for (const auto field : {"indexColumns", "dataColumns"}) {
                for (const auto& column : definition[field].GetArray()) {
                    if (!column.IsString() || !columns.contains(column.GetString())
                        || !indexColumns.insert(column.GetString()).second)
                    {
                        error = TStringBuilder() << "schema record index '" << name << "' has invalid columns";
                        return false;
                    }
                }
            }
            names.push_back(name);
        }

        Sort(names);
        for (const auto& name : names) {
            const auto& definition = table["indexes"][name];
            auto* index = indexes->AddItems();
            index->SetName(name);
            index->SetType(definition["type"].GetString());
            for (const auto& column : definition["indexColumns"].GetArray()) {
                index->AddIndexColumns(column.GetString());
            }
            for (const auto& column : definition["dataColumns"].GetArray()) {
                index->AddDataColumns(column.GetString());
            }
        }
    }

    return true;
}

}
