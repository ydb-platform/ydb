#include "table_query.h"

#include <ydb/services/udf_store/metadata_subscription/wasm_artifact.h>

#include <util/string/builder.h>
#include <util/string/subst.h>

namespace NKikimr::NUdfApi::NQuery {

namespace {

using NUdfStore::ECompileStatus;
using NUdfStore::EUdfType;
using NUdfStore::TUdfModule;

Ydb::TypedValue MakeUtf8Param(const TString& value) {
    Ydb::TypedValue result;
    result.mutable_type()->set_type_id(Ydb::Type::UTF8);
    result.mutable_value()->set_text_value(value);
    return result;
}

Ydb::TypedValue MakeJsonParam(const TString& value) {
    Ydb::TypedValue result;
    result.mutable_type()->set_type_id(Ydb::Type::JSON);
    result.mutable_value()->set_text_value(value);
    return result;
}

Ydb::TypedValue MakeStringParam(const TString& value) {
    Ydb::TypedValue result;
    result.mutable_type()->set_type_id(Ydb::Type::STRING);
    result.mutable_value()->set_bytes_value(value);
    return result;
}

Ydb::TypedValue MakeUint64Param(ui64 value) {
    Ydb::TypedValue result;
    result.mutable_type()->set_type_id(Ydb::Type::UINT64);
    result.mutable_value()->set_uint64_value(value);
    return result;
}

Ydb::TypedValue MakeBoolParam(bool value) {
    Ydb::TypedValue result;
    result.mutable_type()->set_type_id(Ydb::Type::BOOL);
    result.mutable_value()->set_bool_value(value);
    return result;
}

TString EscapeTablePath(const TString& tablePath) {
    // YQL backtick-quoted identifiers escape ` by doubling.
    TString escaped = tablePath;
    SubstGlobal(escaped, "`", "``");
    return escaped;
}

i32 FindColumnIndex(const Ydb::ResultSet& resultSet, TStringBuf columnName) {
    for (i32 i = 0; i < resultSet.columns_size(); ++i) {
        if (resultSet.columns(i).name() == columnName) {
            return i;
        }
    }
    return -1;
}

const Ydb::Value* FindItem(const Ydb::ResultSet& resultSet, const Ydb::Value& row, TStringBuf columnName) {
    const i32 columnIdx = FindColumnIndex(resultSet, columnName);
    if (columnIdx < 0 || columnIdx >= row.items_size()) {
        return nullptr;
    }
    return &row.items(columnIdx);
}

void ReadText(const Ydb::ResultSet& resultSet, const Ydb::Value& row, TStringBuf columnName, TString& value) {
    const auto* item = FindItem(resultSet, row, columnName);
    if (item && item->has_text_value()) {
        value = item->text_value();
    }
}

void ReadUint64(const Ydb::ResultSet& resultSet, const Ydb::Value& row, TStringBuf columnName, ui64& value) {
    const auto* item = FindItem(resultSet, row, columnName);
    if (item && item->has_uint64_value()) {
        value = item->uint64_value();
    }
}

void ReadBool(const Ydb::ResultSet& resultSet, const Ydb::Value& row, TStringBuf columnName, bool& value) {
    const auto* item = FindItem(resultSet, row, columnName);
    if (item && item->has_bool_value()) {
        value = item->bool_value();
    }
}

void ReadInstant(const Ydb::ResultSet& resultSet, const Ydb::Value& row, TStringBuf columnName, TInstant& value) {
    const auto* item = FindItem(resultSet, row, columnName);
    if (item && item->has_uint64_value()) {
        value = TInstant::MicroSeconds(item->uint64_value());
    }
}

const TString& ModuleColumnList() {
    static const TString value =
        "name, uid, md5, size, type, version, chunk_count,"
        " compile_status, compile_error, manifest, created_at, compile_finished_at";
    return value;
}

bool ExtractResultSet(const Ydb::Table::ExecuteDataQueryResponse& response, Ydb::ResultSet& resultSet) {
    if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
        return false;
    }
    if (!response.operation().result().Is<Ydb::Table::ExecuteQueryResult>()) {
        return false;
    }
    Ydb::Table::ExecuteQueryResult result;
    response.operation().result().UnpackTo(&result);
    if (result.result_sets_size() == 0) {
        return false;
    }
    resultSet = result.result_sets(0);
    return true;
}

void ReadModuleRow(const Ydb::ResultSet& resultSet, const Ydb::Value& rawRow, TModuleRow& row) {
    ReadText(resultSet, rawRow, TUdfModule::NameColName, row.Name);
    ReadText(resultSet, rawRow, TUdfModule::UidColName, row.Uid);
    ReadText(resultSet, rawRow, TUdfModule::Md5ColName, row.Md5);
    ReadUint64(resultSet, rawRow, TUdfModule::SizeColName, row.Size);
    TString type;
    ReadText(resultSet, rawRow, TUdfModule::TypeColName, type);
    TUdfModule::TypeFromString(type, row.Type);
    ReadUint64(resultSet, rawRow, TUdfModule::VersionColName, row.Version);
    ReadUint64(resultSet, rawRow, TUdfModule::ChunkCountColName, row.ChunkCount);
    TString compileStatus;
    ReadText(resultSet, rawRow, TUdfModule::CompileStatusColName, compileStatus);
    TUdfModule::CompileStatusFromString(compileStatus, row.CompileStatus);
    ReadText(resultSet, rawRow, TUdfModule::CompileErrorColName, row.CompileError);
    ReadText(resultSet, rawRow, TUdfModule::ManifestColName, row.Manifest);
    ReadInstant(resultSet, rawRow, TUdfModule::CreatedAtColName, row.CreatedAt);
    ReadInstant(resultSet, rawRow, TUdfModule::CompileFinishedAtColName, row.CompileFinishedAt);
}

} // namespace

TString BuildSelectModuleByNameQuery(const TString& modulesTablePath) {
    return TStringBuilder()
        << "DECLARE $name AS Utf8; "
        << "SELECT " << ModuleColumnList() << " FROM `"
        << EscapeTablePath(modulesTablePath)
        << "` WHERE name = $name;";
}

void SetSelectModuleByNameParams(Ydb::Table::ExecuteDataQueryRequest& request, const TString& name) {
    (*request.mutable_parameters())["$name"] = MakeUtf8Param(name);
}

bool ParseModuleRowResponse(const Ydb::Table::ExecuteDataQueryResponse& response, TModuleRow& row) {
    Ydb::ResultSet resultSet;
    if (!ExtractResultSet(response, resultSet) || resultSet.rows().empty()) {
        return false;
    }
    ReadModuleRow(resultSet, resultSet.rows(0), row);
    return true;
}

TString BuildListModulesQuery(const TString& modulesTablePath, const TListFilter& filter) {
    TStringBuilder query;
    query << "DECLARE $offset AS Uint64; DECLARE $limit AS Uint64; ";
    if (filter.Type) {
        query << "DECLARE $type AS Utf8; ";
    }
    if (filter.CompileStatus) {
        query << "DECLARE $compile_status AS Utf8; ";
    }
    query << "SELECT " << ModuleColumnList() << " FROM `" << EscapeTablePath(modulesTablePath) << "`";
    TVector<TString> predicates = {"type IN (\"WASM\", \"LIBRARY\")"};
    if (filter.Type) {
        predicates.push_back("type = $type");
    }
    if (filter.CompileStatus) {
        predicates.push_back("compile_status = $compile_status");
    }
    for (size_t i = 0; i < predicates.size(); ++i) {
        query << (i == 0 ? " WHERE " : " AND ") << predicates[i];
    }
    // Ordering is what makes the offset in a page token mean anything: without
    // it the next page could repeat or skip rows the caller has already seen.
    query << " ORDER BY name LIMIT $limit OFFSET $offset;";
    return query;
}

void SetListModulesParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TListFilter& filter,
    ui64 offset,
    ui64 limit)
{
    (*request.mutable_parameters())["$offset"] = MakeUint64Param(offset);
    (*request.mutable_parameters())["$limit"] = MakeUint64Param(limit);
    if (filter.Type) {
        (*request.mutable_parameters())["$type"] = MakeUtf8Param(TUdfModule::TypeToString(*filter.Type));
    }
    if (filter.CompileStatus) {
        (*request.mutable_parameters())["$compile_status"] =
            MakeUtf8Param(TUdfModule::CompileStatusToString(*filter.CompileStatus));
    }
}

bool ParseModuleRowsResponse(const Ydb::Table::ExecuteDataQueryResponse& response, TVector<TModuleRow>& rows) {
    Ydb::ResultSet resultSet;
    if (!ExtractResultSet(response, resultSet)) {
        return false;
    }
    rows.clear();
    rows.reserve(resultSet.rows().size());
    for (const auto& rawRow : resultSet.rows()) {
        ReadModuleRow(resultSet, rawRow, rows.emplace_back());
    }
    return true;
}

TString BuildFlipModuleQuery(const TString& modulesTablePath, bool withManifest) {
    const TString table = EscapeTablePath(modulesTablePath);
    TStringBuilder query;
    query << "DECLARE $name AS Utf8; "
          << "DECLARE $uid AS Utf8; "
          << "DECLARE $md5 AS Utf8; "
          << "DECLARE $size AS Uint64; "
          << "DECLARE $type AS Utf8; "
          << "DECLARE $version AS Uint64; "
          << "DECLARE $version_given AS Bool; "
          << "DECLARE $chunk_count AS Uint64; "
          << "DECLARE $compile_status AS Utf8; "
          << "DECLARE $expected_uid AS Utf8; "
          << "DECLARE $require_uid AS Bool; "
          << "DECLARE $require_absent AS Bool; "
          << "DECLARE $require_present AS Bool; ";
    if (withManifest) {
        query << "DECLARE $manifest AS Json; ";
    }
    query << "$cur = SELECT uid AS uid, type AS type, version AS version, created_at AS created_at"
          << " FROM `" << table << "` WHERE name = $name; "
          << "$cur_uid = SELECT uid FROM $cur; "
          << "$cur_type = SELECT type FROM $cur; "
          << "$cur_version = SELECT version FROM $cur; "
          << "$cur_created_at = SELECT created_at FROM $cur; "
          << "$existed = $cur_uid IS NOT NULL; "
          << "$applied = COALESCE("
          << "NOT ($existed AND $require_absent)"
          << " AND NOT (NOT $existed AND $require_present)"
          << " AND (NOT $require_uid OR ($existed AND $cur_uid == $expected_uid))"
          // A name identifies the module whatever its type, so replacing a
          // library with a UDF under that name would leave the artifacts of the
          // old kind behind with nobody to collect them.
          << " AND (NOT $existed OR $cur_type == $type), false); "
          // The pre-state is read by the transaction that writes, so the status
          // the caller gets describes the very row the condition looked at.
          << "SELECT $existed AS existed, $applied AS applied,"
          << " $cur_uid AS cur_uid, $cur_type AS cur_type; "
          << "$row = SELECT $name AS name, $uid AS uid, $md5 AS md5, $size AS size, $type AS type,"
          // The module keeps the version it was uploaded with unless this
          // upload names one of its own.
          << " IF($version_given, $version, COALESCE($cur_version, CAST(1 AS Uint64))) AS version,"
          << " $chunk_count AS chunk_count, $compile_status AS compile_status,"
          << " CAST(\"\" AS Utf8) AS compile_error,"
          // created_at belongs to the module, the compile timestamps to an
          // artifact of the uid this row no longer points at.
          << " COALESCE($cur_created_at, CurrentUtcTimestamp()) AS created_at,"
          << " Nothing(Timestamp?) AS compile_started_at,"
          << " Nothing(Timestamp?) AS compile_finished_at";
    if (withManifest) {
        query << ", $manifest AS manifest";
    }
    query << "; UPSERT INTO `" << table << "` SELECT * FROM $row WHERE $applied;";
    return query;
}

void SetFlipModuleParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TModuleRow& row,
    const TWriteConditions& conditions,
    bool versionGiven,
    bool withManifest)
{
    auto& params = *request.mutable_parameters();
    params["$name"] = MakeUtf8Param(row.Name);
    params["$uid"] = MakeUtf8Param(row.Uid);
    params["$md5"] = MakeUtf8Param(row.Md5);
    params["$size"] = MakeUint64Param(row.Size);
    params["$type"] = MakeUtf8Param(TUdfModule::TypeToString(row.Type));
    params["$version"] = MakeUint64Param(row.Version);
    params["$version_given"] = MakeBoolParam(versionGiven);
    params["$chunk_count"] = MakeUint64Param(row.ChunkCount);
    params["$compile_status"] = MakeUtf8Param(TUdfModule::CompileStatusToString(row.CompileStatus));
    params["$expected_uid"] = MakeUtf8Param(conditions.ExpectedUid);
    params["$require_uid"] = MakeBoolParam(!conditions.ExpectedUid.empty());
    params["$require_absent"] = MakeBoolParam(conditions.RequireAbsent);
    params["$require_present"] = MakeBoolParam(conditions.RequirePresent);
    if (withManifest) {
        params["$manifest"] = MakeJsonParam(row.Manifest);
    }
}

TString BuildDeleteModuleQuery(const TString& modulesTablePath, const TString& moduleChunksTablePath) {
    return TStringBuilder()
        << "DECLARE $name AS Utf8; "
        << "DECLARE $expected_uid AS Utf8; "
        << "DECLARE $require_uid AS Bool; "
        << "DECLARE $type AS Utf8; "
        << "DECLARE $require_type AS Bool; "
        << "DECLARE $type_wasm AS Utf8; "
        << "DECLARE $type_library AS Utf8; "
        << "$cur = SELECT uid AS uid, type AS type FROM `" << EscapeTablePath(modulesTablePath)
        << "` WHERE name = $name; "
        << "$cur_uid = SELECT uid FROM $cur; "
        << "$cur_type = SELECT type FROM $cur; "
        << "$existed = $cur_uid IS NOT NULL; "
        << "$applied = COALESCE($existed"
        << " AND (NOT $require_uid OR $cur_uid == $expected_uid)"
        << " AND (NOT $require_type OR $cur_type == $type)"
        // Legacy native modules predate this API and nothing else here writes a
        // type, so a row of any other type is not ours to remove either,
        // whatever kind the caller says they are deleting.
        << " AND ($cur_type == $type_wasm OR $cur_type == $type_library), false); "
        << "SELECT $existed AS existed, $applied AS applied,"
        << " $cur_uid AS cur_uid, $cur_type AS cur_type; "
        // The uid the condition matched is part of the delete, so a re-upload
        // that lands first keeps its row: this transaction then finds the uid
        // changed and aborts instead of dropping content it never saw.
        << "DELETE FROM `" << EscapeTablePath(modulesTablePath) << "` WHERE name = $name AND $applied; "
        << "DELETE FROM `" << EscapeTablePath(moduleChunksTablePath)
        << "` WHERE owner_key = $cur_uid AND $applied;";
}

void SetDeleteModuleParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& name,
    const TString& expectedUid,
    TMaybe<EUdfType> requiredType)
{
    auto& params = *request.mutable_parameters();
    params["$name"] = MakeUtf8Param(name);
    params["$expected_uid"] = MakeUtf8Param(expectedUid);
    params["$require_uid"] = MakeBoolParam(!expectedUid.empty());
    params["$type"] = MakeUtf8Param(requiredType ? TUdfModule::TypeToString(*requiredType) : TString());
    params["$require_type"] = MakeBoolParam(requiredType.Defined());
    params["$type_wasm"] = MakeUtf8Param(TUdfModule::TypeToString(EUdfType::WASM));
    params["$type_library"] = MakeUtf8Param(TUdfModule::TypeToString(EUdfType::LIBRARY));
}

bool ParseModulePreStateResponse(
    const Ydb::Table::ExecuteDataQueryResponse& response,
    TModulePreState& preState)
{
    Ydb::ResultSet resultSet;
    if (!ExtractResultSet(response, resultSet) || resultSet.rows().empty()) {
        return false;
    }
    const auto& row = resultSet.rows(0);
    ReadBool(resultSet, row, "existed", preState.Existed);
    ReadBool(resultSet, row, "applied", preState.Applied);
    ReadText(resultSet, row, "cur_uid", preState.Uid);
    ReadText(resultSet, row, "cur_type", preState.TypeName);
    preState.TypeKnown = TUdfModule::TypeFromString(preState.TypeName, preState.Type);
    return true;
}

TString BuildDeleteSourceChunksQuery(const TString& moduleChunksTablePath) {
    return TStringBuilder()
        << "DECLARE $owner_key AS Utf8; "
        << "DELETE FROM `" << EscapeTablePath(moduleChunksTablePath) << "` WHERE owner_key = $owner_key;";
}

void SetDeleteSourceChunksParams(Ydb::Table::ExecuteDataQueryRequest& request, const TString& ownerKey) {
    (*request.mutable_parameters())["$owner_key"] = MakeUtf8Param(ownerKey);
}

TString BuildUpsertSourceChunkQuery(const TString& moduleChunksTablePath) {
    return TStringBuilder()
        << "DECLARE $owner_key AS Utf8; "
        << "DECLARE $chunk_idx AS Uint64; "
        << "DECLARE $data AS String; "
        << "UPSERT INTO `" << EscapeTablePath(moduleChunksTablePath)
        << "` (owner_key, chunk_idx, data) VALUES ($owner_key, $chunk_idx, $data);";
}

void SetUpsertSourceChunkParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& ownerKey,
    ui64 chunkIdx,
    const TString& data)
{
    auto& params = *request.mutable_parameters();
    params["$owner_key"] = MakeUtf8Param(ownerKey);
    params["$chunk_idx"] = MakeUint64Param(chunkIdx);
    params["$data"] = MakeStringParam(data);
}

TString BuildSelectArtifactReadyQuery(const TString& artifactTablePath) {
    return TStringBuilder()
        << "DECLARE $id AS Utf8; "
        << "DECLARE $kind AS Utf8; "
        << "DECLARE $uid AS Utf8; "
        << "SELECT object_code_chunk_count FROM `" << EscapeTablePath(artifactTablePath)
        << "` WHERE id = $id AND kind = $kind AND uid = $uid;";
}

void SetSelectArtifactReadyParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& id,
    const TString& kind,
    const TString& uid)
{
    auto& params = *request.mutable_parameters();
    params["$id"] = MakeUtf8Param(id);
    params["$kind"] = MakeUtf8Param(kind);
    params["$uid"] = MakeUtf8Param(uid);
}

bool ParseArtifactReadyResponse(const Ydb::Table::ExecuteDataQueryResponse& response, bool& ready) {
    Ydb::ResultSet resultSet;
    if (!ExtractResultSet(response, resultSet)) {
        return false;
    }
    ready = false;
    if (resultSet.rows().empty()) {
        return true;
    }
    ui64 chunkCount = 0;
    ReadUint64(resultSet, resultSet.rows(0), "object_code_chunk_count", chunkCount);
    // A row whose object code is not written yet belongs to a compile still in
    // flight, which is the same thing as not being ready to the caller.
    ready = chunkCount > 0;
    return true;
}

TString BuildDeleteArtifactsByIdQuery(const TString& tablePath) {
    return TStringBuilder()
        << "DECLARE $id AS Utf8; "
        << "DECLARE $kind AS Utf8; "
        << "DELETE FROM `" << EscapeTablePath(tablePath) << "` WHERE id = $id AND kind = $kind;";
}

void SetDeleteArtifactsByIdParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& id,
    const TString& kind)
{
    auto& params = *request.mutable_parameters();
    params["$id"] = MakeUtf8Param(id);
    params["$kind"] = MakeUtf8Param(kind);
}

TString ArtifactKindFor(EUdfType type) {
    return NUdfStore::WasmArtifactKindToString(
        type == EUdfType::LIBRARY ? NUdfStore::EWasmArtifactKind::Library : NUdfStore::EWasmArtifactKind::Module);
}

} // namespace NKikimr::NUdfApi::NQuery
