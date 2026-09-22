#include "table_query.h"

#include "blob_chunks.h"
#include "metadata_subscription/udf_module.h"

#include <ydb/services/metadata/manager/ydb_value_operator.h>

#include <google/protobuf/any.pb.h>

#include <util/string/builder.h>
#include <util/string/subst.h>

namespace NKikimr::NUdfStore::NTableQuery {

namespace {

using namespace NMetadata::NInternal;

Ydb::TypedValue MakeUtf8Param(const TString& value) {
    Ydb::TypedValue result;
    result.mutable_type()->set_type_id(Ydb::Type::UTF8);
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

TString EscapeTablePath(const TString& tablePath) {
    // YQL backtick-quoted identifiers escape ` by doubling.
    TString escaped = tablePath;
    SubstGlobal(escaped, "`", "``");
    return escaped;
}

i32 FindColumnIndex(const Ydb::ResultSet& resultSet, const TString& columnName) {
    for (i32 i = 0; i < resultSet.columns_size(); ++i) {
        if (resultSet.columns(i).name() == columnName) {
            return i;
        }
    }
    return -1;
}

bool ReadUtf8Column(const Ydb::ResultSet& resultSet, const TString& columnName, TString& value) {
    if (resultSet.rows().empty()) {
        return false;
    }
    const i32 columnIdx = FindColumnIndex(resultSet, columnName);
    if (columnIdx < 0) {
        return false;
    }
    const auto& row = resultSet.rows(0);
    if (columnIdx >= row.items_size()) {
        return false;
    }
    const auto& item = row.items(columnIdx);
    if (!item.has_text_value()) {
        return false;
    }
    value = item.text_value();
    return true;
}

bool ReadUint64Column(const Ydb::ResultSet& resultSet, const TString& columnName, ui64& value) {
    if (resultSet.rows().empty()) {
        return false;
    }
    const i32 columnIdx = FindColumnIndex(resultSet, columnName);
    if (columnIdx < 0) {
        return false;
    }
    const auto& row = resultSet.rows(0);
    if (columnIdx >= row.items_size()) {
        return false;
    }
    const auto& item = row.items(columnIdx);
    if (!item.has_uint64_value()) {
        return false;
    }
    value = item.uint64_value();
    return true;
}

bool AppendChunksResultSet(const Ydb::ResultSet& resultSet, TVector<TString>& chunks) {
    if (resultSet.truncated() || static_cast<ui64>(resultSet.rows_size()) > ChunksPerRead) {
        return false;
    }
    const i32 chunkIdxCol = FindColumnIndex(resultSet, "chunk_idx");
    const i32 dataCol = FindColumnIndex(resultSet, "data");
    if (chunkIdxCol < 0 || dataCol < 0) {
        return false;
    }

    // Validate the whole page before appending anything. ORDER BY chunk_idx
    // makes the accumulated count the cursor for the next page.
    ui64 expectedIdx = chunks.size();
    for (const auto& row : resultSet.rows()) {
        if (chunkIdxCol >= row.items_size() || dataCol >= row.items_size()) {
            return false;
        }
        const auto& idxItem = row.items(chunkIdxCol);
        const auto& dataItem = row.items(dataCol);
        if (!idxItem.has_uint64_value() || !dataItem.has_bytes_value()
            || idxItem.uint64_value() != expectedIdx++)
        {
            return false;
        }
    }
    for (const auto& row : resultSet.rows()) {
        chunks.push_back(row.items(dataCol).bytes_value());
    }
    return true;
}

} // namespace

bool ExtractQueryResult(
    const Ydb::Table::ExecuteDataQueryResponse& response,
    Ydb::Table::ExecuteQueryResult& result)
{
    if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
        return false;
    }
    if (!response.operation().result().Is<Ydb::Table::ExecuteQueryResult>()) {
        return false;
    }
    response.operation().result().UnpackTo(&result);
    return result.result_sets_size() > 0;
}

TString BuildSelectModuleByNameQuery(const TString& tablePath) {
    return TStringBuilder()
        << "DECLARE $name AS Utf8; "
        << "DECLARE $type AS Utf8; "
        << "SELECT uid, md5, name, type, version, size, chunk_count, manifest FROM `"
        << EscapeTablePath(tablePath)
        << "` WHERE name = $name AND type = $type;";
}

void SetSelectModuleByNameParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& name,
    const TString& type)
{
    (*request.mutable_parameters())["$name"] = MakeUtf8Param(name);
    (*request.mutable_parameters())["$type"] = MakeUtf8Param(type);
}

bool ParseModuleSourceResponse(const Ydb::Table::ExecuteDataQueryResponse& response, TModuleSourceRow& row) {
    Ydb::Table::ExecuteQueryResult result;
    if (!ExtractQueryResult(response, result)) {
        return false;
    }
    const auto& resultSet = result.result_sets(0);
    if (resultSet.rows().empty()) {
        return false;
    }
    if (!ReadUtf8Column(resultSet, "uid", row.Uid)) {
        return false;
    }
    ReadUtf8Column(resultSet, "md5", row.Md5);
    ReadUtf8Column(resultSet, "name", row.Name);
    ReadUtf8Column(resultSet, "type", row.Type);
    ReadUint64Column(resultSet, "version", row.Version);
    ReadUint64Column(resultSet, "size", row.Size);
    ReadUint64Column(resultSet, "chunk_count", row.ChunkCount);
    ReadUtf8Column(resultSet, "manifest", row.Manifest);
    return true;
}

TString BuildSelectSourceChunksQuery(const TString& tablePath) {
    return TStringBuilder()
        << "DECLARE $owner_key AS Utf8; "
        << "DECLARE $first_chunk AS Uint64; "
        << "SELECT chunk_idx, data FROM `"
        << EscapeTablePath(tablePath)
        << "` WHERE owner_key = $owner_key AND chunk_idx >= $first_chunk "
        << "ORDER BY chunk_idx LIMIT " << ChunksPerRead << ";";
}

void SetSelectSourceChunksParams(Ydb::Table::ExecuteDataQueryRequest& request, const TString& ownerKey, ui64 firstChunk) {
    (*request.mutable_parameters())["$owner_key"] = MakeUtf8Param(ownerKey);
    (*request.mutable_parameters())["$first_chunk"] = MakeUint64Param(firstChunk);
}

bool AppendSourceChunksResponse(const Ydb::Table::ExecuteDataQueryResponse& response, TVector<TString>& chunks) {
    Ydb::Table::ExecuteQueryResult result;
    if (!ExtractQueryResult(response, result)) {
        return false;
    }
    return AppendChunksResultSet(result.result_sets(0), chunks);
}

TString BuildSelectArtifactQuery(const TString& tablePath) {
    return TStringBuilder()
        << "DECLARE $id AS Utf8; "
        << "DECLARE $kind AS Utf8; "
        << "DECLARE $uid AS Utf8; "
        << "SELECT id, kind, uid, version, format, "
        << "wasm_data_size, wasm_data_chunk_count, object_code_size, object_code_chunk_count FROM `"
        << EscapeTablePath(tablePath)
        << "` WHERE id = $id AND kind = $kind AND uid = $uid;";
}

TString BuildEnsurePendingArtifactQuery(const TString& tablePath) {
    const auto path = EscapeTablePath(tablePath);
    return TStringBuilder()
        << "DECLARE $id AS Utf8; DECLARE $kind AS Utf8; DECLARE $uid AS Utf8; "
        << "$existing = SELECT id FROM `" << path
        << "` WHERE id = $id AND kind = $kind AND uid = $uid; "
        << "$pending = SELECT $id AS id, $kind AS kind, $uid AS uid, CAST('pending' AS Utf8) AS compile_status, "
        << "CAST('' AS Utf8) AS compile_error; "
        << "INSERT INTO `" << path << "` (id, kind, uid, compile_status, compile_error) "
        << "SELECT id, kind, uid, compile_status, compile_error FROM $pending "
        << "WHERE NOT EXISTS (SELECT id FROM $existing);";
}

TString BuildMarkArtifactCompilingQuery(const TString& tablePath) {
    return TStringBuilder()
        << "DECLARE $id AS Utf8; DECLARE $kind AS Utf8; DECLARE $uid AS Utf8; "
        << "UPDATE `" << EscapeTablePath(tablePath)
        << "` SET compile_status = CAST('compiling' AS Utf8), compile_error = CAST('' AS Utf8), "
        << "compile_started_at = CurrentUtcTimestamp(), compile_finished_at = NULL "
        << "WHERE id = $id AND kind = $kind AND uid = $uid "
        << "AND compile_status IN ('pending', 'failed');";
}

TString BuildMarkArtifactFailedQuery(const TString& tablePath) {
    return TStringBuilder()
        << "DECLARE $id AS Utf8; DECLARE $kind AS Utf8; DECLARE $uid AS Utf8; "
        << "DECLARE $error AS Utf8; "
        << "UPDATE `" << EscapeTablePath(tablePath)
        << "` SET compile_status = CAST('failed' AS Utf8), compile_error = $error, "
        << "compile_finished_at = CurrentUtcTimestamp() "
        << "WHERE id = $id AND kind = $kind AND uid = $uid AND compile_status = 'compiling';";
}

void SetMarkArtifactFailedParams(Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& id, const TString& kind, const TString& uid, const TString& error)
{
    SetSelectArtifactParams(request, id, kind, uid);
    (*request.mutable_parameters())["$error"] = MakeUtf8Param(error);
}

TString BuildSelectArtifactCompileStateQuery(const TString& tablePath) {
    return TStringBuilder()
        << "DECLARE $id AS Utf8; DECLARE $kind AS Utf8; DECLARE $uid AS Utf8; "
        << "SELECT compile_status, compile_error, compile_started_at, compile_finished_at FROM `"
        << EscapeTablePath(tablePath) << "` WHERE id = $id AND kind = $kind AND uid = $uid;";
}

bool ParseArtifactCompileStateResponse(const Ydb::Table::ExecuteDataQueryResponse& response,
    TMaybe<TArtifactCompileState>& state)
{
    Ydb::Table::ExecuteQueryResult result;
    if (!ExtractQueryResult(response, result)) {
        return false;
    }
    const auto& rows = result.result_sets(0);
    if (rows.truncated() || rows.rows_size() > 1) {
        return false;
    }
    if (rows.rows().empty()) {
        state.Clear();
        return true;
    }
    TArtifactCompileState parsed;
    TString status;
    if (!ReadUtf8Column(rows, "compile_status", status)
        || !TUdfModule::CompileStatusFromString(status, parsed.Status)
        || !ReadUtf8Column(rows, "compile_error", parsed.Error))
    {
        return false;
    }
    auto readTimestamp = [&](const TString& name, TMaybe<TInstant>& value) {
        const i32 index = FindColumnIndex(rows, name);
        if (index < 0 || index >= rows.rows(0).items_size()) {
            return false;
        }
        const auto& item = rows.rows(0).items(index);
        if (item.has_null_flag_value()) {
            return true;
        }
        if (!item.has_uint64_value()) {
            return false;
        }
        value = TInstant::MicroSeconds(item.uint64_value());
        return true;
    };
    if (!readTimestamp("compile_started_at", parsed.StartedAt)
        || !readTimestamp("compile_finished_at", parsed.FinishedAt))
    {
        return false;
    }
    state = std::move(parsed);
    return true;
}

void SetSelectArtifactParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& id,
    const TString& kind,
    const TString& uid)
{
    (*request.mutable_parameters())["$id"] = MakeUtf8Param(id);
    (*request.mutable_parameters())["$kind"] = MakeUtf8Param(kind);
    (*request.mutable_parameters())["$uid"] = MakeUtf8Param(uid);
}

bool ParseArtifactResponse(const Ydb::Table::ExecuteDataQueryResponse& response, TWasmArtifactRow& row) {
    Ydb::Table::ExecuteQueryResult result;
    if (!ExtractQueryResult(response, result)) {
        return false;
    }
    const auto& resultSet = result.result_sets(0);
    if (resultSet.rows().empty()) {
        return false;
    }
    if (!ReadUtf8Column(resultSet, "id", row.Id)) {
        return false;
    }
    ReadUtf8Column(resultSet, "kind", row.Kind);
    ReadUtf8Column(resultSet, "uid", row.Uid);
    ReadUint64Column(resultSet, "version", row.Version);
    ReadUtf8Column(resultSet, "format", row.Format);
    ReadUint64Column(resultSet, "wasm_data_size", row.WasmDataSize);
    ReadUint64Column(resultSet, "wasm_data_chunk_count", row.WasmDataChunkCount);
    ReadUint64Column(resultSet, "object_code_size", row.ObjectCodeSize);
    ReadUint64Column(resultSet, "object_code_chunk_count", row.ObjectCodeChunkCount);
    return row.ObjectCodeChunkCount > 0;
}

TString BuildSelectArtifactKeysQuery(const TString& tablePath) {
    return TStringBuilder()
        << "SELECT id, kind, uid FROM `"
        << EscapeTablePath(tablePath)
        << "` WHERE object_code_chunk_count > 0;";
}

bool ParseArtifactKeysResponse(
    const Ydb::Table::ExecuteDataQueryResponse& response,
    TVector<TArtifactKeyRow>& rows)
{
    Ydb::Table::ExecuteQueryResult result;
    if (!ExtractQueryResult(response, result)) {
        return false;
    }
    const auto& resultSet = result.result_sets(0);
    const i32 idCol = FindColumnIndex(resultSet, "id");
    const i32 kindCol = FindColumnIndex(resultSet, "kind");
    const i32 uidCol = FindColumnIndex(resultSet, "uid");
    if (idCol < 0 || kindCol < 0 || uidCol < 0) {
        return false;
    }

    rows.clear();
    rows.reserve(resultSet.rows().size());
    for (const auto& row : resultSet.rows()) {
        if (idCol >= row.items_size() || kindCol >= row.items_size() || uidCol >= row.items_size()) {
            return false;
        }
        const auto& id = row.items(idCol);
        const auto& kind = row.items(kindCol);
        const auto& uid = row.items(uidCol);
        if (!id.has_text_value() || !kind.has_text_value() || !uid.has_text_value()) {
            return false;
        }
        rows.push_back(TArtifactKeyRow{
            .Id = id.text_value(),
            .Kind = kind.text_value(),
            .Uid = uid.text_value(),
        });
    }
    return true;
}

TString BuildSelectArtifactChunksQuery(const TString& tablePath) {
    return TStringBuilder()
        << "DECLARE $id AS Utf8; "
        << "DECLARE $kind AS Utf8; "
        << "DECLARE $uid AS Utf8; "
        << "DECLARE $blob_kind AS Utf8; "
        << "DECLARE $first_chunk AS Uint64; "
        << "SELECT chunk_idx, data FROM `"
        << EscapeTablePath(tablePath)
        << "` WHERE id = $id AND kind = $kind AND uid = $uid AND blob_kind = $blob_kind "
        << "AND chunk_idx >= $first_chunk ORDER BY chunk_idx LIMIT " << ChunksPerRead << ";";
}

void SetSelectArtifactChunksParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& id,
    const TString& kind,
    const TString& uid,
    const TString& blobKind,
    ui64 firstChunk)
{
    (*request.mutable_parameters())["$id"] = MakeUtf8Param(id);
    (*request.mutable_parameters())["$kind"] = MakeUtf8Param(kind);
    (*request.mutable_parameters())["$uid"] = MakeUtf8Param(uid);
    (*request.mutable_parameters())["$blob_kind"] = MakeUtf8Param(blobKind);
    (*request.mutable_parameters())["$first_chunk"] = MakeUint64Param(firstChunk);
}

bool AppendArtifactChunksResponse(const Ydb::Table::ExecuteDataQueryResponse& response, TVector<TString>& chunks) {
    Ydb::Table::ExecuteQueryResult result;
    if (!ExtractQueryResult(response, result)) {
        return false;
    }
    return AppendChunksResultSet(result.result_sets(0), chunks);
}

TString BuildUpsertArtifactQuery(const TString& tablePath) {
    return TStringBuilder()
        << "DECLARE $id AS Utf8; "
        << "DECLARE $kind AS Utf8; "
        << "DECLARE $uid AS Utf8; "
        << "DECLARE $version AS Uint64; "
        << "DECLARE $format AS Utf8; "
        << "DECLARE $wasm_data_size AS Uint64; "
        << "DECLARE $wasm_data_chunk_count AS Uint64; "
        << "DECLARE $object_code_size AS Uint64; "
        << "DECLARE $object_code_chunk_count AS Uint64; "
        << "UPSERT INTO `"
        << EscapeTablePath(tablePath)
        << "` (id, kind, uid, version, format, "
        << "wasm_data_size, wasm_data_chunk_count, object_code_size, object_code_chunk_count, compiled_at, "
        << "compile_status, compile_error, compile_finished_at) "
        << "VALUES ($id, $kind, $uid, $version, $format, "
        << "$wasm_data_size, $wasm_data_chunk_count, $object_code_size, $object_code_chunk_count, "
        << "CurrentUtcTimestamp(), CAST('ready' AS Utf8), CAST('' AS Utf8), CurrentUtcTimestamp());";
}

void SetUpsertArtifactParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TWasmArtifactRow& row)
{
    (*request.mutable_parameters())["$id"] = MakeUtf8Param(row.Id);
    (*request.mutable_parameters())["$kind"] = MakeUtf8Param(row.Kind);
    (*request.mutable_parameters())["$uid"] = MakeUtf8Param(row.Uid);
    (*request.mutable_parameters())["$version"] = MakeUint64Param(row.Version);
    (*request.mutable_parameters())["$format"] = MakeUtf8Param(row.Format);
    (*request.mutable_parameters())["$wasm_data_size"] = MakeUint64Param(row.WasmDataSize);
    (*request.mutable_parameters())["$wasm_data_chunk_count"] = MakeUint64Param(row.WasmDataChunkCount);
    (*request.mutable_parameters())["$object_code_size"] = MakeUint64Param(row.ObjectCodeSize);
    (*request.mutable_parameters())["$object_code_chunk_count"] = MakeUint64Param(row.ObjectCodeChunkCount);
}

TString BuildDeleteArtifactChunksQuery(const TString& tablePath) {
    return TStringBuilder()
        << "DECLARE $id AS Utf8; "
        << "DECLARE $kind AS Utf8; "
        << "DECLARE $uid AS Utf8; "
        << "DELETE FROM `"
        << EscapeTablePath(tablePath)
        << "` WHERE id = $id AND kind = $kind AND uid = $uid;";
}

void SetDeleteArtifactChunksParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& id,
    const TString& kind,
    const TString& uid)
{
    (*request.mutable_parameters())["$id"] = MakeUtf8Param(id);
    (*request.mutable_parameters())["$kind"] = MakeUtf8Param(kind);
    (*request.mutable_parameters())["$uid"] = MakeUtf8Param(uid);
}

TString BuildDeleteStaleArtifactChunksQuery(
    const TString& artifactChunksTablePath,
    const TString& modulesTablePath)
{
    // EXISTS keeps the delete inside one statement with the modules row it
    // depends on: verify-then-delete across separate requests would let a
    // re-upload land in between and have its artifacts wiped by the loser.
    return TStringBuilder()
        << "DECLARE $id AS Utf8; "
        << "DECLARE $kind AS Utf8; "
        << "DECLARE $uid AS Utf8; "
        << "DECLARE $type AS Utf8; "
        << "DELETE FROM `"
        << EscapeTablePath(artifactChunksTablePath)
        << "` WHERE id = $id AND kind = $kind AND uid != $uid "
        << "AND EXISTS (SELECT 1 FROM `"
        << EscapeTablePath(modulesTablePath)
        << "` AS m WHERE m.name = $id AND m.type = $type AND m.uid = $uid);";
}

TString BuildDeleteStaleArtifactsQuery(
    const TString& artifactTablePath,
    const TString& modulesTablePath)
{
    return TStringBuilder()
        << "DECLARE $id AS Utf8; "
        << "DECLARE $kind AS Utf8; "
        << "DECLARE $uid AS Utf8; "
        << "DECLARE $type AS Utf8; "
        << "DELETE FROM `"
        << EscapeTablePath(artifactTablePath)
        << "` WHERE id = $id AND kind = $kind AND uid != $uid "
        << "AND EXISTS (SELECT 1 FROM `"
        << EscapeTablePath(modulesTablePath)
        << "` AS m WHERE m.name = $id AND m.type = $type AND m.uid = $uid);";
}

void SetDeleteStaleArtifactsParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& id,
    const TString& kind,
    const TString& uid,
    const TString& type)
{
    (*request.mutable_parameters())["$id"] = MakeUtf8Param(id);
    (*request.mutable_parameters())["$kind"] = MakeUtf8Param(kind);
    (*request.mutable_parameters())["$uid"] = MakeUtf8Param(uid);
    (*request.mutable_parameters())["$type"] = MakeUtf8Param(type);
}

TString BuildUpsertArtifactChunkQuery(const TString& tablePath) {
    return TStringBuilder()
        << "DECLARE $id AS Utf8; "
        << "DECLARE $kind AS Utf8; "
        << "DECLARE $uid AS Utf8; "
        << "DECLARE $blob_kind AS Utf8; "
        << "DECLARE $chunk_idx AS Uint64; "
        << "DECLARE $data AS String; "
        << "UPSERT INTO `"
        << EscapeTablePath(tablePath)
        << "` (id, kind, uid, blob_kind, chunk_idx, data) "
        << "VALUES ($id, $kind, $uid, $blob_kind, $chunk_idx, $data);";
}

void SetUpsertArtifactChunkParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& id,
    const TString& kind,
    const TString& uid,
    const TString& blobKind,
    ui64 chunkIdx,
    const TString& data)
{
    (*request.mutable_parameters())["$id"] = MakeUtf8Param(id);
    (*request.mutable_parameters())["$kind"] = MakeUtf8Param(kind);
    (*request.mutable_parameters())["$uid"] = MakeUtf8Param(uid);
    (*request.mutable_parameters())["$blob_kind"] = MakeUtf8Param(blobKind);
    (*request.mutable_parameters())["$chunk_idx"] = MakeUint64Param(chunkIdx);
    (*request.mutable_parameters())["$data"] = MakeStringParam(data);
}

} // namespace NKikimr::NUdfStore::NTableQuery
