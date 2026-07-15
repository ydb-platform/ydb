#include "table_query.h"

#include "metadata_subscription/udf_meta.h"

#include <ydb/services/metadata/manager/ydb_value_operator.h>

#include <google/protobuf/any.pb.h>

#include <util/string/builder.h>
#include <util/string/escape.h>

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
    return TString{tablePath};
}

bool ReadUtf8Column(const Ydb::ResultSet& resultSet, const TString& columnName, TString& value) {
    if (resultSet.rows().empty()) {
        return false;
    }
    i32 columnIdx = -1;
    for (i32 i = 0; i < resultSet.columns_size(); ++i) {
        if (resultSet.columns(i).name() == columnName) {
            columnIdx = i;
            break;
        }
    }
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
    i32 columnIdx = -1;
    for (i32 i = 0; i < resultSet.columns_size(); ++i) {
        if (resultSet.columns(i).name() == columnName) {
            columnIdx = i;
            break;
        }
    }
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

bool ReadStringColumn(const Ydb::ResultSet& resultSet, const TString& columnName, TString& value) {
    if (resultSet.rows().empty()) {
        return false;
    }
    i32 columnIdx = -1;
    for (i32 i = 0; i < resultSet.columns_size(); ++i) {
        if (resultSet.columns(i).name() == columnName) {
            columnIdx = i;
            break;
        }
    }
    if (columnIdx < 0) {
        return false;
    }
    const auto& row = resultSet.rows(0);
    if (columnIdx >= row.items_size()) {
        return false;
    }
    const auto& item = row.items(columnIdx);
    if (!item.has_bytes_value()) {
        return false;
    }
    value = item.bytes_value();
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

TString BuildSelectWasmSourceQuery(const TString& tablePath) {
    return TStringBuilder()
        << "DECLARE $md5 AS Utf8; "
        << "SELECT md5, version, body FROM `"
        << EscapeTablePath(tablePath)
        << "` WHERE md5 = $md5;";
}

void SetSelectWasmSourceParams(Ydb::Table::ExecuteDataQueryRequest& request, const TString& md5) {
    (*request.mutable_parameters())["$md5"] = MakeUtf8Param(md5);
}

bool ParseWasmSourceResponse(const Ydb::Table::ExecuteDataQueryResponse& response, TWasmSourceRow& row) {
    Ydb::Table::ExecuteQueryResult result;
    if (!ExtractQueryResult(response, result)) {
        return false;
    }
    const auto& resultSet = result.result_sets(0);
    if (resultSet.rows().empty()) {
        return false;
    }
    if (!ReadUtf8Column(resultSet, "md5", row.Md5)) {
        return false;
    }
    ReadUint64Column(resultSet, "version", row.Version);
    return ReadStringColumn(resultSet, "body", row.Body);
}

TString BuildSelectLibrarySourceQuery(const TString& tablePath) {
    return TStringBuilder()
        << "DECLARE $name AS Utf8; "
        << "SELECT name, md5, version, body, compile_status, compile_error FROM `"
        << EscapeTablePath(tablePath)
        << "` WHERE name = $name;";
}

void SetSelectLibrarySourceParams(Ydb::Table::ExecuteDataQueryRequest& request, const TString& name) {
    (*request.mutable_parameters())["$name"] = MakeUtf8Param(name);
}

bool ParseLibrarySourceResponse(const Ydb::Table::ExecuteDataQueryResponse& response, TLibrarySourceRow& row) {
    Ydb::Table::ExecuteQueryResult result;
    if (!ExtractQueryResult(response, result)) {
        return false;
    }
    const auto& resultSet = result.result_sets(0);
    if (resultSet.rows().empty()) {
        return false;
    }
    if (!ReadUtf8Column(resultSet, "name", row.Name)) {
        return false;
    }
    ReadUtf8Column(resultSet, "md5", row.Md5);
    ReadUint64Column(resultSet, "version", row.Version);
    if (!ReadStringColumn(resultSet, "body", row.Body)) {
        return false;
    }
    TString compileStatus;
    if (ReadUtf8Column(resultSet, "compile_status", compileStatus)) {
        TUdfMeta::CompileStatusFromString(compileStatus, row.CompileStatus);
    }
    ReadUtf8Column(resultSet, "compile_error", row.CompileError);
    return true;
}

TString BuildSelectArtifactQuery(const TString& tablePath) {
    return TStringBuilder()
        << "DECLARE $id AS Utf8; "
        << "DECLARE $kind AS Utf8; "
        << "SELECT id, kind, source_md5, version, format, wasm_data, object_code FROM `"
        << EscapeTablePath(tablePath)
        << "` WHERE id = $id AND kind = $kind;";
}

void SetSelectArtifactParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& id,
    const TString& kind)
{
    (*request.mutable_parameters())["$id"] = MakeUtf8Param(id);
    (*request.mutable_parameters())["$kind"] = MakeUtf8Param(kind);
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
    ReadUtf8Column(resultSet, "source_md5", row.SourceMd5);
    ReadUint64Column(resultSet, "version", row.Version);
    ReadUtf8Column(resultSet, "format", row.Format);
    ReadStringColumn(resultSet, "wasm_data", row.WasmData);
    return ReadStringColumn(resultSet, "object_code", row.ObjectCode);
}

TString BuildUpsertArtifactQuery(const TString& tablePath) {
    return TStringBuilder()
        << "DECLARE $id AS Utf8; "
        << "DECLARE $kind AS Utf8; "
        << "DECLARE $source_md5 AS Utf8; "
        << "DECLARE $version AS Uint64; "
        << "DECLARE $format AS Utf8; "
        << "DECLARE $wasm_data AS String; "
        << "DECLARE $object_code AS String; "
        << "UPSERT INTO `"
        << EscapeTablePath(tablePath)
        << "` (id, kind, source_md5, version, format, wasm_data, object_code, compiled_at) "
        << "VALUES ($id, $kind, $source_md5, $version, $format, $wasm_data, $object_code, CurrentUtcTimestamp());";
}

void SetUpsertArtifactParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TWasmArtifactRow& row)
{
    (*request.mutable_parameters())["$id"] = MakeUtf8Param(row.Id);
    (*request.mutable_parameters())["$kind"] = MakeUtf8Param(row.Kind);
    (*request.mutable_parameters())["$source_md5"] = MakeUtf8Param(row.SourceMd5);
    (*request.mutable_parameters())["$version"] = MakeUint64Param(row.Version);
    (*request.mutable_parameters())["$format"] = MakeUtf8Param(row.Format);
    (*request.mutable_parameters())["$wasm_data"] = MakeStringParam(row.WasmData);
    (*request.mutable_parameters())["$object_code"] = MakeStringParam(row.ObjectCode);
}

TString BuildUpdateLibraryCompileStatusQuery(const TString& tablePath) {
    return TStringBuilder()
        << "DECLARE $name AS Utf8; "
        << "DECLARE $compile_status AS Utf8; "
        << "DECLARE $compile_error AS Utf8; "
        << "UPDATE `"
        << EscapeTablePath(tablePath)
        << "` SET compile_status = $compile_status, compile_error = $compile_error "
        << "WHERE name = $name;";
}

void SetUpdateLibraryCompileStatusParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& name,
    const TString& status,
    const TString& errorMessage)
{
    (*request.mutable_parameters())["$name"] = MakeUtf8Param(name);
    (*request.mutable_parameters())["$compile_status"] = MakeUtf8Param(status);
    (*request.mutable_parameters())["$compile_error"] = MakeUtf8Param(errorMessage);
}

TString BuildUpdateCompileStatusQuery(const TString& tablePath) {
    return TStringBuilder()
        << "DECLARE $md5 AS Utf8; "
        << "DECLARE $compile_status AS Utf8; "
        << "DECLARE $compile_error AS Utf8; "
        << "UPDATE `"
        << EscapeTablePath(tablePath)
        << "` SET compile_status = $compile_status, compile_error = $compile_error "
        << "WHERE md5 = $md5;";
}

void SetUpdateCompileStatusParams(
    Ydb::Table::ExecuteDataQueryRequest& request,
    const TString& md5,
    const TString& status,
    const TString& errorMessage)
{
    (*request.mutable_parameters())["$md5"] = MakeUtf8Param(md5);
    (*request.mutable_parameters())["$compile_status"] = MakeUtf8Param(status);
    (*request.mutable_parameters())["$compile_error"] = MakeUtf8Param(errorMessage);
}

} // namespace NKikimr::NUdfStore::NTableQuery
