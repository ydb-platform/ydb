#include "external_table_description.h"
#include "table_description.h"
#include "ydb_convert.h"

#include <ydb/core/external_sources/external_source_factory.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/writer/json_value.h>

#include <yql/essentials/public/issue/yql_issue.h>

namespace NKikimr {

namespace {

using namespace NKikimrSchemeOp;

bool Convert(
    const TColumnDescription& in,
    Ydb::Table::ColumnMeta& out,
    Ydb::StatusIds_StatusCode& status,
    TString& error)
{
    try {
        FillColumnDescription(out, in);
    } catch (const std::exception& ex) {
        error = TStringBuilder() << "Unable to fill the column description, error: " << ex.what();
        status = Ydb::StatusIds::INTERNAL_ERROR;
        return false;
    }
    return true;
}

bool ConvertContent(
    const TString& sourceType,
    const TString& in,
    google::protobuf::Map<TProtoStringType, TProtoStringType>& out,
    Ydb::StatusIds_StatusCode& status,
    TString& error)
{
    using namespace NJson;

    const auto externalSourceFactory = NExternalSource::CreateExternalSourceFactory({}, nullptr, 50000, nullptr, false, false, true, NYql::GetAllExternalDataSourceTypes());
    try {
        const auto source = externalSourceFactory->GetOrCreate(sourceType);
        for (const auto& [key, items] : source->GetParameters(in)) {
            TJsonValue json(EJsonValueType::JSON_ARRAY);
            for (const auto& item : items) {
                json.AppendValue(item);
            }
            out[to_upper(key)] = WriteJson(json, false);
        }
    } catch (...) {
        error = TStringBuilder() << "Cannot unpack the content of an external table of type: "
            << sourceType << ", error: " << CurrentExceptionMessage();
        status = Ydb::StatusIds::INTERNAL_ERROR;
        return false;
    }
    return true;
}

} // anonymous namespace

bool FillExternalTableDescription(
    Ydb::Table::DescribeExternalTableResult& out,
    const NKikimrSchemeOp::TExternalTableDescription& inDesc,
    const NKikimrSchemeOp::TDirEntry& inDirEntry,
    Ydb::StatusIds_StatusCode& status,
    TString& error)
{
    ConvertDirectoryEntry(inDirEntry, out.mutable_self(), true);

    out.set_source_type(inDesc.GetSourceType());
    out.set_data_source_path(inDesc.GetDataSourcePath());
    out.set_location(inDesc.GetLocation());
    for (const auto& column : inDesc.GetColumns()) {
        if (!Convert(column, *out.add_columns(), status, error)) {
            return false;
        }
    }
    if (!ConvertContent(inDesc.GetSourceType(), inDesc.GetContent(), *out.mutable_content(), status, error)) {
        return false;
    }
    return true;
}

} // namespace NKikimr
