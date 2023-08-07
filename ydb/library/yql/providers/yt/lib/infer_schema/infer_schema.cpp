#include "infer_schema.h"

#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/utils/log/log.h>

#include <util/generic/yexception.h>


namespace NYql {

TStreamSchemaInferer::TStreamSchemaInferer(const TString& tableName)
    : TableName(tableName)
    , UsedRows(0)
{
}

void TStreamSchemaInferer::AddRow(const NYT::TNode& row)
{
    for (const auto& column : row.AsMap()) {
        if (column.first.StartsWith("$")) {
            // Skip system columns
            continue;
        }
        TString columnType;
        switch (column.second.GetType()) {
            case NYT::TNode::String:
                columnType = "string";
                break;
            case NYT::TNode::Int64:
                columnType = "int64";
                break;
            case NYT::TNode::Uint64:
                columnType = "uint64";
                break;
            case NYT::TNode::Double:
                columnType = "double";
                break;
            case NYT::TNode::Bool:
                columnType = "boolean";
                break;
            case NYT::TNode::List:
                columnType = "any";
                break;
            case NYT::TNode::Map:
                columnType = "any";
                break;
            case NYT::TNode::Null:
                continue;
            default:
                YQL_LOG_CTX_THROW yexception() <<
                    "Cannot infer schema for table " << TableName <<
                    ", undefined NYT::TNode";
        }

        auto& type = ColumnTypes[column.first];
        if (type.empty()) {
            type = columnType;
        } else if (type != columnType) {
            type = "any";
        }
    }
    ++UsedRows;
}

NYT::TNode TStreamSchemaInferer::GetSchema() const
{
    NYT::TNode schema = NYT::TNode::CreateList();
    for (auto& x : ColumnTypes) {
        auto& columnNode = schema.Add();
        columnNode["name"] = x.first;
        columnNode["type"] = x.second;
    }

    if (schema.Empty()) {
        YQL_LOG_CTX_THROW TErrorException(TIssuesIds::YT_INFER_SCHEMA) <<
            "Cannot infer schema for table " << TableName <<
            ", first " << UsedRows << " row(s) has no columns";
    }
    schema.Attributes()["strict"] = false;
    return schema;
}

NYT::TNode InferSchemaFromSample(const NYT::TNode& sample, const TString& tableName, ui32 rows)
{
    TStreamSchemaInferer inferer(tableName);
    const size_t useRows = Min<size_t>(rows, sample.AsList().size());

    for (size_t i = 0; i < useRows; ++i) {
        inferer.AddRow(sample.AsList()[i]);
    }

    return inferer.GetSchema();
}

TMaybe<NYT::TNode> InferSchemaFromTableContents(const NYT::ITransactionPtr& tx, const TString& tableId, const TString& tableName, ui32 rows)
{
    auto reader = tx->CreateTableReader<NYT::TNode>(NYT::TRichYPath(tableId).AddRange(NYT::TReadRange::FromRowIndices(0, rows)));
    if (!reader->IsValid()) {
        return Nothing();
    }

    TStreamSchemaInferer inferer(tableName);
    for (ui32 i = 0; reader->IsValid() && i < rows; ++i) {
        inferer.AddRow(reader->GetRow());
        reader->Next();
    }

    return inferer.GetSchema();
}

} // NYql
