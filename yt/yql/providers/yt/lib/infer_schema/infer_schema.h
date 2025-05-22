#pragma once

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/common.h>

#include <library/cpp/yson/node/node.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/map.h>

#include <functional>
#include <utility>

namespace NYql {

class TStreamSchemaInferer
{
public:
    explicit TStreamSchemaInferer(const TString& tableName);
    void AddRow(const NYT::TNode& row);
    NYT::TNode GetSchema() const;

private:
    const TString TableName;
    TMap<TString, TString> ColumnTypes;
    size_t UsedRows;
};

NYT::TNode InferSchemaFromSample(const NYT::TNode& sample, const TString& tableName, ui32 rows);
TMaybe<NYT::TNode> InferSchemaFromTableContents(const NYT::ITransactionPtr& tx, const TString& tableId, const TString& tableName, ui32 rows);

} // NYql
