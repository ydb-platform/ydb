#pragma once

#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>

#include <yt/cpp/mapreduce/interface/common.h>

#include <library/cpp/yson/node/node.h>

#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>

#include <utility>

namespace NYql {

class TTypeAnnotationNode;

struct TYTSortInfo {
    TVector<std::pair<TString, int>> Keys;
    bool Unique = false;
};


NYT::TNode YTSchemaToRowSpec(const NYT::TNode& schema, const TYTSortInfo* sortInfo = nullptr);
NYT::TNode QB2PremapperToRowSpec(const NYT::TNode& qb2, const NYT::TNode& originalScheme);
NYT::TNode GetSchemaFromAttributes(const NYT::TNode& attributes, bool onlySystem = false, bool ignoreWeakSchema = false);
TYTSortInfo KeyColumnsFromSchema(const NYT::TNode& schema);
bool ValidateTableSchema(const TString& tableName, const NYT::TNode& attributes, bool ignoreYamrDsv, bool ignoreWeakSchema = false);
void MergeInferredSchemeWithSort(NYT::TNode& schema, TYTSortInfo& sortInfo);
NYT::TTableSchema RowSpecToYTSchema(const NYT::TNode& rowSpec, ui64 nativeTypeCompatibility, const NYT::TNode& columnGroupsSpec = {});
NYT::TSortColumns ToYTSortColumns(const TVector<std::pair<TString, bool>>& sortColumns);
TString GetTypeV3String(const TTypeAnnotationNode& type, ui64 nativeTypeCompatibility = NTCF_ALL);

} // NYql
