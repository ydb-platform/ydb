#pragma once

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

#include <util/generic/strbuf.h>
#include <util/generic/map.h>

namespace NYql {

struct TYtSettings;
struct TTypeAnnotationContext;

bool IsRangeComparison(const TStringBuf& operation);

struct TExpressionResorceUsage {
    ui64 Memory = 0;
    double Cpu = 1.0;
};

void ScanResourceUsage(const TExprNode& input, const TYtSettings& config, const TTypeAnnotationContext* types,
    TMap<TStringBuf, ui64>* memoryUsage, TMap<TStringBuf, double>* cpuUsage, size_t* files);
TExpressionResorceUsage ScanExtraResourceUsage(const TExprNode& input, const TYtSettings& config);

void CalcToDictFactors(const TTypeAnnotationNode* keyType, const TTypeAnnotationNode* payloadType,
    EDictType type, bool many, bool compact, double& sizeFactor, ui64& rowFactor);
TMaybe<TIssue> CalcToDictFactors(const TExprNode& toDictNode, TExprContext& ctx, double& sizeFactor, ui64& rowFactor);

bool GetTableContentConsumerNodes(const TExprNode& node, const TExprNode& rootNode,
    const TParentsMap& parentsMap, TNodeSet& consumers);

} // NYql
