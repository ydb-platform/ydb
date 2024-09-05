#pragma once

#include "yql_yt_table.h"
#include "yql_yt_gateway.h"
#include "yql_yt_provider.h"
#include "yql_yt_op_settings.h"

#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/ast/yql_expr.h>

#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/generic/maybe.h>
#include <util/generic/set.h>

namespace NYql {

bool UpdateUsedCluster(TString& usedCluster, const TString& newCluster);
bool IsYtIsolatedLambda(const TExprNode& lambdaBody, TSyncMap& syncList, TString& usedCluster, bool supportsDq);
bool IsYtCompleteIsolatedLambda(const TExprNode& lambdaBody, TSyncMap& syncList, bool supportsDq);
bool IsYtCompleteIsolatedLambda(const TExprNode& lambdaBody, TSyncMap& syncList, TString& usedCluster, bool supportsDq);
TExprNode::TPtr YtCleanupWorld(const TExprNode::TPtr& input, TExprContext& ctx, TYtState::TPtr state);
TVector<TYtTableBaseInfo::TPtr> GetInputTableInfos(NNodes::TExprBase input);
TVector<TYtPathInfo::TPtr> GetInputPaths(NNodes::TExprBase input);
TStringBuf GetClusterName(NNodes::TExprBase input);
bool IsYtProviderInput(NNodes::TExprBase input, bool withVariantList = false);
bool IsConstExpSortDirections(NNodes::TExprBase sortDirections);
TExprNode::TListType GetNodesToCalculate(const TExprNode::TPtr& input);
bool HasNodesToCalculate(const TExprNode::TPtr& input);
std::pair<IGraphTransformer::TStatus, TAsyncTransformCallbackFuture> CalculateNodes(TYtState::TPtr state, const TExprNode::TPtr& input,
    const TString& cluster, const TExprNode::TListType& needCalc, TExprContext& ctx);
TMaybe<ui64> GetLimit(const TExprNode& settings);
TExprNode::TPtr GetLimitExpr(const TExprNode::TPtr& limitSetting, TExprContext& ctx);
IGraphTransformer::TStatus UpdateTableMeta(const TExprNode::TPtr& tableNode, TExprNode::TPtr& newTableNode,
    const TYtTablesData::TPtr& tablesData, bool checkSqlView, bool updateRowSpecType, TExprContext& ctx);
TExprNode::TPtr ValidateAndUpdateTablesMeta(const TExprNode::TPtr& input, TStringBuf cluster, const TYtTablesData::TPtr& tablesData, bool updateRowSpecType, TExprContext& ctx);
TExprNode::TPtr ResetTablesMeta(const TExprNode::TPtr& input, TExprContext& ctx, bool resetTmpOnly, bool isEvaluationInProgress);
NNodes::TExprBase GetOutTable(NNodes::TExprBase ytOutput);
std::pair<NNodes::TExprBase, TString> GetOutTableWithCluster(NNodes::TExprBase ytOutput);
NNodes::TMaybeNode<NNodes::TCoFlatMapBase> GetFlatMapOverInputStream(NNodes::TCoLambda opLambda, const TParentsMap& parentsMap);
NNodes::TMaybeNode<NNodes::TCoFlatMapBase> GetFlatMapOverInputStream(NNodes::TCoLambda opLambda);
IGraphTransformer::TStatus SubstTables(TExprNode::TPtr& input, const TYtState::TPtr& state, bool anonOnly, TExprContext& ctx);

struct TCopyOrTrivialMapOpts {
    TCopyOrTrivialMapOpts& SetLimitNodes(const TExprNode::TListType& limitNodes) {
        LimitNodes = limitNodes;
        return *this;
    }

    TCopyOrTrivialMapOpts& SetTryKeepSortness(bool tryKeepSortness = true) {
        TryKeepSortness = tryKeepSortness;
        return *this;
    }

    TCopyOrTrivialMapOpts& SetSectionUniq(const TDistinctConstraintNode* sectionUniq) {
        SectionUniq = sectionUniq;
        return *this;
    }

    TCopyOrTrivialMapOpts& SetCombineChunks(bool combineChunks = true) {
        CombineChunks = combineChunks;
        return *this;
    }

    TCopyOrTrivialMapOpts& SetRangesResetSort(bool rangesResetSort = true) {
        RangesResetSort = rangesResetSort;
        return *this;
    }

    TCopyOrTrivialMapOpts& SetConstraints(const TConstraintSet& constraints) {
        Constraints = constraints;
        return *this;
    }

    TExprNode::TListType LimitNodes;
    bool TryKeepSortness = false;
    const TDistinctConstraintNode* SectionUniq = nullptr;
    bool CombineChunks = false;
    bool RangesResetSort = true;
    TConstraintSet Constraints;
};

NNodes::TYtPath CopyOrTrivialMap(TPositionHandle pos, NNodes::TExprBase world, NNodes::TYtDSink dataSink,
    const TTypeAnnotationNode& scheme, NNodes::TYtSection section, TYqlRowSpecInfo::TPtr outRowSpec, TExprContext& ctx, const TYtState::TPtr& state,
    const TCopyOrTrivialMapOpts& opts);
bool IsOutputUsedMultipleTimes(const TExprNode& op, const TParentsMap& parentsMap);

TMaybe<TVector<ui64>> EstimateDataSize(const TString& cluster, const TVector<TYtPathInfo::TPtr>& paths,
    const TMaybe<TVector<TString>>& columns, const TYtState& state, TExprContext& ctx);
IGraphTransformer::TStatus TryEstimateDataSize(TVector<ui64>& result, TSet<TString>& requestedColumns,
    const TString& cluster, const TVector<TYtPathInfo::TPtr>& paths,
    const TMaybe<TVector<TString>>& columns, const TYtState& state, TExprContext& ctx);
TMaybe<NYT::TRichYPath> BuildYtPathForStatRequest(const TString& cluster, const TYtPathInfo& pathInfo,
    const TMaybe<TVector<TString>>& overrideColumns, const TYtState& state, TExprContext& ctx);

NNodes::TYtSection UpdateInputFields(NNodes::TYtSection section, NNodes::TExprBase fields, TExprContext& ctx);
NNodes::TYtSection UpdateInputFields(NNodes::TYtSection section, TSet<TStringBuf>&& fields, TExprContext& ctx, bool hasWeakFields);
NNodes::TYtPath MakeUnorderedPath(NNodes::TYtPath path, bool hasLimits, TExprContext& ctx);
template<bool WithUnorderedSetting = false>
NNodes::TYtSection MakeUnorderedSection(NNodes::TYtSection section, TExprContext& ctx);
NNodes::TYtSection ClearUnorderedSection(NNodes::TYtSection section, TExprContext& ctx);
NNodes::TYtDSource GetDataSource(NNodes::TExprBase input, TExprContext& ctx);
TExprNode::TPtr BuildEmptyTablesRead(TPositionHandle pos, const TExprNode& userSchema, TExprContext& ctx);
TExprNode::TPtr GetFlowSettings(TPositionHandle pos, const TYtState& state, TExprContext& ctx, TExprNode::TPtr settings = {});
TVector<TStringBuf> GetKeyFilterColumns(const NNodes::TYtSection& section, EYtSettingTypes kind);
bool HasNonEmptyKeyFilter(const NNodes::TYtSection& section);

NNodes::TYtOutputOpBase GetOutputOp(NNodes::TYtOutput output);

inline bool IsUnorderedOutput(NNodes::TYtOutput out) {
    return out.Mode() && FromString<EYtSettingType>(out.Mode().Cast().Value()) == EYtSettingType::Unordered;
}

template <typename TGateway>
std::function<bool(const TString&)> MakeUserFilesDownloadFilter(const TGateway& gateway, const TString& activeCluster) {
    // hold activeCluster by value to support temp strings
    return [&gateway, activeCluster](const TString& url) {
        if (activeCluster.empty()) {
            // todo: we lost our opportunity to skip download in this case, improve it
            return true;
        }

        TString extractedCluster;
        TString extractedPath;
        if (!gateway.TryParseYtUrl(url, &extractedCluster, &extractedPath/* don't remove - triggers additional check against allowed patterns*/)) {
            return true;
        }

        return (extractedCluster != activeCluster) && (extractedCluster != CurrentYtClusterShortcut);
    };
}

NNodes::TYtReadTable ConvertContentInputToRead(NNodes::TExprBase input, NNodes::TMaybeNode<NNodes::TCoNameValueTupleList> settings, TExprContext& ctx, NNodes::TMaybeNode<NNodes::TCoAtomList> customFields = {});

size_t GetMapDirectOutputsCount(const NNodes::TYtMapReduce& mapReduce);

bool HasYtRowNumber(const TExprNode& node);

}
