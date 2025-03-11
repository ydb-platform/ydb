
#include "yql_yt_cbo_helpers.h"
#include "yql_yt_helpers.h"

#include <yql/essentials/utils/log/log.h>

namespace NYql {
namespace {

void AddJoinColumns(THashMap<TString, THashSet<TString>>& relJoinColumns, const TYtJoinNodeOp& op) {
    for (ui32 i = 0; i < op.LeftLabel->ChildrenSize(); i += 2) {
        auto ltable = op.LeftLabel->Child(i)->Content();
        auto lcolumn = op.LeftLabel->Child(i + 1)->Content();
        auto rtable = op.RightLabel->Child(i)->Content();
        auto rcolumn = op.RightLabel->Child(i + 1)->Content();

        relJoinColumns[TString(ltable)].insert(TString(lcolumn));
        relJoinColumns[TString(rtable)].insert(TString(rcolumn));
    }
}

IGraphTransformer::TStatus ExtractInMemorySize(
    const TYtState::TPtr& state,
    TString cluster,
    TExprContext& ctx,
    TMaybe<ui64>& leftMemorySize,
    TMaybe<ui64>& rightMemorySize,
    ESizeStatCollectMode mode,
    TYtJoinNodeOp* op,
    const TJoinLabels& labels,
    int numLeaves,
    TYtJoinNodeLeaf* leftLeaf,
    bool leftTablesReady,
    const TVector<TYtPathInfo::TPtr>& leftTables,
    const THashSet<TString>& leftJoinKeys,
    const TStructExprType* leftItemType,
    TYtJoinNodeLeaf* rightLeaf,
    bool rightTablesReady,
    const TVector<TYtPathInfo::TPtr>& rightTables,
    const THashSet<TString>& rightJoinKeys,
    const TStructExprType* rightItemType)
{
    TMapJoinSettings mapSettings;
    TJoinSideStats leftStats;
    TJoinSideStats rightStats;
    bool isCross = false;
    auto status = CollectStatsAndMapJoinSettings(mode, mapSettings, leftStats, rightStats,
                                                 leftTablesReady, leftTables, leftJoinKeys, rightTablesReady, rightTables, rightJoinKeys,
                                                 leftLeaf, rightLeaf, *state, isCross, cluster, ctx);
    if (status != IGraphTransformer::TStatus::Ok) {
        YQL_CLOG(WARN, ProviderYt) << "Unable to collect paths and labels: " << status;
        return status;
    }
    if (leftLeaf) {
        const bool needPayload = op->JoinKind->IsAtom("Inner") || op->JoinKind->IsAtom("Right");
        const auto& label = labels.Inputs[0];
        TVector<TString> leftJoinKeyList(leftJoinKeys.begin(), leftJoinKeys.end());
        const ui64 rows = mapSettings.LeftRows;
        ui64 size = 0;
        auto status = CalculateJoinLeafSize(size, mapSettings, leftLeaf->Section, *op, ctx, true, leftItemType, leftJoinKeyList, state, cluster, leftTables);
        if (status != IGraphTransformer::TStatus::Ok) {
            YQL_CLOG(WARN, ProviderYt) << "Unable to calculate left join leaf size: " << status;
            return status;
        }
        if (op->JoinKind->IsAtom("Cross")) {
            leftMemorySize = size + rows * (1ULL + label.InputType->GetSize()) * sizeof(NKikimr::NUdf::TUnboxedValuePod);
        } else {
            leftMemorySize = CalcInMemorySizeNoCrossJoin(
                label, *op, mapSettings, true, ctx, needPayload, size);
        }
    }

    if (rightLeaf) {
        const bool needPayload = op->JoinKind->IsAtom("Inner") || op->JoinKind->IsAtom("Left");
        const auto& label = labels.Inputs[numLeaves - 1];
        TVector<TString> rightJoinKeyList(rightJoinKeys.begin(), rightJoinKeys.end());
        const ui64 rows = mapSettings.RightRows;
        ui64 size = 0;

        auto status = CalculateJoinLeafSize(size, mapSettings, rightLeaf->Section, *op, ctx, false, rightItemType, rightJoinKeyList, state, cluster, rightTables);
        if (status != IGraphTransformer::TStatus::Ok) {
            YQL_CLOG(WARN, ProviderYt) << "Unable to calculate right join leaf size: " << status;
            return status;
        }
        if (op->JoinKind->IsAtom("Cross")) {
            rightMemorySize = size + rows * (1ULL + label.InputType->GetSize()) * sizeof(NKikimr::NUdf::TUnboxedValuePod);
        } else {
            rightMemorySize = CalcInMemorySizeNoCrossJoin(
                label, *op, mapSettings, false, ctx, needPayload, size);
        }
    }
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus CollectCboStatsLeaf(
    const THashMap<TString, THashSet<TString>>& relJoinColumns,
    const TString& cluster,
    TYtJoinNodeLeaf& leaf,
    const TYtState::TPtr& state,
    TExprContext& ctx) {

    const TMaybe<ui64> maxChunkCountExtendedStats = state->Configuration->ExtendedStatsMaxChunkCount.Get();
    TVector<TYtPathInfo::TPtr> tables;
    if (maxChunkCountExtendedStats) {
        TVector<TString> requestedColumnList;
        auto columnsPos = relJoinColumns.find(JoinLeafLabel(leaf.Label));
        if (columnsPos != relJoinColumns.end()) {
            requestedColumnList.assign(columnsPos->second.begin(), columnsPos->second.end());
        }

        THashSet<TString> memSizeColumns(requestedColumnList.begin(), requestedColumnList.end());
        TVector<IYtGateway::TPathStatReq> pathStatReqs;

        ui64 sectionChunkCount = 0;
        for (auto path: leaf.Section.Paths()) {
            auto pathInfo = MakeIntrusive<TYtPathInfo>(path);
            tables.push_back(pathInfo);
            sectionChunkCount += pathInfo->Table->Stat->ChunkCount;

            if (pathInfo->HasColumns()) {
                NYT::TRichYPath path;
                pathInfo->FillRichYPath(path);
                std::copy(path.Columns_->Parts_.begin(), path.Columns_->Parts_.end(), std::inserter(memSizeColumns, memSizeColumns.end()));
            }

            auto ytPath = BuildYtPathForStatRequest(cluster, *pathInfo, requestedColumnList, *state, ctx);

            if (!ytPath) {
                return IGraphTransformer::TStatus::Error;
            }

            pathStatReqs.push_back(
                IYtGateway::TPathStatReq()
                    .Path(*ytPath)
                    .IsTemp(pathInfo->Table->IsTemp)
                    .IsAnonymous(pathInfo->Table->IsAnonymous)
                    .Epoch(pathInfo->Table->Epoch.GetOrElse(0)));
        }

        if (!pathStatReqs.empty() && (*maxChunkCountExtendedStats == 0 || sectionChunkCount <= *maxChunkCountExtendedStats)) {
            IYtGateway::TPathStatOptions pathStatOptions =
                IYtGateway::TPathStatOptions(state->SessionId)
                    .Cluster(cluster)
                    .Paths(pathStatReqs)
                    .Config(state->Configuration->Snapshot())
                    .Extended(true);

            IYtGateway::TPathStatResult pathStats = state->Gateway->TryPathStat(std::move(pathStatOptions));

            if (!pathStats.Success()) {
                leaf.Section = Build<TYtSection>(ctx, leaf.Section.Ref().Pos())
                    .InitFrom(leaf.Section)
                    .Settings(NYql::AddSettingAsColumnList(leaf.Section.Settings().Ref(), EYtSettingType::StatColumns, requestedColumnList, ctx))
                    .Done();
                return IGraphTransformer::TStatus::Repeat;
            }
        }
    }

    TVector<ui64> dataSize;
    return TryEstimateDataSizeChecked(dataSize, leaf.Section, cluster, tables, {}, *state, ctx);
}

IGraphTransformer::TStatus CollectCboStatsNode(THashMap<TString, THashSet<TString>>& relJoinColumns, const TString& cluster, TYtJoinNodeOp& op, const TYtState::TPtr& state, TExprContext& ctx) {
    TYtJoinNodeLeaf* leftLeaf = dynamic_cast<TYtJoinNodeLeaf*>(op.Left.Get());
    TYtJoinNodeLeaf* rightLeaf = dynamic_cast<TYtJoinNodeLeaf*>(op.Right.Get());
    AddJoinColumns(relJoinColumns, op);

    TRelSizeInfo leftSizeInfo;
    TRelSizeInfo rightSizeInfo;
    auto result = PopulateJoinStrategySizeInfo(leftSizeInfo, rightSizeInfo, state, cluster, ctx, &op);
    if (result != IGraphTransformer::TStatus::Ok) {
        return result;
    }

    if (leftLeaf) {
        result = CollectCboStatsLeaf(relJoinColumns, cluster, *leftLeaf, state, ctx);
    } else {
        auto& leftOp = *dynamic_cast<TYtJoinNodeOp*>(op.Left.Get());
        result = CollectCboStatsNode(relJoinColumns, cluster, leftOp, state, ctx);
    }
    if (result != IGraphTransformer::TStatus::Ok) {
        return result;
    }

    if (rightLeaf) {
        result = CollectCboStatsLeaf(relJoinColumns, cluster, *rightLeaf, state, ctx);
    } else {
        auto& rightOp = *dynamic_cast<TYtJoinNodeOp*>(op.Right.Get());
        result = CollectCboStatsNode(relJoinColumns, cluster, rightOp, state, ctx);
    }
    return result;
}

}  // namespace

IGraphTransformer::TStatus PopulateJoinStrategySizeInfo(
    TRelSizeInfo& outLeft,
    TRelSizeInfo& outRight,
    const TYtState::TPtr& state,
    TString cluster,
    TExprContext& ctx,
    TYtJoinNodeOp* op) {
    auto mapJoinUseFlow = state->Configuration->MapJoinUseFlow.Get().GetOrElse(DEFAULT_MAP_JOIN_USE_FLOW);
    if (!mapJoinUseFlow) {
        // Only support flow map joins in CBO.
        return IGraphTransformer::TStatus::Ok;
    }

    TYtJoinNodeLeaf* leftLeaf = dynamic_cast<TYtJoinNodeLeaf*>(op->Left.Get());
    TYtJoinNodeLeaf* rightLeaf = dynamic_cast<TYtJoinNodeLeaf*>(op->Right.Get());

    bool leftTablesReady = false;
    TVector<TYtPathInfo::TPtr> leftTables;
    bool rightTablesReady = false;
    TVector<TYtPathInfo::TPtr> rightTables;
    THashSet<TString> leftJoinKeys, rightJoinKeys;
    int numLeaves = 0;
    TJoinLabels labels;
    const TStructExprType* leftItemType = nullptr;
    const TStructExprType* leftItemTypeBeforePremap = nullptr;
    const TStructExprType* rightItemType = nullptr;
    const TStructExprType* rightItemTypeBeforePremap = nullptr;

    {
        if (leftLeaf) {
            TYtSection section{leftLeaf->Section};
            if (Y_UNLIKELY(!section.Settings().Empty() && section.Settings().Item(0).Name() == "Test")) {
                return IGraphTransformer::TStatus::Ok;
            }

            auto status = CollectPathsAndLabelsReady(leftTablesReady, leftTables, labels, leftItemType, leftItemTypeBeforePremap, *leftLeaf, ctx);
            if (status != IGraphTransformer::TStatus::Ok) {
                YQL_CLOG(WARN, ProviderYt) << "Unable to collect paths and labels: " << status;
                return status;
            }
            if (!labels.Inputs.empty()) {
                leftJoinKeys = BuildJoinKeys(labels.Inputs[0], *op->LeftLabel);
            }
            ++numLeaves;
        }
        if (rightLeaf) {
            TYtSection section{rightLeaf->Section};
            if (Y_UNLIKELY(!section.Settings().Empty() && section.Settings().Item(0).Name() == "Test")) {
                return IGraphTransformer::TStatus::Ok;
            }
            auto status = CollectPathsAndLabelsReady(rightTablesReady, rightTables, labels, rightItemType, rightItemTypeBeforePremap, *rightLeaf, ctx);
            if (status != IGraphTransformer::TStatus::Ok) {
                YQL_CLOG(WARN, ProviderYt) << "Unable to collect paths and labels: " << status;
                return status;
            }
            if (std::ssize(labels.Inputs) > numLeaves) {
                rightJoinKeys = BuildJoinKeys(labels.Inputs[numLeaves], *op->RightLabel);
            }
            ++numLeaves;
        }
    }

    if (numLeaves == 0) {
        return IGraphTransformer::TStatus::Ok;
    }

    auto status = ExtractInMemorySize(state, cluster, ctx, outLeft.MapJoinMemSize, outRight.MapJoinMemSize, ESizeStatCollectMode::ColumnarSize, op, labels,
        numLeaves, leftLeaf, leftTablesReady, leftTables, leftJoinKeys, leftItemType,
        rightLeaf, rightTablesReady, rightTables, rightJoinKeys, rightItemType);
    if (status != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    status = ExtractInMemorySize(state, cluster, ctx, outLeft.LookupJoinMemSize, outRight.LookupJoinMemSize, ESizeStatCollectMode::RawSize, op, labels,
        numLeaves, leftLeaf, leftTablesReady, leftTables, leftJoinKeys, leftItemType,
        rightLeaf, rightTablesReady, rightTables, rightJoinKeys, rightItemType);
    return status;
}

IGraphTransformer::TStatus CollectCboStats(const TString& cluster, TYtJoinNodeOp& op, const TYtState::TPtr& state, TExprContext& ctx) {
    THashMap<TString, THashSet<TString>> relJoinColumns;
    return CollectCboStatsNode(relJoinColumns, cluster, op, state, ctx);
}

}
