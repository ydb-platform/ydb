#include "yql_yt_dq_integration.h"
#include "yql_yt_table.h"
#include "yql_yt_mkql_compiler.h"
#include "yql_yt_helpers.h"
#include "yql_yt_op_settings.h"
#include "yql_yt_provider_impl.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/common/yql_configuration.h>
#include <ydb/library/yql/providers/yt/lib/yson_helpers/yson_helpers.h>

#include <ydb/library/yql/providers/common/dq/yql_dq_integration_impl.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/providers/common/config/yql_dispatch.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/services/yql_transform_pipeline.h>
#include <ydb/library/yql/utils/log/log.h>

#include <yt/cpp/mapreduce/common/helpers.h>

#include <library/cpp/iterator/enumerate.h>
#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/node/node.h>

#include <util/generic/size_literals.h>
#include <util/generic/utility.h>


namespace NYql {

static const THashSet<TStringBuf> UNSUPPORTED_YT_PRAGMAS = {"maxrowweight",  "layerpaths", "operationspec"};
static const THashSet<TStringBuf> POOL_TREES_WHITELIST = {"physical",  "cloud", "cloud_default"};

using namespace NNodes;

class TYtDqIntegration: public TDqIntegrationBase {
public:
    TYtDqIntegration(TYtState* state)
        : State_(state)
    {
    }

    TVector<TVector<ui64>> EstimateColumnStats(TExprContext& ctx, const TString& cluster, const TVector<TVector<TYtPathInfo::TPtr>>& groupIdPathInfos, ui64& sumAllTableSizes) {
        TVector<TVector<ui64>> groupIdColumnarStats;
        groupIdColumnarStats.reserve(groupIdPathInfos.size());
        TVector<bool> lookupsInfo;
        TVector<TYtPathInfo::TPtr> flattenPaths;
        for (const auto& pathInfos: groupIdPathInfos) {
            for (const auto& pathInfo: pathInfos) {
                auto hasLookup = pathInfo->Table->Meta && pathInfo->Table->Meta->Attrs.Value("optimize_for", "scan") == "lookup";
                lookupsInfo.push_back(hasLookup);
                if (!pathInfo->Table->Stat) {
                    continue;
                }
                if (hasLookup) {
                    continue;
                }
                flattenPaths.push_back(pathInfo);
            }
        }
        auto result = EstimateDataSize(cluster, flattenPaths, Nothing(), *State_, ctx);
        size_t statIdx = 0;
        size_t pathIdx = 0;
        for (const auto& [idx, pathInfos]: Enumerate(groupIdPathInfos)) {
            TVector<ui64> columnarStatInner;
            columnarStatInner.reserve(pathInfos.size());
            for (auto& path: pathInfos) {
                const auto& tableInfo = *path->Table;
                if (lookupsInfo[pathIdx++] || !tableInfo.Stat) {
                    columnarStatInner.push_back(tableInfo.Stat ? tableInfo.Stat->DataSize : 0);
                    sumAllTableSizes += columnarStatInner.back();
                    continue;
                }
                columnarStatInner.push_back(result ? result->at(statIdx) : tableInfo.Stat->DataSize);
                sumAllTableSizes += columnarStatInner.back();
                ++statIdx;
            }
            groupIdColumnarStats.emplace_back(std::move(columnarStatInner));
        }
        return groupIdColumnarStats;
    }

    ui64 Partition(const TDqSettings& config, size_t maxTasks, const TExprNode& node,
        TVector<TString>& serializedPartitions, TString* clusterName, TExprContext& ctx, bool canFallback) override
    {
        auto dataSizePerJob = config.DataSizePerJob.Get().GetOrElse(TDqSettings::TDefault::DataSizePerJob);
        if (!TMaybeNode<TYtReadTable>(&node).IsValid()) {
            return 0;
        }

        const auto ytRead = TYtReadTable(&node);
        const auto cluster = ytRead.DataSource().Cluster().StringValue();

        if (clusterName) {
            *clusterName = cluster;
        }

        TVector<TVector<TYtPathInfo::TPtr>> groupIdPathInfos;
        bool hasErasure = false;
        ui64 chunksCount = 0;
        for (const auto& input : ytRead.Input()) {
            TVector<TYtPathInfo::TPtr> pathInfos;
            for (const auto& path : input.Paths()) {
                TYtPathInfo::TPtr pathInfo = new TYtPathInfo(path);
                pathInfos.push_back(pathInfo);
                if (pathInfo->Table->Meta && pathInfo->Table->Meta->Attrs.Value("erasure_codec", "none") != "none") {
                    hasErasure = true;
                }
                chunksCount += pathInfo->Table->Stat->ChunkCount;
            }
            groupIdPathInfos.push_back(pathInfos);
        }

        if (auto maxChunks = State_->Configuration->MaxChunksForDqRead.Get().GetOrElse(DEFAULT_MAX_CHUNKS_FOR_DQ_READ); canFallback && chunksCount > maxChunks) {
            throw TFallbackError() << "DQ cannot execute the query. Cause: table with too many chunks";
        }

        if (hasErasure) {
            if (auto codecCpu = State_->Configuration->ErasureCodecCpuForDq.Get(cluster)) {
                dataSizePerJob = Max(ui64(dataSizePerJob / *codecCpu), 10_KB);
            } else {
                hasErasure = false;
            }
        }

        ui64 maxDataSizePerJob = 0;
        if (State_->Configuration->_EnableYtPartitioning.Get(cluster).GetOrElse(false)) {
            TVector<TYtPathInfo::TPtr> paths;
            TVector<TString> keys;
            TMaybe<double> sample;
            for (const auto& [groupId, pathInfos] : Enumerate(groupIdPathInfos)) {
                if (auto sampleSetting = GetSetting(ytRead.Input().Item(groupId).Settings().Ref(), EYtSettingType::Sample)) {
                    sample = FromString<double>(sampleSetting->Child(1)->Child(1)->Content());
                }
                for (const auto& [pathId, pathInfo] : Enumerate(pathInfos)) {
                    auto tableName = pathInfo->Table->Name;
                    if (pathInfo->Table->IsAnonymous && !TYtTableInfo::HasSubstAnonymousLabel(pathInfo->Table->FromNode.Cast())) {
                        tableName = State_->AnonymousLabels.Value(std::make_pair(cluster, tableName), TString());
                        YQL_ENSURE(tableName, "Unaccounted anonymous table: " << pathInfo->Table->Name);
                        pathInfo->Table->Name = tableName;
                    }

                    paths.push_back(pathInfo);
                    keys.emplace_back(TStringBuilder() << groupId << "/" << pathId);
                }
            }
            if (sample && *sample > 0) {
                dataSizePerJob /= *sample;
            }

            auto res = State_->Gateway->GetTablePartitions(NYql::IYtGateway::TGetTablePartitionsOptions(State_->SessionId)
                .Cluster(cluster)
                .MaxPartitions(maxTasks)
                .DataSizePerJob(dataSizePerJob)
                .AdjustDataWeightPerPartition(!canFallback)
                .Config(State_->Configuration->Snapshot())
                .Paths(std::move(paths)));
            if (!res.Success()) {
                const auto message = TStringBuilder() << "DQ cannot execute the query. Cause: failed to partition table";
                YQL_CLOG(ERROR, ProviderDq) << message;
                auto issue = YqlIssue(TPosition(), TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR, message);
                for (auto& subIssue: res.Issues()) {
                    issue.AddSubIssue(MakeIntrusive<TIssue>(subIssue));
                }

                if (canFallback) {
                    throw TFallbackError(MakeIntrusive<TIssue>(std::move(issue))) << message;
                } else {
                    ctx.IssueManager.RaiseIssue(issue);
                    throw TErrorException(TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR) << message;
                }
            }

            serializedPartitions.reserve(res.Partitions.Partitions.size());
            for (const auto& partition : res.Partitions.Partitions) {
                NYT::TNode part = NYT::TNode::CreateMap();
                for (const auto& [pathId, path]: Enumerate(partition.TableRanges)) {
                    // n.b. we're expecting YT API to return ranges in the same order as they were passed
                    part[keys[pathId]] = NYT::PathToNode(path);
                }
                serializedPartitions.push_back(NYT::NodeToYsonString(part));
                YQL_CLOG(TRACE, ProviderDq) << "Partition: " << NYT::NodeToYsonString(part, ::NYson::EYsonFormat::Pretty);
            }
        } else {
            TVector<TVector<std::tuple<ui64, ui64, NYT::TRichYPath>>> partitionTuplesArr;
            ui64 sumAllTableSizes = 0;
            TVector<TVector<ui64>> groupIdColumnarStats = EstimateColumnStats(ctx, cluster, {groupIdPathInfos}, sumAllTableSizes);
            ui64 parts = (sumAllTableSizes + dataSizePerJob - 1) / dataSizePerJob;
            if (canFallback && hasErasure && parts > maxTasks) {
                std::string_view message = "DQ cannot execute the query. Cause: too big table with erasure codec";
                YQL_CLOG(INFO, ProviderDq) << message;
                throw TFallbackError() << message;
            }
            parts = Min<ui64>(parts, maxTasks);
            parts = Max<ui64>(parts, 1);
            partitionTuplesArr.resize(parts);
            serializedPartitions.resize(parts);

            for (const auto& [groupId, input] : Enumerate(ytRead.Input())) {
                TMaybe<double> sample;
                auto sampleSetting = GetSetting(input.Settings().Ref(), EYtSettingType::Sample);
                if (sampleSetting) {
                    sample = FromString<double>(sampleSetting->Child(1)->Child(1)->Content());
                }
                auto& groupStats = groupIdColumnarStats[groupId];
                for (const auto& [pathId, path] : Enumerate(groupIdPathInfos[groupId])) {
                    const auto& tableInfo = *path->Table;
                    YQL_ENSURE(tableInfo.Stat, "Table has no stat.");
                    ui64 dataSize = groupStats[pathId];
                    if (sample) {
                        dataSize *=* sample;
                    }
                    maxDataSizePerJob = Max(maxDataSizePerJob, (dataSize + parts - 1) / parts);
                    ui64 rowsPerPart = (tableInfo.Stat->RecordsCount + parts - 1) / parts;
                    for (ui64 from = 0, i = 0; from < tableInfo.Stat->RecordsCount; from += rowsPerPart, i++) {
                        ui64 to = Min(from + rowsPerPart, tableInfo.Stat->RecordsCount);
                        NYT::TRichYPath path;
                        path.AddRange(NYT::TReadRange::FromRowIndices(from, to));
                        partitionTuplesArr[i].push_back({groupId, pathId, path});
                    }
                }
            }
            int i = 0;
            for (const auto& partitionTuples: partitionTuplesArr) {
                TStringStream out;
                NYson::TYsonWriter writer((IOutputStream*)&out);
                writer.OnBeginMap();
                for (const auto& partitionTuple : partitionTuples) {
                    writer.OnKeyedItem(TStringBuilder() << std::get<0>(partitionTuple) << "/" << std::get<1>(partitionTuple));
                    writer.OnRaw(NYT::NodeToYsonString(NYT::PathToNode(std::get<2>(partitionTuple))));
                }
                writer.OnEndMap();
                serializedPartitions[i++] = out.Str();
            }
        }
        return maxDataSizePerJob;
    }

    void AddInfo(TExprContext& ctx, const TString& message, bool skipIssues) {
        if (!skipIssues) {
            YQL_CLOG(INFO, ProviderDq) << message;
            TIssue info("DQ cannot execute the query. Cause: " + message);
            info.Severity = TSeverityIds::S_INFO;
            ctx.IssueManager.RaiseIssue(info);
        }
    }

    bool CheckPragmas(const TExprNode& node, TExprContext& ctx, bool skipIssues) override {
        if (TYtConfigure::Match(&node)) {
            if (node.ChildrenSize() >= 5) {
                if (node.Child(2)->Content() == "Attr" && node.Child(3)->Content() == "maxrowweight") {
                    if (FromString<NSize::TSize>(node.Child(4)->Content()).GetValue()>NSize::FromMegaBytes(128)) {
                        State_->OnlyNativeExecution = true;
                        return false;
                    } else {
                        return true;
                    }
                }
            }

            if (node.ChildrenSize() >= 4 && node.Child(2)->Content() == "Attr") {
                auto pragma = node.Child(3)->Content();
                if (UNSUPPORTED_YT_PRAGMAS.contains(pragma)) {
                    AddInfo(ctx, TStringBuilder() << "unsupported yt pragma: " << pragma, skipIssues);
                    State_->OnlyNativeExecution = true;
                    return false;
                }

                if (pragma == "pooltrees") {
                    auto pools = NPrivate::GetDefaultParser<TVector<TString>>()(TString{node.Child(4)->Content()});
                    for (const auto& pool : pools) {
                        if (!POOL_TREES_WHITELIST.contains(pool)) {
                            AddInfo(ctx, TStringBuilder() << "unsupported pool tree: " << pool, skipIssues);
                            State_->OnlyNativeExecution = true;
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }

    bool CanRead(const TExprNode& node, TExprContext& ctx, bool skipIssues) override {
        if (TYtConfigure::Match(&node)) {
            return CheckPragmas(node, ctx, skipIssues);
        } else if (auto maybeRead = TMaybeNode<TYtReadTable>(&node)) {
            auto cluster = maybeRead.Cast().DataSource().Cluster().StringValue();
            if (!State_->Configuration->_EnableDq.Get(cluster).GetOrElse(true)) {
                AddInfo(ctx, TStringBuilder() << "disabled for cluster " << cluster, skipIssues);
                return false;
            }
            const auto canUseYtPartitioningApi = State_->Configuration->_EnableYtPartitioning.Get(cluster).GetOrElse(false);
            ui64 chunksCount = 0ull;
            for (auto section: maybeRead.Cast().Input()) {
                if (HasSettingsExcept(maybeRead.Cast().Input().Item(0).Settings().Ref(), DqReadSupportedSettings)) {
                    TStringBuilder info;
                    info << "unsupported path settings: ";
                    if (maybeRead.Cast().Input().Item(0).Settings().Size() > 0) {
                        for (auto& setting : maybeRead.Cast().Input().Item(0).Settings().Ref().Children()) {
                            if (setting->ChildrenSize() != 0) {
                                info << setting->Child(0)->Content() << ",";
                            }
                        }
                    }
                    AddInfo(ctx, info, skipIssues);
                    return false;
                }
                for (auto path: section.Paths()) {
                    if (!path.Table().Maybe<TYtTable>()) {
                        AddInfo(ctx, "non-table path", skipIssues);
                        return false;
                    } else {
                        auto pathInfo = TYtPathInfo(path);
                        auto tableInfo = pathInfo.Table;
                        auto epoch = TEpochInfo::Parse(path.Table().Maybe<TYtTable>().CommitEpoch().Ref());
                        if (!tableInfo->Stat) {
                            AddInfo(ctx, "table without statistics", skipIssues);
                            return false;
                        } else if (!tableInfo->RowSpec) {
                            AddInfo(ctx, "table without row spec", skipIssues);
                            return false;
                        } else if (!tableInfo->Meta) {
                            AddInfo(ctx, "table without meta", skipIssues);
                            return false;
                        } else if (tableInfo->IsAnonymous) {
                            AddInfo(ctx, "anonymous table", skipIssues);
                            return false;
                        } else if ((!epoch.Empty() && *epoch.Get() > 0)) {
                            AddInfo(ctx, "table with non-empty epoch", skipIssues);
                            return false;
                        } else if (NYql::HasSetting(tableInfo->Settings.Ref(), EYtSettingType::WithQB)) {
                            AddInfo(ctx, "table with QB2 premapper", skipIssues);
                            return false;
                        } else if (pathInfo.Ranges && !canUseYtPartitioningApi) {
                            AddInfo(ctx, "table with ranges", skipIssues);
                            return false;
                        } else if (tableInfo->Meta->IsDynamic && !canUseYtPartitioningApi) {
                            AddInfo(ctx, "dynamic table", skipIssues);
                            return false;
                        }

                        chunksCount += tableInfo->Stat->ChunkCount;
                    }
                }
            }
            if (auto maxChunks = State_->Configuration->MaxChunksForDqRead.Get().GetOrElse(DEFAULT_MAX_CHUNKS_FOR_DQ_READ); chunksCount > maxChunks) {
                AddInfo(ctx, "table with too many chunks", skipIssues);
                return false;
            }
            return true;
        }
        AddInfo(ctx, TStringBuilder() << "unsupported callable: " << node.Content(), skipIssues);
        return false;
    }

    bool CanBlockRead(const NNodes::TExprBase& node, TExprContext&, TTypeAnnotationContext&) override {
        auto wrap = node.Cast<TDqReadWideWrap>();
        auto maybeRead = wrap.Input().Maybe<TYtReadTable>();
        if (!maybeRead) {
            return false;
        }


        if (!State_->Configuration->UseRPCReaderInDQ.Get(maybeRead.Cast().DataSource().Cluster().StringValue()).GetOrElse(DEFAULT_USE_RPC_READER_IN_DQ)) {
            return false;
        }

        const auto structType = GetSeqItemType(maybeRead.Raw()->GetTypeAnn()->Cast<TTupleExprType>()->GetItems().back())->Cast<TStructExprType>();
        if (!CanBlockReadTypes(structType)) {
            return false;
        }

        const TYtSectionList& sectionList = wrap.Input().Cast<TYtReadTable>().Input();
        for (size_t i = 0; i < sectionList.Size(); ++i) {
            auto section = sectionList.Item(i);
            if (!NYql::GetSettingAsColumnList(section.Settings().Ref(), EYtSettingType::SysColumns).empty()) {
                return false;
            }
        }
        return true;
    }

    TMaybe<TOptimizerStatistics> ReadStatistics(const TExprNode::TPtr& read, TExprContext& ctx) override {
        Y_UNUSED(ctx);
        TOptimizerStatistics stat(0, 0);
        if (auto maybeRead = TMaybeNode<TYtReadTable>(read)) {
            auto input = maybeRead.Cast().Input();
            for (auto section: input) {
                for (const auto& path: section.Paths()) {
                    auto pathInfo = MakeIntrusive<TYtPathInfo>(path);
                    auto tableInfo = pathInfo->Table;
                    YQL_ENSURE(tableInfo);

                    if (tableInfo->Stat) {
                        stat.Nrows += tableInfo->Stat->RecordsCount;
                    }
                    if (pathInfo->Columns && pathInfo->Columns->GetColumns()) {
                        stat.Ncols += pathInfo->Columns->GetColumns()->size();
                    }
                }
            }
        }
        stat.Cost = stat.Nrows * std::max(stat.Ncols, 1);
        return stat;
    }

    TMaybe<ui64> EstimateReadSize(ui64 dataSizePerJob, ui32 maxTasksPerStage, const TVector<const TExprNode*>& nodes, TExprContext& ctx) override {
        TVector<bool> hasErasurePerNode;
        hasErasurePerNode.reserve(nodes.size());
        TVector<ui64> dataSizes(nodes.size());
        THashMap<TString, TVector<std::pair<const TExprNode*, bool>>> clusterToNodesAndErasure;
        THashMap<TString, TVector<TVector<TYtPathInfo::TPtr>>> clusterToGroups;
        const auto maxChunks = State_->Configuration->MaxChunksForDqRead.Get().GetOrElse(DEFAULT_MAX_CHUNKS_FOR_DQ_READ);
        ui64 chunksCount = 0u;

        for (const auto &node_: nodes) {
            if (auto maybeRead = TMaybeNode<TYtReadTable>(node_)) {

                bool hasErasure = false;
                auto cluster = maybeRead.Cast().DataSource().Cluster().StringValue();
                auto& groupIdPathInfo = clusterToGroups[cluster];

                const auto canUseYtPartitioningApi = State_->Configuration->_EnableYtPartitioning.Get(cluster).GetOrElse(false);

                auto input = maybeRead.Cast().Input();
                for (auto section: input) {
                    groupIdPathInfo.emplace_back();
                    for (const auto& path: section.Paths()) {
                        auto pathInfo = MakeIntrusive<TYtPathInfo>(path);
                        auto tableInfo = pathInfo->Table;

                        YQL_ENSURE(tableInfo);

                        if (pathInfo->Ranges && !canUseYtPartitioningApi) {
                            AddErrorWrap(ctx, node_->Pos(), "table with ranges");
                            return Nothing();
                        } else if (tableInfo->Meta->IsDynamic && !canUseYtPartitioningApi) {
                            AddErrorWrap(ctx, node_->Pos(), "dynamic table");
                            return Nothing();
                        } else { //
                            if (tableInfo->Meta->Attrs.Value("erasure_codec", "none") != "none") {
                                hasErasure = true;
                            }
                            if (tableInfo->Stat) {
                                chunksCount += tableInfo->Stat->ChunkCount;
                            }
                        }
                        groupIdPathInfo.back().emplace_back(pathInfo);
                    }
                }
                if (chunksCount > maxChunks) {
                    AddErrorWrap(ctx, node_->Pos(), "table with too many chunks");
                    return Nothing();
                }
                clusterToNodesAndErasure[cluster].push_back({node_, hasErasure});
            } else {
                AddErrorWrap(ctx, node_->Pos(), TStringBuilder() << "unsupported callable: " << node_->Content());
                return Nothing();
            }
        }
        ui64 dataSize = 0;
        for (auto& [cluster, info]: clusterToNodesAndErasure) {
            auto res = EstimateColumnStats(ctx, cluster, clusterToGroups[cluster], dataSize);
            auto codecCpu = State_->Configuration->ErasureCodecCpuForDq.Get(cluster);
            if (!codecCpu) {
                continue;
            }
            size_t idx = 0;
            for (auto& [node, hasErasure]: info) {
                if (!hasErasure) {
                    ++idx;
                    continue;
                }
                ui64 readSize = std::accumulate(res[idx].begin(), res[idx].end(), 0);
                ++idx;
                dataSizePerJob = Max(ui64(dataSizePerJob / *codecCpu), 10_KB);
                const ui64 parts = (readSize + dataSizePerJob - 1) / dataSizePerJob;
                if (parts > maxTasksPerStage) {
                    AddErrorWrap(ctx, node->Pos(), "too big table with erasure codec");
                    return Nothing();
                }
            }
        }
        return dataSize;
    }

    void AddErrorWrap(TExprContext& ctx, const NYql::TPositionHandle& where, const TString& cause) {
        ctx.AddError(YqlIssue(ctx.GetPosition(where), TIssuesIds::DQ_OPTIMIZE_ERROR, TStringBuilder() << "DQ cannot execute the query. Cause: " << cause));
    }

    TExprNode::TPtr WrapRead(const TDqSettings&, const TExprNode::TPtr& read, TExprContext& ctx) override {
        if (auto maybeYtReadTable = TMaybeNode<TYtReadTable>(read)) {
            TMaybeNode<TCoSecureParam> secParams;
            if (State_->Configuration->Auth.Get().GetOrElse(TString())) {
                const auto cluster = maybeYtReadTable.Cast().DataSource().Cluster();
                secParams = Build<TCoSecureParam>(ctx, read->Pos()).Name().Build(TString("cluster:default_").append(cluster)).Done();
            }
            return Build<TDqReadWrap>(ctx, read->Pos())
                .Input(maybeYtReadTable.Cast())
                .Flags().Build()
                .Token(secParams)
                .Done().Ptr();
        }
        return read;
    }

    TMaybe<bool> CanWrite(const TExprNode& node, TExprContext& ctx) override {
        if (auto maybeWrite = TMaybeNode<TYtWriteTable>(&node)) {
            auto cluster = TString{maybeWrite.Cast().DataSink().Cluster().Value()};
            auto tableName = TString{TYtTableInfo::GetTableLabel(maybeWrite.Cast().Table())};
            auto epoch = TEpochInfo::Parse(maybeWrite.Cast().Table().CommitEpoch().Ref());

            auto tableDesc = State_->TablesData->GetTable(cluster, tableName, epoch);

            if (!State_->Configuration->_EnableDq.Get(cluster).GetOrElse(true)) {
                AddInfo(ctx, TStringBuilder() << "disabled for cluster " << cluster, false);
                return false;
            }

            if (!tableDesc.Meta) {
                AddInfo(ctx, "write to table without meta", false);
                return false;
            }
            if (tableDesc.Meta->IsDynamic) {
                AddInfo(ctx, "write to dynamic table", false);
                return false;
            }

            const auto content = maybeWrite.Cast().Content().Raw();
            if (const auto sorted = content->GetConstraint<TSortedConstraintNode>()) {
                if (const auto distinct = content->GetConstraint<TDistinctConstraintNode>()) {
                    if (distinct->IsOrderBy(*sorted)) {
                        AddInfo(ctx, "unsupported write of unique data", false);
                        return false;
                    }
                }
                if (!content->IsCallable({"Sort", "TopSort", "AssumeSorted"})) {
                    AddInfo(ctx, "unsupported write of sorted data", false);
                    return false;
                }
            }
            return true;
        }

        return Nothing();
    }

    void RegisterMkqlCompiler(NCommon::TMkqlCallableCompilerBase& compiler) override {
        RegisterDqYtMkqlCompilers(compiler, State_);
        State_->Gateway->RegisterMkqlCompiler(compiler);
    }

    bool CanFallback() override {
        return true;
    }

    void Annotate(const TExprNode& node, THashMap<TString, TString>& params) override {
        if (!node.IsCallable("YtDqWideWrite")) {
            return;
        }

        YQL_ENSURE(!params.contains("yt.write"), "Duplicate 'yt.write' graph parameter");

        TString server;
        TString tx;
        TString token;

        for (const auto& setting: node.Child(1)->Children()) {
            if (setting->ChildrenSize() != 2) {
                continue;
            }

            if (setting->Child(0)->IsAtom("server")) {
                server = setting->Child(1)->Content();
            } else if (setting->Child(0)->IsAtom("tx")) {
                tx = setting->Child(1)->Content();
            } else if (setting->Child(0)->IsAtom("secureParams")) {
                if (setting->ChildrenSize() > 1) {
                    TCoSecureParam secure(setting->Child(1));
                    token = secure.Name().StringValue();
                }
            }
        }
        YQL_ENSURE(server, "YtDqWideWrite: server parameter is expected");
        YQL_ENSURE(tx, "YtDqWideWrite: tx parameter is expected");

        auto param = NYT::NodeToYsonString(NYT::TNode()("root_tx", tx)("server", server)("token", token));
        params["yt.write"] = param;
        YQL_CLOG(INFO, ProviderYt) << "DQ annotate: adding yt.write=" << param;
    }

    bool PrepareFullResultTableParams(const TExprNode& root, TExprContext& ctx, THashMap<TString, TString>& params, THashMap<TString, TString>& secureParams) override {
        const auto resOrPull = TResOrPullBase(&root);

        if (FromString<bool>(resOrPull.Discard().Value())) {
            return false;
        }

        auto input = resOrPull.Input().Ptr();
        std::set<std::string_view> usedClusters;
        VisitExpr(*input, [&usedClusters](const TExprNode& node) {
            if (auto ds = TMaybeNode<TYtDSource>(&node)) {
                usedClusters.insert(ds.Cast().Cluster().Value());
                return false;
            }
            if (auto ds = TMaybeNode<TYtDSink>(&node)) {
                usedClusters.insert(ds.Cast().Cluster().Value());
                return false;
            }
            return true;
        });
        TString cluster;
        if (usedClusters.empty()) {
            cluster = State_->Configuration->DefaultCluster.Get().GetOrElse(State_->Gateway->GetDefaultClusterName());
        } else {
            cluster = TString{*usedClusters.begin()};
        }

        const auto type = GetSequenceItemType(input->Pos(), input->GetTypeAnn(), false, ctx);
        YQL_ENSURE(type);
        TYtOutTableInfo outTableInfo(type->Cast<TStructExprType>(), State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);

        const auto res = State_->Gateway->PrepareFullResultTable(
            IYtGateway::TFullResultTableOptions(State_->SessionId)
                .Cluster(cluster)
                .Config(State_->Configuration->GetSettingsForNode(resOrPull.Origin().Ref()))
                .OutTable(outTableInfo)
        );

        auto param = NYT::TNode()("cluster", cluster)("server", res.Server)("path", res.Path)("refName", res.RefName)("codecSpec", res.CodecSpec)("tableAttrs", res.TableAttrs);
        if (res.RootTransactionId) {
            param("root_tx", *res.RootTransactionId);
            if (res.ExternalTransactionId) {
                param("external_tx", *res.ExternalTransactionId);
            }
        } else if (auto externalTx = State_->Configuration->ExternalTx.Get().GetOrElse(TGUID())) {
            param("external_tx", GetGuidAsString(externalTx));
        }
        TString tokenName;
        if (auto auth = State_->Configuration->Auth.Get().GetOrElse(TString())) {
            tokenName = TString("cluster:default_").append(cluster);
            if (!secureParams.contains(tokenName)) {
                secureParams[tokenName] = auth;
            }
        }
        param("token", tokenName);

        const auto strParam = NYT::NodeToYsonString(param);
        params["yt.full_result_table"] = strParam;
        YQL_CLOG(INFO, ProviderYt) << "DQ prepare full result table params: adding yt.full_result_table=" << strParam;
        return true;
    }

    void WriteFullResultTableRef(NYson::TYsonWriter& writer, const TVector<TString>& columns, const THashMap<TString, TString>& graphParams) override {
        auto p = graphParams.FindPtr("yt.full_result_table");
        YQL_ENSURE(p, "Expected 'yt.full_result_table' parameter");
        auto param = NYT::NodeFromYsonString(*p);
        const auto cluster = param["cluster"];
        YQL_ENSURE(cluster.IsString(), "Expected 'cluster' sub-parameter");
        const auto refName = param["refName"];
        YQL_ENSURE(refName.IsString(), "Expected 'refName' sub-parameter");
        NYql::WriteTableReference(writer, YtProviderName, cluster.AsString(), refName.AsString(), true, columns);
    }

    virtual void ConfigurePeepholePipeline(bool beforeDqTransforms, const THashMap<TString, TString>& providerParams, TTransformationPipeline* pipeline) override {
        if (!beforeDqTransforms) {
            return;
        }

        auto state = TYtState::TPtr(State_);
        pipeline->Add(CreateFunctorTransformer([state](TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
            return OptimizeExpr(input, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
                if (TYtReadTable::Match(node.Get()) && !node->Head().IsWorld()) {
                    YQL_CLOG(INFO, ProviderYt) << "Peephole-YtTrimWorld";
                    return ctx.ChangeChild(*node, 0, ctx.NewWorld(node->Pos()));
                }
                return node;
            }, ctx, TOptimizeExprSettings{state->Types});
        }), "YtTrimWorld", TIssuesIds::DEFAULT_ERROR);

        pipeline->Add(CreateSinglePassFunctorTransformer([state, providerParams](TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
            output = input;
            auto status = SubstTables(output, state, true, ctx);
            if (status.Level != IGraphTransformer::TStatus::Error && input != output) {
                YQL_CLOG(INFO, ProviderYt) << "Peephole-YtSubstTables";
            }
            return status;
        }), "YtSubstTables", TIssuesIds::DEFAULT_ERROR);

        pipeline->Add(CreateYtPeepholeTransformer(TYtState::TPtr(State_), providerParams), "YtPeepHole", TIssuesIds::DEFAULT_ERROR);
    }

private:
    TYtState* State_;
};

THolder<IDqIntegration> CreateYtDqIntegration(TYtState* state) {
    Y_ABORT_UNLESS(state);
    return MakeHolder<TYtDqIntegration>(state);
}

}
