#include "yql_yt_provider_impl.h"
#include "yql_yt_op_settings.h"
#include "yql_yt_op_hash.h"
#include "yql_yt_helpers.h"
#include "yql_yt_optimize.h"

#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/yt/gateway/lib/yt_helpers.h>
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/common/yql_configuration.h>
#include <ydb/library/yql/providers/yt/lib/expr_traits/yql_expr_traits.h>
#include <ydb/library/yql/providers/yt/lib/hash/yql_hash_builder.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_helpers.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/transform/yql_exec.h>
#include <ydb/library/yql/core/type_ann/type_ann_expr.h>
#include <ydb/library/yql/core/yql_execution.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/ast/yql_ast.h>

#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>

#include <yt/cpp/mapreduce/common/helpers.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/writer.h>

#include <util/generic/overloaded.h>
#include <util/generic/xrange.h>
#include <util/generic/ylimits.h>
#include <util/generic/guid.h>
#include <util/generic/maybe.h>
#include <util/generic/scope.h>
#include <util/string/cast.h>
#include <util/string/hex.h>

#include <algorithm>
#include <memory>

namespace NYql {

namespace {

using namespace NNodes;
using namespace NThreading;

bool NeedFallback(const TIssues& issues) {
    for (const auto& issue : issues)
        if (TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR == issue.GetCode())
            return true;

    return false;
}

TIssue WrapIssuesOnHybridFallback(TPosition pos, const TIssues& issues, TString fallbackOpName) {
    TIssue result(pos, "Hybrid execution fallback on YT: " + fallbackOpName);
    result.SetCode(TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR, TSeverityIds::S_INFO);

    const std::function<void(TIssue& issue)> toInfo = [&](TIssue& issue) {
        if (issue.Severity == TSeverityIds::S_ERROR
            || issue.Severity == TSeverityIds::S_FATAL
            || issue.Severity == TSeverityIds::S_WARNING) {
            issue.Severity = TSeverityIds::S_INFO;
        }
        for (const auto& subissue : issue.GetSubIssues()) {
            toInfo(*subissue);
        }
    };

    for (const auto& issue : issues) {
        TIssuePtr info(new TIssue(issue));
        toInfo(*info);
        result.AddSubIssue(std::move(info));
    }

    return result;
}

class TYtDataSinkExecTransformer : public TExecTransformerBase {
public:
    TYtDataSinkExecTransformer(TYtState::TPtr state)
        : State_(state)
        , Delegated_(new TNodeMap<TDelegatedInfo>())
    {
        AddHandler(
            {
                TYtSort::CallableName(),
                TYtMap::CallableName(),
                TYtCopy::CallableName(),
                TYtMerge::CallableName(),
                TYtMapReduce::CallableName(),
            },
            RequireAllOf({TYtTransientOpBase::idx_World, TYtTransientOpBase::idx_Input}),
            Hndl(&TYtDataSinkExecTransformer::HandleOutputOp<true>)
        );
        AddHandler(
            {
                TYtFill::CallableName(),
                TYtTouch::CallableName(),
            },
            RequireFirst(),
            Hndl(&TYtDataSinkExecTransformer::HandleOutputOp<true>)
        );
        AddHandler({TYtReduce::CallableName()}, RequireAllOf({TYtTransientOpBase::idx_World, TYtTransientOpBase::idx_Input}), Hndl(&TYtDataSinkExecTransformer::HandleReduce));
        AddHandler({TYtOutput::CallableName()}, RequireFirst(), Pass());
        AddHandler({TYtPublish::CallableName()}, RequireAllOf({TYtPublish::idx_World, TYtPublish::idx_Input}), Hndl(&TYtDataSinkExecTransformer::HandlePublish));
        AddHandler({TYtDropTable::CallableName()}, RequireFirst(), Hndl(&TYtDataSinkExecTransformer::HandleDrop));
        AddHandler({TCoCommit::CallableName()}, RequireFirst(), Hndl(&TYtDataSinkExecTransformer::HandleCommit));
        AddHandler({TYtEquiJoin::CallableName()}, RequireSequenceOf({TYtEquiJoin::idx_World, TYtEquiJoin::idx_Input}),
            Hndl(&TYtDataSinkExecTransformer::HandleEquiJoin));
        AddHandler({TYtStatOut::CallableName()}, RequireAllOf({TYtStatOut::idx_World, TYtStatOut::idx_Input}),
            Hndl(&TYtDataSinkExecTransformer::HandleStatOut));
        AddHandler({TYtDqProcessWrite::CallableName()}, RequireFirst(),
            Hndl(&TYtDataSinkExecTransformer::HandleYtDqProcessWrite));
        AddHandler({TYtTryFirst::CallableName()}, RequireFirst(), Hndl(&TYtDataSinkExecTransformer::HandleTryFirst));
    }

    void Rewind() override {
        Delegated_->clear();
        TExecTransformerBase::Rewind();
    }

private:
    static void PushHybridStats(const TYtState::TPtr& state, TStringBuf statName, TStringBuf opName, const TStringBuf& folderName = "") {
        with_lock(state->StatisticsMutex) {
            state->HybridStatistics[folderName].Entries.emplace_back(TString{statName}, 0, 0, 0, 0, 1);
            state->HybridOpStatistics[opName][folderName].Entries.emplace_back(TString{statName}, 0, 0, 0, 0, 1);
        }
    }

    static TExprNode::TPtr FinalizeOutputOp(const TYtState::TPtr& state, const TString& operationHash,
        const IYtGateway::TRunResult& res, const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx, bool markFinished)
    {
        if (markFinished && !TYtDqProcessWrite::Match(input.Get())) {
            PushHybridStats(state, "YtExecution", input->Content());
        }
        auto outSection = TYtOutputOpBase(input).Output();
        YQL_ENSURE(outSection.Size() == res.OutTableStats.size(), "Invalid output table count in IYtGateway::TRunResult");
        TExprNode::TListType newOutTables;
        for (size_t i: xrange(outSection.Size())) {
            TYtOutTable outTable = outSection.Item(i);
            TExprNode::TListType children = outTable.Raw()->ChildrenList();
            if (auto& name = children[TYtOutTable::idx_Name]; name->IsAtom("") && !res.OutTableStats[i].first.empty())
                name = ctx.NewAtom(name->Pos(), res.OutTableStats[i].first);
            if (const auto stat = res.OutTableStats[i].second)
                children[TYtOutTable::idx_Stat] = stat->ToExprNode(ctx, outTable.Pos()).Ptr();

            newOutTables.push_back(ctx.ChangeChildren(outTable.Ref(), std::move(children)));
        }
        output = ctx.ChangeChild(*input, TYtOutputOpBase::idx_Output, ctx.NewList(outSection.Pos(), std::move(newOutTables)));
        state->NodeHash.emplace(output->UniqueId(), operationHash);
        return markFinished ? ctx.NewWorld(input->Pos()) : ctx.NewAtom(input->Pos(), "");
    }

    using TLaunchOpResult = std::variant<TFuture<IYtGateway::TRunResult>, TStatusCallbackPair>;
    TLaunchOpResult LaunchOutputOp(TString& operationHash, const TExprNode::TPtr& input, TExprContext& ctx) {
        TYtOutputOpBase op(input);

        if (auto opInput = op.Maybe<TYtTransientOpBase>().Input()) {
            bool error = false;
            for (auto section: opInput.Cast()) {
                for (auto path: section.Paths()) {
                    if (auto table = path.Table().Maybe<TYtTable>()) {
                        auto tableInfo = TYtTableInfo(table.Cast());
                        if (!tableInfo.Meta) {
                            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Table " << tableInfo.Name.Quote() << " has no metadata"));
                            error = true;
                        }
                        if (!tableInfo.Stat) {
                            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Table " << tableInfo.Name.Quote() << " has no stat"));
                            error = true;
                        }
                    }
                }
            }
            if (error) {
                return SyncError();
            }
        }

        auto cluster = TString{op.DataSink().Cluster().Value()};
        // Scan entire node because inner lambda may contain YtTableContent with YtPath
        TExprNode::TListType needCalc = GetNodesToCalculate(input);
        if (!needCalc.empty()) {
            YQL_CLOG(DEBUG, ProviderYt) << "Calculating nodes for " << input->Content() << " (UniqueId=" << input->UniqueId() << ")";
            return CalculateNodes(State_, input, cluster, needCalc, ctx);
        }

        auto outSection = op.Output();

        size_t outWithoutName = 0;
        for (auto out: outSection) {
            if (out.Name().Value().empty()) {
                ++outWithoutName;
            }
        }
        if (outWithoutName != outSection.Size()) {
            ctx.AddError(TIssue(ctx.GetPosition(outSection.Pos()), TStringBuilder() << "Incomplete execution of "
                << input->Content() << ", #" << input->UniqueId()));
            return SyncError();
        }

        input->SetState(TExprNode::EState::ExecutionInProgress);

        auto newWorld = ctx.NewWorld(input->Child(0)->Pos());
        newWorld->SetTypeAnn(input->Child(0)->GetTypeAnn());
        newWorld->SetState(TExprNode::EState::ConstrComplete);

        TExprNode::TPtr clonedNode = ctx.ChangeChild(*input, 0, std::move(newWorld));
        clonedNode->SetTypeAnn(input->GetTypeAnn());
        clonedNode->CopyConstraints(*input);

        TExprNode::TPtr optimizedNode = clonedNode;
        if (const auto status = SubstTables(optimizedNode, State_, false, ctx); status.Level == TStatus::Error) {
            return SyncStatus(status);
        }

        const auto settings = State_->Configuration->GetSettingsForNode(*input);
        TUserDataTable crutches = State_->Types->UserDataStorageCrutches;
        if (const auto& defaultGeobase = settings->GeobaseDownloadUrl.Get(cluster)) {
            auto& userDataBlock = (crutches[TUserDataKey::File(TStringBuf("/home/geodata6.bin"))] = TUserDataBlock{EUserDataType::URL, {}, *defaultGeobase, {}, {}});
            userDataBlock.Usage.Set(EUserDataBlockUsage::Path);
        }

        bool hasNonDeterministicFunctions = false;
        if (const auto status = PeepHoleOptimizeBeforeExec(optimizedNode, optimizedNode, State_, hasNonDeterministicFunctions, ctx); status.Level != TStatus::Ok) {
            return SyncStatus(status);
        }

        TUserDataTable files;
        auto filesRes = NCommon::FreezeUsedFiles(*optimizedNode, files, *State_->Types, ctx, MakeUserFilesDownloadFilter(*State_->Gateway, TString(cluster)), crutches);
        if (filesRes.first.Level != TStatus::Ok) {
            if (filesRes.first.Level != TStatus::Error) {
                YQL_CLOG(DEBUG, ProviderYt) << "Freezing files for " << input->Content() << " (UniqueId=" << input->UniqueId() << ")";
            }
            return filesRes;
        }

        THashMap<TString, TString> secureParams;
        NCommon::FillSecureParams(optimizedNode, *State_->Types, secureParams);

        auto config = State_->Configuration->GetSettingsForNode(*input);
        const auto queryCacheMode = config->QueryCacheMode.Get().GetOrElse(EQueryCacheMode::Disable);
        if (queryCacheMode != EQueryCacheMode::Disable) {
            if (!hasNonDeterministicFunctions) {
                operationHash = TYtNodeHashCalculator(State_, cluster, config).GetHash(*optimizedNode);
            }
            YQL_CLOG(DEBUG, ProviderYt) << "Operation hash: " << HexEncode(operationHash).Quote()
                << ", cache mode: " << queryCacheMode;
        }

        YQL_CLOG(DEBUG, ProviderYt) << "Executing " << input->Content() << " (UniqueId=" << input->UniqueId() << ")";

        return State_->Gateway->Run(optimizedNode, ctx,
            IYtGateway::TRunOptions(State_->SessionId)
                .UserDataBlocks(files)
                .UdfModules(State_->Types->UdfModules)
                .UdfResolver(State_->Types->UdfResolver)
                .UdfValidateMode(State_->Types->ValidateMode)
                .PublicId(State_->Types->TranslateOperationId(input->UniqueId()))
                .Config(std::move(config))
                .OptLLVM(State_->Types->OptLLVM.GetOrElse(TString()))
                .OperationHash(operationHash)
                .SecureParams(secureParams)
            );
    }

    template <bool MarkFinished>
    TStatusCallbackPair HandleOutputOp(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (input->HasResult() && input->GetResult().Type() == TExprNode::World) {
            return SyncOk();
        }

        TString operationHash;
        TLaunchOpResult callbackPairOrFuture = LaunchOutputOp(operationHash, input, ctx);
        return std::visit(TOverloaded{
            [&](const TStatusCallbackPair& pair) {
                return pair;
            },
            [&](TFuture<IYtGateway::TRunResult>& future) {
                return WrapModifyFuture(future, [operationHash, state = State_](const IYtGateway::TRunResult& res,
                                                                                const TExprNode::TPtr& input,
                                                                                TExprNode::TPtr& output,
                                                                                TExprContext& ctx)
                {
                    return FinalizeOutputOp(state, operationHash, res, input, output, ctx, MarkFinished);
                });
            }
        }, callbackPairOrFuture);
    }

    TStatusCallbackPair HandleTryFirst(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext&) {

        switch (input->Head().GetState()) {
            case TExprNode::EState::ExecutionComplete:
                PushHybridStats(State_, "Execution", input->TailPtr()->Content());
                output = input->HeadPtr();
                break;
            case TExprNode::EState::Error: {
                PushHybridStats(State_, "Fallback", input->TailPtr()->Content());
                if (State_->Configuration->HybridDqExecutionFallback.Get().GetOrElse(true)) {
                    output = input->TailPtr();
                } else {
                    input->SetState(TExprNode::EState::Error);
                    return SyncError();
                }
                break;
            }
            default:
                Y_UNREACHABLE();
        }
        return SyncStatus(TStatus(TStatus::Repeat, true));
    }

    TStatusCallbackPair HandleReduce(const TExprNode::TPtr& input, TExprContext& ctx) {
        TYtReduce reduce(input);

        if (!NYql::HasSetting(reduce.Settings().Ref(), EYtSettingType::FirstAsPrimary)) {
            return HandleOutputOp<true>(input, ctx);
        }

        if (input->HasResult() && input->GetResult().Type() == TExprNode::World) {
            return SyncOk();
        }

        TString operationHash;
        TLaunchOpResult callbackPairOrFuture = LaunchOutputOp(operationHash, input, ctx);
        if (auto* pair = std::get_if<TStatusCallbackPair>(&callbackPairOrFuture)) {
            return *pair;
        }

        auto future = std::get<TFuture<IYtGateway::TRunResult>>(callbackPairOrFuture);
        return std::make_pair(IGraphTransformer::TStatus::Async, future.Apply(
            [operationHash, state = State_](const TFuture<IYtGateway::TRunResult>& completedFuture) {
                return TAsyncTransformCallback(
                    [completedFuture, operationHash, state](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                        output = input;
                        const auto& res = completedFuture.GetValue();
                        if (!res.Success()) {
                            YQL_ENSURE(!res.Issues().Empty());
                            if (res.Issues().begin()->IssueCode == TIssuesIds::YT_MAX_DATAWEIGHT_PER_JOB_EXCEEDED) {
                                YQL_CLOG(INFO, ProviderYt) << "Execution of node: " << input->Content()
                                                           << " with FirstAsPrimary exceeds max dataweight per job, rebuilding node";
                                TYtReduce reduce(input);
                                const bool joinReduceForSecond = NYql::UseJoinReduceForSecondAsPrimary(
                                    reduce.Settings().Ref());
                                auto settings = NYql::RemoveSettings(reduce.Settings().Ref(),
                                    EYtSettingType::FirstAsPrimary | EYtSettingType::JoinReduce, ctx);
                                if (joinReduceForSecond) {
                                    settings = NYql::AddSetting(*settings, EYtSettingType::JoinReduce, nullptr, ctx);
                                }

                                output = ctx.ChangeChild(*input, TYtReduce::idx_Settings, std::move(settings));
                                output->SetState(TExprNode::EState::Initial);
                                return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
                            }
                        }

                        TIssueScopeGuard issueScope(ctx.IssueManager, [&]() {
                            return MakeIntrusive<TIssue>(
                                ctx.GetPosition(input->Pos()),
                                TStringBuilder() << "Execution of node: " << input->Content() << " with TryFirstAsPrimary");
                        });
                        res.ReportIssues(ctx.IssueManager);

                        if (!res.Success()) {
                            input->SetState(TExprNode::EState::Error);
                            return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Error);
                        } else {
                            TExprNode::TPtr resultNode = FinalizeOutputOp(state, operationHash, res, input, output, ctx, true);
                            input->SetState(TExprNode::EState::ExecutionComplete);
                            output->SetResult(std::move(resultNode));
                            if (input != output) {
                                return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
                            }
                            return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Ok);
                        }
                    });
            }));
    }

    TStatusCallbackPair HandleDrop(const TExprNode::TPtr& input, TExprContext& ctx) {
        input->SetState(TExprNode::EState::ExecutionInProgress);

        auto drop = TYtDropTable(input);

        auto newWorld = ctx.ShallowCopy(*input->Child(0));
        newWorld->SetTypeAnn(input->Child(0)->GetTypeAnn());
        newWorld->SetState(TExprNode::EState::ExecutionComplete);

        TExprNode::TPtr clonedNode = ctx.ChangeChild(*input, 0, std::move(newWorld));
        clonedNode->SetTypeAnn(input->GetTypeAnn());
        clonedNode->CopyConstraints(*input);

        auto status = SubstTables(clonedNode, State_, true, ctx);
        if (status.Level == TStatus::Error) {
            return SyncStatus(status);
        }

        auto future = State_->Gateway->Run(clonedNode, ctx,
            IYtGateway::TRunOptions(State_->SessionId)
                .PublicId(State_->Types->TranslateOperationId(input->UniqueId()))
                .Config(State_->Configuration->GetSettingsForNode(*input))
            );

        return WrapFuture(future, [](const IYtGateway::TRunResult& /*res*/, const TExprNode::TPtr& input, TExprContext& ctx) {
            return ctx.NewWorld(input->Pos());
        });
    }

    TStatusCallbackPair HandlePublish(const TExprNode::TPtr& input, TExprContext& ctx) {
        auto publish = TYtPublish(input);
        auto cluster = TString{publish.DataSink().Cluster().Value()};
        auto path = TString{ publish.Publish().Name().Value() };

        auto commitEpoch = TEpochInfo::Parse(publish.Publish().CommitEpoch().Ref()).GetOrElse(0);
        TYtTableDescription& nextDescription = State_->TablesData->GetModifTable(cluster, path, commitEpoch);

        auto config = State_->Configuration->GetSettingsForNode(*input);

        const auto mode = NYql::GetSetting(publish.Settings().Ref(), EYtSettingType::Mode);
        const bool initial = NYql::HasSetting(publish.Settings().Ref(), EYtSettingType::Initial);

        auto dataHash = TYtNodeHashCalculator(State_, cluster, config).GetHash(publish.Input().Ref());
        YQL_CLOG(INFO, ProviderYt) << "Publish data hash \"" << HexEncode(dataHash) << "\" for table " << cluster << "." << path << "#" << commitEpoch;
        TString nextHash;
        if (nextDescription.IsReplaced && initial) {
            nextHash = dataHash;
        } else {
            auto epoch = TEpochInfo::Parse(publish.Publish().Epoch().Ref()).GetOrElse(0);
            const TYtTableDescription& readDescription = State_->TablesData->GetTable(cluster, path, epoch);
            TString prevHash;
            if (!initial) {
                prevHash = nextDescription.Hash.GetOrElse({});
            } else {
                if (readDescription.Hash) {
                    prevHash = *readDescription.Hash;
                } else {
                    prevHash = TYtNodeHashCalculator(State_, cluster, config).GetHash(publish.Publish().Ref());
                }
            }
            YQL_CLOG(INFO, ProviderYt) << "Publish prev content hash \"" << HexEncode(prevHash) << "\" for table " << cluster << "." << path << "#" << commitEpoch;
            if (!prevHash.empty() && !dataHash.empty()) {
                THashBuilder builder;
                builder << TYtNodeHashCalculator::MakeSalt(config, cluster) << prevHash << dataHash;
                nextHash = builder.Finish();
            }
        }
        nextDescription.Hash = nextHash;
        if (!nextDescription.Hash->Empty()) {
            YQL_CLOG(INFO, ProviderYt) << "Using publish hash \"" << HexEncode(*nextDescription.Hash) << "\" for table " << cluster << "." << path << "#" << commitEpoch;
        }

        input->SetState(TExprNode::EState::ExecutionInProgress);

        auto newWorld = ctx.NewWorld(input->Child(0)->Pos());
        newWorld->SetTypeAnn(input->Child(0)->GetTypeAnn());
        newWorld->SetState(TExprNode::EState::ExecutionComplete);

        TExprNode::TPtr clonedNode = ctx.ChangeChild(*input, TYtPublish::idx_World, std::move(newWorld));
        clonedNode->SetTypeAnn(publish.Ref().GetTypeAnn());
        clonedNode->SetState(TExprNode::EState::ConstrComplete);

        auto status = SubstTables(clonedNode, State_, true, ctx);
        if (status.Level == TStatus::Error) {
            return SyncStatus(status);
        }

        auto future = State_->Gateway->Publish(clonedNode, ctx,
            IYtGateway::TPublishOptions(State_->SessionId)
                .PublicId(State_->Types->TranslateOperationId(input->UniqueId()))
                .DestinationRowSpec(nextDescription.RowSpec)
                .Config(std::move(config))
                .OptLLVM(State_->Types->OptLLVM.GetOrElse(TString()))
                .OperationHash(nextDescription.Hash.GetOrElse(""))
            );

        return WrapFuture(future, [](const IYtGateway::TPublishResult& /*res*/, const TExprNode::TPtr& input, TExprContext& ctx) {
            return ctx.NewWorld(input->Pos());
        });
    }

    TStatusCallbackPair HandleCommit(const TExprNode::TPtr& input, TExprContext& ctx) {
        auto commit = TCoCommit(input);
        auto settings = NCommon::ParseCommitSettings(commit, ctx);
        YQL_ENSURE(settings.Epoch);

        ui32 epoch = FromString(settings.Epoch.Cast().Value());
        auto cluster = commit.DataSink().Cast<TYtDSink>().Cluster().Value();

        auto settingsVer = State_->Configuration->FindNodeVer(*input);
        auto future = State_->Gateway->Commit(
            IYtGateway::TCommitOptions(State_->SessionId)
                .Cluster(TString{cluster})
            );

        if (State_->EpochDependencies.contains(epoch)) {
            auto state = State_;
            return WrapModifyFuture(future, [state, epoch, settingsVer](const IYtGateway::TCommitResult& /*res*/, const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                input->SetState(TExprNode::EState::ExecutionComplete); // Don't execute this node again
                output = ctx.ExactShallowCopy(*input);
                state->LoadEpochMetadata.ConstructInPlace(epoch, settingsVer);
                ctx.Step.Repeat(TExprStep::LoadTablesMetadata);
                return ctx.NewWorld(input->Pos());
            });
        }

        return WrapFuture(future, [](const IYtGateway::TCommitResult& /*res*/, const TExprNode::TPtr& input, TExprContext& ctx) {
            return ctx.NewWorld(input->Pos());
        });
    }

    TStatusCallbackPair HandleEquiJoin(const TExprNode::TPtr& input, TExprContext& ctx) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Execution of "
            << input->Content() << ", #" << input->UniqueId() << " should not be reached"));
        return SyncError();
    }

    TStatusCallbackPair HandleStatOut(const TExprNode::TPtr& input, TExprContext& ctx) {
        Y_UNUSED(ctx);

        auto config = State_->Configuration->GetSettingsForNode(*input);
        auto future = State_->Gateway->Run(input, ctx,
            IYtGateway::TRunOptions(State_->SessionId)
                .Config(std::move(config))
                .PublicId(State_->Types->TranslateOperationId(input->UniqueId()))
            );

        return WrapFuture(
            future,
            [](const NCommon::TOperationResult& result, const TExprNode::TPtr& input, TExprContext& ctx) {
                Y_UNUSED(result);
                return ctx.NewWorld(input->Pos());
            }
        );
    }

    TStatusCallbackPair HandleYtDqProcessWrite(const TExprNode::TPtr& input, TExprContext& ctx) {
        const TYtDqProcessWrite op(input);
        const auto section = op.Output().Cast<TYtOutSection>();
        Y_ENSURE(section.Size() == 1, "TYtDqProcessWrite expects 1 output table but got " << section.Size());
        const TYtOutTable tmpTable = section.Item(0);

        if (!input->HasResult()) {
            if (!tmpTable.Name().Value().empty()) {
                ctx.AddError(TIssue(ctx.GetPosition(section.Pos()), TStringBuilder() << "Incomplete execution of "
                    << input->Content() << ", #" << input->UniqueId()));
                return SyncError();
            }

            input->SetState(TExprNode::EState::ExecutionInProgress);

            return MakeTableForDqWrite(input, ctx);
        }
        else if (const auto& result = input->GetResult(); result.IsAtom()) {
            const auto publicId = State_->Types->TranslateOperationId(input->UniqueId());
            if (result.IsAtom("")) {
                // Second iteration: do the actual write.
                if (publicId) {
                    if (State_->HybridInFlightOprations.empty())
                        State_->HybridStartTime = NMonotonic::TMonotonic::Now();
                    State_->HybridInFlightOprations.emplace(*publicId);
                }
                return RunDqWrite(input, ctx, tmpTable);
            } else {
                if (publicId) {
                    YQL_ENSURE(State_->HybridInFlightOprations.erase(*publicId), "Operation " << *publicId << " not found.");
                    if (State_->HybridInFlightOprations.empty()) {
                        const auto interval = NMonotonic::TMonotonic::Now() - State_->HybridStartTime;
                        State_->TimeSpentInHybrid += interval;
                        with_lock(State_->StatisticsMutex) {
                            State_->Statistics[Max<ui32>()].Entries.emplace_back("HybridTimeSpent", 0, 0, 0, 0, interval.MilliSeconds());
                        }
                    }
                }

                if (result.IsAtom("FallbackOnError"))
                    return SyncOk();

                YQL_ENSURE(result.IsAtom("DQ_completed"), "Unexpected result atom: " << result.Content());
                // Third iteration: collect temporary table statistics.
                return CollectDqWrittenTableStats(input, ctx);
            }
        }
        else {
            // Fourth iteration: everything is done, return ok status.
            Y_ENSURE(input->GetResult().Type() == TExprNode::World, "Unexpected result type: " << input->GetResult().Type());
            return SyncOk();
        }
    }

private:
    TStatusCallbackPair MakeTableForDqWrite(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (input->HasResult() && input->GetResult().Type() == TExprNode::World) {
            return SyncOk();
        }

        auto newWorld = ctx.NewWorld(input->Head().Pos());
        newWorld->SetTypeAnn(input->Head().GetTypeAnn());
        newWorld->SetState(TExprNode::EState::ConstrComplete);

        auto optimizedNode = ctx.ChangeChild(*input, 0, std::move(newWorld));
        optimizedNode->SetTypeAnn(input->GetTypeAnn());
        optimizedNode->CopyConstraints(*input);

        if (const auto status = SubstTables(optimizedNode, State_, false, ctx); status.Level == TStatus::Error) {
            return SyncStatus(status);
        }

        const TYtDqProcessWrite op(input);
        const auto cluster = op.DataSink().Cluster().StringValue();
        const auto config = State_->Configuration->GetSettingsForNode(*input);

        TUserDataTable crutches = State_->Types->UserDataStorageCrutches;
        if (const auto& defaultGeobase = config->GeobaseDownloadUrl.Get(cluster)) {
            auto& userDataBlock = (crutches[TUserDataKey::File(TStringBuf("/home/geodata6.bin"))] = TUserDataBlock{EUserDataType::URL, {}, *defaultGeobase, {}, {}});
            userDataBlock.Usage.Set(EUserDataBlockUsage::Path);
        }

        bool hasNonDeterministicFunctions = false;
        if (const auto status = PeepHoleOptimizeBeforeExec(optimizedNode, optimizedNode, State_, hasNonDeterministicFunctions, ctx); status.Level != TStatus::Ok) {
            return SyncStatus(status);
        }

        TUserDataTable files;
        if (const auto filesRes = NCommon::FreezeUsedFiles(*optimizedNode, files, *State_->Types, ctx, MakeUserFilesDownloadFilter(*State_->Gateway, TString(cluster)), crutches);
            filesRes.first.Level != TStatus::Ok) {
            if (filesRes.first.Level != TStatus::Error) {
                YQL_CLOG(DEBUG, ProviderYt) << "Freezing files for " << input->Content() << " (UniqueId=" << input->UniqueId() << ")";
            }
            return filesRes;
        }

        TString operationHash;
        if (const auto queryCacheMode = config->QueryCacheMode.Get().GetOrElse(EQueryCacheMode::Disable); queryCacheMode != EQueryCacheMode::Disable) {
            if (!hasNonDeterministicFunctions) {
                operationHash = TYtNodeHashCalculator(State_, cluster, config).GetHash(*input);
            }
            YQL_CLOG(DEBUG, ProviderYt) << "Operation hash: " << HexEncode(operationHash).Quote() << ", cache mode: " << queryCacheMode;
        }

        TVector<TString> securityTags;
        VisitExpr(input, [&securityTags](const TExprNode::TPtr& node) -> bool {
            if (TYtOutTable::Match(node.Get())) {
                return false;
            }
            if (TYtTableBase::Match(node.Get())) {
                const TYtTableBase table(node);
                const auto &curTags = TYtTableStatInfo(table.Stat()).SecurityTags;
                Copy(curTags.begin(), curTags.end(), std::back_inserter(securityTags));
                return false;
            }
            return true;
        });

        YQL_CLOG(DEBUG, ProviderYt) << "Preparing " << input->Content() << " (UniqueId=" << input->UniqueId() << ")";

        auto future = State_->Gateway->Prepare(input, ctx,
            IYtGateway::TPrepareOptions(State_->SessionId)
                .PublicId(State_->Types->TranslateOperationId(input->UniqueId()))
                .Config(std::move(config))
                .OperationHash(operationHash)
                .SecurityTags(securityTags)
            );

        return WrapModifyFuture(future, [operationHash, state = State_](const IYtGateway::TRunResult& res,
                                                                        const TExprNode::TPtr& input,
                                                                        TExprNode::TPtr& output,
                                                                        TExprContext& ctx)
        {
            return FinalizeOutputOp(state, operationHash, res, input, output, ctx, bool(res.OutTableStats.front().second));
        });
    }

    TStatusCallbackPair RunDqWrite(const TExprNode::TPtr& input, TExprContext& ctx, const TYtOutTable& tmpTable) {
        const TYtDqProcessWrite op(input);

        IDataProvider* dqProvider = nullptr;
        TExprNode::TPtr delegatedNode;

        if (auto it = Delegated_->find(input.Get()); it != Delegated_->end()) {
            dqProvider = it->second.DelegatedProvider;
            delegatedNode = it->second.DelegatedNode;
        }

        if (!delegatedNode) {
            const auto cluster = op.DataSink().Cluster();
            const auto config = State_->Configuration->GetSettingsForNode(*input);
            const auto tmpFolder = GetTablesTmpFolder(*config);
            auto clusterStr = TString{cluster.Value()};

            delegatedNode = input->ChildPtr(TYtDqProcessWrite::idx_Input);

            auto server = State_->Gateway->GetClusterServer(clusterStr);
            YQL_ENSURE(server, "Invalid YT cluster: " << clusterStr);

            NYT::TRichYPath realTable = State_->Gateway->GetWriteTable(State_->SessionId, clusterStr, tmpTable.Name().StringValue(), tmpFolder);
            realTable.Append(true);
            YQL_ENSURE(realTable.TransactionId_.Defined(), "Expected TransactionId");

            NYT::TNode writerOptions = NYT::TNode::CreateMap();
            if (auto maxRowWeight = config->MaxRowWeight.Get(clusterStr)) {
                writerOptions["max_row_weight"] = static_cast<i64>(maxRowWeight->GetValue());
            }

            NYT::TNode outSpec;
            NYT::TNode type;
            {
                auto rowSpec = TYqlRowSpecInfo(tmpTable.RowSpec());
                NYT::TNode spec;
                rowSpec.FillCodecNode(spec[YqlRowSpecAttribute]);
                outSpec = NYT::TNode::CreateMap()(TString{YqlIOSpecTables}, NYT::TNode::CreateList().Add(spec));
                type = rowSpec.GetTypeNode();
            }

            // These settings will be passed to YT peephole callback from DQ
            auto settings = Build<TCoNameValueTupleList>(ctx, delegatedNode->Pos())
                .Add()
                    .Name().Value("yt_cluster", TNodeFlags::Default).Build()
                    .Value<TCoAtom>().Value(clusterStr).Build()
                .Build()
                .Add()
                    .Name().Value("yt_server", TNodeFlags::Default).Build()
                    .Value<TCoAtom>().Value(server).Build()
                .Build()
                .Add()
                    .Name().Value("yt_table", TNodeFlags::Default).Build()
                    .Value<TCoAtom>().Value(NYT::NodeToYsonString(NYT::PathToNode(realTable))).Build()
                .Build()
                .Add()
                    .Name().Value("yt_tableName", TNodeFlags::Default).Build()
                    .Value<TCoAtom>().Value(tmpTable.Name().Value()).Build()
                .Build()
                .Add()
                    .Name().Value("yt_tableType", TNodeFlags::Default).Build()
                    .Value<TCoAtom>().Value(NYT::NodeToYsonString(type)).Build()
                .Build()
                .Add()
                    .Name().Value("yt_writeOptions", TNodeFlags::Default).Build()
                    .Value<TCoAtom>().Value(NYT::NodeToYsonString(writerOptions)).Build()
                .Build()
                .Add()
                    .Name().Value("yt_outSpec", TNodeFlags::Default).Build()
                    .Value<TCoAtom>().Value(NYT::NodeToYsonString(outSpec)).Build()
                .Build()
                .Add()
                    .Name().Value("yt_tx", TNodeFlags::Default).Build()
                    .Value<TCoAtom>().Value(GetGuidAsString(*realTable.TransactionId_), TNodeFlags::Default).Build()
                .Build()
                .Done().Ptr();

            auto atomType = ctx.MakeType<TUnitExprType>();

            for (auto child: settings->Children()) {
                child->Child(0)->SetTypeAnn(atomType);
                child->Child(0)->SetState(TExprNode::EState::ConstrComplete);
                child->Child(1)->SetTypeAnn(atomType);
                child->Child(1)->SetState(TExprNode::EState::ConstrComplete);
            }

            delegatedNode = Build<TPull>(ctx, delegatedNode->Pos())
                    .Input(std::move(delegatedNode))
                    .BytesLimit()
                        .Value(TString())
                    .Build()
                    .RowsLimit()
                        .Value(0U)
                    .Build()
                    .FormatDetails()
                        .Value(ui32(NYson::EYsonFormat::Binary))
                    .Build()
                    .Settings(settings)
                    .Format()
                        .Value(0U)
                    .Build()
                    .PublicId()
                        .Value(ToString(State_->Types->TranslateOperationId(input->UniqueId())))
                    .Build()
                    .Discard()
                        .Value(ToString(true), TNodeFlags::Default)
                    .Build()
                    .Origin(input)
                    .Done().Ptr();

            for (auto idx: {TResOrPullBase::idx_BytesLimit, TResOrPullBase::idx_RowsLimit, TResOrPullBase::idx_FormatDetails,
                TResOrPullBase::idx_Settings, TResOrPullBase::idx_Format, TResOrPullBase::idx_PublicId, TResOrPullBase::idx_Discard }) {
                delegatedNode->Child(idx)->SetTypeAnn(atomType);
                delegatedNode->Child(idx)->SetState(TExprNode::EState::ConstrComplete);
            }

            delegatedNode->SetTypeAnn(input->GetTypeAnn());
            delegatedNode->SetState(TExprNode::EState::ConstrComplete);
        }

        if (!dqProvider) {
            if (auto p = State_->Types->DataSourceMap.FindPtr(DqProviderName)) {
                dqProvider = p->Get();
            }
        }
        YQL_ENSURE(dqProvider);

        input->SetState(TExprNode::EState::ExecutionInProgress);
        TExprNode::TPtr delegatedNodeOutput;

        if (auto status = dqProvider->GetCallableExecutionTransformer().Transform(delegatedNode, delegatedNodeOutput, ctx); status.Level != TStatus::Async) {
            YQL_ENSURE(status.Level != TStatus::Ok, "Asynchronous execution is expected in a happy path.");
            if (const auto flags = op.Flags()) {
                TString fallbackOpName;
                for (const auto& atom : flags.Cast()) {
                    TStringBuf flagName = atom.Value();
                    if (flagName.SkipPrefix("FallbackOp")) {
                        fallbackOpName = flagName;
                        break;
                    }
                }
                for (const auto& atom : flags.Cast()) {
                    if (atom.Value() == "FallbackOnError") {
                        input->SetResult(atom.Ptr());
                        input->SetState(TExprNode::EState::Error);
                        if (const auto issies = ctx.AssociativeIssues.extract(delegatedNode.Get())) {
                            if (NeedFallback(issies.mapped())) {
                                ctx.IssueManager.RaiseIssue(WrapIssuesOnHybridFallback(ctx.GetPosition(input->Pos()), issies.mapped(), fallbackOpName));
                                return SyncStatus(IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true));
                            } else {
                                ctx.IssueManager.RaiseIssues(issies.mapped());
                                return SyncStatus(status);
                            }
                        }
                    }
                }
            }

            if (const auto issies = ctx.AssociativeIssues.extract(delegatedNode.Get())) {
                ctx.IssueManager.RaiseIssues(issies.mapped());
            }
            return SyncStatus(status);
        }

        (*Delegated_)[input.Get()] = TDelegatedInfo{dqProvider, delegatedNode};

        auto dqFuture = dqProvider->GetCallableExecutionTransformer().GetAsyncFuture(*delegatedNode);

        TAsyncTransformCallbackFuture callbackFuture = dqFuture.Apply(
            [delegated = std::weak_ptr<TNodeMap<TDelegatedInfo>>(Delegated_), state = State_](const TFuture<void>& completedFuture) {
                return TAsyncTransformCallback(
                    [completedFuture, delegated, state](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                        completedFuture.GetValue();
                        TExprNode::TPtr delegatedNode;
                        IDataProvider* dqProvider = nullptr;
                        auto lock = delegated.lock();
                        if (lock) {
                            if (auto it = lock->find(input.Get()); it != lock->end()) {
                                delegatedNode = it->second.DelegatedNode;
                                dqProvider = it->second.DelegatedProvider;
                            }
                        }
                        YQL_ENSURE(delegatedNode && dqProvider);
                        TExprNode::TPtr delegatedNodeOutput;
                        const auto dqWriteStatus = dqProvider->GetCallableExecutionTransformer()
                            .ApplyAsyncChanges(delegatedNode, delegatedNodeOutput, ctx);

                        YQL_ENSURE(dqWriteStatus != TStatus::Async, "ApplyAsyncChanges should not return Async.");

                        if (dqWriteStatus == TStatus::Repeat) {
                            output = input;
                            input->SetState(TExprNode::EState::ExecutionRequired);
                            return dqWriteStatus;
                        }
                        lock->erase(input.Get());

                        if (dqWriteStatus != TStatus::Ok) {
                            output = input;
                            if (const auto flags = TYtDqProcessWrite(input).Flags()) {
                                TString fallbackOpName;
                                for (const auto& atom : flags.Cast()) {
                                    TStringBuf flagName = atom.Value();
                                    if (flagName.SkipPrefix("FallbackOp")) {
                                        fallbackOpName = flagName;
                                        break;
                                    }
                                }
                                for (const auto& atom : flags.Cast()) {
                                    if (atom.Value() == "FallbackOnError") {
                                        output->SetResult(atom.Ptr());
                                        output->SetState(TExprNode::EState::Error);
                                        if (const auto issies = ctx.AssociativeIssues.extract(delegatedNode.Get())) {
                                            if (NeedFallback(issies.mapped())) {
                                                ctx.IssueManager.RaiseIssue(WrapIssuesOnHybridFallback(ctx.GetPosition(input->Pos()), issies.mapped(), fallbackOpName));
                                                return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
                                            } else {
                                                ctx.IssueManager.RaiseIssues(issies.mapped());
                                                return dqWriteStatus;
                                            }
                                        }
                                    }
                                }
                            }
                            if (const auto issies = ctx.AssociativeIssues.extract(delegatedNode.Get())) {
                                ctx.IssueManager.RaiseIssues(issies.mapped());
                            }
                            return dqWriteStatus;
                        }

                        input->SetState(TExprNode::EState::ExecutionComplete);
                        output = ctx.ShallowCopy(*input);
                        output->SetResult(ctx.NewAtom(input->Pos(), "DQ_completed"));
                        if (const auto it = state->NodeHash.find(input->UniqueId()); state->NodeHash.cend() != it) {
                            auto hash = state->NodeHash.extract(it);
                            hash.key() = output->UniqueId();
                            state->NodeHash.insert(std::move(hash));
                        }
                        return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
                    });
            });

        return std::make_pair(IGraphTransformer::TStatus::Async, callbackFuture);
    }

    TStatusCallbackPair CollectDqWrittenTableStats(const TExprNode::TPtr& input, TExprContext& ctx) {
        auto statsFuture = State_->Gateway->GetTableStat(input, ctx,
            IYtGateway::TPrepareOptions(State_->SessionId)
                .PublicId(State_->Types->TranslateOperationId(input->UniqueId()))
                .Config(State_->Configuration->GetSettingsForNode(*input))
                .OperationHash(State_->NodeHash[input->UniqueId()])
            );

        return WrapFutureCallback(
            statsFuture,
            [state = State_](const IYtGateway::TRunResult& res, const TExprNode::TPtr& in, TExprNode::TPtr& out, TExprContext& ctx) {
                auto result = FinalizeOutputOp(state, state->NodeHash[in->UniqueId()], res, in, out, ctx, true);
                out->SetResult(std::move(result));
                in->SetState(TExprNode::EState::ExecutionComplete);
                if (in != out) {
                    return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
                }
                return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Ok);
            }
        );
    }

    const TYtState::TPtr State_;

    struct TDelegatedInfo {
        IDataProvider* DelegatedProvider;
        TExprNode::TPtr DelegatedNode;
    };
    std::shared_ptr<TNodeMap<TDelegatedInfo>> Delegated_;
};

}

THolder<TExecTransformerBase> CreateYtDataSinkExecTransformer(TYtState::TPtr state) {
    return THolder(new TYtDataSinkExecTransformer(state));
}

}
