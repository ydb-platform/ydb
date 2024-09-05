#include "yql_yt_provider_impl.h"
#include "yql_yt_helpers.h"
#include "yql_yt_optimize.h"
#include "yql_yt_op_hash.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_helpers.h>
#include <ydb/library/yql/providers/yt/lib/hash/yql_hash_builder.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/transform/yql_exec.h>
#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/type_ann/type_ann_expr.h>
#include <ydb/library/yql/core/yql_execution.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/utils/log/log.h>

#include <util/string/hex.h>

namespace NYql {

using namespace NNodes;

namespace {

class TYtDataSourceExecTransformer : public TExecTransformerBase {
public:
    TYtDataSourceExecTransformer(TYtState::TPtr state)
        : State_(state)
    {

        AddHandler({TStringBuf("Result"), TStringBuf("Pull")}, RequireNone(), Hndl(&TYtDataSourceExecTransformer::HandleResOrPull));
        AddHandler(
            {
                TYtReadTableScheme::CallableName(),
                TYtPath::CallableName(),
                TYtSection::CallableName(),
            },
            RequireFirst(),
            Pass());
        AddHandler({TYtReadTable::CallableName()}, RequireSequenceOf({TYtReadTable::idx_World, TYtReadTable::idx_Input}), Pass());
        AddHandler({TYtTable::CallableName()}, RequireNone(), Pass());
        AddHandler({TYtConfigure::CallableName()}, RequireFirst(), Hndl(&TYtDataSourceExecTransformer::HandleConfigure));
    }

protected:
    TString WriteTableScheme(TYtReadTableScheme readScheme, NYson::EYsonFormat ysonFormat, bool withType) {
        TStringStream out;
        NYson::TYsonWriter writer(&out, ysonFormat, ::NYson::EYsonType::Node, true);
        writer.OnBeginMap();

        if (withType) {
            writer.OnKeyedItem("Type");
            auto valueType = readScheme.Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems()[1];
            NCommon::WriteTypeToYson(writer, valueType);
        }

        auto cluster = TString{readScheme.DataSource().Cluster().Value()};
        auto tableName = TString{readScheme.Table().Name().Value()};
        auto view = NYql::GetSetting(readScheme.Table().Settings().Ref(), EYtSettingType::View);
        TString viewName = view ? TString{view->Child(1)->Content()} : TString();

        const TYtTableDescription& tableDesc = State_->TablesData->GetTable(cluster, tableName,
            TEpochInfo::Parse(readScheme.Table().Epoch().Ref()));

        writer.OnKeyedItem("Data");
        tableDesc.ToYson(writer, cluster, tableName, viewName);

        writer.OnEndMap();
        return out.Str();
    }

protected:
    TStatusCallbackPair HandleResOrPull(const TExprNode::TPtr& input, TExprContext& ctx) {
        YQL_CLOG(DEBUG, ProviderYt) << "Executing " << input->Content() << " (UniqueId=" << input->UniqueId() << ")";
        TResOrPullBase resOrPull(input);

        IDataProvider::TFillSettings fillSettings = NCommon::GetFillSettings(resOrPull.Ref());
        YQL_ENSURE(fillSettings.Format == IDataProvider::EResultFormat::Yson || fillSettings.Format == IDataProvider::EResultFormat::Skiff);

        auto data = resOrPull.Input();
        if (auto maybePull = resOrPull.Maybe<TPull>()) {
            TExprNode::TListType needCalc = GetNodesToCalculate(data.Ptr());
            if (!needCalc.empty()) {
                return CalculateNodes(State_, data.Ptr(), TString{GetClusterName(data)}, needCalc, ctx);
            }

            if (auto maybeScheme = data.Maybe<TCoRight>().Input().Maybe<TYtReadTableScheme>()) {
                TString result = WriteTableScheme(maybeScheme.Cast(), NCommon::GetYsonFormat(fillSettings),
                    NCommon::HasResOrPullOption(resOrPull.Ref(), "type"));
                input->SetState(TExprNode::EState::ExecutionComplete);
                input->SetResult(ctx.NewAtom(input->Pos(), result));
                return SyncOk();
            }

            if (auto read = data.Maybe<TCoRight>().Input().Maybe<TYtReadTable>()) {
                auto newRead = ctx.ExactShallowCopy(read.Cast().Ref());
                newRead->ChildRef(0) = ctx.NewWorld(read.Cast().World().Pos());
                newRead->Child(0)->SetTypeAnn(ctx.MakeType<TWorldExprType>());
                newRead->Child(0)->SetState(TExprNode::EState::ExecutionComplete);
                newRead->CopyConstraints(read.Cast().Ref());

                auto newRight = Build<TCoRight>(ctx, data.Pos())
                    .Input(newRead)
                    .Done();
                newRight.Ptr()->SetTypeAnn(data.Ref().GetTypeAnn());
                newRight.Ptr()->CopyConstraints(data.Ref());

                data = newRight;
            }
        }

        TString usedCluster;
        TSyncMap syncList;
        if (!IsYtIsolatedLambda(data.Ref(), syncList, usedCluster, false)) {
            ctx.AddError(TIssue(ctx.GetPosition(data.Pos()), TStringBuilder() << "Failed to execute node due to bad graph: " << input->Content()));
            return SyncError();
        }
        if (usedCluster.empty()) {
            usedCluster = State_->Configuration->DefaultCluster.Get().GetOrElse(State_->Gateway->GetDefaultClusterName());
        }

        TExprNode::TPtr optimizedInput = data.Ptr();
        if (const auto status = SubstTables(optimizedInput, State_, false, ctx); status.Level == TStatus::Error) {
            return SyncStatus(status);
        }

        const auto settings = State_->Configuration->GetSettingsForNode(*input);
        TUserDataTable crutches = State_->Types->UserDataStorageCrutches;
        if (const auto& defaultGeobase = settings->GeobaseDownloadUrl.Get(usedCluster)) {
            auto& userDataBlock = (crutches[TUserDataKey::File(TStringBuf("/home/geodata6.bin"))] = TUserDataBlock{EUserDataType::URL, {}, *defaultGeobase, {}, {}});
            userDataBlock.Usage.Set(EUserDataBlockUsage::Path);
        }

        bool hasNonDeterministicFunctions = false;
        if (const auto status = PeepHoleOptimizeBeforeExec(optimizedInput, optimizedInput, State_, hasNonDeterministicFunctions, ctx); status.Level != IGraphTransformer::TStatus::Ok) {
            return SyncStatus(status);
        }

        TUserDataTable files;
        auto filesRes = NCommon::FreezeUsedFiles(*optimizedInput, files, *State_->Types, ctx, MakeUserFilesDownloadFilter(*State_->Gateway, usedCluster), crutches);
        if (filesRes.first.Level != TStatus::Ok) {
            return filesRes;
        }

        THashMap<TString, TString> secureParams;
        NCommon::FillSecureParams(optimizedInput, *State_->Types, secureParams);

        auto optimizeChildren = input->ChildrenList();
        optimizeChildren[0] = optimizedInput;
        resOrPull = TResOrPullBase(ctx.ExactChangeChildren(resOrPull.Ref(), std::move(optimizeChildren)));

        TString operationHash;
        if (resOrPull.Maybe<TResult>()) {
            const auto queryCacheMode = settings->QueryCacheMode.Get().GetOrElse(EQueryCacheMode::Disable);
            if (queryCacheMode != EQueryCacheMode::Disable) {
                if (!hasNonDeterministicFunctions && settings->QueryCacheUseForCalc.Get().GetOrElse(true)) {
                    operationHash = TYtNodeHashCalculator(State_, usedCluster, settings).GetHash(*optimizedInput);
                    if (!operationHash.empty()) {
                        // Update hash with columns hint. See YQL-10405
                        TVector<TString> columns = NCommon::GetResOrPullColumnHints(resOrPull.Ref());
                        THashBuilder builder;
                        builder << TYtNodeHashCalculator::MakeSalt(settings, usedCluster) << operationHash << columns.size();
                        for (auto& col: columns) {
                            builder << col;
                        }
                        operationHash = builder.Finish();
                    }
                }
                YQL_CLOG(DEBUG, ProviderYt) << "Operation hash: " << HexEncode(operationHash).Quote()
                    << ", cache mode: " << queryCacheMode;
            }
        }

        auto publicId = resOrPull.PublicId().Value()
            ? MakeMaybe(FromString<ui32>(resOrPull.PublicId().Value()))
            : Nothing();

        auto future = State_->Gateway->ResOrPull(resOrPull.Ptr(), ctx,
            IYtGateway::TResOrPullOptions(State_->SessionId)
                .FillSettings(fillSettings)
                .UserDataBlocks(files)
                .UdfModules(State_->Types->UdfModules)
                .UdfResolver(State_->Types->UdfResolver)
                .UdfValidateMode(State_->Types->ValidateMode)
                .PublicId(publicId)
                .Config(State_->Configuration->GetSettingsForNode(resOrPull.Origin().Ref()))
                .UsedCluster(usedCluster)
                .OptLLVM(State_->Types->OptLLVM.GetOrElse(TString()))
                .OperationHash(operationHash)
                .SecureParams(secureParams)
            );

        return WrapFuture(future, [](const IYtGateway::TResOrPullResult& res, const TExprNode::TPtr& input, TExprContext& ctx) {
            auto ret = ctx.NewAtom(input->Pos(), res.Data);
            return ret;
        });
    }

    TStatusCallbackPair HandleConfigure(const TExprNode::TPtr& input, TExprContext& ctx) {
        YQL_CLOG(DEBUG, ProviderYt) << "Executing " << input->Content() << " (UniqueId=" << input->UniqueId() << ")";
        auto configure = TYtConfigure(input);
        auto clusterName = TString{configure.DataSource().Cluster().Value()};
        State_->Configuration->FreezeZeroVersion();
        if (configure.Arg(2).Cast<TCoAtom>().Value() == TStringBuf("Attr")) {
            auto name = TString{configure.Arg(3).Cast<TCoAtom>().Value()};
            TMaybe<TString> value;
            if (configure.Args().Count() == 5) {
                value = TString{configure.Arg(4).Cast<TCoAtom>().Value()};
            }
            if (State_->Configuration->IsRuntime(name)) {
                if (!State_->Configuration->Dispatch(clusterName, name, value, NCommon::TSettingDispatcher::EStage::RUNTIME, NCommon::TSettingDispatcher::GetErrorCallback(input->Pos(), ctx))) {
                    return SyncError();
                }
                State_->Configuration->PromoteVersion(*input);
                YQL_CLOG(DEBUG, ProviderYt) << "Setting pragma "
                    << (NCommon::ALL_CLUSTERS == clusterName ? YtProviderName : clusterName) << '.'
                    << name << '=' << (value ? value->Quote() : "(default)")
                    << " in ver." << State_->Configuration->GetLastVersion();
            }
        }

        input->SetState(TExprNode::EState::ExecutionComplete);
        input->SetResult(ctx.NewWorld(input->Pos()));
        return SyncOk();
    }

    void Rewind() final {
        State_->Configuration->ClearVersions();
        TExecTransformerBase::Rewind();
    }

private:
    const TYtState::TPtr State_;
};

}

THolder<TExecTransformerBase> CreateYtDataSourceExecTransformer(TYtState::TPtr state) {
    return THolder(new TYtDataSourceExecTransformer(state));
}

}
