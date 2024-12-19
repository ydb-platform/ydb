#include "yql_pure_provider.h"

#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/peephole_opt/yql_opt_peephole_physical.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/codec/yql_codec.h>
#include <yql/essentials/providers/common/schema/expr/yql_expr_schema.h>
#include <yql/essentials/providers/common/transform/yql_exec.h>
#include <yql/essentials/providers/common/transform/yql_lazy_init.h>
#include <yql/essentials/providers/common/mkql/yql_provider_mkql.h>
#include <yql/essentials/providers/common/mkql_simple_file/mkql_simple_file.h>
#include <yql/essentials/providers/result/expr_nodes/yql_res_expr_nodes.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_opt_literal.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/parser/pg_wrapper/interface/comp_factory.h>
#include <yql/essentials/providers/common/comp_nodes/yql_factory.h>

#include <util/stream/length.h>

namespace NYql {

namespace {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

class TPureDataSinkExecTransformer : public TExecTransformerBase {
public:
    TPureDataSinkExecTransformer(const TPureState::TPtr state)
        : State_(state)
    {
        AddHandler({TStringBuf("Result")}, RequireNone(), Hndl(&TPureDataSinkExecTransformer::HandleRes));
    }

    void Rewind() override {
        TExecTransformerBase::Rewind();
    }

    TStatusCallbackPair HandleRes(const TExprNode::TPtr& input, TExprContext& ctx) {
        YQL_CLOG(DEBUG, ProviderPure) << "Executing " << input->Content() << " (UniqueId=" << input->UniqueId() << ")";
        if (TStringBuf("Result") != input->Content()) {
            ythrow yexception() << "Don't know how to execute " << input->Content();
        }

        NNodes::TResOrPullBase resOrPull(input);

        IDataProvider::TFillSettings fillSettings = NCommon::GetFillSettings(resOrPull.Ref());
        YQL_ENSURE(fillSettings.Format == IDataProvider::EResultFormat::Yson);

        auto lambda = resOrPull.Input();

        if (!IsPureIsolatedLambda(lambda.Ref())) {
            ctx.AddError(TIssue(ctx.GetPosition(lambda.Pos()), TStringBuilder() << "Failed to execute node due to bad graph: " << input->Content()));
            return SyncError();
        }

        auto optimized = lambda.Ptr();
        bool hasNonDeterministicFunctions;
        auto status = PeepHoleOptimizeNode(optimized, optimized, ctx, *State_->Types, nullptr, hasNonDeterministicFunctions);
        if (status.Level == IGraphTransformer::TStatus::Error) {
            return SyncStatus(status);
        }

        TUserDataTable crutches = State_->Types->UserDataStorageCrutches;
        TUserDataTable files;
        auto filesRes = NCommon::FreezeUsedFiles(*optimized, files, *State_->Types, ctx, [](const TString&) { return true; }, crutches);
        if (filesRes.first.Level != TStatus::Ok) {
            return filesRes;
        }

        TVector<TString> columns(NCommon::GetResOrPullColumnHints(*input));
        if (columns.empty()) {
            columns = NCommon::GetStructFields(lambda.Ref().GetTypeAnn());
        }

        TStringStream out;
        NYson::TYsonWriter writer(&out, NCommon::GetYsonFormat(fillSettings), ::NYson::EYsonType::Node, false);
        writer.OnBeginMap();
        if (NCommon::HasResOrPullOption(*input, "type")) {
            writer.OnKeyedItem("Type");
            NCommon::WriteResOrPullType(writer, lambda.Ref().GetTypeAnn(), TColumnOrder(columns));
        }

        TScopedAlloc alloc(__LOCATION__, TAlignedPagePoolCounters(), State_->FunctionRegistry->SupportsSizedAllocators());
        TTypeEnvironment env(alloc);
        TProgramBuilder pgmBuilder(env, *State_->FunctionRegistry);
        NCommon::TMkqlCommonCallableCompiler compiler;

        NCommon::TMkqlBuildContext mkqlCtx(compiler, pgmBuilder, ctx);
        auto root = NCommon::MkqlBuildExpr(*optimized, mkqlCtx);

        root = TransformProgram(root, files, env);

        TExploringNodeVisitor explorer;
        explorer.Walk(root.GetNode(), env);
        auto compFactory = GetCompositeWithBuiltinFactory({
            GetYqlFactory(),
            GetPgFactory()
        });

        TComputationPatternOpts patternOpts(alloc.Ref(), env, compFactory, State_->FunctionRegistry,
            State_->Types->ValidateMode, NUdf::EValidatePolicy::Exception, State_->Types->OptLLVM.GetOrElse(TString()),
            EGraphPerProcess::Multi);

        auto pattern = MakeComputationPattern(explorer, root, {}, patternOpts);
        const TComputationOptsFull computeOpts(nullptr, alloc.Ref(), env,
            *State_->Types->RandomProvider, *State_->Types->TimeProvider,
            NUdf::EValidatePolicy::Exception, nullptr, nullptr);
        auto graph = pattern->Clone(computeOpts);
        const TBindTerminator bind(graph->GetTerminator());
        graph->Prepare();
        auto value = graph->GetValue();
        bool truncated = false;
        auto type = root.GetStaticType();
        TString data;
        TStringOutput dataOut(data);
        TCountingOutput dataCountingOut(&dataOut);
        NYson::TYsonWriter dataWriter(&dataCountingOut, NCommon::GetYsonFormat(fillSettings), ::NYson::EYsonType::Node, false);
        if (type->IsList()) {
            auto inputType = AS_TYPE(TListType, type)->GetItemType();
            TMaybe<ui64> rowsLimit = fillSettings.RowsLimitPerWrite;
            TMaybe<ui64> bytesLimit = fillSettings.AllResultsBytesLimit;
            TMaybe<TVector<ui32>> structPositions = NCommon::CreateStructPositions(inputType, &columns);
            dataWriter.OnBeginList();
            const auto it = value.GetListIterator();
            ui64 rows = 0;
            for (NUdf::TUnboxedValue item; it.Next(item); ++rows) {
                if ((rowsLimit && rows >= *rowsLimit) || (bytesLimit && dataCountingOut.Counter() >= *bytesLimit)) {
                    truncated = true;
                    break;
                }

                dataWriter.OnListItem();
                NCommon::WriteYsonValue(dataWriter, item, inputType, structPositions.Get());
            }
            dataWriter.OnEndList();
        } else {
            NCommon::WriteYsonValue(dataWriter, value, type, nullptr);
        }

        writer.OnKeyedItem("Data");
        writer.OnRaw(fillSettings.Discard ? "#" : data);

        if (truncated) {
            writer.OnKeyedItem("Truncated");
            writer.OnBooleanScalar(true);
        }

        writer.OnEndMap();
        input->SetState(TExprNode::EState::ExecutionComplete);
        input->SetResult(ctx.NewAtom(input->Pos(), out.Str()));
        return SyncOk();
    }

private:
    TRuntimeNode TransformProgram(TRuntimeNode root, const TUserDataTable& files, TTypeEnvironment& env) {
        TExploringNodeVisitor explorer;
        explorer.Walk(root.GetNode(), env);
        bool wereChanges = false;
        TRuntimeNode program = SinglePassVisitCallables(root, explorer,
            TSimpleFileTransformProvider(State_->FunctionRegistry, files), env, true, wereChanges);
        program = LiteralPropagationOptimization(program, env, true);
        return program;
    }

private:
    const TPureState::TPtr State_;
};

THolder<TExecTransformerBase> CreatePureDataSourceExecTransformer(const TPureState::TPtr& state) {
    return THolder(new TPureDataSinkExecTransformer(state));
}

class TPureProvider : public TDataProviderBase {
public:
    TPureProvider(const TPureState::TPtr& state)
        : State_(state)
        , ExecTransformer_([this]() { return CreatePureDataSourceExecTransformer(State_); })
    {}

    TStringBuf GetName() const final {
        return PureProviderName;
    }

    IGraphTransformer& GetCallableExecutionTransformer() override {
        return *ExecTransformer_;
    }

private:
    const TPureState::TPtr State_;
    TLazyInitHolder<TExecTransformerBase> ExecTransformer_;
};

}

TIntrusivePtr<IDataProvider> CreatePureProvider(const TPureState::TPtr& state) {
    return MakeIntrusive<TPureProvider>(state);
}

TDataProviderInitializer GetPureDataProviderInitializer() {
    return [] (
        const TString& userName,
        const TString& sessionId,
        const TGatewaysConfig* gatewaysConfig,
        const IFunctionRegistry* functionRegistry,
        TIntrusivePtr<IRandomProvider> randomProvider,
        TIntrusivePtr<TTypeAnnotationContext> typeCtx,
        const TOperationProgressWriter& progressWriter,
        const TYqlOperationOptions& operationOptions,
        THiddenQueryAborter hiddenAborter,
        const TQContext& qContext
    ) {
        Y_UNUSED(userName);
        Y_UNUSED(sessionId);
        Y_UNUSED(gatewaysConfig);
        Y_UNUSED(randomProvider);
        Y_UNUSED(typeCtx);
        Y_UNUSED(progressWriter);
        Y_UNUSED(operationOptions);
        Y_UNUSED(hiddenAborter);
        Y_UNUSED(qContext);

        TDataProviderInfo info;
        info.Names.insert(TString{PureProviderName});

        auto state = MakeIntrusive<TPureState>();
        state->Types = typeCtx.Get();
        state->FunctionRegistry = functionRegistry;

        info.Source = CreatePureProvider(state);
        info.OpenSession = [state](
            const TString& sessionId,
            const TString& username,
            const TOperationProgressWriter& progressWriter,
            const TYqlOperationOptions& operationOptions,
            TIntrusivePtr<IRandomProvider> randomProvider,
            TIntrusivePtr<ITimeProvider> timeProvider) {
            Y_UNUSED(sessionId);
            Y_UNUSED(username);
            Y_UNUSED(progressWriter);
            Y_UNUSED(operationOptions);
            Y_UNUSED(randomProvider);
            Y_UNUSED(timeProvider);
            return NThreading::MakeFuture();
        };

        info.CloseSessionAsync = [](const TString& sessionId) {
            Y_UNUSED(sessionId);
            return NThreading::MakeFuture();
        };

        return info;
    };
}

}
