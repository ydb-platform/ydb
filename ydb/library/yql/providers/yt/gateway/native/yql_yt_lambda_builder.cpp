#include "yql_yt_lambda_builder.h"

#include "yql_yt_native.h"
#include "yql_yt_session.h"

#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/comp_nodes/yql_mkql_output.h>
#include <ydb/library/yql/providers/yt/comp_nodes/yql_mkql_table_content.h>

#include <ydb/library/yql/providers/common/mkql/yql_provider_mkql.h>
#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>
#include <ydb/library/yql/providers/common/comp_nodes/yql_factory.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/comp_factory.h>

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders_codegen.h>

#include <ydb/library/yql/utils/yql_panic.h>

#include <util/generic/maybe.h>
#include <util/generic/ylimits.h>

namespace NYql {

namespace NNative {

using namespace NNodes;
using namespace NCommon;
using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

NKikimr::NMiniKQL::TComputationNodeFactory GetGatewayNodeFactory(TCodecContext* codecCtx, TMkqlWriterImpl* writer, TUserFiles::TPtr files, TStringBuf filePrefix) {
    TMaybe<ui32> exprContextObject;
    return [exprContextObject, codecCtx, writer, files, filePrefix](NMiniKQL::TCallable& callable, const TComputationNodeFactoryContext& ctx) mutable -> IComputationNode* {
        if (callable.GetType()->GetName() == TYtTablePath::CallableName()
            || callable.GetType()->GetName() == TYtTableIndex::CallableName()
            || callable.GetType()->GetName() == TYtTableRecord::CallableName()
            || callable.GetType()->GetName() == TYtIsKeySwitch::CallableName()
            || callable.GetType()->GetName() == TYtRowNumber::CallableName()
        ) {
            return ctx.NodeFactory.CreateImmutableNode(NUdf::TUnboxedValuePod::Zero());
        }

        if (files) {
            if (callable.GetType()->GetName() == "FilePathJob" || callable.GetType()->GetName() == "FileContentJob") {
                const TString fullFileName(AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().AsStringRef());
                const auto fileInfo = files->GetFile(fullFileName);
                YQL_ENSURE(fileInfo, "Unknown file path " << fullFileName);
                const auto path = fileInfo->Path->GetPath();
                const auto content = callable.GetType()->GetName() == "FileContentJob" ? TFileInput(path).ReadAll() : path.GetPath();
                return ctx.NodeFactory.CreateImmutableNode(MakeString(content));
            }
        }

        if (callable.GetType()->GetName() == "YtOutput") {
            YQL_ENSURE(writer);
            return WrapYtOutput(callable, ctx, *writer);
        }

        if (callable.GetType()->GetName() == "YtTableContentJob") {
            YQL_ENSURE(codecCtx);
            return WrapYtTableContent(*codecCtx, ctx.Mutables, callable, "OFF" /* no LLVM for local exec */, filePrefix);
        }

        if (!exprContextObject) {
           exprContextObject = ctx.Mutables.CurValueIndex++;
        }

        auto yql = GetYqlFactory(*exprContextObject)(callable, ctx);
        if (yql) {
            return yql;
        }

        auto pg = GetPgFactory()(callable, ctx);
        if (pg) {
            return pg;
        }

        return GetBuiltinFactory()(callable, ctx);
    };
}


TNativeYtLambdaBuilder::TNativeYtLambdaBuilder(TScopedAlloc& alloc, const IFunctionRegistry* functionRegistry, const TSession& session,
    const NKikimr::NUdf::ISecureParamsProvider* secureParamsProvider)
    : TLambdaBuilder(functionRegistry, alloc, nullptr,
        session.RandomProvider_, session.TimeProvider_, nullptr, nullptr, secureParamsProvider)
{
}

TNativeYtLambdaBuilder::TNativeYtLambdaBuilder(TScopedAlloc& alloc, const TYtNativeServices& services, const TSession& session)
    : TNativeYtLambdaBuilder(alloc, services.FunctionRegistry, session)
{
}

TString TNativeYtLambdaBuilder::BuildLambdaWithIO(const IMkqlCallableCompiler& compiler, TCoLambda lambda, TExprContext& exprCtx) {
    TProgramBuilder pgmBuilder(GetTypeEnvironment(), *FunctionRegistry);
    TArgumentsMap arguments(1U);
    if (lambda.Args().Size() > 0) {
        const auto arg = lambda.Args().Arg(0);
        const auto argType = arg.Ref().GetTypeAnn();
        auto inputItemType = NCommon::BuildType(arg.Ref(), *GetSeqItemType(argType), pgmBuilder);
        switch (bool isStream = true; argType->GetKind()) {
        case ETypeAnnotationKind::Flow:
            if (ETypeAnnotationKind::Multi == argType->Cast<TFlowExprType>()->GetItemType()->GetKind()) {
                arguments.emplace(arg.Raw(), TRuntimeNode(TCallableBuilder(GetTypeEnvironment(), "YtInput", pgmBuilder.NewFlowType(inputItemType)).Build(), false));
                break;
            }
            isStream = false;
            [[fallthrough]]; // AUTOGENERATED_FALLTHROUGH_FIXME
        case ETypeAnnotationKind::Stream: {
            auto inputStream = pgmBuilder.SourceOf(isStream ?
                pgmBuilder.NewStreamType(pgmBuilder.GetTypeEnvironment().GetTypeOfVoidLazy()) : pgmBuilder.NewFlowType(pgmBuilder.GetTypeEnvironment().GetTypeOfVoidLazy()));

            inputItemType = pgmBuilder.NewOptionalType(inputItemType);
            inputStream = pgmBuilder.Map(inputStream, [&] (TRuntimeNode item) {
                TCallableBuilder inputCall(GetTypeEnvironment(), "YtInput", inputItemType);
                inputCall.Add(item);
                return TRuntimeNode(inputCall.Build(), false);
            });
            inputStream = pgmBuilder.TakeWhile(inputStream, [&] (TRuntimeNode item) {
                return pgmBuilder.Exists(item);
            });

            inputStream = pgmBuilder.FlatMap(inputStream, [&] (TRuntimeNode item) {
                return item;
            });

            arguments[arg.Raw()] = inputStream;
            break;
        }
        default:
            YQL_ENSURE(false, "Unsupported lambda argument type: " << arg.Ref().GetTypeAnn()->GetKind());
        }
    }
    TMkqlBuildContext ctx(compiler, pgmBuilder, exprCtx, lambda.Ref().UniqueId(), std::move(arguments));
    TRuntimeNode outStream = MkqlBuildExpr(lambda.Body().Ref(), ctx);
    if (outStream.GetStaticType()->IsFlow()) {
        TCallableBuilder outputCall(GetTypeEnvironment(), "YtOutput", pgmBuilder.NewFlowType(GetTypeEnvironment().GetTypeOfVoidLazy()));
        outputCall.Add(outStream);
        outStream = TRuntimeNode(outputCall.Build(), false);
    } else {
        outStream = pgmBuilder.Map(outStream, [&] (TRuntimeNode item) {
            TCallableBuilder outputCall(GetTypeEnvironment(), "YtOutput", GetTypeEnvironment().GetTypeOfVoidLazy());
            outputCall.Add(item);
            return TRuntimeNode(outputCall.Build(), false);
        });
    }
    outStream = pgmBuilder.Discard(outStream);

    return SerializeRuntimeNode(outStream, GetTypeEnvironment());
}

} // NNative

} // NYql
