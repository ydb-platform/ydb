#include "yql_yt_lambda_builder.h"

#include "yql_yt_native.h"
#include "yql_yt_session.h"

#include <yql/essentials/core/yql_opt_utils.h>
#include <yt/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <yt/yql/providers/yt/comp_nodes/yql_mkql_output.h>
#include <yt/yql/providers/yt/comp_nodes/yql_mkql_block_table_content.h>
#include <yt/yql/providers/yt/comp_nodes/yql_mkql_table_content.h>

#include <yql/essentials/providers/common/mkql/yql_provider_mkql.h>
#include <yql/essentials/providers/common/mkql/yql_type_mkql.h>
#include <yql/essentials/providers/common/comp_nodes/yql_factory.h>
#include <yql/essentials/parser/pg_wrapper/interface/comp_factory.h>

#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>

#include <yql/essentials/utils/yql_panic.h>

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

        if (callable.GetType()->GetName() == "YtBlockTableContentJob") {
            YQL_ENSURE(codecCtx);
            return WrapYtBlockTableContent(*codecCtx, ctx.Mutables, callable, filePrefix);
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
    const NKikimr::NUdf::ISecureParamsProvider* secureParamsProvider, TLangVersion langver)
    : TGatewayLambdaBuilder(functionRegistry, alloc, nullptr,
        session.RandomProvider_, session.TimeProvider_, nullptr, nullptr, secureParamsProvider, nullptr, langver)
{
}

TNativeYtLambdaBuilder::TNativeYtLambdaBuilder(TScopedAlloc& alloc, const TYtNativeServices& services, const TSession& session, TLangVersion langver)
    : TNativeYtLambdaBuilder(alloc, services.FunctionRegistry, session, nullptr, langver)
{
}

TString TNativeYtLambdaBuilder::BuildLambdaWithIO(const IMkqlCallableCompiler& compiler, TCoLambda lambda, TExprContext& exprCtx) {
    return TGatewayLambdaBuilder::BuildLambdaWithIO("Yt", compiler, lambda, exprCtx);
}

} // NNative

} // NYql
