#pragma once

#include <ydb/library/yql/providers/yt/lib/lambda_builder/lambda_builder.h>
#include <ydb/library/yql/providers/yt/codec/yt_codec_io.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/providers/yt/gateway/lib/user_files.h>

namespace NKikimr::NMiniKQL {

class TScopedAlloc;
class IFunctionRegistry;

}

namespace NYql {

namespace NUdf {
class ISecureParamsProvider;
}

namespace NCommon {
class IMkqlCallableCompiler;
}

struct TYtNativeServices;

namespace NNative {

struct TSession;

struct TNativeYtLambdaBuilder: public TLambdaBuilder {
    TNativeYtLambdaBuilder(NKikimr::NMiniKQL::TScopedAlloc& alloc, const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        const TSession& session, const NKikimr::NUdf::ISecureParamsProvider* secureParamsProvider = nullptr);

    TNativeYtLambdaBuilder(NKikimr::NMiniKQL::TScopedAlloc& alloc, const TYtNativeServices& services, const TSession& session);

    TString BuildLambdaWithIO(const NCommon::IMkqlCallableCompiler& compiler, NNodes::TCoLambda lambda, TExprContext& exprCtx);
};

NKikimr::NMiniKQL::TComputationNodeFactory GetGatewayNodeFactory(NYql::NCommon::TCodecContext* codecCtx,
     TMkqlWriterImpl* writer, TUserFiles::TPtr files, TStringBuf filePrefix);

} // NNative

} // NYql
