#pragma once

#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h> 
#include <ydb/library/yql/providers/dq/interface/yql_dq_task_preprocessor.h> 

namespace NYql {

TIntrusivePtr<IDqGateway> CreateLocalDqGateway(NYdb::TDriver driver, const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    NKikimr::NMiniKQL::TComputationNodeFactory compFactory,
    TTaskTransformFactory taskTransformFactory, const TDqTaskPreprocessorFactoryCollection& dqTaskPreprocessorFactories, IHTTPGateway::TPtr gateway = {});

} // namespace NYql
