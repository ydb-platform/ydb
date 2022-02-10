#pragma once

#include "compile_result.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/core/client/minikql_compile/db_key_resolver.h>
#include <ydb/core/engine/kikimr_program_builder.h>

#include <util/generic/hash.h>
#include <util/generic/list.h>
#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>


namespace NYql {

using NKikimr::NMiniKQL::IFunctionRegistry;
using NKikimr::NMiniKQL::TParametersBuilder;
using NKikimr::NMiniKQL::TTypeEnvironment;
using NKikimr::NMiniKQL::TType;
using NKikimr::NMiniKQL::TRuntimeNode;
using NKikimr::NMiniKQL::TKikimrProgramBuilder;


struct TContext : public TAtomicRefCount<TContext> {
    using TPtr = TIntrusivePtr<TContext>;
    struct TTableState {
        IDbSchemeResolver::TTable Request;
        TMaybe<IDbSchemeResolver::TTableResult> Response;
    };

    using TTableMap = THashMap<TString, TTableState>;

    TContext(const IFunctionRegistry* funcRegistry,
             const TTypeEnvironment* typeEnv);

    bool NewParamsBuilder();
    TRuntimeNode NewParam(TStringBuf name, TType* type);

    void AddTableLookup(const IDbSchemeResolver::TTable& request);
    template<typename TStringType>
    IDbSchemeResolver::TTableResult* GetTableLookup(const TExprNode& node, const TStringType& tableName);
    TTableMap& GetTablesToResolve();

    TConvertResult Finish(TRuntimeNode convertedNode);

public:
    const IFunctionRegistry* FuncRegistry;
    const TTypeEnvironment* TypeEnv;
    NKikimr::NUdf::ITypeInfoHelper::TPtr TypeInfoHelper;
    TAutoPtr<TKikimrProgramBuilder> PgmBuilder;
    TMaybe<TParametersBuilder> ParamsBuilder;

private:
    bool WasParams;
    TTableMap Tables;
};

} // namespace NYql
