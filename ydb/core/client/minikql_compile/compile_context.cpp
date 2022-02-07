#include "compile_context.h"

#include <ydb/core/base/domain.h>
#include <ydb/core/engine/kikimr_program_builder.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>

namespace NYql {

TContext::TContext(const IFunctionRegistry* funcRegistry,
                   const TTypeEnvironment* typeEnv)
    : FuncRegistry(funcRegistry)
    , TypeEnv(typeEnv)
    , TypeInfoHelper(new NKikimr::NMiniKQL::TTypeInfoHelper)
    , PgmBuilder(new TKikimrProgramBuilder(*typeEnv, *funcRegistry))
    , WasParams(false)
{
}

bool TContext::NewParamsBuilder() {
    if (!ParamsBuilder) {
        ParamsBuilder = PgmBuilder->GetParametersBuilder();
        return true;
    }
    return false;
}

TRuntimeNode TContext::NewParam(TStringBuf name, TType* type) {
    WasParams = true;
    return PgmBuilder->Parameter(TString(name), type);
}

void TContext::AddTableLookup(const IDbSchemeResolver::TTable& request) {
    auto state = Tables.FindPtr(request.TableName);
    if (state) {
        state->Request.ColumnNames.insert(request.ColumnNames.begin(), request.ColumnNames.end());
    } else {
        Tables.insert({request.TableName, TTableState{request, TMaybe<IDbSchemeResolver::TTableResult>()}});
    }
}

template<typename TStringType>
IDbSchemeResolver::TTableResult* TContext::GetTableLookup(const TExprNode& node, const TStringType& tableName) {
    auto entry = Tables.FindPtr(tableName);
    if (!entry) {
        ythrow TNodeException(node) << "Table is not found: " << tableName;
    }

    if (!entry->Response.Defined()) {
        ythrow TNodeException(node) << "Table is not resolved: " << tableName;
    }

    return entry->Response.Get();
}

template
typename IDbSchemeResolver::TTableResult* TContext::GetTableLookup(const TExprNode& node, const TString& tableName);
template
typename IDbSchemeResolver::TTableResult* TContext::GetTableLookup(const TExprNode& node, const TStringBuf& tableName);

TContext::TTableMap& TContext::GetTablesToResolve() {
    return Tables;
}

TConvertResult TContext::Finish(TRuntimeNode convertedNode) {
    TConvertResult convRes;

    if (!ParamsBuilder) {
        // Program.
        convRes.Node = PgmBuilder->Prepare(convertedNode);
    } else if (!WasParams && ParamsBuilder) {
        // Params without params.
        convRes.Node = convertedNode;
    } else {
        // Params with params.
        convRes.Errors.AddIssue(TPosition(1, 1), "Params program can't contains params.");
    }
    return convRes;
}

} // namespace NYql
