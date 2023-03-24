#pragma once

#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>
#include <ydb/core/kqp/runtime/kqp_program_builder.h>

#include <ydb/library/yql/providers/common/mkql/yql_provider_mkql.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>

#include <util/generic/ptr.h>

namespace NKikimr {
namespace NKqp {

class TKqlCompileContext {
public:
    TKqlCompileContext(const TString& cluster,
        const TIntrusivePtr<NYql::TKikimrTablesData>& tablesData,
        const NMiniKQL::TTypeEnvironment& typeEnv,
        const NMiniKQL::IFunctionRegistry& funcRegistry)
        : Cluster_(cluster)
        , TablesData_(tablesData)
        , PgmBuilder_(MakeHolder<NMiniKQL::TKqpProgramBuilder>(typeEnv, funcRegistry)) {}

    NMiniKQL::TKqpProgramBuilder& PgmBuilder() const { return *PgmBuilder_; }
    const NYql::TKikimrTableMetadata& GetTableMeta(const NYql::NNodes::TKqpTable& table) const;

private:
    TString Cluster_;
    TIntrusivePtr<NYql::TKikimrTablesData> TablesData_;
    THolder<NMiniKQL::TKqpProgramBuilder> PgmBuilder_;
};

TIntrusivePtr<NYql::NCommon::IMkqlCallableCompiler> CreateKqlCompiler(const TKqlCompileContext& ctx, NYql::TTypeAnnotationContext& typesCtx);

} // namespace NKqp
} // namespace NKikimr
