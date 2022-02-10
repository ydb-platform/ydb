#include "yql_clickhouse_mkql_compiler.h"
#include "yql_clickhouse_util.h"

#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h> 
#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h> 
#include <ydb/library/yql/minikql/mkql_node_cast.h> 
#include <ydb/library/yql/minikql/mkql_node.h> 
#include <ydb/library/yql/core/yql_opt_utils.h> 

#include <library/cpp/json/json_writer.h>

#include <algorithm>

namespace NYql {

using namespace NKikimr::NMiniKQL;
using namespace NNodes;

namespace {

TRuntimeNode BuildNativeParseCall(TRuntimeNode input, TType* inputItemType, TType* outputItemType, const std::string_view& timezone, NCommon::TMkqlBuildContext& ctx)
{
    const auto userType = ctx.ProgramBuilder.NewTupleType({ctx.ProgramBuilder.NewTupleType({inputItemType}), ctx.ProgramBuilder.NewStructType({}), outputItemType});
    const auto flow = ctx.ProgramBuilder.ToFlow(ctx.ProgramBuilder.Apply(ctx.ProgramBuilder.Udf("ClickHouseClient.ParseFormat", {}, userType, "Native"), {input, ctx.ProgramBuilder.NewOptional(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::Utf8>(timezone)) }));
    const auto structType = static_cast<const TStructType*>(outputItemType);
    return ctx.ProgramBuilder.ExpandMap(flow,
        [&](TRuntimeNode item) {
            TRuntimeNode::TList fields;
            fields.reserve(structType->GetMembersCount());
            auto j = 0U;
            std::generate_n(std::back_inserter(fields), structType->GetMembersCount(), [&](){ return ctx.ProgramBuilder.Member(item, structType->GetMemberName(j++)); });
            return fields;
        });
}

TRuntimeNode BuildClickHouseInputCall(
    TType* outputType,
    TType* itemType,
    const TString& cluster,
    TStringBuf table,
    TStringBuf timezone,
    const TClickHouseState::TPtr& state,
    NCommon::TMkqlBuildContext& ctx)
{
    TStringBuf db, dbTable;
    if (!table.TrySplit(".", db, dbTable)) {
        db = "default";
        dbTable = table;
    }

    auto structType = AS_TYPE(TStructType, itemType);
    auto endpoint = state->Configuration->Endpoints.FindPtr(cluster); 
    Y_ENSURE(endpoint); 

    const auto host = endpoint->first;
    const auto colonPos = host.rfind(':');
    YQL_ENSURE(colonPos != TString::npos, "Missing port: " << host);

    const auto hostWithoutPort = host.substr(0, colonPos);
    const auto port = FromString<ui16>(host.substr(colonPos + 1));
    const bool secure = endpoint->second;

    TString typeConfig;
    TStringOutput stream(typeConfig);
    NJson::TJsonWriter writer(&stream, NJson::TJsonWriterConfig());

    writer.OpenMap();

    writer.Write("db", db);
    writer.Write("table", dbTable);
    writer.Write("secure", secure);
    writer.Write("host", hostWithoutPort);
    writer.Write("port", port);
    writer.Write("token", TString("cluster:default_") + cluster);

    writer.OpenArray("columns");
    for (ui32 i = 0; i < structType->GetMembersCount(); ++i) { 
        writer.Write(structType->GetMemberName(i));
    } 
    writer.CloseArray();

    writer.CloseMap();
    writer.Flush();

    const auto voidType = ctx.ProgramBuilder.NewVoid().GetStaticType();
    TCallableTypeBuilder callbackTypeBuilder(ctx.ProgramBuilder.GetTypeEnvironment(), "", voidType); 
    const auto callbackType = callbackTypeBuilder.Build();
 
    const auto strType = ctx.ProgramBuilder.NewDataType(NUdf::TDataType<char*>::Id);
    const auto argsType = ctx.ProgramBuilder.NewTupleType({ strType });
    const auto userType = ctx.ProgramBuilder.NewTupleType({ argsType });
    const auto retType = ctx.ProgramBuilder.NewStreamType(itemType);

    TCallableTypeBuilder funcTypeBuilder(ctx.ProgramBuilder.GetTypeEnvironment(), "UDF", retType);
    funcTypeBuilder.Add(strType);
 
    const auto funcType = funcTypeBuilder.Build();

    TCallableBuilder callbackBuilder(ctx.ProgramBuilder.GetTypeEnvironment(), "DqNotify", callbackType); 
    auto callback = TRuntimeNode(callbackBuilder.Build(), false); 
 
    auto flow = ctx.ProgramBuilder.ToFlow(
        ctx.ProgramBuilder.Apply(
            ctx.ProgramBuilder.TypedUdf("ClickHouse.remoteSource", funcType,
                ctx.ProgramBuilder.NewVoid(), userType, typeConfig),
                TArrayRef<const TRuntimeNode>({ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(timezone)})
    ));

    if (!AS_TYPE(TFlowType, outputType)->GetItemType()->IsSameType(*itemType)) {
        flow = ctx.ProgramBuilder.ExpandMap(flow,
            [&](TRuntimeNode item) {
                TRuntimeNode::TList fields;
                fields.reserve(structType->GetMembersCount());
                auto i = 0U;
                std::generate_n(std::back_inserter(fields), structType->GetMembersCount(), [&](){ return ctx.ProgramBuilder.Member(item, structType->GetMemberName(i++)); });
                return fields;
            });
    }

    return flow;
}

}

void RegisterDqClickHouseMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler, const TClickHouseState::TPtr& state) {
    compiler.ChainCallable(TDqSourceWideWrap::CallableName(),
        [state](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
            if (const auto wrapper = TDqSourceWideWrap(&node); wrapper.DataSource().Category().Value() == ClickHouseProviderName) {
                const auto input = MkqlBuildExpr(wrapper.Input().Ref(), ctx);
                const auto inputItemType = NCommon::BuildType(wrapper.Input().Ref(), *wrapper.Input().Ref().GetTypeAnn(), ctx.ProgramBuilder);
                const auto outputItemType = NCommon::BuildType(wrapper.RowType().Ref(), *wrapper.RowType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType(), ctx.ProgramBuilder);
                return BuildNativeParseCall(input, inputItemType, outputItemType, state->Timezones[wrapper.DataSource().Cast<TClDataSource>().Cluster().Value()], ctx);
            }

            return TRuntimeNode();
        });

    compiler.ChainCallable(TDqReadWideWrap::CallableName(),
        [state](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
            if (const auto wrapper = TDqReadWrapBase(&node); wrapper.Input().Maybe<TClReadTable>().IsValid()) {
                const auto clRead = wrapper.Input().Cast<TClReadTable>();
                const auto readType = clRead.Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems().back();
                const auto inputItemType = NCommon::BuildType(wrapper.Input().Ref(), *GetSeqItemType(readType), ctx.ProgramBuilder);
                const auto cluster = clRead.DataSource().Cluster().StringValue();
                const auto outputType = NCommon::BuildType(wrapper.Ref(), *wrapper.Ref().GetTypeAnn(), ctx.ProgramBuilder);
                return BuildClickHouseInputCall(outputType, inputItemType, cluster, clRead.Table(), clRead.Timezone(), state, ctx);
            }

            return TRuntimeNode();
        });
}

}
