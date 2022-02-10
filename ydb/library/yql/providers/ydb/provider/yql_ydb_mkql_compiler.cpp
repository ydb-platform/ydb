#include "yql_ydb_mkql_compiler.h" 
 
#include <ydb/library/yql/providers/ydb/expr_nodes/yql_ydb_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
 
#include <library/cpp/yson/node/node.h> 
#include <library/cpp/yson/node/node_io.h> 
 
#include <algorithm> 
 
namespace NYql { 
 
using namespace NKikimr::NMiniKQL; 
using namespace NNodes; 
 
namespace { 
 
TRuntimeNode BuildYdbParseCall(TRuntimeNode input, TType* itemType, NCommon::TMkqlBuildContext& ctx) 
{ 
    const auto flow = ctx.ProgramBuilder.ToFlow(ctx.ProgramBuilder.Apply(ctx.ProgramBuilder.Udf("ClickHouseClient.ParseFromYdb", {}, itemType), {input})); 
    const auto structType = static_cast<const TStructType*>(itemType); 
    return ctx.ProgramBuilder.ExpandMap(flow, 
        [&](TRuntimeNode item) { 
            TRuntimeNode::TList fields; 
            fields.reserve(structType->GetMembersCount()); 
            auto j = 0U; 
            std::generate_n(std::back_inserter(fields), structType->GetMembersCount(), [&](){ return ctx.ProgramBuilder.Member(item, structType->GetMemberName(j++)); }); 
            return fields; 
        }); 
} 
 
template<bool Async> 
TRuntimeNode BuildYdbInputCall( 
    const TType* outputType, 
    TType* itemType, 
    const std::string_view& database,
    const std::string_view& endpoint, 
    bool secure, 
    const std::string_view& token, 
    const std::string_view& table, 
    const std::string_view& snapshot, 
    const std::vector<NUdf::TDataTypeId>& keysTypes, 
    TRuntimeNode limit, 
    NCommon::TMkqlBuildContext& ctx) 
{ 
    const auto streamType = ctx.ProgramBuilder.NewStreamType(ctx.ProgramBuilder.NewDataType(NUdf::TDataType<char*>::Id)); 
    const auto structType = static_cast<const TStructType*>(itemType); 
 
    TRuntimeNode::TList columns; 
    columns.reserve(structType->GetMembersCount()); 
    auto i = 0U; 
    std::generate_n(std::back_inserter(columns), structType->GetMembersCount(), [&](){ return ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(structType->GetMemberName(i++)); }); 
 
    TRuntimeNode::TList keys; 
    keys.reserve(keysTypes.size()); 
    std::transform(keysTypes.cbegin(), keysTypes.cend(), std::back_inserter(keys), [&ctx](const NUdf::TDataTypeId id){ return ctx.ProgramBuilder.NewDataLiteral(id); }); 
 
    TCallableBuilder scan(ctx.ProgramBuilder.GetTypeEnvironment(), Async ? "KikScanAsync" :  "KikScan", streamType); 
 
    scan.Add(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(table)); 
    scan.Add(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(database));
    scan.Add(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(endpoint)); 
    scan.Add(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(token)); 
    scan.Add(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>(snapshot)); 
    scan.Add(ctx.ProgramBuilder.NewTuple(columns)); 
    scan.Add(ctx.ProgramBuilder.NewTuple(keys)); 
    scan.Add(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>("")); 
    scan.Add(ctx.ProgramBuilder.NewDataLiteral<NUdf::EDataSlot::String>("")); 
    scan.Add(limit); 
    scan.Add(ctx.ProgramBuilder.NewDataLiteral(secure)); 
 
    auto flow = ctx.ProgramBuilder.ToFlow(ctx.ProgramBuilder.Apply(ctx.ProgramBuilder.Udf("ClickHouseClient.ParseFromYdb", {}, itemType), {TRuntimeNode(scan.Build(), false)})); 
 
    if (!AS_TYPE(TFlowType, outputType)->GetItemType()->IsSameType(*itemType)) { 
        flow = ctx.ProgramBuilder.ExpandMap(flow, 
            [&](TRuntimeNode item) { 
                TRuntimeNode::TList fields; 
                fields.reserve(structType->GetMembersCount()); 
                auto j = 0U; 
                std::generate_n(std::back_inserter(fields), structType->GetMembersCount(), [&](){ return ctx.ProgramBuilder.Member(item, structType->GetMemberName(j++)); }); 
                return fields; 
            }); 
    } 
 
    return flow; 
} 
 
} 
 
void RegisterDqYdbMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler, const TYdbState::TPtr& state) { 
    compiler.ChainCallable(TDqSourceWideWrap::CallableName(), 
        [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) { 
            if (const auto wrapper = TDqSourceWideWrap(&node); wrapper.DataSource().Category().Value() == YdbProviderName) { 
                const auto input = MkqlBuildExpr(wrapper.Input().Ref(), ctx); 
                const auto inputItemType = NCommon::BuildType(wrapper.RowType().Ref(), *wrapper.RowType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType(), ctx.ProgramBuilder); 
                return BuildYdbParseCall(input, inputItemType, ctx); 
            } 
 
            return TRuntimeNode(); 
        }); 
 
    compiler.ChainCallable(TDqReadWideWrap::CallableName(), 
        [state](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) { 
            if (const auto wrapper = TDqReadWrapBase(&node); wrapper.Input().Maybe<TYdbReadTable>().IsValid()) { 
                const auto read = wrapper.Input().Cast<TYdbReadTable>(); 
                const auto cluster = read.DataSource().Cluster().StringValue(); 
                const auto readType = read.Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems().back(); 
                const auto inputItemType = NCommon::BuildType(wrapper.Input().Ref(), *GetSeqItemType(readType), ctx.ProgramBuilder); 
                const auto outputType = NCommon::BuildType(wrapper.Ref(), *wrapper.Ref().GetTypeAnn(), ctx.ProgramBuilder); 
                const auto limit = read.LimitHint() ? 
                    ctx.ProgramBuilder.NewOptional(MkqlBuildExpr(read.LimitHint().Cast().Ref(), ctx)): 
                    ctx.ProgramBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui64>::Id); 
                const auto& meta = state->Tables[std::make_pair(cluster, read.Table().StringValue())]; 
                const auto& config = state->Configuration->Clusters[cluster]; 
                if (meta.ReadAsync) 
                    return BuildYdbInputCall<true>( 
                        outputType, 
                        inputItemType, 
                        config.Database, 
                        config.Endpoint, 
                        config.Secure, 
                        wrapper.Token().Name().Cast().Value(), 
                        read.Table(), 
                        meta.Snapshot.GetSnapshotId(), 
                        meta.KeyTypes, 
                        limit, 
                        ctx 
                    ); 
                else 
                    return BuildYdbInputCall<false>( 
                        outputType, 
                        inputItemType, 
                        config.Database, 
                        config.Endpoint, 
                        config.Secure, 
                        wrapper.Token().Name().Cast().Value(), 
                        read.Table(), 
                        meta.Snapshot.GetSnapshotId(), 
                        meta.KeyTypes, 
                        limit, 
                        ctx 
                    ); 
            } 
 
            return TRuntimeNode(); 
        }); 
} 
 
} 
