#include <yql/essentials/core/yql_expr_type_annotation.h>
#include "yql_yt_message_stream_impl.h"
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_message_stream_expr_nodes.h>
#include <ydb/library/yql/providers/yt/proto/source.pb.h>
#include <ydb/library/yql/providers/common/message_stream/provider.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <yql/essentials/providers/common/mkql/yql_provider_mkql.h>
#include <yql/essentials/providers/common/mkql/yql_type_mkql.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <util/string/cast.h>

namespace NYql {
using namespace NNodes;
namespace {
class TYtMessageStreamDqIntegration final : public TDqIntegrationBase {
public:
    explicit TYtMessageStreamDqIntegration(std::shared_ptr<TYtMessageStreamState> state) : State_(std::move(state)) {}
    bool CanRead(const TExprNode& read, TExprContext&, bool) override {
        return TYtMessageStreamReadTable::Match(&read);
    }
    TMaybe<ui64> EstimateReadSize(ui64, ui32, const TVector<const TExprNode*>& reads, TExprContext&) override {
        for (auto* read : reads) {
            if (!TYtMessageStreamReadTable::Match(read)) {
                return Nothing();
            }
        }
        return 0;
    }
    TExprNode::TPtr WrapRead(const TExprNode::TPtr& read, TExprContext& ctx, const TWrapReadSettings&) override {
        if (!TYtMessageStreamReadTable::Match(read.Get())) {
            return read;
        }
        const TYtMessageStreamReadTable queue(read);
        const auto cluster = queue.DataSource().Cluster().StringValue();
        auto settings = ctx.NewCallable(read->Pos(), TYtMessageStreamSourceSettings::CallableName(), {
            queue.World().Ptr(), queue.Table().Ptr(),
            Build<TCoSecureParam>(ctx, read->Pos()).Name().Build(TString("cluster:default_") + cluster).Done().Ptr(),
            ctx.NewList(read->Pos(), {ctx.NewAtom(read->Pos(), "Data")}), queue.Consumer().Ptr(),
            ctx.NewAtom(read->Pos(), ToString(State_->Partitions.at(std::make_pair(cluster, queue.Table().StringValue()))))});
        return Build<TDqSourceWrap>(ctx, read->Pos())
            .Input(settings)
            .DataSource(queue.DataSource().Cast<TCoDataSource>())
            .RowType(ExpandType(read->Pos(), *NFq::NMessageStream::MakeRawRowType(ctx), ctx))
            .Done().Ptr();
    }
    ui64 Partition(const TExprNode& node, TVector<TString>& partitions, TString*, TExprContext&, const TPartitionSettings&) override {
        partitions.clear();
        ui64 count = 0;
        VisitExpr(node, [&](const TExprNode& part) {
            if (TYtMessageStreamSourceSettings::Match(&part)) {
                count = FromString<ui64>(part.Child(5)->Content());
            }
            return true;
        });
        Y_ENSURE(count, "No QYT partitions to read");
        for (ui64 i = 0; i < count; ++i) {
            partitions.push_back(ToString(i));
        }
        return 0;
    }
    void FillSourceSettings(const TExprNode& node, google::protobuf::Any& settings, TString& sourceType, size_t, TExprContext&) override {
        const TDqSource source(&node);
        const auto& input = source.Settings().Ref();
        const auto cluster = source.DataSource().Cast<TYtMessageStreamDataSource>().Cluster().StringValue();
        NQyt::NProto::TSource desc;
        desc.SetEndpoint(State_->Clusters.at(cluster).Endpoint);
        desc.SetPath(TString(input.Child(1)->Content()));
        desc.SetToken(TCoSecureParam(input.ChildPtr(2)).Name().StringValue());
        desc.SetConsumer(TString(input.Child(4)->Content()));
        settings.PackFrom(desc);
        sourceType = "QytSource";
    }
    void RegisterMkqlCompiler(NCommon::TMkqlCallableCompilerBase& compiler) override {
        compiler.ChainCallable(TDqSourceWideWrap::CallableName(), [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
            const TDqSourceWideWrap wrap(&node);
            if (!TYtMessageStreamDataSource::Match(&wrap.DataSource().Ref())) {
                return NKikimr::NMiniKQL::TRuntimeNode();
            }
            auto input = NCommon::MkqlBuildExpr(wrap.Input().Ref(), ctx);
            return ctx.ProgramBuilder.ExpandMap(ctx.ProgramBuilder.ToFlow(input, {}), [](NKikimr::NMiniKQL::TRuntimeNode item) {
                return NKikimr::NMiniKQL::TRuntimeNode::TList{item};
            });
        });
    }
private:
    std::shared_ptr<TYtMessageStreamState> State_;
};

}
THolder<IDqIntegration> CreateYtMessageStreamDqIntegration(std::shared_ptr<TYtMessageStreamState> state) { return MakeHolder<TYtMessageStreamDqIntegration>(std::move(state)); }
}
