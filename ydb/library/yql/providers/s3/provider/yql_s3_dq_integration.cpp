#include "yql_s3_dq_integration.h"
#include "yql_s3_mkql_compiler.h"

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h> 
#include <ydb/library/yql/providers/common/dq/yql_dq_integration_impl.h> 
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h> 
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h> 
#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h> 
#include <ydb/library/yql/providers/s3/proto/range.pb.h> 
#include <ydb/library/yql/providers/s3/proto/source.pb.h> 
#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

class TS3DqIntegration: public TDqIntegrationBase {
public:
    TS3DqIntegration(TS3State::TPtr state)
        : State_(state)
    {
    }

    ui64 Partition(const TDqSettings&, size_t maxPartitions, const TExprNode& node, TVector<TString>& partitions, TString*, TExprContext&, bool) override {
        TString cluster;
        std::vector<std::vector<TString>> parts;
        if (const TMaybeNode<TDqSource> source = &node) {
            cluster = source.Cast().DataSource().Cast<TS3DataSource>().Cluster().Value();
            const auto settings = source.Cast().Settings().Cast<TS3SourceSettings>();
            parts.reserve(settings.Paths().Size());
            for (auto i = 0u; i < settings.Paths().Size(); ++i)
                parts.emplace_back(std::vector<TString>(1U, settings.Paths().Item(i).Path().StringValue()));
        }

        if (maxPartitions && parts.size() > maxPartitions) {
            if (const auto extraParts = parts.size() - maxPartitions; extraParts > maxPartitions) {
                const auto partsPerTask = (parts.size() - 1ULL) / maxPartitions + 1ULL;
                for (auto it = parts.begin(); parts.end() > it;) {
                    const auto to = it;
                    const auto up = to + std::min<std::size_t>(partsPerTask, std::distance(to, parts.end()));
                    for (auto jt = ++it; jt < up; ++jt)
                        std::move(jt->begin(), jt->end(), std::back_inserter(*to));
                    it = parts.erase(it, up);
                }
            } else {
                const auto dropEachPart = maxPartitions / extraParts;
                for (auto it = parts.begin(); parts.size() > maxPartitions;) {
                    const auto to = it + dropEachPart;
                    it = to - 1U;
                    std::move(to->begin(), to->end(), std::back_inserter(*it));
                    it = parts.erase(to);
                }
            }
        }

        partitions.reserve(parts.size());
        for (const auto& part : parts) {
            NS3::TRange range;
            std::for_each(part.cbegin(), part.cend(), [&range](const TString& path) { range.AddPath(path); });

            partitions.emplace_back();
            TStringOutput out(partitions.back());
            range.Save(&out);
        }

        return 0;
    }

    TMaybe<ui64> CanRead(const TDqSettings&, const TExprNode& read, TExprContext&, bool) override {
        if (TS3ReadObject::Match(&read)) {
            return 0ul; // TODO: return real size
        }

        return Nothing();
    }

    TExprNode::TPtr WrapRead(const TDqSettings&, const TExprNode::TPtr& read, TExprContext& ctx) override {
        if (const auto& maybeS3ReadObject = TMaybeNode<TS3ReadObject>(read)) {
            const auto& s3ReadObject = maybeS3ReadObject.Cast();

            const auto rowType = s3ReadObject.Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems().back()->Cast<TListExprType>()->GetItemType();
            const auto& clusterName = s3ReadObject.DataSource().Cluster().StringValue();

            TExprNode::TListType settings(1U,
                ctx.Builder(s3ReadObject.Object().Pos())
                    .List()
                        .Atom(0, "format", TNodeFlags::Default)
                        .Add(1, s3ReadObject.Object().Format().Ptr())
                    .Seal().Build()
            );

            if (const auto& objectSettings = s3ReadObject.Object().Settings()) {
                settings.emplace_back(
                    ctx.Builder(objectSettings.Cast().Pos())
                        .List()
                            .Atom(0, "settings", TNodeFlags::Default)
                            .Add(1, objectSettings.Cast().Ptr())
                        .Seal().Build()
                );
            }

            const auto token = "cluster:default_" + clusterName;
            YQL_CLOG(INFO, ProviderS3) << "Wrap " << read->Content() << " with token: " << token;

            return Build<TDqSourceWrap>(ctx, read->Pos())
                .Input<TS3SourceSettings>()
                    .Paths(s3ReadObject.Object().Paths())
                    .Token<TCoSecureParam>()
                        .Name().Build(token)
                        .Build()
                    .Build()
                .RowType(ExpandType(s3ReadObject.Pos(), *rowType, ctx))
                .DataSource(s3ReadObject.DataSource().Cast<TCoDataSource>())
                .Settings(ctx.NewList(s3ReadObject.Object().Pos(), std::move(settings)))
                .Done().Ptr();
        }
        return read;
    }

    void FillSourceSettings(const TExprNode& node, ::google::protobuf::Any& protoSettings, TString& sourceType) override {
        const TDqSource source(&node);
        if (const auto maySettings = source.Settings().Maybe<TS3SourceSettings>()) {
            const auto settings = maySettings.Cast();
            const auto& cluster = source.DataSource().Cast<TS3DataSource>().Cluster().StringValue();
            const auto& connect = State_->Configuration->Clusters.at(cluster);

            NS3::TSource srcDesc;
            srcDesc.SetUrl(connect.Url);
            srcDesc.SetToken(settings.Token().Name().StringValue());

            const auto& paths = settings.Paths();
            for (auto i = 0U; i < paths.Size(); ++i) {
                const auto p = srcDesc.AddPath();
                p->SetPath(paths.Item(i).Path().StringValue());
                p->SetSize(FromString<ui64>(paths.Item(i).Size().Value()));
            }

            protoSettings.PackFrom(srcDesc);
            sourceType = "S3Source";
        }
    }

    void RegisterMkqlCompiler(NCommon::TMkqlCallableCompilerBase& compiler) override {
        RegisterDqS3MkqlCompilers(compiler, State_);
    }

private:
    const TS3State::TPtr State_;
};

}

THolder<IDqIntegration> CreateS3DqIntegration(TS3State::TPtr state) {
    return MakeHolder<TS3DqIntegration>(state);
}

}
