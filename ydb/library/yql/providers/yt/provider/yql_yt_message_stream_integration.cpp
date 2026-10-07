#include "yql_yt_message_stream_impl.h"
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_message_stream_expr_nodes.h>
#include <ydb/library/yql/providers/common/message_stream/provider.h>

namespace NYql {
using namespace NNodes;
namespace {
class TYtMessageStreamIntegration final : public IYtMessageStreamIntegration {
public:
    explicit TYtMessageStreamIntegration(std::shared_ptr<TYtMessageStreamState> state)
        : State_(std::move(state)), Annotation_(CreateYtMessageStreamTypeAnnotation())
        , Integration_(CreateYtMessageStreamDqIntegration(State_)), LoadMetadata_(CreateYtMessageStreamLoadMetadata(State_)) {}
    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        if (!TYtMessageStreamDataSource::Match(&node) || !State_->Names.contains(node.Child(1)->Content())) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Unknown YT MessageStream data source"));
            return false;
        }
        cluster = node.Child(1)->Content();
        return true;
    }
    void AddCluster(const TString& name, const THashMap<TString, TString>& properties) override {
        Y_ENSURE(properties.Value("source_type", "") == "YT", "Expected YT external data source");
        const auto endpoint = properties.Value("location", "");
        Y_ENSURE(!endpoint.empty(), "YT endpoint is missing");
        const auto token = NFq::NMessageStream::ComposeAuthToken(properties, properties.Value("token", ""));
        State_->Clusters[name] = {endpoint, token};
        State_->Tokens[name] = token;
        State_->Names.insert(name);
    }
    bool CanParse(const TExprNode& node) const override {
        return (node.IsCallable("Read!") && node.ChildrenSize() > 1 && TYtMessageStreamDataSource::Match(node.Child(1))) || Annotation_->CanParse(node);
    }
    IGraphTransformer& GetLoadTableMetadataTransformer() override { return *LoadMetadata_; }
    IGraphTransformer& GetTypeAnnotationTransformer() override { return *Annotation_; }
    IDqIntegration& GetDqIntegration() override { return *Integration_; }
    const THashMap<TString, TString>& GetClusterTokens() const override { return State_->Tokens; }
    const THashSet<TString>& GetValidClusters() const override { return State_->Names; }
    bool IsRead(const TExprNode& node) const override { return TYtMessageStreamReadTable::Match(&node); }
    TExprNode::TPtr RewriteIO(const TExprNode::TPtr& node, TExprContext& ctx) override {
        if ((!node->IsCallable("Right!") && !node->IsCallable("Left!"))
            || !node->Head().IsCallable("Read!") || !TYtMessageStreamDataSource::Match(node->Head().Child(1))) {
            return node;
        }
        const auto read = node->ChildPtr(0);
        if (node->IsCallable("Left!")) {
            return read->ChildPtr(0);
        }
        try {
            const auto [path, consumer] = ParseYtMessageStreamReadSettings(*read);
            return ctx.NewCallable(node->Pos(), "Right!", {ctx.NewCallable(read->Pos(), TYtMessageStreamReadTable::CallableName(), {
                read->ChildPtr(0), read->ChildPtr(1), ctx.NewAtom(read->Pos(), path),
                read->ChildPtr(3), ctx.NewAtom(read->Pos(), consumer)})});
        } catch (const std::exception& error) {
            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), error.what()));
            return {};
        }
    }
private:
    std::shared_ptr<TYtMessageStreamState> State_;
    THolder<TVisitorTransformerBase> Annotation_;
    THolder<IDqIntegration> Integration_;
    THolder<IGraphTransformer> LoadMetadata_;
};
}

std::shared_ptr<IYtMessageStreamIntegration> CreateYtMessageStreamIntegrationImpl(std::shared_ptr<TYtMessageStreamState> state) { return std::make_shared<TYtMessageStreamIntegration>(std::move(state)); }
}
