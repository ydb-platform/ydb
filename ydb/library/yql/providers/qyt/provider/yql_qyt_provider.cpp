#include "yql_qyt_provider_impl.h"

#include <ydb/library/yql/providers/common/message_stream/provider.h>
#include <ydb/library/yql/providers/qyt/expr_nodes/yql_qyt_expr_nodes.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>

namespace NYql {
namespace {
using namespace NNodes;

class TQytDataSourceProvider final: public TDataProviderBase {
public:
    explicit TQytDataSourceProvider(std::shared_ptr<TQytState> state)
        : State_(std::move(state))
        , Annotation_(CreateQytTypeAnnotation(State_))
        , Integration_(CreateQytDqIntegration(State_))
        , LoadMetadata_(CreateQytLoadMetadata(State_))
        , Discovery_(CreateFunctorTransformer([this](TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
            return OptimizeExpr(input, output, [this](const TExprNode::TPtr& node, TExprContext& ctx) {
                return RewriteIO(node, ctx);
            }, ctx, TOptimizeExprSettings(nullptr));
        }))
    {
    }

    TStringBuf GetName() const override {
        return QytProviderName;
    }

    void AddCluster(const TString& name, const THashMap<TString, TString>& properties) override {
        Y_ENSURE(properties.Value("source_type", "") == "YT", "Expected YT external data source");
        const auto endpoint = properties.Value("location", "");
        Y_ENSURE(!endpoint.empty(), "YT endpoint is missing");
        const auto token = NFq::NMessageStream::ComposeAuthToken(properties, properties.Value("token", ""));
        State_->Clusters[name] = { endpoint, token };
        State_->Tokens[name] = token;
        State_->Names.insert(name);
    }

    const THashMap<TString, TString>* GetClusterTokens() override {
        return &State_->Tokens;
    }

    const THashSet<TString>& GetValidClusters() override {
        return State_->Names;
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        if (!TQytDataSource::Match(&node) || !State_->Names.contains(node.Child(1)->Content())) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Unknown QYT data source"));
            return false;
        }
        cluster = node.Child(1)->Content();
        return true;
    }

    bool CanParse(const TExprNode& node) override {
        return (node.IsCallable("Read!") && node.ChildrenSize() > 1 && TQytDataSource::Match(node.Child(1))) || Annotation_->CanParse(node);
    }

    bool IsRead(const TExprNode& node) override {
        return TQytReadTable::Match(&node);
    }

    TExprNode::TPtr RewriteIO(const TExprNode::TPtr& node, TExprContext& ctx) override {
        if ((!node->IsCallable("Right!") && !node->IsCallable("Left!")) || node->ChildrenSize() != 1 || !node->Head().IsCallable("Read!") ||
            node->Head().ChildrenSize() < 2 || !TQytDataSource::Match(node->Head().Child(1))) {
            return node;
        }
        const auto read = node->ChildPtr(0);
        if (node->IsCallable("Left!")) {
            return read->ChildPtr(0);
        }
        try {
            const auto [path, consumer] = ParseQytReadSettings(*read);
            return ctx.NewCallable(node->Pos(), "Right!", { ctx.NewCallable(read->Pos(), TQytReadTable::CallableName(),
                                                              { read->ChildPtr(0), read->ChildPtr(1), ctx.NewAtom(read->Pos(), path),
                                                                  read->ChildPtr(3), ctx.NewAtom(read->Pos(), consumer) }) });
        } catch (const std::exception& error) {
            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), error.what()));
            return {};
        }
    }

    IGraphTransformer& GetIODiscoveryTransformer() override {
        return *Discovery_;
    }

    IGraphTransformer& GetTypeAnnotationTransformer(bool) override {
        return *Annotation_;
    }

    IGraphTransformer& GetLoadTableMetadataTransformer() override {
        return *LoadMetadata_;
    }

    IDqIntegration* GetDqIntegration() override {
        return Integration_.Get();
    }

    bool GetDependencies(const TExprNode& node, TExprNode::TListType& children, bool) override {
        if (!IsRead(node)) {
            return false;
        }
        children.push_back(node.Child(0));
        return true;
    }

private:
    const std::shared_ptr<TQytState> State_;
    const THolder<TVisitorTransformerBase> Annotation_;
    const THolder<IDqIntegration> Integration_;
    const THolder<IGraphTransformer> LoadMetadata_;
    const THolder<IGraphTransformer> Discovery_;
};
}   // namespace

TIntrusivePtr<IDataProvider> CreateQytDataSource(IStructuredTokenCredentialsFactory::TPtr credentialsFactory) {
    auto state = std::make_shared<TQytState>();
    state->Credentials = std::move(credentialsFactory);
    return MakeIntrusive<TQytDataSourceProvider>(std::move(state));
}
}   // namespace NYql
