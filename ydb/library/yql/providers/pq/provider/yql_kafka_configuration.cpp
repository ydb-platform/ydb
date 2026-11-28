#include "yql_pq_provider.h"
#include "yql_pq_settings.h"

#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/providers/common/config/transformer/yql_configuration_transformer.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>

namespace NYql {
namespace {

// Kafka reuses PQ planning/parsing; only its public pragma namespace is separate.
class TKafkaConfigurationProvider final: public TDataProviderBase {
public:
    explicit TKafkaConfigurationProvider(TPqState::TPtr state)
        : State(std::move(state))
        , Configuration(State->KafkaConfiguration, *State->Types, "kafka")
        , TypeAnnotation(CreateFunctorTransformer([](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
            output = input;
            if (!EnsureMinArgsCount(*input, 2, ctx) || !EnsureWorldType(input->Head(), ctx) ||
                !EnsureSpecificDataSource(*input->Child(1), "kafka", ctx)) {
                return IGraphTransformer::TStatus::Error;
            }
            input->SetTypeAnn(input->Head().GetTypeAnn());
            return IGraphTransformer::TStatus::Ok;
        }))
    {
    }

    TStringBuf GetName() const override {
        return "kafka";
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        if (node.IsCallable("DataSource") && node.ChildrenSize() == 2 && node.Child(0)->Content() == "kafka" &&
            node.Child(1)->Content() == NCommon::ALL_CLUSTERS) {
            cluster = NCommon::ALL_CLUSTERS;
            return true;
        }
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Kafka provider supports global configuration pragmas only"));
        return false;
    }

    IGraphTransformer& GetConfigurationTransformer() override {
        return Configuration;
    }

    IGraphTransformer& GetTypeAnnotationTransformer(bool instantOnly) override {
        Y_UNUSED(instantOnly);
        return *TypeAnnotation;
    }

private:
    const TPqState::TPtr State;
    NCommon::TProviderConfigurationTransformer Configuration;
    THolder<IGraphTransformer> TypeAnnotation;
};

}   // namespace

TIntrusivePtr<IDataProvider> CreateKafkaConfigurationProvider(TPqState::TPtr state) {
    return new TKafkaConfigurationProvider(std::move(state));
}

}   // namespace NYql
