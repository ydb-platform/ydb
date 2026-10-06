#pragma once

#include <yql/essentials/core/yql_data_provider.h>
#include <yql/essentials/core/dq_integration/yql_dq_integration.h>

namespace NYql {

class IYtMessageStreamIntegration {
public:
    virtual ~IYtMessageStreamIntegration() = default;
    virtual void AddCluster(const TString& name, const THashMap<TString, TString>& properties) = 0;
    virtual const THashMap<TString, TString>& GetClusterTokens() const = 0;
    virtual const THashSet<TString>& GetValidClusters() const = 0;
    virtual bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) = 0;
    virtual bool CanParse(const TExprNode& node) const = 0;
    virtual bool IsRead(const TExprNode& node) const = 0;
    virtual IGraphTransformer& GetLoadTableMetadataTransformer() = 0;
    virtual IGraphTransformer& GetTypeAnnotationTransformer() = 0;
    virtual IDqIntegration& GetDqIntegration() = 0;
    virtual TExprNode::TPtr RewriteIO(const TExprNode::TPtr& node, TExprContext& ctx) = 0;
};

} // namespace NYql
