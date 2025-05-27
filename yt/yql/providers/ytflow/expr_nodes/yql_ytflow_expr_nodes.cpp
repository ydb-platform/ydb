#include "yql_ytflow_expr_nodes.h"


namespace NYql::NNodes {

TYtflowDSource::TYtflowDSource(const TExprNode* node)
    : TYtflowDSourceStub(node)
{
}

TYtflowDSource::TYtflowDSource(const TExprNode::TPtr& node)
    : TYtflowDSourceStub(node)
{
}

bool TYtflowDSource::Match(const TExprNode* node) {
    if (!TYtflowDSourceStub::Match(node)) {
        return false;
    }

    if (node->Child(TYtflowDSource::idx_Category)->Content() != YtflowProviderName) {
        return false;
    }

    return true;
}


TYtflowDSink::TYtflowDSink(const TExprNode* node)
    : TYtflowDSinkStub(node)
{
}

TYtflowDSink::TYtflowDSink(const TExprNode::TPtr& node)
    : TYtflowDSinkStub(node)
{
}

bool TYtflowDSink::Match(const TExprNode* node) {
    if (!TYtflowDSinkStub::Match(node)) {
        return false;
    }

    if (node->Child(TYtflowDSink::idx_Category)->Content() != YtflowProviderName) {
        return false;
    }

    return true;
}

} // namespace NYql::NNodes
