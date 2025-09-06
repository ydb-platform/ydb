#pragma once

#include <ydb/services/metadata/optimization/abstract.h>

namespace NKikimr::NKqp {

class TStreamingQueryOptimizer : public NMetadata::NModifications::IOptimizationManager {
public:
    NYql::TExprNode::TPtr ExtractWorldFeatures(NYql::NNodes::TCoNameValueTupleList& features, NYql::TExprContext& ctx) const override;

    NYql::IGraphTransformer::TStatus ValidateObjectNodeAnnotation(NYql::TExprNode::TPtr node, NYql::TExprContext& ctx) const override;
};

}  // namespace NKikimr::NKqp
