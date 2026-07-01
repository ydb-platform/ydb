#include "yql_s3_datasink_constraints.h"
#include "yql_s3_provider.h"

#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>

#include <yql/essentials/core/yql_expr_constraint.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>

namespace NYql {

using namespace NNodes;

namespace {

class TS3DataSourceConstraintTransformer : public TVisitorTransformerBase {
    using TBase = TVisitorTransformerBase;

public:
    TS3DataSourceConstraintTransformer()
        : TBase(/* failOnUnknown */ true)
    {
        AddHandler({TS3SinkOutput::CallableName()}, Hndl(&TS3DataSourceConstraintTransformer::HandleS3SinkOutput));
        AddHandler({
            TCoCommit::CallableName(),
            TS3WriteObject::CallableName(),
            TS3Target::CallableName(),
            TS3SinkSettings::CallableName(),
            TS3Insert::CallableName(),
        }, Hndl(&TS3DataSourceConstraintTransformer::HandleDefault));
    }

    TStatus HandleDefault(TExprBase node, TExprContext&) {
        return UpdateAllChildLambdasConstraints(node.Ref());
    }

    TStatus HandleS3SinkOutput(TExprBase node, TExprContext&) {
        if (const auto* c = node.Ref().Head().GetConstraint<TStreamingConstraintNode>()) {
            node.Ptr()->AddConstraint(c);
        }
        return TStatus::Ok;
    }
};

} // anonymous namespace

TAutoPtr<IGraphTransformer> CreateS3DataSinkConstraintTransformer(TIntrusivePtr<TS3State> state) {
    if (!state->EnableS3ConstraintsTransformer) {
        return CreateDefCallableConstraintTransformer();
    }
    return new TS3DataSourceConstraintTransformer();
}

} // namespace NYql
