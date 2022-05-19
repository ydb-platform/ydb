#include "dq_function_provider_impl.h"

#include <ydb/library/yql/providers/common/dq/yql_dq_integration_impl.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/function/expr_nodes/dq_function_expr_nodes.h>
#include <ydb/library/yql/providers/function/proto/dq_function.pb.h>
#include <ydb/library/yql/providers/common/schema/mkql/yql_mkql_schema.h>
#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>

#include <util/generic/ptr.h>

namespace NYql::NDqFunction {
namespace {

using namespace NNodes;

class TDqFunctionDqIntegration: public TDqIntegrationBase {
public:
    TDqFunctionDqIntegration(TDqFunctionState::TPtr state)
        : State(state)
    {
    }

    void FillTransformSettings(const TExprNode& node, ::google::protobuf::Any& transformSettings) override {
        auto maybeDqTransform = TMaybeNode<TDqTransform>(&node);
        if (!maybeDqTransform)
            return;

        auto maybeSettings = TMaybeNode<TFunctionTransformSettings>(maybeDqTransform.Cast().Settings().Raw());
        if (!maybeSettings) {
            return;
        }

        auto functionSettings = maybeSettings.Cast();
        NYql::NProto::TFunctionTransform transform;
        transform.SetInvokeUrl(TString{functionSettings.InvokeUrl().Value()});

        transformSettings.PackFrom(transform);
    }

private:
    TDqFunctionState::TPtr State;
};
} // namespace

THolder<IDqIntegration> CreateDqFunctionDqIntegration(TDqFunctionState::TPtr state) {
    return MakeHolder<TDqFunctionDqIntegration>(state);
}

}
