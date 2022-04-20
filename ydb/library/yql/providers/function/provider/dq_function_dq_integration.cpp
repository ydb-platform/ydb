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
        auto maybeDqSink = TMaybeNode<TDqSink>(&node);
        if (!maybeDqSink)
            return;

        auto dqSink = maybeDqSink.Cast();
        auto maybeSettings = TMaybeNode<TTransformSettings>(dqSink.Settings().Raw());
        if (!maybeSettings) {
            return;
        }

        auto functionSettings = maybeSettings.Cast();
        NYql::NProto::TFunctionTransform transform;
        for (size_t i = 0; i < functionSettings.Other().Size(); i++) {
            TCoNameValueTuple setting = functionSettings.Other().Item(i);
            const TStringBuf name = Name(setting);
            if (name == "invoke_url") {
                transform.SetInvokeUrl(TString(Value(setting)));
            }
        }

        transformSettings.PackFrom(transform);
    }

    static TStringBuf Name(const TCoNameValueTuple& nameValue) {
        return nameValue.Name().Value();
    }

    static TStringBuf Value(const TCoNameValueTuple& nameValue) {
        if (TMaybeNode<TExprBase> maybeValue = nameValue.Value()) {
            const TExprNode& value = maybeValue.Cast().Ref();
            YQL_ENSURE(value.IsAtom());
            return value.Content();
        }

        return {};
    }

private:
    TDqFunctionState::TPtr State;
};
} // namespace

THolder<IDqIntegration> CreateDqFunctionDqIntegration(TDqFunctionState::TPtr state) {
    return MakeHolder<TDqFunctionDqIntegration>(state);
}

}