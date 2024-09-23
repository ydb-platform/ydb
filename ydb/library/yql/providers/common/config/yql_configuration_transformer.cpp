#include "yql_configuration_transformer.h"

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

#include <util/generic/maybe.h>
#include <util/string/vector.h>

namespace NYql {
namespace NCommon {

using namespace NNodes;

TProviderConfigurationTransformer::TProviderConfigurationTransformer(TSettingDispatcher::TPtr dispatcher,
    const TTypeAnnotationContext& types, const TString& provider, const THashSet<TStringBuf>& configureCallables)
    : Dispatcher(dispatcher)
    , Types(types)
    , Provider(provider)
    , ConfigureCallables(configureCallables)
{
    if (ConfigureCallables.empty()) {
        ConfigureCallables.insert(TCoConfigure::CallableName());
    }
}

IGraphTransformer::TStatus TProviderConfigurationTransformer::DoTransform(TExprNode::TPtr input,
    TExprNode::TPtr& output, TExprContext& ctx)
{
    output = input;
    if (ctx.Step.IsDone(TExprStep::Configure)) {
        return TStatus::Ok;
    }

    TOptimizeExprSettings settings(nullptr);
    settings.VisitChanges = true;
    auto status = OptimizeExpr(input, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
        if (node->IsCallable(ConfigureCallables)) {
            if (!EnsureMinArgsCount(*node, 2, ctx)) {
                return nullptr;
            }

            if (!TCoDataSource::Match(node->Child(TCoConfigure::idx_DataSource))) {
                return node;
            }
            auto ds = node->Child(TCoConfigure::idx_DataSource);
            if (ds->Child(TCoDataSource::idx_Category)->Content() != Provider) {
                return node;
            }
            if (!EnsureMinArgsCount(*ds, 2, ctx)) {
                return nullptr;
            }
            if (!EnsureAtom(*ds->Child(1), ctx)) {
                return nullptr;
            }
            auto clusterName = TString(ds->Child(1)->Content());

            if (!EnsureMinArgsCount(*node, 3, ctx)) {
                return nullptr;
            }
            if (!EnsureAtom(*node->Child(2), ctx)) {
                return nullptr;
            }

            auto atom = node->Child(2)->Content();
            if (atom == TStringBuf("Attr")) {
                if (!EnsureMinArgsCount(*node, 4, ctx)) {
                    return nullptr;
                }

                if (!EnsureMaxArgsCount(*node, 5, ctx)) {
                    return nullptr;
                }

                if (!EnsureAtom(*node->Child(3), ctx)) {
                    return nullptr;
                }

                auto name = TString(node->Child(3)->Content());
                if (name.StartsWith('_')) {
                    ctx.AddError(TIssue(ctx.GetPosition(node->Child(3)->Pos()),
                        TStringBuilder() << "Failed to override system setting: " << name));
                    return nullptr;
                }

                TMaybe<TString> value;
                if (node->ChildrenSize() == 5) {
                    if (node->Child(4)->IsCallable("EvaluateAtom")) {
                        return node;
                    }

                    if (!EnsureAtom(*node->Child(4), ctx)) {
                        return nullptr;
                    }

                    value = TString(node->Child(4)->Content());
                }

                if (!HandleAttr(node->Child(3)->Pos(), clusterName, name, value, ctx)) {
                    return nullptr;
                }
            } else if (atom == TStringBuf("Auth")) {
                if (!EnsureArgsCount(*node, 4, ctx)) {
                    return nullptr;
                }

                if (!EnsureAtom(*node->Child(3), ctx)) {
                    return nullptr;
                }

                auto credAlias = TString(node->Child(3)->Content());
                if (!HandleAuth(node->Child(3)->Pos(), clusterName, credAlias, ctx)) {
                    return nullptr;
                }
            } else {
                ctx.AddError(TIssue(ctx.GetPosition(node->Child(2)->Pos()), TStringBuilder()
                    << "Unsupported configuration option: " << atom));
                return nullptr;
            }
        }

        return node;
    }, ctx, settings);

    return status;
}

bool TProviderConfigurationTransformer::HandleAttr(TPositionHandle pos, const TString& cluster, const TString& name,
    const TMaybe<TString>& value, TExprContext& ctx)
{
    Y_UNUSED(pos);
    Y_UNUSED(ctx);
    return Dispatcher->Dispatch(cluster, name, value, TSettingDispatcher::EStage::STATIC, TSettingDispatcher::GetErrorCallback(pos, ctx));
}

bool TProviderConfigurationTransformer::HandleAuth(TPositionHandle pos, const TString& cluster, const TString& alias,
    TExprContext& ctx)
{
    auto cred = Types.Credentials->FindCredential(alias);
    if (!cred) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Unknown credential: " << alias));
        return false;
    }

    if (cred->Category != Provider) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder()
            << "Mismatch credential category, expected: "
            << Provider << ", but found: " << cred->Category));
        return false;
    }

    return Dispatcher->Dispatch(cluster, "Auth", cred->Content, TSettingDispatcher::EStage::STATIC, TSettingDispatcher::GetErrorCallback(pos, ctx));
}

THolder<IGraphTransformer> CreateProviderConfigurationTransformer(
    TSettingDispatcher::TPtr dispatcher,
    const TTypeAnnotationContext& types,
    const TString& provider) {
    return THolder(new TProviderConfigurationTransformer(dispatcher, types, provider));
}

} // namespace NCommon
} // namespace NYql
