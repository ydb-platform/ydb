#pragma once

#include "yql_graph_transformer.h"
#include "yql_type_annotation.h"
#include "yql_expr_type_annotation.h"
#include <yql/essentials/core/sql_types/yql_callable_names.h>

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/public/issue/yql_issue_manager.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/ptr.h>
#include <util/string/builder.h>

#include <utility>

namespace NYql {

class TCallableTransformerParsers : public TGraphTransformerBase {
public:
    explicit TCallableTransformerParsers(TTypeAnnotationContext& types)
        : Types_(types)
    {}

protected:
    IDataProvider* ParseCommit(const TExprNode& input, TExprContext& ctx, bool& isUniversal);
    IDataProvider* ParseRead(const TExprNode& input, TExprContext& ctx);
    IDataProvider* ParseWrite(const TExprNode& input, TExprContext& ctx);
    IDataProvider* ParseMaterialize(const TExprNode& input, TExprContext& ctx);
    IDataProvider* ParseConfigure(const TExprNode& input, TExprContext& ctx, bool& isUniversal);

protected:
    TTypeAnnotationContext& Types_;
};

template <class TDerived>
class TCallableTransformerBase : public TCallableTransformerParsers {
public:
    TCallableTransformerBase(TTypeAnnotationContext& types, bool instantOnly)
        : TCallableTransformerParsers(types)
        , InstantOnly_(instantOnly)
    {}

    IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;

        if (input->IsList()) {
            if (const auto maybeStatus = static_cast<TDerived*>(this)->ProcessList(input, output, ctx)) {
                return *maybeStatus;
            }
        }

        auto name = input->Content();
        TIssueScopeGuard issueScope(ctx.IssueManager, [&]() {
            return MakeIntrusive<TIssue>(ctx.GetPosition(input->Pos()),
                TStringBuilder() << "At function: " << NormalizeCallableName(name));
        });

        TStatus status = TStatus::Ok;
        if (auto maybeStatus = static_cast<TDerived*>(this)->ProcessCore(input, output, ctx)) {
            status = *maybeStatus;
        } else {
            if (name == CommitName) {
                bool isUniversal;
                auto datasink = ParseCommit(*input, ctx, isUniversal);
                if (isUniversal) {
                    input->SetTypeAnn(ctx.MakeType<TUniversalExprType>());
                } else if (!datasink) {
                    status = TStatus::Error;
                } else {
                    status = ProcessDataProviderAnnotation(*datasink, input, output, ctx);
                    if (status == TStatus::Ok) {
                        status = static_cast<TDerived*>(this)->ValidateProviderCommitResult(input, ctx);
                    }
                }
            } else if (name == ReadName) {
                auto datasource = ParseRead(*input, ctx);
                if (!datasource) {
                    status = TStatus::Error;
                } else {
                    status = ProcessDataProviderAnnotation(*datasource, input, output, ctx);
                    if (status == TStatus::Ok) {
                        status = static_cast<TDerived*>(this)->ValidateProviderReadResult(input, ctx);
                    }
                }
            } else if (name == WriteName) {
                auto datasink = ParseWrite(*input, ctx);
                if (!datasink) {
                    status = TStatus::Error;
                } else {
                    status = ProcessDataProviderAnnotation(*datasink, input, output, ctx);
                    if (status == TStatus::Ok) {
                        status = static_cast<TDerived*>(this)->ValidateProviderWriteResult(input, ctx);
                    }
                }
            } else if (name == MaterializeName) {
                auto datasink = ParseMaterialize(*input, ctx);
                if (!datasink) {
                    status = TStatus::Error;
                } else {
                    status = ProcessDataProviderAnnotation(*datasink, input, output, ctx);
                    if (status == TStatus::Ok) {
                        status = static_cast<TDerived*>(this)->ValidateProviderMaterializeResult(input, ctx);
                    }
                }
            } else if (name == ConfigureName) {
                bool isUniversal;
                auto provider = ParseConfigure(*input, ctx, isUniversal);
                if (isUniversal) {
                    input->SetTypeAnn(ctx.MakeType<TUniversalExprType>());
                } else if (!provider) {
                    status = TStatus::Error;
                } else {
                    status = ProcessDataProviderAnnotation(*provider, input, output, ctx);
                    if (status == TStatus::Ok) {
                        status = static_cast<TDerived*>(this)->ValidateProviderConfigureResult(input, ctx);
                    }
                }
            } else {
                bool foundFunc = false;
                for (auto& datasource : Types_.DataSources) {
                    if (!datasource->CanParse(*input)) {
                        continue;
                    }

                    foundFunc = true;
                    status = ProcessDataProviderAnnotation(*datasource, input, output, ctx);
                    break;
                }

                if (!foundFunc) {
                    for (auto& datasink : Types_.DataSinks) {
                        if (!datasink->CanParse(*input)) {
                            continue;
                        }

                        foundFunc = true;
                        status = ProcessDataProviderAnnotation(*datasink, input, output, ctx);
                        break;
                    }
                }

                if (!foundFunc) {
                    return static_cast<TDerived*>(this)->ProcessUnknown(input, ctx);
                }
            }
        }

        return status;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) final {
        const auto it = PendingNodes_.find(&input);
        YQL_ENSURE(it != PendingNodes_.cend());
        return static_cast<TDerived*>(this)->GetTransformer(*it->second.second).GetAsyncFuture(input);
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        const auto it = PendingNodes_.find(input.Get());
        YQL_ENSURE(it != PendingNodes_.cend());
        const auto provider = it->second.second;
        IGraphTransformer& transformer = static_cast<TDerived*>(this)->GetTransformer(*provider);
        const auto status = transformer.ApplyAsyncChanges(it->second.first, output, ctx);
        PendingNodes_.erase(it);
        return status;
    }

    void Rewind() override {
        PendingNodes_.clear();
    }

protected:
    IGraphTransformer::TStatus ProcessDataProviderAnnotation(IDataProvider& dataProvider,
        const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        auto status = static_cast<TDerived*>(this)->GetTransformer(dataProvider).Transform(input, output, ctx);
        if (status.Level == IGraphTransformer::TStatus::Async) {
            if (InstantOnly_) {
                ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() <<
                    "Async status is not allowed for instant transform, provider name: " << dataProvider.GetName()));
                return IGraphTransformer::TStatus::Error;
            }

            PendingNodes_[input.Get()] = std::make_pair(input, &dataProvider);
        }

        return status;
    }

protected:
    const bool InstantOnly_;
    TNodeMap<std::pair<TExprNode::TPtr, IDataProvider*>> PendingNodes_;
};

} // NYql
