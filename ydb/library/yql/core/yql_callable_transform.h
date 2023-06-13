#pragma once

#include "yql_graph_transformer.h"
#include "yql_type_annotation.h"
#include "yql_expr_type_annotation.h"
#include "yql_callable_names.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/public/issue/yql_issue_manager.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/ptr.h>
#include <util/string/builder.h>

#include <utility>

namespace NYql {

template <class TDerived>
class TCallableTransformerBase : public TGraphTransformerBase {
public:
    TCallableTransformerBase(TTypeAnnotationContext& types, bool instantOnly)
        : Types(types)
        , InstantOnly(instantOnly)
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
                auto datasink = ParseCommit(*input, ctx);
                if (!datasink) {
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
            } else if (name == ConfigureName) {
                auto provider = ParseConfigure(*input, ctx);
                if (!provider) {
                    status = TStatus::Error;
                } else {
                    status = ProcessDataProviderAnnotation(*provider, input, output, ctx);
                    if (status == TStatus::Ok) {
                        status = static_cast<TDerived*>(this)->ValidateProviderConfigureResult(input, ctx);
                    }
                }
            } else {
                bool foundFunc = false;
                for (auto& datasource : Types.DataSources) {
                    if (!datasource->CanParse(*input)) {
                        continue;
                    }

                    foundFunc = true;
                    status = ProcessDataProviderAnnotation(*datasource, input, output, ctx);
                    break;
                }

                if (!foundFunc) {
                    for (auto& datasink : Types.DataSinks) {
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
        const auto it = PendingNodes.find(&input);
        YQL_ENSURE(it != PendingNodes.cend());
        return static_cast<TDerived*>(this)->GetTransformer(*it->second.second).GetAsyncFuture(input);
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        const auto it = PendingNodes.find(input.Get());
        YQL_ENSURE(it != PendingNodes.cend());
        const auto provider = it->second.second;
        IGraphTransformer& transformer = static_cast<TDerived*>(this)->GetTransformer(*provider);
        const auto status = transformer.ApplyAsyncChanges(it->second.first, output, ctx);
        PendingNodes.erase(it);
        return status;
    }

    void Rewind() override {
        PendingNodes.clear();
    }

protected:
    IDataProvider* ParseCommit(const TExprNode& input, TExprContext& ctx) {
        if (!EnsureMinArgsCount(input, 2, ctx)) {
            return nullptr;
        }

        if (!EnsureMaxArgsCount(input, 3, ctx)) {
            return nullptr;
        }

        if (!EnsureWorldType(*input.Child(0), ctx)) {
            return nullptr;
        }

        if (!EnsureDataSink(*input.Child(1), ctx)) {
            return nullptr;
        }

        if (input.ChildrenSize() == 3) {
            for (auto& setting : input.Child(2)->Children()) {
                if (!EnsureTupleSize(*setting, 2, ctx)) {
                    return nullptr;
                }

                auto nameNode = setting->Child(0);
                if (!EnsureAtom(*nameNode, ctx)) {
                    return nullptr;
                }
            }
        }

        auto datasinkName = input.Child(1)->Child(0)->Content();
        auto datasink = Types.DataSinkMap.FindPtr(datasinkName);
        if (!datasink) {
            ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder() << "Unsupported datasink: " << datasinkName));
            return nullptr;
        }

        return (*datasink).Get();
    }

    IDataProvider* ParseRead(const TExprNode& input, TExprContext& ctx) {
        if (!EnsureMinArgsCount(input, 2, ctx)) {
            return nullptr;
        }

        if (!EnsureWorldType(*input.Child(0), ctx)) {
            return nullptr;
        }

        if (!EnsureDataSource(*input.Child(1), ctx)) {
            return nullptr;
        }

        auto datasourceName = input.Child(1)->Child(0)->Content();
        auto datasource = Types.DataSourceMap.FindPtr(datasourceName);
        if (!datasource) {
            ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder() << "Unsupported datasource: " << datasourceName));
            return nullptr;
        }

        return (*datasource).Get();
    }

    IDataProvider* ParseWrite(const TExprNode& input, TExprContext& ctx) {
        if (!EnsureMinArgsCount(input, 2, ctx)) {
            return nullptr;
        }

        if (!EnsureWorldType(*input.Child(0), ctx)) {
            return nullptr;
        }

        if (!EnsureDataSink(*input.Child(1), ctx)) {
            return nullptr;
        }

        auto datasinkName = input.Child(1)->Child(0)->Content();
        auto datasink = Types.DataSinkMap.FindPtr(datasinkName);
        if (!datasink) {
            ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder() << "Unsupported datasink: " << datasinkName));
            return nullptr;
        }

        return (*datasink).Get();
    }

    IDataProvider* ParseConfigure(const TExprNode& input, TExprContext& ctx) {
        if (!EnsureMinArgsCount(input, 2, ctx)) {
            return nullptr;
        }

        if (!EnsureWorldType(*input.Child(0), ctx)) {
            return nullptr;
        }

        if (!EnsureDataProvider(*input.Child(1), ctx)) {
            return nullptr;
        }

        if (input.Child(1)->IsCallable("DataSource")) {
            auto datasourceName = input.Child(1)->Child(0)->Content();
            auto datasource = Types.DataSourceMap.FindPtr(datasourceName);
            if (!datasource) {
                ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder() << "Unsupported datasource: " << datasourceName));
                return nullptr;
            }

            return (*datasource).Get();
        }

        if (input.Child(1)->IsCallable("DataSink")) {
            auto datasinkName = input.Child(1)->Child(0)->Content();
            auto datasink = Types.DataSinkMap.FindPtr(datasinkName);
            if (!datasink) {
                ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder() << "Unsupported datasink: " << datasinkName));
                return nullptr;
            }

            return (*datasink).Get();
        }

        YQL_ENSURE(false, "Unexpected provider class");
    }

    IGraphTransformer::TStatus ProcessDataProviderAnnotation(IDataProvider& dataProvider,
        const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        auto status = static_cast<TDerived*>(this)->GetTransformer(dataProvider).Transform(input, output, ctx);
        if (status.Level == IGraphTransformer::TStatus::Async) {
            if (InstantOnly) {
                ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() <<
                    "Async status is not allowed for instant transform, provider name: " << dataProvider.GetName()));
                return IGraphTransformer::TStatus::Error;
            }

            PendingNodes[input.Get()] = std::make_pair(input, &dataProvider);
        }

        return status;
    }

protected:
    TTypeAnnotationContext& Types;
    const bool InstantOnly;
    TNodeMap<std::pair<TExprNode::TPtr, IDataProvider*>> PendingNodes;
};

} // NYql
