#include "replace_table_reads.h"

#include <ydb/library/yql/public/purecalc/common/names.h>

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>

using namespace NYql;
using namespace NYql::NPureCalc;

namespace {
    class TTableReadsReplacer: public TSyncTransformerBase {
    private:
        ui32 InputsNumber_;
        bool UseSystemColumns_;
        TString TablePrefix_;
        TString CallableName_;
        bool Complete_ = false;

    public:
        explicit TTableReadsReplacer(
            ui32 inputsNumber,
            bool useSystemColumns,
            TString tablePrefix,
            TString inputNodeName
        )
            : InputsNumber_(inputsNumber)
            , UseSystemColumns_(useSystemColumns)
            , TablePrefix_(std::move(tablePrefix))
            , CallableName_(std::move(inputNodeName))
        {
        }

        TTableReadsReplacer(TVector<const TStructExprType*>&&, TString, TString) = delete;

    public:
        TStatus DoTransform(const TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
            output = input;
            if (Complete_) {
                return TStatus::Ok;
            }

            TOptimizeExprSettings settings(nullptr);

            auto status = OptimizeExpr(input, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
                if (node->IsCallable(NNodes::TCoRight::CallableName())) {
                    TIssueScopeGuard issueScope(ctx.IssueManager, [&]() {
                        return new TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "At function: " << node->Content());
                    });

                    if (!EnsureMinArgsCount(*node, 1, ctx)) {
                        return nullptr;
                    }

                    if (!node->Child(0)->IsCallable(NNodes::TCoRead::CallableName())) {
                        ctx.AddError(TIssue(ctx.GetPosition(node->Child(0)->Pos()), TStringBuilder() << "Expected Read!"));
                        return nullptr;
                    }

                    return BuildInputFromRead(node->Pos(), node->ChildPtr(0), ctx);
                }

                return node;
            }, ctx, settings);

            if (status.Level == TStatus::Ok) {
                Complete_ = true;
            }
            return status;
        }

        void Rewind() override {
            Complete_ = false;
        }

    private:
        TExprNode::TPtr BuildInputFromRead(TPositionHandle replacePos, const TExprNode::TPtr& node, TExprContext& ctx) {
            TIssueScopeGuard issueScope(ctx.IssueManager, [&]() {
                return MakeIntrusive<TIssue>(ctx.GetPosition(node->Pos()), TStringBuilder() << "At function: " << node->Content());
            });

            if (!EnsureMinArgsCount(*node, 3, ctx)) {
                return nullptr;
            }

            const auto source = node->ChildPtr(2);
            if (source->IsCallable(NNodes::TCoKey::CallableName())) {
                return BuildInputFromKey(replacePos, source, ctx);
            }
            if (source->IsCallable("DataTables")) {
                return BuildInputFromDataTables(replacePos, source, ctx);
            }

            ctx.AddError(TIssue(ctx.GetPosition(source->Pos()), TStringBuilder() << "Unsupported read source: " << source->Content()));

            return nullptr;
        }

        TExprNode::TPtr BuildInputFromKey(TPositionHandle replacePos, const TExprNode::TPtr& node, TExprContext& ctx) {
            TIssueScopeGuard issueScope(ctx.IssueManager, [&]() {
                return MakeIntrusive<TIssue>(ctx.GetPosition(node->Pos()), TStringBuilder() << "At function: " << node->Content());
            });

            ui32 inputIndex;
            TExprNode::TPtr inputTableName;

            if (!TryFetchInputIndexFromKey(node, ctx, inputIndex, inputTableName)) {
                return nullptr;
            }

            YQL_ENSURE(inputTableName->IsCallable(NNodes::TCoString::CallableName()));

            auto inputNode = ctx.Builder(replacePos)
                .Callable(CallableName_)
                    .Atom(0, ToString(inputIndex))
                .Seal()
                .Build();

            if (UseSystemColumns_) {
                auto mapLambda = ctx.Builder(replacePos)
                    .Lambda()
                        .Param("row")
                        .Callable(0, NNodes::TCoAddMember::CallableName())
                            .Arg(0, "row")
                            .Atom(1, PurecalcSysColumnTablePath)
                            .Add(2, inputTableName)
                        .Seal()
                    .Seal()
                    .Build();

                return ctx.Builder(replacePos)
                    .Callable(NNodes::TCoMap::CallableName())
                        .Add(0, std::move(inputNode))
                        .Add(1, std::move(mapLambda))
                    .Seal()
                    .Build();
            }

            return inputNode;
        }

        TExprNode::TPtr BuildInputFromDataTables(TPositionHandle replacePos, const TExprNode::TPtr& node, TExprContext& ctx) {
            TIssueScopeGuard issueScope(ctx.IssueManager, [&]() {
                return MakeIntrusive<TIssue>(ctx.GetPosition(node->Pos()), TStringBuilder() << "At function: " << node->Content());
            });

            if (!InputsNumber_) {
                ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "No inputs provided by input spec"));
                return nullptr;
            }

            if (!EnsureArgsCount(*node, 0, ctx)) {
                return nullptr;
            }

            auto builder = ctx.Builder(replacePos);

            if (InputsNumber_ > 1) {
                auto listBuilder = builder.List();

                for (ui32 i = 0; i < InputsNumber_; ++i) {
                    listBuilder.Callable(i, CallableName_).Atom(0, ToString(i)).Seal();
                }

                return listBuilder.Seal().Build();
            }

            return builder.Callable(CallableName_).Atom(0, "0").Seal().Build();
        }

        bool TryFetchInputIndexFromKey(const TExprNode::TPtr& node, TExprContext& ctx, ui32& resultIndex, TExprNode::TPtr& resultTableName) {
            if (!EnsureArgsCount(*node, 1, ctx)) {
                return false;
            }

            const auto* keyArg = node->Child(0);
            if (!keyArg->IsList() || keyArg->ChildrenSize() != 2 || !keyArg->Child(0)->IsAtom("table") ||
                !keyArg->Child(1)->IsCallable(NNodes::TCoString::CallableName()))
            {
                ctx.AddError(TIssue(ctx.GetPosition(keyArg->Pos()), "Expected single table name"));
                return false;
            }

            resultTableName = keyArg->ChildPtr(1);

            auto tableName = resultTableName->Child(0)->Content();

            if (!tableName.StartsWith(TablePrefix_)) {
                ctx.AddError(TIssue(ctx.GetPosition(resultTableName->Child(0)->Pos()),
                    TStringBuilder() << "Invalid table name " << TString{tableName}.Quote() << ": prefix must be " << TablePrefix_.Quote()));
                return false;
            }

            tableName.SkipPrefix(TablePrefix_);

            if (!tableName) {
                resultIndex = 0;
            } else if (!TryFromString(tableName, resultIndex)) {
                ctx.AddError(TIssue(ctx.GetPosition(resultTableName->Child(0)->Pos()),
                    TStringBuilder() << "Invalid table name " << TString{tableName}.Quote() << ": suffix must be UI32 number"));
                return false;
            }

            return true;
        }
    };
}

TAutoPtr<IGraphTransformer> NYql::NPureCalc::MakeTableReadsReplacer(
    ui32 inputsNumber,
    bool useSystemColumns,
    TString tablePrefix,
    TString callableName
) {
    return new TTableReadsReplacer(inputsNumber, useSystemColumns, std::move(tablePrefix), std::move(callableName));
}
