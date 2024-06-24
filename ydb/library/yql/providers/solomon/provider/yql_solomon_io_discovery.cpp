#include "yql_solomon_provider_impl.h"

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/solomon/expr_nodes/yql_solomon_expr_nodes.h>
#include <ydb/library/yql/providers/solomon/scheme/yql_solomon_scheme.h>

#include <util/string/split.h>
#include <util/string/strip.h>

namespace NYql {

namespace {
using namespace NNodes;

std::array<TExprNode::TPtr, 2U> ExtractSchema(TExprNode::TListType& settings) {
    for (auto it = settings.cbegin(); settings.cend() != it; ++it) {
        if (const auto item = *it; item->Head().IsAtom("userschema")) {
            settings.erase(it);
            return {item->ChildPtr(1), item->ChildrenSize() > 2 ? item->TailPtr() : TExprNode::TPtr()};
        }
    }

    return {};
}

TVector<TCoAtom> ExtractUserLabels(TPositionHandle pos, TExprContext& ctx, TExprNode::TListType& settings) {
    for (auto it = settings.cbegin(); settings.cend() != it; ++it) {
        if (const auto item = *it; item->Head().IsAtom("labels")) {
            TVector<TCoAtom> result;
            auto labels = StringSplitter(TString(item->Tail().Content())).Split(',').SkipEmpty().ToList<TString>();
            result.reserve(labels.size());
            for (TString& label : labels) {
                auto v = Build<TCoAtom>(ctx, pos).Value(StripString(label)).Done();
                result.emplace_back(std::move(v));
            }
            settings.erase(it);
            return result;
        }
    }

    return {};
}

const TStructExprType* BuildScheme(TPositionHandle pos, const TVector<TCoAtom>& userLabels, TExprContext& ctx, TVector<TCoAtom>& systemColumns) {
    auto allSystemColumns = {SOLOMON_SCHEME_KIND, SOLOMON_SCHEME_LABELS, SOLOMON_SCHEME_VALUE, SOLOMON_SCHEME_TS, SOLOMON_SCHEME_TYPE};
    TVector<const TItemExprType*> columnTypes;
    columnTypes.reserve(systemColumns.size() + userLabels.size());
    const TTypeAnnotationNode* stringType = ctx.MakeType<TDataExprType>(EDataSlot::String);
    for (auto systemColumn : allSystemColumns) {
        const TTypeAnnotationNode* type = nullptr;
        if (systemColumn == SOLOMON_SCHEME_LABELS && !userLabels.empty()) {
            continue;
        }

        if (systemColumn == SOLOMON_SCHEME_TS) {
            type = ctx.MakeType<TDataExprType>(EDataSlot::Datetime);
        } else if (systemColumn == SOLOMON_SCHEME_VALUE) {
            type = ctx.MakeType<TDataExprType>(EDataSlot::Double);
        } else if (systemColumn == SOLOMON_SCHEME_LABELS) {
            type = ctx.MakeType<NYql::TDictExprType>(stringType, stringType);
        } else if (IsIn({ SOLOMON_SCHEME_KIND, SOLOMON_SCHEME_TYPE }, systemColumn)) {
            type = stringType;
        } else {
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Unknown system column " << systemColumn));
            return nullptr;
        }

        systemColumns.push_back(Build<TCoAtom>(ctx, pos).Value(systemColumn).Done());
        columnTypes.push_back(ctx.MakeType<TItemExprType>(systemColumn, type));
    }

    for (const auto& label : userLabels) {
        if (IsIn(allSystemColumns, label.Value())) {
            // tmp constraint
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "System column should not be used as label name: " << label.Value()));
            return nullptr;
        }
        columnTypes.push_back(ctx.MakeType<TItemExprType>(label.Value(), stringType));
    }

    return ctx.MakeType<TStructExprType>(columnTypes);
}

}

class TSolomonIODiscoveryTransformer : public TSyncTransformerBase {
public:
    TSolomonIODiscoveryTransformer(TSolomonState::TPtr state)
        : State_(state)
    {
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        output = input;

        if (ctx.Step.IsDone(ctx.Step.DiscoveryIO)) {
            return TStatus::Ok;
        }

        auto status = OptimizeExpr(input, output, [this] (const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
            if (auto maybeRead = TMaybeNode<TSoRead>(node)) {
                auto read = maybeRead.Cast();
                if (read.DataSource().Category().Value() != SolomonProviderName) {
                    return node;
                }

                auto& object = read.Arg(2).Ref();
                if (!object.IsCallable("MrTableConcat")) {
                    ctx.AddError(TIssue(ctx.GetPosition(object.Pos()), TStringBuilder() << "Expected MrTableConcat"));
                    return {};
                }

                const auto maybeKey = TMaybeNode<TCoKey>(&object.Head());
                if (!maybeKey) {
                    ctx.AddError(TIssue(ctx.GetPosition(object.Pos()), TStringBuilder() << "Expected Key"));
                    return {};
                }

                const auto& keyArg = maybeKey.Cast().Ref().Head();
                if (!keyArg.IsList() || keyArg.ChildrenSize() != 2U
                    || !keyArg.Head().IsAtom("table") || !keyArg.Tail().IsCallable(TCoString::CallableName())) {
                    ctx.AddError(TIssue(ctx.GetPosition(keyArg.Pos()), TStringBuilder() << "Expected single table name"));
                    return {};
                }

                const auto project = TString(keyArg.Tail().Head().Content());
                auto cluster = read.DataSource().Cluster().StringValue();
                if (!this->State_->Configuration->_EnableReading.Get().GetOrElse(false)) {
                    ctx.AddError(TIssue(ctx.GetPosition(object.Pos()), TStringBuilder() << "Reading is disabled for monitoring cluster " << cluster));
                    return {};
                }

                auto settings = read.Ref().Child(4);
                auto settingsList = read.Ref().Child(4)->ChildrenList();
                auto userSchema = ExtractSchema(settingsList);
                TVector<TCoAtom> userLabels = ExtractUserLabels(settings->Pos(), ctx, settingsList);

                auto newSettings = Build<TCoNameValueTupleList>(ctx, settings->Pos())
                                        .Add(settingsList)
                                    .Done();

                auto soObject = Build<TSoObject>(ctx, read.Pos())
                                  .Project<TCoAtom>().Build(project)
                                  .Settings(newSettings)
                                .Done();
                
                TVector<TCoAtom> systemColumns;
                auto* scheme = BuildScheme(settings->Pos(), userLabels, ctx, systemColumns);
                if (!scheme) {
                    return {};
                }

                auto rowTypeNode = ExpandType(read.Pos(), *scheme, ctx);
                auto systemColumnsNode = Build<TCoAtomList>(ctx, read.Pos())
                                            .Add(systemColumns)
                                        .Done();

                auto labelNamesNode = Build<TCoAtomList>(ctx, read.Pos())
                                            .Add(userLabels)
                                        .Done();

                return userSchema.back()
                    ? Build<TSoReadObject>(ctx, read.Pos())
                        .World(read.World())
                        .DataSource(read.DataSource())
                        .Object(soObject)
                        .SystemColumns(systemColumnsNode)
                        .LabelNames(labelNamesNode)
                        .RowType(rowTypeNode)
                        .ColumnOrder(std::move(userSchema.back()))
                      .Done().Ptr()
                    : Build<TSoReadObject>(ctx, read.Pos())
                        .World(read.World())
                        .DataSource(read.DataSource())
                        .Object(soObject)
                        .SystemColumns(systemColumnsNode)
                        .LabelNames(labelNamesNode)
                        .RowType(rowTypeNode)
                      .Done().Ptr();
            }

            return node;
        }, ctx, TOptimizeExprSettings {nullptr});

        return status;
    }

    void Rewind() final {
    }

private:
    TSolomonState::TPtr State_;
};

THolder<IGraphTransformer> CreateSolomonIODiscoveryTransformer(TSolomonState::TPtr state) {
    return THolder(new TSolomonIODiscoveryTransformer(state));
}

} // namespace NYql
