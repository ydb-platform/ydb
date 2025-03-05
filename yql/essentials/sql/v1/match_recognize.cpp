#include "match_recognize.h"
#include "source.h"
#include "context.h"

#include <util/generic/overloaded.h>

namespace NSQLTranslationV1 {

namespace {

constexpr auto VarDataName = "data";
constexpr auto VarMatchedVarsName = "vars";
constexpr auto VarLastRowIndexName = "lri";

class TMatchRecognizeColumnAccessNode final : public TAstListNode {
public:
    TMatchRecognizeColumnAccessNode(TPosition pos, TString var, TString column)
    : TAstListNode(pos)
    , Var(std::move(var))
    , Column(std::move(column)) {
    }

    const TString* GetColumnName() const override {
        return std::addressof(Column);
    }

    bool DoInit(TContext& ctx, ISource* /* src */) override {
        switch (ctx.GetColumnReferenceState()) {
        case EColumnRefState::MatchRecognizeMeasures:
            if (!ctx.SetMatchRecognizeAggrVar(Var)) {
                return false;
            }
            Add(
                "Member",
                BuildAtom(Pos, "row"),
                Q(Column)
            );
            break;
        case EColumnRefState::MatchRecognizeDefine:
            if (ctx.GetMatchRecognizeDefineVar() != Var) {
                ctx.Error() << "Row pattern navigation function is required";
                return false;
            }
            BuildLookup(VarLastRowIndexName);
            break;
        case EColumnRefState::MatchRecognizeDefineAggregate:
            if (!ctx.SetMatchRecognizeAggrVar(Var)) {
                return false;
            }
            BuildLookup("index");
            break;
        default:
            ctx.Error(Pos) << "Unexpected column reference state";
            return false;
        }
        return true;
    }

    TNodePtr DoClone() const override {
        return MakeIntrusive<TMatchRecognizeColumnAccessNode>(Pos, Var, Column);
    }

private:
    void BuildLookup(TString varKeyName) {
        Add(
            "Member",
            Y(
                "Lookup",
                Y(
                    "ToIndexDict",
                    BuildAtom(Pos, VarDataName)
                ),
                BuildAtom(Pos, std::move(varKeyName))
            ),
            Q(Column)
        );
    }

private:
    TString Var;
    TString Column;
};

class TMatchRecognizeDefineAggregate final : public TAstListNode {
public:
    TMatchRecognizeDefineAggregate(TPosition pos, TString name, TVector<TNodePtr> args)
    : TAstListNode(pos)
    , Name(std::move(name))
    , Args(std::move(args)) {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (EColumnRefState::MatchRecognizeDefine != ctx.GetColumnReferenceState()) {
            ctx.Error(Pos) << "Unexpected column reference state";
            return false;
        }
        TColumnRefScope scope(ctx, EColumnRefState::MatchRecognizeDefineAggregate, false, ctx.GetMatchRecognizeDefineVar());
        if (Args.size() != 1) {
            ctx.Error() << "Exactly one argument is required in MATCH_RECOGNIZE navigation function";
            return false;
        }
        const auto arg = Args[0];
        if (!arg || !arg->Init(ctx, src)) {
            return false;
        }

        const auto body = [&]() -> TNodePtr {
            if ("first" == Name) {
                return Y("Member", Y("Head", "item"), Q("From"));
            } else if ("last" == Name) {
                return Y("Member", Y("Last", "item"), Q("To"));
            } else {
                ctx.Error() << "Unknown row pattern navigation function: " << Name;
                return {};
            }
        }();
        if (!body) {
            return false;
        }
        Add("Apply", BuildLambda(Pos, Y("index"), arg), body);
        return true;
    }

    TNodePtr DoClone() const override {
        return MakeIntrusive<TMatchRecognizeDefineAggregate>(Pos, Name, Args);
    }

private:
    TString Name;
    TVector<TNodePtr> Args;
};

class TMatchRecognizeVarAccessNode final : public INode {
public:
    TMatchRecognizeVarAccessNode(TPosition pos, TNodePtr aggr)
    : INode(pos)
    , Aggr(std::move(aggr)) {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!Aggr || !Aggr->Init(ctx, src)) {
            return false;
        }
        auto var = ctx.ExtractMatchRecognizeAggrVar();
        Expr = [&]() -> TNodePtr {
            switch (ctx.GetColumnReferenceState()) {
            case EColumnRefState::MatchRecognizeMeasures: {
                ctx.GetMatchRecognizeAggregations().emplace_back(std::move(var), Aggr->GetAggregation());
                return Aggr;
            }
            case EColumnRefState::MatchRecognizeDefine:
                return Y(
                    "Apply",
                    BuildLambda(Pos, Y("item"), Aggr),
                    Y(
                        "Member",
                        BuildAtom(ctx.Pos(), VarMatchedVarsName),
                        Q(std::move(var))
                    )
                );
            default:
                ctx.Error(Pos) << "Unexpected column reference state";
                return {};
            }
        }();
        return Expr && Expr->Init(ctx, src);
    }

    TNodePtr DoClone() const override {
        return MakeIntrusive<TMatchRecognizeVarAccessNode>(Pos, Aggr);
    }

    TAstNode* Translate(TContext& ctx) const override {
        return Expr->Translate(ctx);
    }

private:
    TNodePtr Aggr;
    TNodePtr Expr;
};

class TMatchRecognize final : public TAstListNode {
public:
    TMatchRecognize(
        TPosition pos,
        TString label,
        TNodePtr partitionKeySelector,
        TNodePtr partitionColumns,
        TVector<TSortSpecificationPtr> sortSpecs,
        TVector<TNamedFunction> measures,
        TNodePtr rowsPerMatch,
        TNodePtr skipTo,
        TNodePtr pattern,
        TNodePtr patternVars,
        TNodePtr subset,
        TVector<TNamedFunction> definitions)
    : TAstListNode(pos)
    , Label(std::move(label))
    , PartitionKeySelector(std::move(partitionKeySelector))
    , PartitionColumns(std::move(partitionColumns))
    , SortSpecs(std::move(sortSpecs))
    , Measures(std::move(measures))
    , RowsPerMatch(std::move(rowsPerMatch))
    , SkipTo(std::move(skipTo))
    , Pattern(std::move(pattern))
    , PatternVars(std::move(patternVars))
    , Subset(std::move(subset))
    , Definitions(std::move(definitions)) {
    }

private:
    bool DoInit(TContext& ctx, ISource* src) override {
        auto inputRowType = Y("ListItemType", Y("TypeOf", Label));

        if (!PartitionKeySelector || !PartitionKeySelector->Init(ctx, src)) {
            return false;
        }
        if (!PartitionColumns || !PartitionColumns->Init(ctx, src)) {
            return false;
        }

        const auto sortTraits = SortSpecs.empty() ? Y("Void") : src->BuildSortSpec(SortSpecs, Label, true, false);
        if (!sortTraits || !sortTraits->Init(ctx, src)) {
            return false;
        }

        auto measureNames = Y();
        auto measuresCallables = Y();
        for (auto& m: Measures) {
            TColumnRefScope scope(ctx, EColumnRefState::MatchRecognizeMeasures);
            if (!m.Callable || !m.Callable->Init(ctx, src)) {
                return false;
            }
            const auto pos = m.Callable->GetPos();
            measureNames = L(measureNames, BuildQuotedAtom(m.Callable->GetPos(), std::move(m.Name)));
            auto measuresVars = Y();
            auto measuresAggregates = Y();
            for (auto& [var, aggr] : ctx.GetMatchRecognizeAggregations()) {
                if (!aggr) {
                    return false;
                }
                auto [traits, result] = aggr->AggregationTraits(Y("TypeOf", Label), false, false, false, ctx);
                if (!result) {
                    return false;
                }
                measuresVars = L(measuresVars, BuildQuotedAtom(pos, std::move(var)));
                measuresAggregates = L(measuresAggregates, std::move(traits));
            }
            ctx.GetMatchRecognizeAggregations().clear();
            measuresCallables = L(
                measuresCallables,
                Y(
                    "MatchRecognizeMeasuresCallable",
                    BuildLambda(pos, Y("row"), std::move(m.Callable)),
                    Q(measuresVars),
                    Q(measuresAggregates)
                )
            );
        }
        auto measuresNode = Y("MatchRecognizeMeasuresCallables", inputRowType, Q(PatternVars), Q(measureNames), Q(measuresCallables));

        if (!RowsPerMatch || !RowsPerMatch->Init(ctx, src)) {
            return false;
        }

        if (!SkipTo || !SkipTo->Init(ctx, src)) {
            return false;
        }

        if (!Pattern || !Pattern->Init(ctx, src)) {
            return false;
        }

        if (!PatternVars || !PatternVars->Init(ctx, src)) {
            return false;
        }

        auto defineNames = Y();
        for (auto& d: Definitions) {
            defineNames = L(defineNames, BuildQuotedAtom(d.Callable->GetPos(), d.Name));
        }
        auto defineNode = Y("MatchRecognizeDefines", inputRowType, Q(PatternVars), Q(defineNames));
        for (auto& d: Definitions) {
            TColumnRefScope scope(ctx, EColumnRefState::MatchRecognizeDefine, true, d.Name);
            if (!d.Callable || !d.Callable->Init(ctx, src)) {
                return false;
            }
            const auto pos = d.Callable->GetPos();
            defineNode = L(defineNode, BuildLambda(pos, Y(VarDataName, VarMatchedVarsName, VarLastRowIndexName), std::move(d.Callable)));
        }

        Add(
            "block",
            Q(Y(
                Y("let", "input", Label),
                Y("let", "partitionKeySelector", PartitionKeySelector),
                Y("let", "partitionColumns", PartitionColumns),
                Y("let", "sortTraits", sortTraits),
                Y("let", "measures", measuresNode),
                Y("let", "rowsPerMatch", RowsPerMatch),
                Y("let", "skipTo", SkipTo),
                Y("let", "pattern", Pattern),
                Y("let", "subset", Subset ? Subset : Q("")),
                Y("let", "define", defineNode),
                Y("let", "res", Y("MatchRecognize",
                    "input",
                    "partitionKeySelector",
                    "partitionColumns",
                    "sortTraits",
                    Y("MatchRecognizeParams",
                        "measures",
                        "rowsPerMatch",
                        "skipTo",
                        "pattern",
                        "define"
                    )
                )),
                Y("return", "res")
            ))
        );
        return true;
    }

    TNodePtr DoClone() const override {
        return MakeIntrusive<TMatchRecognize>(
            Pos,
            Label,
            PartitionKeySelector,
            PartitionColumns,
            SortSpecs,
            Measures,
            RowsPerMatch,
            SkipTo,
            Pattern,
            PatternVars,
            Subset,
            Definitions
        );
    }

private:
    TString Label;
    TNodePtr PartitionKeySelector;
    TNodePtr PartitionColumns;
    TVector<TSortSpecificationPtr> SortSpecs;
    TVector<TNamedFunction> Measures;
    TNodePtr RowsPerMatch;
    TNodePtr SkipTo;
    TNodePtr Pattern;
    TNodePtr PatternVars;
    TNodePtr Subset;
    TVector<TNamedFunction> Definitions;
};

} // anonymous namespace

TNodePtr TMatchRecognizeBuilder::Build(TContext& ctx, TString label, ISource* src) {
    const auto node = MakeIntrusive<TMatchRecognize>(
        Pos,
        std::move(label),
        std::move(PartitionKeySelector),
        std::move(PartitionColumns),
        std::move(SortSpecs),
        std::move(Measures),
        std::move(RowsPerMatch),
        std::move(SkipTo),
        std::move(Pattern),
        std::move(PatternVars),
        std::move(Subset),
        std::move(Definitions)
    );
    if (!node->Init(ctx, src)) {
        return {};
    }
    return node;
}

TNodePtr BuildMatchRecognizeColumnAccess(TPosition pos, TString var, TString column) {
    return MakeIntrusive<TMatchRecognizeColumnAccessNode>(pos, std::move(var), std::move(column));
}

TNodePtr BuildMatchRecognizeDefineAggregate(TPosition pos, TString name, TVector<TNodePtr> args) {
    const auto result = MakeIntrusive<TMatchRecognizeDefineAggregate>(pos, std::move(name), std::move(args));
    return BuildMatchRecognizeVarAccess(pos, std::move(result));
}

TNodePtr BuildMatchRecognizeVarAccess(TPosition pos, TNodePtr aggr) {
    return MakeIntrusive<TMatchRecognizeVarAccessNode>(pos, std::move(aggr));
}

} // namespace NSQLTranslationV1
