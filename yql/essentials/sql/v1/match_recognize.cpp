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
            Y_ABORT("Unexpected column reference state");
        }
        return true;
    }

    TNodePtr DoClone() const override {
        return new TMatchRecognizeColumnAccessNode(Pos, Var, Column);
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
        Y_DEBUG_ABORT_UNLESS(ctx.GetColumnReferenceState() == EColumnRefState::MatchRecognizeDefine);
        TColumnRefScope scope(ctx, EColumnRefState::MatchRecognizeDefineAggregate, false, ctx.GetMatchRecognizeDefineVar());
        if (Args.size() != 1) {
            ctx.Error() << "Exactly one argument is required in MATCH_RECOGNIZE navigation function";
            return false;
        }
        const auto arg = Args[0];
        if (!arg->Init(ctx, src)) {
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
        return new TMatchRecognizeDefineAggregate(Pos, Name, Args);
    }

private:
    TString Name;
    TVector<TNodePtr> Args;
};

class TMatchRecognizeVarAccessNode final : public INode {
public:
    TMatchRecognizeVarAccessNode(TPosition pos, TNodePtr arg)
    : INode(pos)
    , Arg(std::move(arg)) {
    }

    [[nodiscard]] const TString& GetVar() const noexcept {
        return Var;
    }

    TAggregationPtr GetAggregation() const override {
        return Arg->GetAggregation();
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!Arg->Init(ctx, src)) {
            return false;
        }
        Var = ctx.ExtractMatchRecognizeAggrVar();
        Expr = [&]() -> TNodePtr {
            switch (ctx.GetColumnReferenceState()) {
            case EColumnRefState::MatchRecognizeMeasures:
                return Arg;
            case EColumnRefState::MatchRecognizeDefine:
                return Y(
                    "Apply",
                    BuildLambda(Pos, Y("item"), Arg),
                    Y(
                        "Member",
                        BuildAtom(ctx.Pos(), VarMatchedVarsName),
                        Q(Var)
                    )
                );
            default:
                Y_ABORT("Unexpected column reference state");
            }
        }();
        return Expr->Init(ctx, src);
    }

    TNodePtr DoClone() const override {
        return new TMatchRecognizeVarAccessNode(Pos, Arg);
    }

    TAstNode* Translate(TContext& ctx) const override {
        return Expr->Translate(ctx);
    }

private:
    TString Var;
    TNodePtr Arg;
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

        if (!PartitionKeySelector->Init(ctx, src)) {
            return false;
        }
        if (!PartitionColumns->Init(ctx, src)) {
            return false;
        }

        const auto sortTraits = SortSpecs.empty() ? Y("Void") : src->BuildSortSpec(SortSpecs, Label, true, false);
        if (!sortTraits->Init(ctx, src)) {
            return false;
        }

        auto measureNames = Y();
        for (const auto& m: Measures) {
            measureNames->Add(BuildQuotedAtom(m.Callable->GetPos(), m.Name));
        }
        auto measureVars = Y();
        for (const auto& m: Measures) {
            TColumnRefScope scope(ctx, EColumnRefState::MatchRecognizeMeasures);
            if (!m.Callable->Init(ctx, src)) {
                return false;
            }
            const auto varAccess = dynamic_cast<TMatchRecognizeVarAccessNode*>(m.Callable.Get());
            auto var = varAccess ? varAccess->GetVar() : "";
            measureVars->Add(BuildQuotedAtom(m.Callable->GetPos(), std::move(var)));
        }
        auto measuresNode = Y("MatchRecognizeMeasuresAggregates", inputRowType, Q(PatternVars), Q(measureNames), Q(measureVars));
        for (const auto& m: Measures) {
            auto aggr = m.Callable->GetAggregation();
            if (!aggr) {
                // TODO(YQL-16508): support aggregations inside expressions
                // ctx.Error(m.Callable->GetPos()) << "Cannot use aggregations inside expression";
                // return false;
                measuresNode->Add(m.Callable);
            } else {
                const auto [traits, result] = aggr->AggregationTraits(Y("TypeOf", Label), false, false, false, ctx);
                if (!result) {
                    return false;
                }
                measuresNode->Add(traits);
            }
        }

        if (!RowsPerMatch->Init(ctx, src)) {
            return false;
        }

        if (!SkipTo->Init(ctx, src)) {
            return false;
        }

        if (!Pattern->Init(ctx, src)) {
            return false;
        }

        if (!PatternVars->Init(ctx, src)) {
            return false;
        }

        auto defineNames = Y();
        for (const auto& d: Definitions) {
            defineNames->Add(BuildQuotedAtom(d.Callable->GetPos(), d.Name));
        }
        auto defineNode = Y("MatchRecognizeDefines", inputRowType, Q(PatternVars), Q(defineNames));
        for (const auto& d: Definitions) {
            TColumnRefScope scope(ctx, EColumnRefState::MatchRecognizeDefine, true, d.Name);
            if (!d.Callable->Init(ctx, src)) {
                return false;
            }
            defineNode->Add(BuildLambda(d.Callable->GetPos(), Y(VarDataName, VarMatchedVarsName, VarLastRowIndexName), d.Callable));
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
        return new TMatchRecognize(
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
    TNodePtr node = new TMatchRecognize(
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

TNodePtr BuildMatchRecognizeVarAccess(TPosition pos, TNodePtr extractor) {
    return MakeIntrusive<TMatchRecognizeVarAccessNode>(pos, std::move(extractor));
}

} // namespace NSQLTranslationV1
