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
    , Var_(std::move(var))
    , Column_(std::move(column)) {
    }

    const TString* GetColumnName() const override {
        return std::addressof(Column_);
    }

    bool DoInit(TContext& ctx, ISource* /* src */) override {
        switch (ctx.GetColumnReferenceState()) {
        case EColumnRefState::MatchRecognizeMeasures:
            if (!ctx.SetMatchRecognizeAggrVar(Var_)) {
                return false;
            }
            Add(
                "Member",
                BuildAtom(Pos_, "row"),
                Q(Column_)
            );
            break;
        case EColumnRefState::MatchRecognizeDefine:
            if (ctx.GetMatchRecognizeDefineVar() != Var_) {
                ctx.Error() << "Row pattern navigation function is required";
                return false;
            }
            BuildLookup(VarLastRowIndexName);
            break;
        case EColumnRefState::MatchRecognizeDefineAggregate:
            if (!ctx.SetMatchRecognizeAggrVar(Var_)) {
                return false;
            }
            BuildLookup("index");
            break;
        default:
            ctx.Error(Pos_) << "Unexpected column reference state";
            return false;
        }
        return true;
    }

    TNodePtr DoClone() const override {
        return MakeIntrusive<TMatchRecognizeColumnAccessNode>(Pos_, Var_, Column_);
    }

private:
    void BuildLookup(TString varKeyName) {
        Add(
            "Member",
            Y(
                "Lookup",
                Y(
                    "ToIndexDict",
                    BuildAtom(Pos_, VarDataName)
                ),
                BuildAtom(Pos_, std::move(varKeyName))
            ),
            Q(Column_)
        );
    }

private:
    TString Var_;
    TString Column_;
};

class TMatchRecognizeDefineAggregate final : public TAstListNode {
public:
    TMatchRecognizeDefineAggregate(TPosition pos, TString name, TVector<TNodePtr> args)
    : TAstListNode(pos)
    , Name_(std::move(name))
    , Args_(std::move(args)) {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (EColumnRefState::MatchRecognizeDefine != ctx.GetColumnReferenceState()) {
            ctx.Error(Pos_) << "Unexpected column reference state";
            return false;
        }
        TColumnRefScope scope(ctx, EColumnRefState::MatchRecognizeDefineAggregate, false, ctx.GetMatchRecognizeDefineVar());
        if (Args_.size() != 1) {
            ctx.Error() << "Exactly one argument is required in MATCH_RECOGNIZE navigation function";
            return false;
        }
        const auto arg = Args_[0];
        if (!arg || !arg->Init(ctx, src)) {
            return false;
        }

        const auto body = [&]() -> TNodePtr {
            if ("first" == Name_) {
                return Y("Member", Y("Head", "item"), Q("From"));
            } else if ("last" == Name_) {
                return Y("Member", Y("Last", "item"), Q("To"));
            } else {
                ctx.Error() << "Unknown row pattern navigation function: " << Name_;
                return {};
            }
        }();
        if (!body) {
            return false;
        }
        Add("Apply", BuildLambda(Pos_, Y("index"), arg), body);
        return true;
    }

    TNodePtr DoClone() const override {
        return MakeIntrusive<TMatchRecognizeDefineAggregate>(Pos_, Name_, Args_);
    }

private:
    TString Name_;
    TVector<TNodePtr> Args_;
};

class TMatchRecognizeVarAccessNode final : public INode {
public:
    TMatchRecognizeVarAccessNode(TPosition pos, TNodePtr aggr)
    : INode(pos)
    , Aggr_(std::move(aggr)) {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!Aggr_ || !Aggr_->Init(ctx, src)) {
            return false;
        }
        auto var = ctx.ExtractMatchRecognizeAggrVar();
        Expr_ = [&]() -> TNodePtr {
            switch (ctx.GetColumnReferenceState()) {
            case EColumnRefState::MatchRecognizeMeasures: {
                ctx.GetMatchRecognizeAggregations().emplace_back(std::move(var), Aggr_->GetAggregation());
                return Aggr_;
            }
            case EColumnRefState::MatchRecognizeDefine:
                return Y(
                    "Apply",
                    BuildLambda(Pos_, Y("item"), Aggr_),
                    Y(
                        "Member",
                        BuildAtom(ctx.Pos(), VarMatchedVarsName),
                        Q(std::move(var))
                    )
                );
            default:
                ctx.Error(Pos_) << "Unexpected column reference state";
                return {};
            }
        }();
        return Expr_ && Expr_->Init(ctx, src);
    }

    TNodePtr DoClone() const override {
        return MakeIntrusive<TMatchRecognizeVarAccessNode>(Pos_, Aggr_);
    }

    TAstNode* Translate(TContext& ctx) const override {
        return Expr_->Translate(ctx);
    }

private:
    TNodePtr Aggr_;
    TNodePtr Expr_;
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
    , Label_(std::move(label))
    , PartitionKeySelector_(std::move(partitionKeySelector))
    , PartitionColumns_(std::move(partitionColumns))
    , SortSpecs_(std::move(sortSpecs))
    , Measures_(std::move(measures))
    , RowsPerMatch_(std::move(rowsPerMatch))
    , SkipTo_(std::move(skipTo))
    , Pattern_(std::move(pattern))
    , PatternVars_(std::move(patternVars))
    , Subset_(std::move(subset))
    , Definitions_(std::move(definitions)) {
    }

private:
    bool DoInit(TContext& ctx, ISource* src) override {
        auto inputRowType = Y("ListItemType", Y("TypeOf", Label_));

        if (!PartitionKeySelector_ || !PartitionKeySelector_->Init(ctx, src)) {
            return false;
        }
        if (!PartitionColumns_ || !PartitionColumns_->Init(ctx, src)) {
            return false;
        }

        const auto sortTraits = SortSpecs_.empty() ? Y("Void") : src->BuildSortSpec(SortSpecs_, Label_, true, false);
        if (!sortTraits || !sortTraits->Init(ctx, src)) {
            return false;
        }

        auto measureNames = Y();
        auto measuresCallables = Y();
        for (auto& m: Measures_) {
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
                auto [traits, result] = aggr->AggregationTraits(Y("TypeOf", Label_), false, false, false, ctx);
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
        auto measuresNode = Y("MatchRecognizeMeasuresCallables", inputRowType, Q(PatternVars_), Q(measureNames), Q(measuresCallables));

        if (!RowsPerMatch_ || !RowsPerMatch_->Init(ctx, src)) {
            return false;
        }

        if (!SkipTo_ || !SkipTo_->Init(ctx, src)) {
            return false;
        }

        if (!Pattern_ || !Pattern_->Init(ctx, src)) {
            return false;
        }

        if (!PatternVars_ || !PatternVars_->Init(ctx, src)) {
            return false;
        }

        auto defineNames = Y();
        for (auto& d: Definitions_) {
            defineNames = L(defineNames, BuildQuotedAtom(d.Callable->GetPos(), d.Name));
        }
        auto defineNode = Y("MatchRecognizeDefines", inputRowType, Q(PatternVars_), Q(defineNames));
        for (auto& d: Definitions_) {
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
                Y("let", "input", Label_),
                Y("let", "partitionKeySelector", PartitionKeySelector_),
                Y("let", "partitionColumns", PartitionColumns_),
                Y("let", "sortTraits", sortTraits),
                Y("let", "measures", measuresNode),
                Y("let", "rowsPerMatch", RowsPerMatch_),
                Y("let", "skipTo", SkipTo_),
                Y("let", "pattern", Pattern_),
                Y("let", "subset", Subset_ ? Subset_ : Q("")),
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
            Pos_,
            Label_,
            PartitionKeySelector_,
            PartitionColumns_,
            SortSpecs_,
            Measures_,
            RowsPerMatch_,
            SkipTo_,
            Pattern_,
            PatternVars_,
            Subset_,
            Definitions_
        );
    }

private:
    TString Label_;
    TNodePtr PartitionKeySelector_;
    TNodePtr PartitionColumns_;
    TVector<TSortSpecificationPtr> SortSpecs_;
    TVector<TNamedFunction> Measures_;
    TNodePtr RowsPerMatch_;
    TNodePtr SkipTo_;
    TNodePtr Pattern_;
    TNodePtr PatternVars_;
    TNodePtr Subset_;
    TVector<TNamedFunction> Definitions_;
};

} // anonymous namespace

TNodePtr TMatchRecognizeBuilder::Build(TContext& ctx, TString label, ISource* src) {
    const auto node = MakeIntrusive<TMatchRecognize>(
        Pos_,
        std::move(label),
        std::move(PartitionKeySelector_),
        std::move(PartitionColumns_),
        std::move(SortSpecs_),
        std::move(Measures_),
        std::move(RowsPerMatch_),
        std::move(SkipTo_),
        std::move(Pattern_),
        std::move(PatternVars_),
        std::move(Subset_),
        std::move(Definitions_)
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
