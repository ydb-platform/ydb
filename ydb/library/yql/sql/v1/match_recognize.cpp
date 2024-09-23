#include "match_recognize.h"
#include "source.h"
#include "context.h"

namespace NSQLTranslationV1 {

namespace {

const auto VarDataName = "data";
const auto VarMatchedVarsName = "vars";
const auto VarLastRowIndexName = "lri";

} //namespace {

class TMatchRecognize: public TAstListNode {
public:
    TMatchRecognize(
            TPosition pos,
            ISource* source,
            const TString& inputTable,
            std::pair<TPosition, TVector<TNamedFunction>>&& partitioners,
            std::pair<TPosition, TVector<TSortSpecificationPtr>>&& sortSpecs,
            std::pair<TPosition, TVector<TNamedFunction>>&& measures,
            std::pair<TPosition, ERowsPerMatch>&& rowsPerMatch,
            std::pair<TPosition, TAfterMatchSkipTo>&& skipTo,
            std::pair<TPosition, NYql::NMatchRecognize::TRowPattern>&& pattern,
            std::pair<TPosition, TNodePtr>&& subset,
            std::pair<TPosition, TVector<TNamedFunction>>&& definitions
            ): TAstListNode(pos, {BuildAtom(pos, "block")})
    {
        Add(BuildBlockStatements(
                    pos,
                    source,
                    inputTable,
                    std::move(partitioners),
                    std::move(sortSpecs),
                    std::move(measures),
                    std::move(rowsPerMatch),
                    std::move(skipTo),
                    std::move(pattern),
                    std::move(subset),
                    std::move(definitions)
        ));
    }
private:
    TMatchRecognize(const TMatchRecognize& other)
        : TAstListNode(other.Pos)
    {
        Nodes = CloneContainer(other.Nodes);
    }

    TNodePtr BuildBlockStatements(
            TPosition pos,
            ISource* source,
            const TString& inputTable,
            std::pair<TPosition, TVector<TNamedFunction>>&& partitioners,
            std::pair<TPosition, TVector<TSortSpecificationPtr>>&& sortSpecs,
            std::pair<TPosition, TVector<TNamedFunction>>&& measures,
            std::pair<TPosition, ERowsPerMatch>&& rowsPerMatch,
            std::pair<TPosition, TAfterMatchSkipTo>&& skipTo,
            std::pair<TPosition, NYql::NMatchRecognize::TRowPattern>&& pattern,
            std::pair<TPosition, TNodePtr>&& subset,
            std::pair<TPosition, TVector<TNamedFunction>>&& definitions
            ) {
        Y_UNUSED(pos);

        auto inputRowType = Y("ListItemType",Y("TypeOf", inputTable));

        auto patternNode = Pattern(pattern.first, pattern.second);

        auto partitionColumns = Y();
        for (const auto& p: partitioners.second){
            partitionColumns->Add(BuildQuotedAtom(p.callable->GetPos(), p.name));
        }
        partitionColumns = Q(partitionColumns);
        auto partitionKeySelector = Y();
        for (const auto& p: partitioners.second){
            partitionKeySelector->Add(p.callable);
        }
        partitionKeySelector = BuildLambda(partitioners.first, Y("row"), Q(partitionKeySelector));

        auto measureNames = Y();
        for (const auto& m: measures.second){
            measureNames->Add(BuildQuotedAtom(m.callable->GetPos(), m.name));
        }
        TNodePtr measuresNode = Y("MatchRecognizeMeasures", inputRowType, patternNode, Q(measureNames));
        for (const auto& m: measures.second){
            measuresNode->Add(BuildLambda(m.callable->GetPos(), Y(VarDataName, VarMatchedVarsName), m.callable));
        }
        auto defineNames = Y();
        for (const auto& d: definitions.second) {
            defineNames->Add(BuildQuotedAtom(d.callable->GetPos(), d.name));
        }

        TNodePtr defineNode = Y("MatchRecognizeDefines", inputRowType, patternNode, Q(defineNames));
        for (const auto& d: definitions.second) {
            defineNode->Add(BuildLambda(d.callable->GetPos(), Y(VarDataName, VarMatchedVarsName, VarLastRowIndexName), d.callable));
        }

        return Q(Y(
                Y("let", "input", inputTable),
                Y("let", "partitionKeySelector", partitionKeySelector),
                Y("let", "partitionColumns", partitionColumns),
                Y("let", "sortTraits", sortSpecs.second.empty()? Y("Void") : source->BuildSortSpec(sortSpecs.second, inputTable, true, false)),
                Y("let", "measures", measuresNode),
                Y("let", "rowsPerMatch", BuildQuotedAtom(rowsPerMatch.first, "RowsPerMatch_" + ToString(rowsPerMatch.second))),
                Y("let", "skipTo", BuildTuple(skipTo.first, {Q("AfterMatchSkip_" + ToString(skipTo.second.To)), Q(ToString(skipTo.second.Var))})),
                Y("let", "pattern", patternNode),
                Y("let", "subset", subset.second ? subset.second : Q("")),
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
        ));
    }

    TPtr PatternFactor(const TPosition& pos, const NYql::NMatchRecognize::TRowPatternFactor& factor) {
        return BuildTuple(pos, {
                factor.Primary.index() == 0 ?
                    BuildQuotedAtom(pos, std::get<0>(factor.Primary)) :
                    Pattern(pos, std::get<1>(factor.Primary)),
                BuildQuotedAtom(pos, ToString(factor.QuantityMin)),
                BuildQuotedAtom(pos, ToString(factor.QuantityMax)),
                BuildQuotedAtom(pos, ToString(factor.Greedy)),
                BuildQuotedAtom(pos, ToString(factor.Output)),
                BuildQuotedAtom(pos, ToString(factor.Unused))
        });
    }


    TPtr PatternTerm(const TPosition& pos, const NYql::NMatchRecognize::TRowPatternTerm& term) {
        auto factors = Y();
        for (const auto& f: term)
            factors->Add(PatternFactor(pos, f));
        return Q(std::move(factors));
    }

    TPtr Pattern(const TPosition& pos, const NYql::NMatchRecognize::TRowPattern& pattern) {
        TNodePtr patternNode = Y("MatchRecognizePattern");
        for (const auto& t: pattern) {
            patternNode->Add(PatternTerm(pos, t));
        }
        return patternNode;
    }

    TPtr DoClone() const final{
        return new TMatchRecognize(*this);
    }
};

TNodePtr TMatchRecognizeBuilder::Build(TContext& ctx, TString&& inputTable, ISource* source){
    TNodePtr node = new TMatchRecognize(
            Pos,
            source,
            std::move(inputTable),
            std::move(Partitioners),
            std::move(SortSpecs),
            std::move(Measures),
            std::move(RowsPerMatch),
            std::move(SkipTo),
            std::move(Pattern),
            std::move(Subset),
            std::move(Definitions)
    );
    if (!node->Init(ctx, source))
        return nullptr;
    return node;
}

namespace {
const auto DefaultNavigatingFunction = "MatchRecognizeDefaultNavigating";
}

bool TMatchRecognizeVarAccessNode::DoInit(TContext& ctx, ISource* src) {
        //If referenced var is the var that is currently being defined
        //then it's a reference to the last row in a partition
        Node = new TMatchRecognizeNavigate(ctx.Pos(), DefaultNavigatingFunction, TVector<TNodePtr>{this->Clone()});
        return Node->Init(ctx, src);
}

bool TMatchRecognizeNavigate::DoInit(TContext& ctx, ISource* src) {
    Y_UNUSED(src);
    if (Args.size() != 1) {
        ctx.Error(Pos) << "Exactly one argument is required in MATCH_RECOGNIZE navigation function";
        return false;
    }
    const auto varColumn = dynamic_cast<TMatchRecognizeVarAccessNode *>(Args[0].Get());
    if (not varColumn) {
        ctx.Error(Pos) << "Row pattern navigation operations are applicable to row pattern variable only";
        return false;
    }
    const auto varData = BuildAtom(ctx.Pos(), VarDataName);
    const auto varMatchedVars = BuildAtom(ctx.Pos(), VarMatchedVarsName);
    const auto varLastRowIndex = BuildAtom(ctx.Pos(), VarLastRowIndexName);

    const auto matchedRanges = Y("Member", varMatchedVars, Q(varColumn->GetVar()));
    TNodePtr navigatedRowIndex;
    if (DefaultNavigatingFunction == Name) {
        if (not varColumn->IsTheSameVar()) {
            ctx.Error(Pos) << "Row pattern navigation function is required";
            return false;
        }
        navigatedRowIndex =  varLastRowIndex;
    }
    else if ("PREV" == Name) {
        if (not varColumn->IsTheSameVar()) {
            ctx.Error(Pos) << "PREV relative to matched vars is not implemented yet";
            return false;
        }
        navigatedRowIndex = Y(
            "-",
            varLastRowIndex,
            Y("Uint64", Q("1"))
        );
    } else if ("FIRST" == Name) {
        navigatedRowIndex = Y(
            "Member",
            Y("Head", matchedRanges),
            Q("From")
        );
    } else if ("LAST" == Name) {
        navigatedRowIndex = Y(
            "Member",
            Y("Last", matchedRanges),
            Q("To")
        );
    } else {
        ctx.Error(Pos) << "Internal logic error";
        return false;
    }
    Add("Member");
    Add(
        Y(
            "Lookup",
            Y("ToIndexDict", varData),
            navigatedRowIndex
        )
    ),
    Add(Q(varColumn->GetColumn()));
    return true;
}

} // namespace NSQLTranslationV1
