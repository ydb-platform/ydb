#include "match_recognize.h"
#include "source.h"

namespace NSQLTranslationV1 {

class TMatchRecognize: public TCallNode {
public:
    TMatchRecognize(
            TPosition pos,
            ISource* source,
            const TString& inputTable,
            std::pair<TPosition, TVector<TPartitioner>>&& partitioners,
            std::pair<TPosition, TVector<TSortSpecificationPtr>>&& sortSpecs,
            std::pair<TPosition, TVector<TNamedLambda>>&& measures,
            std::pair<TPosition, ERowsPerMatch>&& rowsPerMatch,
            std::pair<TPosition, TAfterMatchSkipTo>&& skipTo,
            std::pair<TPosition, TVector<TRowPatternTerm>>&& pattern,
            std::pair<TPosition, TNodePtr>&& subset,
            std::pair<TPosition, TVector<TNamedLambda>>&& definitions
            ): TCallNode(pos, "block", {BuildBlockStatements(
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
            )})
    {}
private:
    TNodePtr BuildBlockStatements(
            TPosition pos,
            ISource* source,
            const TString& inputTable,
            std::pair<TPosition, TVector<TPartitioner>>&& partitioners,
            std::pair<TPosition, TVector<TSortSpecificationPtr>>&& sortSpecs,
            std::pair<TPosition, TVector<TNamedLambda>>&& measures,
            std::pair<TPosition, ERowsPerMatch>&& rowsPerMatch,
            std::pair<TPosition, TAfterMatchSkipTo>&& skipTo,
            std::pair<TPosition, TVector<TRowPatternTerm>>&& pattern,
            std::pair<TPosition, TNodePtr>&& subset,
            std::pair<TPosition, TVector<TNamedLambda>>&& definitions
            ) {
        Y_UNUSED(pos);

        auto inputRowType = Y("ListItemType",Y("TypeOf", inputTable));
        TNodePtr patternNode = Y();
        for (const auto& t: pattern.second) {
           patternNode->Add(PatternTerm(pos, t));
        }
        patternNode = Q(patternNode);

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
            measureNames->Add(BuildQuotedAtom(m.lambda->GetPos(), m.name));
        }
        TNodePtr measuresNode = Y("MatchRecognizeMeasures", inputRowType, patternNode, Q(measureNames));
        for (const auto& m: measures.second){
            measuresNode->Add(m.lambda);
        }
        auto defineNames = Y();
        for (const auto& d: definitions.second) {
            defineNames->Add(BuildQuotedAtom(d.lambda->GetPos(), d.name));
        }

        TNodePtr defineNode = Y("MatchRecognizeDefines", inputRowType, patternNode, Q(defineNames));
        for (const auto& d: definitions.second) {
            defineNode->Add(d.lambda);
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

    TPtr PatternFactor(const TPosition& pos, const TRowPatternFactor& factor) {
        return BuildTuple(pos, {
                BuildQuotedAtom(pos, factor.Name),
                BuildQuotedAtom(pos, ToString(factor.QuantityMin)),
                BuildQuotedAtom(pos, ToString(factor.QuantityMax)),
                BuildQuotedAtom(pos, ToString(factor.Greedy)),
                BuildQuotedAtom(pos, ToString(factor.Output)),
        });
    }


    TPtr PatternTerm(const TPosition& pos, const TRowPatternTerm& term) {
        auto factors = Y();
        for (const auto& f: term)
            factors->Add(PatternFactor(pos, f));
        return Q(std::move(factors));
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

} // namespace NSQLTranslationV1