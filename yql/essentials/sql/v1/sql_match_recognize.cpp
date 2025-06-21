#include "sql_match_recognize.h"

#include "node.h"
#include "sql_expression.h"

#include <yql/essentials/core/sql_types/match_recognize.h>

namespace NSQLTranslationV1 {

TSqlMatchRecognizeClause::TSqlMatchRecognizeClause(TContext& ctx, NSQLTranslation::ESqlMode mode) : TSqlTranslation(ctx, mode) {}

TMatchRecognizeBuilderPtr TSqlMatchRecognizeClause::CreateBuilder(const NSQLv1Generated::TRule_row_pattern_recognition_clause &matchRecognizeClause) {
    auto pos = GetPos(matchRecognizeClause.GetToken1());
    if (!Ctx_.FeatureR010) {
        Ctx_.Error(pos, TIssuesIds::CORE) << "Unexpected MATCH_RECOGNIZE";
        return {};
    }

    auto [partitionKeySelector, partitionColumns] = ParsePartitionBy(
        pos,
        matchRecognizeClause.HasBlock3()
            ? std::addressof(matchRecognizeClause.GetBlock3().GetRule_window_partition_clause1())
            : nullptr
    );

    auto sortSpecs = ParseOrderBy(
        matchRecognizeClause.HasBlock4()
            ? std::addressof(matchRecognizeClause.GetBlock4().GetRule_order_by_clause1())
            : nullptr
    );
    if (!sortSpecs) {
        return {};
    }

    auto measures = ParseMeasures(
        matchRecognizeClause.HasBlock5()
            ? std::addressof(matchRecognizeClause.GetBlock5().GetRule_row_pattern_measures1().GetRule_row_pattern_measure_list2())
            : nullptr
    );

    auto rowsPerMatch = ParseRowsPerMatch(
        pos,
        matchRecognizeClause.HasBlock6()
            ? std::addressof(matchRecognizeClause.GetBlock6().GetRule_row_pattern_rows_per_match1())
            : nullptr
    );
    if (!rowsPerMatch) {
        return {};
    }

    const auto& commonSyntax = matchRecognizeClause.GetRule_row_pattern_common_syntax7();

    if (commonSyntax.HasBlock2()) {
        const auto& initialOrSeek = commonSyntax.GetBlock2().GetRule_row_pattern_initial_or_seek1();
        Ctx_.Error(GetPos(initialOrSeek.GetToken1())) << "InitialOrSeek subclause is not allowed in FROM clause";
        return {};
    }

    PatternVarNames_.clear();
    PatternVars_ = BuildList(pos);
    auto pattern = ParsePattern(pos, commonSyntax.GetRule_row_pattern5(), 0, true);
    if (!pattern) {
        return {};
    }

    auto skipTo = ParseAfterMatchSkipTo(
        pos,
        commonSyntax.HasBlock1()
            ? std::addressof(commonSyntax.GetBlock1().GetRule_row_pattern_skip_to3())
            : nullptr
    );
    if (!skipTo) {
        return {};
    }

    auto subset = ParseSubset(
        pos,
        commonSyntax.HasBlock7()
            ? std::addressof(commonSyntax.GetBlock7().GetRule_row_pattern_subset_clause1())
            : nullptr
    );
    if (!subset) {
        return {};
    }

    auto definitions = ParseDefinitions(commonSyntax.GetRule_row_pattern_definition_list9());
    for (const auto& [callable, name]: definitions) {
        if (!PatternVarNames_.contains(name)) {
            Ctx_.Error(callable->GetPos()) << "ROW PATTERN VARIABLE " << name << " is defined, but not mentioned in the PATTERN";
            return {};
        }
    }

    return new TMatchRecognizeBuilder(
        pos,
        std::move(partitionKeySelector),
        std::move(partitionColumns),
        std::move(*sortSpecs),
        std::move(measures),
        std::move(rowsPerMatch),
        std::move(skipTo),
        std::move(pattern),
        std::move(PatternVars_),
        std::move(*subset),
        std::move(definitions)
    );
}

std::tuple<TNodePtr, TNodePtr> TSqlMatchRecognizeClause::ParsePartitionBy(TPosition pos, const TRule_window_partition_clause* node) {
    auto [partitionKeySelector, partitionColumns] = [&]() -> std::tuple<TNodePtr, TNodePtr> {
        auto partitionKeySelector = BuildList(pos);
        auto partitionColumns = BuildList(pos);
        if (!node) {
            return {partitionKeySelector, partitionColumns};
        }
        TColumnRefScope scope(Ctx_, EColumnRefState::Allow);
        TVector<TNodePtr> partitionExprs;
        if (!NamedExprList(node->GetRule_named_expr_list4(), partitionExprs)) {
            return {partitionKeySelector, partitionColumns};
        }
        for (const auto& p : partitionExprs) {
            auto label = p->GetLabel();
            if (!label && p->GetColumnName()) {
                label = *p->GetColumnName();
            }
            partitionKeySelector->Add(p);
            partitionColumns->Add(BuildQuotedAtom(p->GetPos(), label));
        }
        return {partitionKeySelector, partitionColumns};
    }();
    return {
        BuildLambda(pos, BuildList(pos, {BuildAtom(pos, "row")}), BuildQuote(pos, std::move(partitionKeySelector))),
        BuildQuote(pos, std::move(partitionColumns))
    };
}

TMaybe<TVector<TSortSpecificationPtr>> TSqlMatchRecognizeClause::ParseOrderBy(const TRule_order_by_clause* node) {
    if (!node) {
        return TVector<TSortSpecificationPtr>{};
    }
    TVector<TSortSpecificationPtr> result;
    if (!OrderByClause(*node, result)) {
        return {};
    }
    return result;
}

TNamedFunction TSqlMatchRecognizeClause::ParseOneMeasure(const TRule_row_pattern_measure_definition& node) {
    TColumnRefScope scope(Ctx_, EColumnRefState::MatchRecognizeMeasures);
    auto callable = TSqlExpression(Ctx_, Mode_).Build(node.GetRule_expr1());
    auto measureName = Id(node.GetRule_an_id3(), *this);
    // Each measure must be a lambda, that accepts 2 args:
    // - List<InputTableColumns + _yql_Classifier, _yql_MatchNumber>
    // - Struct that maps row pattern variables to ranges in the queue
    return {std::move(callable), std::move(measureName)};
}

TVector<TNamedFunction> TSqlMatchRecognizeClause::ParseMeasures(const TRule_row_pattern_measure_list* node) {
    if (!node) {
        return {};
    }
    TVector<TNamedFunction> result{ParseOneMeasure(node->GetRule_row_pattern_measure_definition1())};
    for (const auto& m: node->GetBlock2()) {
        result.push_back(ParseOneMeasure(m.GetRule_row_pattern_measure_definition2()));
    }
    return result;
}

TNodePtr TSqlMatchRecognizeClause::ParseRowsPerMatch(TPosition pos, const TRule_row_pattern_rows_per_match* node) {
    const auto result = [&]() -> NYql::NMatchRecognize::ERowsPerMatch {
        if (!node) {
            return NYql::NMatchRecognize::ERowsPerMatch::OneRow;
        }
        switch (node->GetAltCase()) {
        case TRule_row_pattern_rows_per_match::kAltRowPatternRowsPerMatch1: {
            const auto& rowsPerMatch = node->GetAlt_row_pattern_rows_per_match1();
            pos = GetPos(rowsPerMatch.GetToken1());
            return NYql::NMatchRecognize::ERowsPerMatch::OneRow;
        }
        case TRule_row_pattern_rows_per_match::kAltRowPatternRowsPerMatch2: {
            const auto& rowsPerMatch = node->GetAlt_row_pattern_rows_per_match2();
            pos = GetPos(rowsPerMatch.GetToken1());
            return NYql::NMatchRecognize::ERowsPerMatch::AllRows;
        }
        case TRule_row_pattern_rows_per_match::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
        }
    }();
    return BuildQuotedAtom(pos, "RowsPerMatch_" + ToString(result));
}

TNodePtr TSqlMatchRecognizeClause::ParseAfterMatchSkipTo(TPosition pos, const TRule_row_pattern_skip_to* node) {
    auto skipToPos = pos;
    auto varPos = pos;
    const auto result = [&]() -> TMaybe<NYql::NMatchRecognize::TAfterMatchSkipTo> {
        if (!node) {
            return NYql::NMatchRecognize::TAfterMatchSkipTo{NYql::NMatchRecognize::EAfterMatchSkipTo::PastLastRow, ""};
        }
        switch (node->GetAltCase()) {
        case TRule_row_pattern_skip_to::kAltRowPatternSkipTo1: {
            const auto& skipTo = node->GetAlt_row_pattern_skip_to1();
            skipToPos = GetPos(skipTo.GetToken1());
            return NYql::NMatchRecognize::TAfterMatchSkipTo{NYql::NMatchRecognize::EAfterMatchSkipTo::NextRow, ""};
        }
        case TRule_row_pattern_skip_to::kAltRowPatternSkipTo2: {
            const auto& skipTo = node->GetAlt_row_pattern_skip_to2();
            skipToPos = GetPos(skipTo.GetToken1());
            return NYql::NMatchRecognize::TAfterMatchSkipTo{NYql::NMatchRecognize::EAfterMatchSkipTo::PastLastRow, ""};
        }
        case TRule_row_pattern_skip_to::kAltRowPatternSkipTo3: {
            const auto& skipTo = node->GetAlt_row_pattern_skip_to3();
            skipToPos = GetPos(skipTo.GetToken1());
            const auto& identifier = skipTo.GetRule_row_pattern_skip_to_variable_name4().GetRule_row_pattern_variable_name1().GetRule_identifier1();
            auto var = identifier.GetToken1().GetValue();
            varPos = GetPos(identifier.GetToken1());
            if (!PatternVarNames_.contains(var)) {
                Ctx_.Error(varPos) << "Unknown pattern variable in AFTER MATCH SKIP TO FIRST";
                return {};
            }
            return NYql::NMatchRecognize::TAfterMatchSkipTo{NYql::NMatchRecognize::EAfterMatchSkipTo::ToFirst, std::move(var)};
        }
        case TRule_row_pattern_skip_to::kAltRowPatternSkipTo4: {
            const auto& skipTo = node->GetAlt_row_pattern_skip_to4();
            skipToPos = GetPos(skipTo.GetToken1());
            const auto& identifier = skipTo.GetRule_row_pattern_skip_to_variable_name4().GetRule_row_pattern_variable_name1().GetRule_identifier1();
            auto var = identifier.GetToken1().GetValue();
            varPos = GetPos(identifier.GetToken1());
            if (!PatternVarNames_.contains(var)) {
                Ctx_.Error(varPos) << "Unknown pattern variable in AFTER MATCH SKIP TO LAST";
                return {};
            }
            return NYql::NMatchRecognize::TAfterMatchSkipTo{NYql::NMatchRecognize::EAfterMatchSkipTo::ToLast, std::move(var)};
        }
        case TRule_row_pattern_skip_to::kAltRowPatternSkipTo5: {
            const auto& skipTo = node->GetAlt_row_pattern_skip_to5();
            skipToPos = GetPos(skipTo.GetToken1());
            const auto& identifier = skipTo.GetRule_row_pattern_skip_to_variable_name3().GetRule_row_pattern_variable_name1().GetRule_identifier1();
            auto var = identifier.GetToken1().GetValue();
            varPos = GetPos(identifier.GetToken1());
            if (!PatternVarNames_.contains(var)) {
                Ctx_.Error(varPos) << "Unknown pattern variable in AFTER MATCH SKIP TO";
                return {};
            }
            return NYql::NMatchRecognize::TAfterMatchSkipTo{NYql::NMatchRecognize::EAfterMatchSkipTo::To, std::move(var)};
        }
        case TRule_row_pattern_skip_to::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
        }
    }();
    if (!result) {
        return {};
    }
    return BuildTuple(pos, {
        BuildQuotedAtom(skipToPos, "AfterMatchSkip_" + ToString(result->To)),
        BuildQuotedAtom(varPos, std::move(result->Var))
    });
}

TNodePtr TSqlMatchRecognizeClause::BuildPatternFactor(TPosition pos, TNodePtr primary, std::tuple<ui64, ui64, bool, bool, bool> quantifier) {
    return std::apply([&](const auto& ...args) {
        return BuildTuple(pos, {std::move(primary), BuildQuotedAtom(pos, ToString(args))...});
    }, quantifier);
}

TNodePtr TSqlMatchRecognizeClause::ParsePatternFactor(TPosition pos, const TRule_row_pattern_factor& node, size_t nestingLevel, bool output) {
    if (nestingLevel > MaxPatternNesting) {
        Ctx_.Error(pos) << "To big nesting level in the pattern";
        return {};
    }
    auto primary = [&]() -> TNodePtr {
        const auto& primaryAlt = node.GetRule_row_pattern_primary1();
        switch (primaryAlt.GetAltCase()) {
        case TRule_row_pattern_primary::kAltRowPatternPrimary1: {
            const auto& primary = primaryAlt.GetAlt_row_pattern_primary1();
            const auto& identifier = primary.GetRule_row_pattern_primary_variable_name1().GetRule_row_pattern_variable_name1().GetRule_identifier1();
            const auto varName = Id(identifier, *this);
            const auto var = BuildQuotedAtom(GetPos(identifier.GetToken1()), varName);
            if (PatternVarNames_.insert(varName).second) {
                PatternVars_->Add(var);
            }
            return var;
        }
        case TRule_row_pattern_primary::kAltRowPatternPrimary2: {
            const auto& primary = primaryAlt.GetAlt_row_pattern_primary2();
            const auto& token = primary.GetToken1();
            const auto varName = token.GetValue();
            const auto var = BuildQuotedAtom(GetPos(token), varName);
            if (PatternVarNames_.insert(varName).second) {
                PatternVars_->Add(var);
            }
            return var;
        }
        case TRule_row_pattern_primary::kAltRowPatternPrimary3: {
            const auto& primary = primaryAlt.GetAlt_row_pattern_primary3();
            const auto& token = primary.GetToken1();
            const auto varName = token.GetValue();
            const auto var = BuildQuotedAtom(GetPos(token), varName);
            if (PatternVarNames_.insert(varName).second) {
                PatternVars_->Add(var);
            }
            return var;
        }
        case TRule_row_pattern_primary::kAltRowPatternPrimary4: {
            const auto& primary = primaryAlt.GetAlt_row_pattern_primary4();
            return ParsePattern(pos, primary.GetBlock2().GetRule_row_pattern1(), nestingLevel + 1, output);
        }
        case TRule_row_pattern_primary::kAltRowPatternPrimary5: {
            const auto& primary = primaryAlt.GetAlt_row_pattern_primary5();
            output = false;
            return ParsePattern(pos, primary.GetRule_row_pattern3(), nestingLevel + 1, output);
        }
        case TRule_row_pattern_primary::kAltRowPatternPrimary6: {
            const auto& primary = primaryAlt.GetAlt_row_pattern_primary6();
            std::vector<TNodePtr> items{
                ParsePattern(pos, primary.GetRule_row_pattern_permute1().GetRule_row_pattern3(), nestingLevel + 1, output)
            };
            for (const auto& p: primary.GetRule_row_pattern_permute1().GetBlock4()) {
                items.push_back(ParsePattern(pos, p.GetRule_row_pattern2(), nestingLevel + 1, output));
            }
            if (items.size() > MaxPermutedItems) {
                Ctx_.Error(GetPos(primary.GetRule_row_pattern_permute1().GetToken1())) << "Too many items in permute";
                return {};
            }
            std::vector<size_t> indexes(items.size());
            Iota(indexes.begin(), indexes.end(), 0);
            std::vector<TNodePtr> result;
            do {
                std::vector<TNodePtr> term;
                term.reserve(items.size());
                for (auto index : indexes) {
                    term.push_back(BuildPatternFactor(pos, items[index], std::tuple{1, 1, true, output, false}));
                }
                result.push_back(BuildPatternTerm(pos, std::move(term)));
            } while (std::next_permutation(indexes.begin(), indexes.end()));
            return BuildPattern(pos, std::move(result));
        }
        case TRule_row_pattern_primary::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
        }
    }();
    if (!primary) {
        return {};
    }

    const auto quantifier = [&]() {
        if (!node.HasBlock2()) {
            const auto quantity = static_cast<ui64>(1);
            return std::tuple{quantity, quantity, true, output, false};
        }
        const auto& quantifierAlt = node.GetBlock2().GetRule_row_pattern_quantifier1();
        switch (quantifierAlt.GetAltCase()) {
        case TRule_row_pattern_quantifier::kAltRowPatternQuantifier1: { // *
            const auto& quantifier = quantifierAlt.GetAlt_row_pattern_quantifier1();
            pos = GetPos(quantifier.GetToken1());
            return std::tuple{static_cast<ui64>(0), static_cast<ui64>(Max()), !quantifier.HasBlock2(), output, false};
        }
        case TRule_row_pattern_quantifier::kAltRowPatternQuantifier2: { // +
            const auto& quantifier = quantifierAlt.GetAlt_row_pattern_quantifier2();
            pos = GetPos(quantifier.GetToken1());
            return std::tuple{static_cast<ui64>(1), static_cast<ui64>(Max()), !quantifier.HasBlock2(), output, false};
        }
        case TRule_row_pattern_quantifier::kAltRowPatternQuantifier3: { // ?
            const auto& quantifier = quantifierAlt.GetAlt_row_pattern_quantifier3();
            pos = GetPos(quantifier.GetToken1());
            return std::tuple{static_cast<ui64>(0), static_cast<ui64>(1), !quantifier.HasBlock2(), output, false};
        }
        case TRule_row_pattern_quantifier::kAltRowPatternQuantifier4: { // {n?, m?}
            const auto& quantifier = quantifierAlt.GetAlt_row_pattern_quantifier4();
            pos = GetPos(quantifier.GetToken1());
            return std::tuple{
                quantifier.HasBlock2()
                    ? FromString(quantifier.GetBlock2().GetRule_integer1().GetToken1().GetValue())
                    : static_cast<ui64>(0),
                quantifier.HasBlock4()
                    ? FromString(quantifier.GetBlock4().GetRule_integer1().GetToken1().GetValue())
                    : static_cast<ui64>(Max()),
                !quantifier.HasBlock6(),
                output,
                false
            };
        }
        case TRule_row_pattern_quantifier::kAltRowPatternQuantifier5: { // {n}
            const auto quantifier = quantifierAlt.GetAlt_row_pattern_quantifier5();
            pos = GetPos(quantifier.GetToken1());
            const auto quantity = static_cast<ui64>(FromString(quantifier.GetRule_integer2().GetToken1().GetValue()));
            return std::tuple{quantity, quantity, true, output, false};
        }
        case TRule_row_pattern_quantifier::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
        }
    }();
    return BuildPatternFactor(pos, std::move(primary), std::move(quantifier));
}

TNodePtr TSqlMatchRecognizeClause::BuildPatternTerm(TPosition pos, std::vector<TNodePtr> term) {
    auto result = BuildList(pos);
    for (auto& factor : term) {
        if (!factor) {
            return {};
        }
        result->Add(std::move(factor));
    }
    return BuildQuote(pos, std::move(result));
}

TNodePtr TSqlMatchRecognizeClause::ParsePatternTerm(TPosition pos, const TRule_row_pattern_term& node, size_t nestingLevel, bool output) {
    std::vector<TNodePtr> result;
    result.reserve(node.GetBlock1().size());
    for (const auto& factor: node.GetBlock1()) {
        result.push_back(ParsePatternFactor(pos, factor.GetRule_row_pattern_factor1(), nestingLevel, output));
    }
    return BuildPatternTerm(pos, std::move(result));
}

TNodePtr TSqlMatchRecognizeClause::BuildPattern(TPosition pos, std::vector<TNodePtr> pattern) {
    const auto result = BuildList(pos, {BuildAtom(pos, "MatchRecognizePattern")});
    for (auto& term: pattern) {
        if (!term) {
            return {};
        }
        result->Add(std::move(term));
    }
    return result;
}

TNodePtr TSqlMatchRecognizeClause::ParsePattern(TPosition pos, const TRule_row_pattern& node, size_t nestingLevel, bool output) {
    std::vector<TNodePtr> result;
    result.reserve(1 + node.GetBlock2().size());
    result.push_back(ParsePatternTerm(pos, node.GetRule_row_pattern_term1(), nestingLevel, output));
    for (const auto& term: node.GetBlock2()) {
        result.push_back(ParsePatternTerm(pos, term.GetRule_row_pattern_term2(), nestingLevel, output));
    }
    return BuildPattern(pos, std::move(result));
}

TMaybe<TNodePtr> TSqlMatchRecognizeClause::ParseSubset(TPosition pos, const TRule_row_pattern_subset_clause* node) {
    if (!node) {
        return TNodePtr{};
    }
    pos = GetPos(node->GetToken1());
    // TODO https://st.yandex-team.ru/YQL-16225
    Ctx_.Error(pos) << "SUBSET is not implemented yet";
    return {};
}

TNamedFunction TSqlMatchRecognizeClause::ParseOneDefinition(const TRule_row_pattern_definition& node) {
    const auto& identifier = node.GetRule_row_pattern_definition_variable_name1().GetRule_row_pattern_variable_name1().GetRule_identifier1();
    auto defineName = Id(identifier, *this);
    TColumnRefScope scope(Ctx_, EColumnRefState::MatchRecognizeDefine, true, defineName);
    const auto& searchCondition = node.GetRule_row_pattern_definition_search_condition3().GetRule_search_condition1().GetRule_expr1();
    auto callable = TSqlExpression(Ctx_, Mode_).Build(searchCondition);
    // Each define must be a predicate lambda, that accepts 3 args:
    // - List<input table rows>
    // - A struct that maps row pattern variables to ranges in the queue
    // - An index of the current row
    return {std::move(callable), std::move(defineName)};
}

TVector<TNamedFunction> TSqlMatchRecognizeClause::ParseDefinitions(const TRule_row_pattern_definition_list& node) {
    TVector<TNamedFunction> result{ParseOneDefinition(node.GetRule_row_pattern_definition1())};
    for (const auto& d: node.GetBlock2()) {
        result.push_back(ParseOneDefinition(d.GetRule_row_pattern_definition2()));
    }
    return result;
}

} // namespace NSQLTranslationV1
