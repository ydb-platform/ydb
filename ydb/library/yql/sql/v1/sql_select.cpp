#include "sql_select.h"
#include "sql_call_expr.h"
#include "sql_expression.h"
#include "sql_group_by.h"
#include "sql_values.h"
#include "sql_match_recognize.h"

namespace NSQLTranslationV1 {

using namespace NSQLv1Generated;

namespace {

bool IsColumnsOnly(const TVector<TSortSpecificationPtr>& container) {
    for (const auto& elem: container) {
        if (!elem->OrderExpr->GetColumnName()) {
            return false;
        }
    }
    return true;
}

bool CollectJoinLinkSettings(TPosition pos, TJoinLinkSettings& linkSettings, TContext& ctx) {
    linkSettings = {};
    auto hints = ctx.PullHintForToken(pos);
    for (const auto& hint: hints) {
        const auto canonizedName = to_lower(hint.Name);
        auto newStrategy =  TJoinLinkSettings::EStrategy::Default;
        if (canonizedName == "merge") {
            newStrategy = TJoinLinkSettings::EStrategy::SortedMerge;
        } else if (canonizedName == "streamlookup") {
            newStrategy = TJoinLinkSettings::EStrategy::StreamLookup;
        } else if (canonizedName == "map") {
            newStrategy = TJoinLinkSettings::EStrategy::ForceMap;
        } else if (canonizedName == "grace") {
            newStrategy = TJoinLinkSettings::EStrategy::ForceGrace;
        } else {
            ctx.Warning(hint.Pos, TIssuesIds::YQL_UNUSED_HINT) << "Unsupported join strategy: " << hint.Name;
        }

        if (TJoinLinkSettings::EStrategy::Default == linkSettings.Strategy) {
            linkSettings.Strategy = newStrategy;
        } else if (newStrategy == linkSettings.Strategy) {
            ctx.Error() << "Duplicate join strategy hint";
            return false;
        } else {
            ctx.Error() << "Conflicting join strategy hints";
            return false;
        }
    }
    return true;
}

} // namespace

bool TSqlSelect::JoinOp(ISource* join, const TRule_join_source::TBlock3& block, TMaybe<TPosition> anyPos) {
    // block: (join_op (ANY)? flatten_source join_constraint?)
    // join_op:
    //    COMMA
    //  | (NATURAL)? ((LEFT (ONLY | SEMI)? | RIGHT (ONLY | SEMI)? | EXCLUSION | FULL)? (OUTER)? | INNER | CROSS) JOIN
    //;
    const auto& node = block.GetRule_join_op1();
    TString joinOp("Inner");
    TJoinLinkSettings linkSettings;
    switch (node.Alt_case()) {
        case TRule_join_op::kAltJoinOp1: {
            joinOp = "Cross";
            if (!Ctx.AnsiImplicitCrossJoin) {
                Error() << "Cartesian product of tables is disabled. Please use "
                           "explicit CROSS JOIN or enable it via PRAGMA AnsiImplicitCrossJoin";
                return false;
            }
            auto alt = node.GetAlt_join_op1();
            if (!CollectJoinLinkSettings(Ctx.TokenPosition(alt.GetToken1()), linkSettings, Ctx)) {
                return false;
            }
            Ctx.IncrementMonCounter("sql_join_operations", "CartesianProduct");
            break;
        }
        case TRule_join_op::kAltJoinOp2: {
            auto alt = node.GetAlt_join_op2();
            if (alt.HasBlock1()) {
                Ctx.IncrementMonCounter("sql_join_operations", "Natural");
                Error() << "Natural join is not implemented yet";
                return false;
            }
            if (!CollectJoinLinkSettings(Ctx.TokenPosition(alt.GetToken3()), linkSettings, Ctx)) {
                return false;
            }
            switch (alt.GetBlock2().Alt_case()) {
                case TRule_join_op::TAlt2::TBlock2::kAlt1:
                    if (alt.GetBlock2().GetAlt1().HasBlock1()) {
                        auto block = alt.GetBlock2().GetAlt1().GetBlock1();
                        switch (block.Alt_case()) {
                        case TRule_join_op_TAlt2_TBlock2_TAlt1_TBlock1::kAlt1:
                            // left
                            joinOp = Token(block.GetAlt1().GetToken1());
                            if (block.GetAlt1().HasBlock2()) {
                                joinOp += " " + Token(block.GetAlt1().GetBlock2().GetToken1());
                            }
                            break;
                        case TRule_join_op_TAlt2_TBlock2_TAlt1_TBlock1::kAlt2:
                            // right
                            joinOp = Token(block.GetAlt2().GetToken1());
                            if (block.GetAlt2().HasBlock2()) {
                                joinOp += " " + Token(block.GetAlt2().GetBlock2().GetToken1());
                            }

                            break;
                        case TRule_join_op_TAlt2_TBlock2_TAlt1_TBlock1::kAlt3:
                            // exclusion
                            joinOp = Token(block.GetAlt3().GetToken1());
                            break;
                        case TRule_join_op_TAlt2_TBlock2_TAlt1_TBlock1::kAlt4:
                            // full
                            joinOp = Token(block.GetAlt4().GetToken1());
                            break;
                        case TRule_join_op_TAlt2_TBlock2_TAlt1_TBlock1::ALT_NOT_SET:
                            Ctx.IncrementMonCounter("sql_errors", "UnknownJoinOperation");
                            AltNotImplemented("join_op", node);
                            return false;
                        }
                    }
                    if (alt.GetBlock2().GetAlt1().HasBlock2()) {
                        TString normalizedOp = alt.GetBlock2().GetAlt1().HasBlock1() ? joinOp : "";
                        normalizedOp.to_upper();
                        if (!(normalizedOp == "LEFT" || normalizedOp == "RIGHT" || normalizedOp == "FULL")) {
                            Token(alt.GetBlock2().GetAlt1().GetBlock2().GetToken1());
                            Error() << "Invalid join type: " << normalizedOp << (normalizedOp.empty() ? "" : " ") << "OUTER JOIN. "
                                    << "OUTER keyword is optional and can only be used after LEFT, RIGHT or FULL";
                            Ctx.IncrementMonCounter("sql_errors", "BadJoinType");
                            return false;
                        }
                    }
                    break;
                case TRule_join_op::TAlt2::TBlock2::kAlt2:
                    joinOp = Token(alt.GetBlock2().GetAlt2().GetToken1());
                    break;
                case TRule_join_op::TAlt2::TBlock2::kAlt3:
                    joinOp = Token(alt.GetBlock2().GetAlt3().GetToken1());
                    break;
                case TRule_join_op::TAlt2::TBlock2::ALT_NOT_SET:
                    Ctx.IncrementMonCounter("sql_errors", "UnknownJoinOperation");
                    AltNotImplemented("join_op", node);
                    return false;
            }
            Ctx.IncrementMonCounter("sql_features", "Join");
            Ctx.IncrementMonCounter("sql_join_operations", joinOp);
            break;
        }
        case TRule_join_op::ALT_NOT_SET:
            Ctx.IncrementMonCounter("sql_errors", "UnknownJoinOperation2");
            AltNotImplemented("join_op", node);
            return false;
    }
    joinOp = NormalizeJoinOp(joinOp);
    if (linkSettings.Strategy != TJoinLinkSettings::EStrategy::Default && joinOp == "Cross") {
        Ctx.Warning(Ctx.Pos(), TIssuesIds::YQL_UNUSED_HINT) << "Non-default join strategy will not be used for CROSS JOIN";
        linkSettings.Strategy = TJoinLinkSettings::EStrategy::Default;
    }

    TNodePtr joinKeyExpr;
    if (block.HasBlock4()) {
        if (joinOp == "Cross") {
            Error() << "Cross join should not have ON or USING expression";
            Ctx.IncrementMonCounter("sql_errors", "BadJoinExpr");
            return false;
        }

        joinKeyExpr = JoinExpr(join, block.GetBlock4().GetRule_join_constraint1());
        if (!joinKeyExpr) {
            Ctx.IncrementMonCounter("sql_errors", "BadJoinExpr");
            return false;
        }
    }
    else {
        if (joinOp != "Cross") {
            Error() << "Expected ON or USING expression";
            Ctx.IncrementMonCounter("sql_errors", "BadJoinExpr");
            return false;
        }
    }

    if (joinOp == "Cross" && anyPos) {
        Ctx.Error(*anyPos) << "ANY should not be used with Cross JOIN";
        Ctx.IncrementMonCounter("sql_errors", "BadJoinAny");
        return false;
    }

    Y_DEBUG_ABORT_UNLESS(join->GetJoin());
    join->GetJoin()->SetupJoin(joinOp, joinKeyExpr, linkSettings);

    return true;
}

TNodePtr TSqlSelect::JoinExpr(ISource* join, const TRule_join_constraint& node) {
    switch (node.Alt_case()) {
        case TRule_join_constraint::kAltJoinConstraint1: {
            auto& alt = node.GetAlt_join_constraint1();
            Token(alt.GetToken1());
            TColumnRefScope scope(Ctx, EColumnRefState::Allow);
            TSqlExpression expr(Ctx, Mode);
            return expr.Build(alt.GetRule_expr2());
        }
        case TRule_join_constraint::kAltJoinConstraint2: {
            auto& alt = node.GetAlt_join_constraint2();
            Token(alt.GetToken1());
            TPosition pos(Ctx.Pos());
            TVector<TDeferredAtom> names;
            if (!PureColumnOrNamedListStr(alt.GetRule_pure_column_or_named_list2(), *this, names)) {
                return nullptr;
            }

            Y_DEBUG_ABORT_UNLESS(join->GetJoin());
            return join->GetJoin()->BuildJoinKeys(Ctx, names);
        }
        case TRule_join_constraint::ALT_NOT_SET:
            Ctx.IncrementMonCounter("sql_errors", "UnknownJoinConstraint");
            AltNotImplemented("join_constraint", node);
            break;
    }
    return nullptr;
}

bool TSqlSelect::FlattenByArg(const TString& sourceLabel, TVector<TNodePtr>& flattenByColumns, TVector<TNodePtr>& flattenByExprs,
                              const TRule_flatten_by_arg& node)
{
    // flatten_by_arg:
    //     named_column
    //  |  LPAREN named_expr_list COMMA? RPAREN
    // ;

    flattenByColumns.clear();
    flattenByExprs.clear();

    TVector<TNodePtr> namedExprs;
    switch (node.Alt_case()) {
        case TRule_flatten_by_arg::kAltFlattenByArg1: {
            TVector<TNodePtr> columns;
            if (!NamedColumn(columns, node.GetAlt_flatten_by_arg1().GetRule_named_column1())) {
                return false;
            }
            YQL_ENSURE(columns.size() == 1);
            auto& column = columns.back();
            auto columnNamePtr = column->GetColumnName();
            YQL_ENSURE(columnNamePtr && *columnNamePtr);

            auto sourcePtr = column->GetSourceName();
            const bool isEmptySource = !sourcePtr || !*sourcePtr;
            if (isEmptySource || *sourcePtr == sourceLabel) {
                // select * from T      flatten by x
                // select * from T as s flatten by x
                // select * from T as s flatten by s.x
                flattenByColumns.emplace_back(std::move(column));
            } else {
                // select * from T as s flatten by x.y as z
                if (!column->GetLabel()) {
                    Ctx.Error(column->GetPos()) << "Unnamed expression after FLATTEN BY is not allowed";
                    return false;
                }
                flattenByColumns.emplace_back(BuildColumn(column->GetPos(), column->GetLabel()));

                TVector<INode::TIdPart> ids;
                ids.push_back(BuildColumn(column->GetPos()));
                ids.push_back(*sourcePtr);
                ids.push_back(*columnNamePtr);
                auto node = BuildAccess(column->GetPos(), ids, false);
                node->SetLabel(column->GetLabel());
                flattenByExprs.emplace_back(std::move(node));
            }

            break;
        }
        case TRule_flatten_by_arg::kAltFlattenByArg2: {
            TColumnRefScope scope(Ctx, EColumnRefState::Allow);
            if (!NamedExprList(node.GetAlt_flatten_by_arg2().GetRule_named_expr_list2(), namedExprs) || Ctx.HasPendingErrors) {
                return false;
            }
            for (auto& namedExprNode : namedExprs) {
                YQL_ENSURE(!namedExprNode->ContentListPtr());

                auto sourcePtr = namedExprNode->GetSourceName();
                const bool isEmptySource = !sourcePtr || !*sourcePtr;
                auto columnNamePtr = namedExprNode->GetColumnName();
                if (columnNamePtr && (isEmptySource || *sourcePtr == sourceLabel)) {
                    namedExprNode->AssumeColumn();
                    flattenByColumns.emplace_back(std::move(namedExprNode));
                } else {
                    auto nodeLabel = namedExprNode->GetLabel();
                    if (!nodeLabel) {
                        Ctx.Error(namedExprNode->GetPos()) << "Unnamed expression after FLATTEN BY is not allowed";
                        return false;
                    }
                    flattenByColumns.emplace_back(BuildColumn(namedExprNode->GetPos(), nodeLabel));
                    flattenByExprs.emplace_back(std::move(namedExprNode));
                }
            }
            break;
        }
        case TRule_flatten_by_arg::ALT_NOT_SET:
            Ctx.IncrementMonCounter("sql_errors", "UnknownFlattenByArg");
            AltNotImplemented("flatten_by_arg", node);
            return false;
    }
    return true;
}

TSourcePtr TSqlSelect::FlattenSource(const TRule_flatten_source& node) {
    auto source = NamedSingleSource(node.GetRule_named_single_source1(), true);
    if (!source) {
        return nullptr;
    }
    if (node.HasBlock2()) {
        auto flatten = node.GetBlock2();
        auto flatten2 = flatten.GetBlock2();
        switch (flatten2.Alt_case()) {
        case TRule_flatten_source::TBlock2::TBlock2::kAlt1: {
            TString mode = "auto";
            if (flatten2.GetAlt1().HasBlock1()) {
                mode = to_lower(Token(flatten2.GetAlt1().GetBlock1().GetToken1()));
            }

            TVector<TNodePtr> flattenByColumns;
            TVector<TNodePtr> flattenByExprs;
            if (!FlattenByArg(source->GetLabel(), flattenByColumns, flattenByExprs, flatten2.GetAlt1().GetRule_flatten_by_arg3())) {
                return nullptr;
            }

            Ctx.IncrementMonCounter("sql_features", "FlattenByColumns");
            if (!source->AddExpressions(Ctx, flattenByColumns, EExprSeat::FlattenBy)) {
                return nullptr;
            }

            if (!source->AddExpressions(Ctx, flattenByExprs, EExprSeat::FlattenByExpr)) {
                return nullptr;
            }

            source->SetFlattenByMode(mode);
            break;
        }
        case TRule_flatten_source::TBlock2::TBlock2::kAlt2: {
            Ctx.IncrementMonCounter("sql_features", "FlattenColumns");
            source->MarkFlattenColumns();
            break;
        }

        case TRule_flatten_source::TBlock2::TBlock2::ALT_NOT_SET:
            Ctx.IncrementMonCounter("sql_errors", "UnknownOrdinaryNamedColumn");
            AltNotImplemented("flatten_source", flatten2);
        }
    }
    return source;
}

TSourcePtr TSqlSelect::JoinSource(const TRule_join_source& node) {
    // join_source: (ANY)? flatten_source (join_op (ANY)? flatten_source join_constraint?)*;
    if (node.HasBlock1() && !node.Block3Size()) {
        Error() << "ANY is not allowed without JOIN";
        return nullptr;
    }

    TSourcePtr source(FlattenSource(node.GetRule_flatten_source2()));
    if (!source) {
        return nullptr;
    }

    if (node.Block3Size()) {
        TPosition pos(Ctx.Pos());
        TVector<TSourcePtr> sources;
        TVector<TMaybe<TPosition>> anyPositions;
        TVector<bool> anyFlags;

        sources.emplace_back(std::move(source));
        anyPositions.emplace_back(node.HasBlock1() ? Ctx.TokenPosition(node.GetBlock1().GetToken1()) : TMaybe<TPosition>());
        anyFlags.push_back(bool(anyPositions.back()));

        for (auto& block: node.GetBlock3()) {
            sources.emplace_back(FlattenSource(block.GetRule_flatten_source3()));
            if (!sources.back()) {
                Ctx.IncrementMonCounter("sql_errors", "NoJoinWith");
                return nullptr;
            }

            anyPositions.emplace_back(block.HasBlock2() ? Ctx.TokenPosition(block.GetBlock2().GetToken1()) : TMaybe<TPosition>());
            anyFlags.push_back(bool(anyPositions.back()));
        }

        source = BuildEquiJoin(pos, std::move(sources), std::move(anyFlags), Ctx.Scoped->StrictJoinKeyTypes);
        size_t idx = 1;
        for (auto& block: node.GetBlock3()) {
            YQL_ENSURE(idx < anyPositions.size());
            TMaybe<TPosition> leftAny = (idx == 1) ? anyPositions[0] : Nothing();
            TMaybe<TPosition> rightAny = anyPositions[idx];

            if (!JoinOp(source.Get(), block, leftAny ? leftAny : rightAny)) {
                Ctx.IncrementMonCounter("sql_errors", "NoJoinOp");
                return nullptr;
            }
            ++idx;
        }
    }

    return source;
}

bool TSqlSelect::SelectTerm(TVector<TNodePtr>& terms, const TRule_result_column& node) {
    // result_column:
    //     opt_id_prefix ASTERISK
    //   | expr ((AS an_id) | an_id_pure)?
    // ;
    switch (node.Alt_case()) {
        case TRule_result_column::kAltResultColumn1: {
            auto alt = node.GetAlt_result_column1();

            Token(alt.GetToken2());
            auto idAsteriskQualify = OptIdPrefixAsStr(alt.GetRule_opt_id_prefix1(), *this);
            Ctx.IncrementMonCounter("sql_features", idAsteriskQualify ? "QualifyAsterisk" : "Asterisk");
            terms.push_back(BuildColumn(Ctx.Pos(), "*", idAsteriskQualify));
            break;
        }
        case TRule_result_column::kAltResultColumn2: {
            auto alt = node.GetAlt_result_column2();
            TColumnRefScope scope(Ctx, EColumnRefState::Allow);
            TSqlExpression expr(Ctx, Mode);
            TNodePtr term(expr.Build(alt.GetRule_expr1()));
            if (!term) {
                Ctx.IncrementMonCounter("sql_errors", "NoTerm");
                return false;
            }
            if (alt.HasBlock2()) {
                TString label;
                bool implicitLabel = false;
                switch (alt.GetBlock2().Alt_case()) {
                    case TRule_result_column_TAlt2_TBlock2::kAlt1:
                        label = Id(alt.GetBlock2().GetAlt1().GetRule_an_id_or_type2(), *this);
                        break;
                    case TRule_result_column_TAlt2_TBlock2::kAlt2:
                        label = Id(alt.GetBlock2().GetAlt2().GetRule_an_id_as_compat1(), *this);
                        if (!Ctx.AnsiOptionalAs) {
                            // AS is mandatory
                            Ctx.Error() << "Expecting mandatory AS here. Did you miss comma? Please add PRAGMA AnsiOptionalAs; for ANSI compatibility";
                            return false;
                        }
                        implicitLabel = true;
                        break;
                    case TRule_result_column_TAlt2_TBlock2::ALT_NOT_SET:
                        Y_ABORT("You should change implementation according to grammar changes");
                }
                term->SetLabel(label, Ctx.Pos());
                term->MarkImplicitLabel(implicitLabel);
            }
            terms.push_back(term);
            break;
        }
        case TRule_result_column::ALT_NOT_SET:
            Ctx.IncrementMonCounter("sql_errors", "UnknownResultColumn");
            AltNotImplemented("result_column", node);
            return false;
    }
    return true;
}

bool TSqlSelect::ValidateSelectColumns(const TVector<TNodePtr>& terms) {
    TSet<TString> labels;
    TSet<TString> asteriskSources;
    for (const auto& term: terms) {
        const auto& label = term->GetLabel();
        if (!Ctx.PragmaAllowDotInAlias && label.find('.') != TString::npos) {
            Ctx.Error(term->GetPos()) << "Unable to use '.' in column name. Invalid column name: " << label;
            return false;
        }
        if (!label.empty()) {
            if (!labels.insert(label).second) {
                Ctx.Error(term->GetPos()) << "Unable to use duplicate column names. Collision in name: " << label;
                return false;
            }
        }
        if (term->IsAsterisk()) {
            const auto& source = *term->GetSourceName();
            if (source.empty() && terms.ysize() > 1) {
                Ctx.Error(term->GetPos()) << "Unable to use plain '*' with other projection items. Please use qualified asterisk instead: '<table>.*' (<table> can be either table name or table alias).";
                return false;
            } else if (!asteriskSources.insert(source).second) {
                Ctx.Error(term->GetPos()) << "Unable to use twice same quialified asterisk. Invalid source: " << source;
                return false;
            }
        } else if (label.empty()) {
            const auto* column = term->GetColumnName();
            if (column && !column->empty()) {
                const auto& source = *term->GetSourceName();
                const auto usedName = source.empty() ? *column : source + '.' + *column;
                if (!labels.insert(usedName).second) {
                    Ctx.Error(term->GetPos()) << "Unable to use duplicate column names. Collision in name: " << usedName;
                    return false;
                }
            }
        }
    }
    return true;
}

TSourcePtr TSqlSelect::SingleSource(const TRule_single_source& node, const TVector<TString>& derivedColumns, TPosition derivedColumnsPos, bool unorderedSubquery) {
    switch (node.Alt_case()) {
        case TRule_single_source::kAltSingleSource1: {
            const auto& alt = node.GetAlt_single_source1();
            const auto& table_ref = alt.GetRule_table_ref1();

            if (auto maybeSource = AsTableImpl(table_ref)) {
                auto source = *maybeSource;
                if (!source) {
                    return nullptr;
                }

                return source;
            } else {
                TTableRef table;
                if (!TableRefImpl(alt.GetRule_table_ref1(), table, unorderedSubquery)) {
                    return nullptr;
                }

                if (table.Source) {
                    return table.Source;
                }

                TPosition pos(Ctx.Pos());
                Ctx.IncrementMonCounter("sql_select_clusters", table.Cluster.GetLiteral() ? *table.Cluster.GetLiteral() : "unknown");
                return BuildTableSource(pos, table);
            }
        }
        case TRule_single_source::kAltSingleSource2: {
            const auto& alt = node.GetAlt_single_source2();
            Token(alt.GetToken1());
            TSqlSelect innerSelect(Ctx, Mode);
            TPosition pos;
            auto source = innerSelect.Build(alt.GetRule_select_stmt2(), pos);
            if (!source) {
                return nullptr;
            }
            return BuildInnerSource(pos, BuildSourceNode(pos, std::move(source)), Ctx.Scoped->CurrService, Ctx.Scoped->CurrCluster);
        }
        case TRule_single_source::kAltSingleSource3: {
            const auto& alt = node.GetAlt_single_source3();
            TPosition pos;
            return TSqlValues(Ctx, Mode).Build(alt.GetRule_values_stmt2(), pos, derivedColumns, derivedColumnsPos);
        }
        case TRule_single_source::ALT_NOT_SET:
            AltNotImplemented("single_source", node);
            Ctx.IncrementMonCounter("sql_errors", "UnknownSingleSource");
            return nullptr;
    }
}

TSourcePtr TSqlSelect::NamedSingleSource(const TRule_named_single_source& node, bool unorderedSubquery) {
    // named_single_source: single_source match_recognize_clause? (((AS an_id) | an_id_as_compat) pure_column_list?)? (sample_clause | tablesample_clause)?;
    TVector<TString> derivedColumns;
    TPosition derivedColumnsPos;
    if (node.HasBlock3() && node.GetBlock3().HasBlock2()) {
        const auto& columns = node.GetBlock3().GetBlock2().GetRule_pure_column_list1();
        Token(columns.GetToken1());
        derivedColumnsPos = Ctx.Pos();

        if (node.GetRule_single_source1().Alt_case() != TRule_single_source::kAltSingleSource3) {
            Error() << "Derived column list is only supported for VALUES";
            return nullptr;
        }

        PureColumnListStr(columns, *this, derivedColumns);
    }

    auto singleSource = SingleSource(node.GetRule_single_source1(), derivedColumns, derivedColumnsPos, unorderedSubquery);
    if (!singleSource) {
        return nullptr;
    }
    if (node.HasBlock2()) {
        if (node.HasBlock4()) {
            //CAN/CSA-ISO/IEC 9075-2:18 7.6 <table reference>
            //4) TF shall not simply contain both a <sample clause> and a <row pattern recognition clause and name>.
            Ctx.Error() << "Source shall not simply contain both a sample clause and a row pattern recognition clause";
            return {};
        }
        auto matchRecognizeClause = TSqlMatchRecognizeClause(Ctx, Mode);
        auto matchRecognize = matchRecognizeClause.CreateBuilder(node.GetBlock2().GetRule_row_pattern_recognition_clause1());
        singleSource->SetMatchRecognize(matchRecognize);
    }
    if (node.HasBlock3()) {
        TString label;
        switch (node.GetBlock3().GetBlock1().Alt_case()) {
            case TRule_named_single_source_TBlock3_TBlock1::kAlt1:
                label = Id(node.GetBlock3().GetBlock1().GetAlt1().GetRule_an_id2(), *this);
                break;
            case TRule_named_single_source_TBlock3_TBlock1::kAlt2:
                label = Id(node.GetBlock3().GetBlock1().GetAlt2().GetRule_an_id_as_compat1(), *this);
                if (!Ctx.AnsiOptionalAs) {
                    // AS is mandatory
                    Ctx.Error() << "Expecting mandatory AS here. Did you miss comma? Please add PRAGMA AnsiOptionalAs; for ANSI compatibility";
                    return {};
                }
                break;
            case TRule_named_single_source_TBlock3_TBlock1::ALT_NOT_SET:
                Y_ABORT("You should change implementation according to grammar changes");
        }
        singleSource->SetLabel(label);
    }
    if (node.HasBlock4()) {
        ESampleClause sampleClause;
        ESampleMode mode;
        TSqlExpression expr(Ctx, Mode);
        TNodePtr samplingRateNode;
        TNodePtr samplingSeedNode;
        const auto& sampleBlock = node.GetBlock4();
        TPosition pos;
        switch (sampleBlock.Alt_case()) {
        case TRule_named_single_source::TBlock4::kAlt1:
            {
                sampleClause = ESampleClause::Sample;
                mode = ESampleMode::Bernoulli;
                const auto& sampleExpr = sampleBlock.GetAlt1().GetRule_sample_clause1().GetRule_expr2();
                samplingRateNode = expr.Build(sampleExpr);
                if (!samplingRateNode) {
                    return nullptr;
                }
                pos = GetPos(sampleBlock.GetAlt1().GetRule_sample_clause1().GetToken1());
                Ctx.IncrementMonCounter("sql_features", "SampleClause");
            }
            break;
        case TRule_named_single_source::TBlock4::kAlt2:
            {
                sampleClause = ESampleClause::TableSample;
                const auto& tableSampleClause = sampleBlock.GetAlt2().GetRule_tablesample_clause1();
                const auto& modeToken = tableSampleClause.GetRule_sampling_mode2().GetToken1();
                const TCiString& token = Token(modeToken);
                if (token == "system") {
                    mode = ESampleMode::System;
                } else if (token == "bernoulli") {
                    mode = ESampleMode::Bernoulli;
                } else {
                    Ctx.Error(GetPos(modeToken)) << "Unsupported sampling mode: " << token;
                    Ctx.IncrementMonCounter("sql_errors", "UnsupportedSamplingMode");
                    return nullptr;
                }
                const auto& tableSampleExpr = tableSampleClause.GetRule_expr4();
                samplingRateNode = expr.Build(tableSampleExpr);
                if (!samplingRateNode) {
                    return nullptr;
                }
                if (tableSampleClause.HasBlock6()) {
                    const auto& repeatableExpr = tableSampleClause.GetBlock6().GetRule_repeatable_clause1().GetRule_expr3();
                    samplingSeedNode = expr.Build(repeatableExpr);
                    if (!samplingSeedNode) {
                        return nullptr;
                    }
                }
                pos = GetPos(sampleBlock.GetAlt2().GetRule_tablesample_clause1().GetToken1());
                Ctx.IncrementMonCounter("sql_features", "SampleClause");
            }
            break;
        case TRule_named_single_source::TBlock4::ALT_NOT_SET:
            Y_ABORT("SampleClause: does not corresond to grammar changes");
        }
        if (!singleSource->SetSamplingOptions(Ctx, pos, sampleClause, mode, samplingRateNode, samplingSeedNode)) {
            Ctx.IncrementMonCounter("sql_errors", "IncorrectSampleClause");
            return nullptr;
        }
    }
    return singleSource;
}

bool TSqlSelect::ColumnName(TVector<TNodePtr>& keys, const TRule_column_name& node) {
    const auto sourceName = OptIdPrefixAsStr(node.GetRule_opt_id_prefix1(), *this);
    const auto columnName = Id(node.GetRule_an_id2(), *this);
    if (columnName.empty()) {
        // TDOD: Id() should return TMaybe<TString>
        if (!Ctx.HasPendingErrors) {
            Ctx.Error() << "Empty column name is not allowed";
        }
        return false;
    }
    keys.push_back(BuildColumn(Ctx.Pos(), columnName, sourceName));
    return true;
}

bool TSqlSelect::ColumnName(TVector<TNodePtr>& keys, const TRule_without_column_name& node) {
    // without_column_name: (an_id DOT an_id) | an_id_without;
    TString sourceName;
    TString columnName;
    switch (node.Alt_case()) {
        case TRule_without_column_name::kAltWithoutColumnName1:
            sourceName = Id(node.GetAlt_without_column_name1().GetRule_an_id1(), *this);
            columnName = Id(node.GetAlt_without_column_name1().GetRule_an_id3(), *this);
            break;
        case TRule_without_column_name::kAltWithoutColumnName2:
            columnName = Id(node.GetAlt_without_column_name2().GetRule_an_id_without1(), *this);
            break;
        case TRule_without_column_name::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }

    if (columnName.empty()) {
        // TDOD: Id() should return TMaybe<TString>
        if (!Ctx.HasPendingErrors) {
            Ctx.Error() << "Empty column name is not allowed";
        }
        return false;
    }
    keys.push_back(BuildColumn(Ctx.Pos(), columnName, sourceName));
    return true;
}

template<typename TRule>
bool TSqlSelect::ColumnList(TVector<TNodePtr>& keys, const TRule& node) {
    bool result;
    if constexpr (std::is_same_v<TRule, TRule_column_list>) {
        result = ColumnName(keys, node.GetRule_column_name1());
    } else {
        result = ColumnName(keys, node.GetRule_without_column_name1());
    }

    if (!result) {
        return false;
    }

    for (auto b: node.GetBlock2()) {
        Token(b.GetToken1());
        if constexpr (std::is_same_v<TRule, TRule_column_list>) {
            result = ColumnName(keys, b.GetRule_column_name2());
        } else {
            result = ColumnName(keys, b.GetRule_without_column_name2());
        }
        if (!result) {
            return false;
        }
    }
    return true;
}

bool TSqlSelect::NamedColumn(TVector<TNodePtr>& columnList, const TRule_named_column& node) {
    if (!ColumnName(columnList, node.GetRule_column_name1())) {
        return false;
    }
    if (node.HasBlock2()) {
        const auto label = Id(node.GetBlock2().GetRule_an_id2(), *this);
        columnList.back()->SetLabel(label);
    }
    return true;
}

TSourcePtr TSqlSelect::ProcessCore(const TRule_process_core& node, const TWriteSettings& settings, TPosition& selectPos) {
    // PROCESS STREAM? named_single_source (COMMA named_single_source)* (USING using_call_expr (AS an_id)?
    // (WITH external_call_settings)?
    // (WHERE expr)? (HAVING expr)? (ASSUME order_by_clause)?)?

    Token(node.GetToken1());
    TPosition startPos(Ctx.Pos());

    if (!selectPos) {
        selectPos = startPos;
    }

    const bool hasUsing = node.HasBlock5();
    const bool unorderedSubquery = hasUsing;
    TSourcePtr source(NamedSingleSource(node.GetRule_named_single_source3(), unorderedSubquery));
    if (!source) {
        return nullptr;
    }
    if (node.GetBlock4().size()) {
        TVector<TSourcePtr> sources(1, source);
        for (auto& s: node.GetBlock4()) {
            sources.push_back(NamedSingleSource(s.GetRule_named_single_source2(), unorderedSubquery));
            if (!sources.back()) {
                return nullptr;
            }
        }
        auto pos = source->GetPos();
        source = BuildMuxSource(pos, std::move(sources));
    }

    const bool processStream = node.HasBlock2();

    if (!hasUsing) {
        return BuildProcess(startPos, std::move(source), nullptr, false, {}, false, processStream, settings, {});
    }

    const auto& block5 = node.GetBlock5();
    if (block5.HasBlock5()) {
        TSqlExpression expr(Ctx, Mode);
        TColumnRefScope scope(Ctx, EColumnRefState::Allow);
        TNodePtr where = expr.Build(block5.GetBlock5().GetRule_expr2());
        if (!where || !source->AddFilter(Ctx, where)) {
            return nullptr;
        }
        Ctx.IncrementMonCounter("sql_features", "ProcessWhere");
    } else {
        Ctx.IncrementMonCounter("sql_features", processStream ? "ProcessStream" : "Process");
    }

    if (block5.HasBlock6()) {
        Ctx.Error() << "PROCESS does not allow HAVING yet! You may request it on yql@ maillist.";
        return nullptr;
    }

    bool listCall = false;
    TSqlCallExpr call(Ctx, Mode);
    bool initRet = call.Init(block5.GetRule_using_call_expr2());
    if (initRet) {
        call.IncCounters();
    }

    if (!initRet) {
        return nullptr;
    }

    auto args = call.GetArgs();
    for (auto& arg: args) {
        if (auto placeholder = dynamic_cast<TTableRows*>(arg.Get())) {
            if (listCall) {
                Ctx.Error() << "Only one TableRows() argument is allowed.";
                return nullptr;
            }
            listCall = true;
        }
    }

    if (!call.IsExternal() && block5.HasBlock4()) {
        Ctx.Error() << "PROCESS without USING EXTERNAL FUNCTION doesn't allow WITH block";
        return nullptr;
    }

    if (block5.HasBlock4()) {
        const auto& block54 = block5.GetBlock4();
        if (!call.ConfigureExternalCall(block54.GetRule_external_call_settings2())) {
            return nullptr;
        }
    }

    TSqlCallExpr finalCall(call, args);
    TNodePtr with(finalCall.IsExternal() ? finalCall.BuildCall() : finalCall.BuildUdf(/* forReduce = */ false));
    if (!with) {
        return {};
    }
    args = finalCall.GetArgs();
    if (call.IsExternal())
        listCall = true;

    if (block5.HasBlock3()) {
        with->SetLabel(Id(block5.GetBlock3().GetRule_an_id2(), *this));
    }

    if (call.IsExternal() && block5.HasBlock7()) {
        Ctx.Error() << "PROCESS with USING EXTERNAL FUNCTION doesn't allow ASSUME block";
        return nullptr;
    }

    TVector<TSortSpecificationPtr> assumeOrderBy;
    if (block5.HasBlock7()) {
        if (!OrderByClause(block5.GetBlock7().GetRule_order_by_clause2(), assumeOrderBy)) {
            return nullptr;
        }
        Ctx.IncrementMonCounter("sql_features", IsColumnsOnly(assumeOrderBy) ? "AssumeOrderBy" : "AssumeOrderByExpr");
    }

    return BuildProcess(startPos, std::move(source), with, finalCall.IsExternal(), std::move(args), listCall, processStream, settings, assumeOrderBy);
}

TSourcePtr TSqlSelect::ReduceCore(const TRule_reduce_core& node, const TWriteSettings& settings, TPosition& selectPos) {
    // REDUCE named_single_source (COMMA named_single_source)* (PRESORT sort_specification_list)?
    // ON column_list USING ALL? using_call_expr (AS an_id)?
    // (WHERE expr)? (HAVING expr)? (ASSUME order_by_clause)?
    Token(node.GetToken1());
    TPosition startPos(Ctx.Pos());
    if (!selectPos) {
        selectPos = startPos;
    }

    TSourcePtr source(NamedSingleSource(node.GetRule_named_single_source2(), true));
    if (!source) {
        return {};
    }
    if (node.GetBlock3().size()) {
        TVector<TSourcePtr> sources(1, source);
        for (auto& s: node.GetBlock3()) {
            sources.push_back(NamedSingleSource(s.GetRule_named_single_source2(), true));
            if (!sources.back()) {
                return nullptr;
            }
        }
        auto pos = source->GetPos();
        source = BuildMuxSource(pos, std::move(sources));
    }

    TVector<TSortSpecificationPtr> orderBy;
    if (node.HasBlock4()) {
        if (!SortSpecificationList(node.GetBlock4().GetRule_sort_specification_list2(), orderBy)) {
            return {};
        }
    }

    TVector<TNodePtr> keys;
    if (!ColumnList(keys, node.GetRule_column_list6())) {
        return nullptr;
    }

    if (node.HasBlock11()) {
        TColumnRefScope scope(Ctx, EColumnRefState::Allow);
        TSqlExpression expr(Ctx, Mode);
        TNodePtr where = expr.Build(node.GetBlock11().GetRule_expr2());
        if (!where || !source->AddFilter(Ctx, where)) {
            return nullptr;
        }
        Ctx.IncrementMonCounter("sql_features", "ReduceWhere");
    } else {
        Ctx.IncrementMonCounter("sql_features", "Reduce");
    }

    TNodePtr having;
    if (node.HasBlock12()) {
        TColumnRefScope scope(Ctx, EColumnRefState::Allow);
        TSqlExpression expr(Ctx, Mode);
        having = expr.Build(node.GetBlock12().GetRule_expr2());
        if (!having) {
            return nullptr;
        }
    }

    bool listCall = false;
    TSqlCallExpr call(Ctx, Mode);
    bool initRet = call.Init(node.GetRule_using_call_expr9());
    if (initRet) {
        call.IncCounters();
    }

    if (!initRet) {
        return nullptr;
    }

    auto args = call.GetArgs();
    for (auto& arg: args) {
        if (auto placeholder = dynamic_cast<TTableRows*>(arg.Get())) {
            if (listCall) {
                Ctx.Error() << "Only one TableRows() argument is allowed.";
                return nullptr;
            }
            listCall = true;
        }
    }

    TSqlCallExpr finalCall(call, args);

    TNodePtr udf(finalCall.BuildUdf(/* forReduce = */ true));
    if (!udf) {
        return {};
    }

    if (node.HasBlock10()) {
        udf->SetLabel(Id(node.GetBlock10().GetRule_an_id2(), *this));
    }

    const auto reduceMode = node.HasBlock8() ? ReduceMode::ByAll : ReduceMode::ByPartition;

    TVector<TSortSpecificationPtr> assumeOrderBy;
    if (node.HasBlock13()) {
        if (!OrderByClause(node.GetBlock13().GetRule_order_by_clause2(), assumeOrderBy)) {
            return nullptr;
        }
        Ctx.IncrementMonCounter("sql_features", IsColumnsOnly(assumeOrderBy) ? "AssumeOrderBy" : "AssumeOrderByExpr");
    }

    return BuildReduce(startPos, reduceMode, std::move(source), std::move(orderBy), std::move(keys), std::move(args), udf, having,
        settings, assumeOrderBy, listCall);
}

TSourcePtr TSqlSelect::SelectCore(const TRule_select_core& node, const TWriteSettings& settings, TPosition& selectPos,
    TMaybe<TSelectKindPlacement> placement, TVector<TSortSpecificationPtr>& selectOpOrderBy, bool& selectOpAssumeOrderBy)
{
    // (FROM join_source)? SELECT STREAM? opt_set_quantifier result_column (COMMA result_column)* COMMA? (WITHOUT column_list)? (FROM join_source)? (WHERE expr)?
    // group_by_clause? (HAVING expr)? window_clause? ext_order_by_clause?
    selectOpOrderBy = {};
    selectOpAssumeOrderBy = false;
    if (node.HasBlock1()) {
        Token(node.GetBlock1().GetToken1());
    } else {
        Token(node.GetToken2());
    }

    TPosition startPos(Ctx.Pos());
    if (!selectPos) {
        selectPos = Ctx.Pos();
    }

    const auto hints = Ctx.PullHintForToken(selectPos);
    TColumnsSets uniqueSets, distinctSets;
    for (const auto& hint : hints) {
        if (const auto& name = to_lower(hint.Name); name == "unique")
            uniqueSets.insert_unique(NSorted::TSimpleSet<TString>(hint.Values.cbegin(), hint.Values.cend()));
        else if (name == "distinct") {
            uniqueSets.insert_unique(NSorted::TSimpleSet<TString>(hint.Values.cbegin(), hint.Values.cend()));
            distinctSets.insert_unique(NSorted::TSimpleSet<TString>(hint.Values.cbegin(), hint.Values.cend()));
        } else {
            Ctx.Warning(hint.Pos, TIssuesIds::YQL_UNUSED_HINT) << "Hint " << hint.Name << " will not be used";
        }
    }

    const bool distinct = IsDistinctOptSet(node.GetRule_opt_set_quantifier4());
    if (distinct) {
        Ctx.IncrementMonCounter("sql_features", "DistinctInSelect");
    }

    TSourcePtr source(BuildFakeSource(selectPos, /* missingFrom = */ true, Mode == NSQLTranslation::ESqlMode::SUBQUERY));
    if (node.HasBlock1() && node.HasBlock9()) {
        Token(node.GetBlock9().GetToken1());
        Ctx.IncrementMonCounter("sql_errors", "DoubleFrom");
        Ctx.Error() << "Only one FROM clause is allowed";
        return nullptr;
    }
    if (node.HasBlock1()) {
        source = JoinSource(node.GetBlock1().GetRule_join_source2());
        Ctx.IncrementMonCounter("sql_features", "FromInFront");
    } else if (node.HasBlock9()) {
        source = JoinSource(node.GetBlock9().GetRule_join_source2());
    }
    if (!source) {
        return nullptr;
    }

    const bool selectStream = node.HasBlock3();
    TVector<TNodePtr> without;
    if (node.HasBlock8()) {
        if (!ColumnList(without, node.GetBlock8().GetRule_without_column_list2())) {
            return nullptr;
        }
    }
    if (node.HasBlock10()) {
        auto block = node.GetBlock10();
        Token(block.GetToken1());
        TPosition pos(Ctx.Pos());
        TNodePtr where;
        {
            TColumnRefScope scope(Ctx, EColumnRefState::Allow);
            TSqlExpression expr(Ctx, Mode);
            where = expr.Build(block.GetRule_expr2());
        }
        if (!where) {
            Ctx.IncrementMonCounter("sql_errors", "WhereInvalid");
            return nullptr;
        }
        if (!source->AddFilter(Ctx, where)) {
            Ctx.IncrementMonCounter("sql_errors", "WhereNotSupportedBySource");
            return nullptr;
        }
        Ctx.IncrementMonCounter("sql_features", "Where");
    }

    /// \todo merge gtoupByExpr and groupBy in one
    TVector<TNodePtr> groupByExpr, groupBy;
    TLegacyHoppingWindowSpecPtr legacyHoppingWindowSpec;
    bool compactGroupBy = false;
    TString groupBySuffix;
    if (node.HasBlock11()) {
        TGroupByClause clause(Ctx, Mode);
        if (!clause.Build(node.GetBlock11().GetRule_group_by_clause1())) {
            return nullptr;
        }
        bool hasHopping = (bool)clause.GetLegacyHoppingWindow();
        for (const auto& exprAlias: clause.Aliases()) {
            YQL_ENSURE(exprAlias.first == exprAlias.second->GetLabel());
            groupByExpr.emplace_back(exprAlias.second);
            hasHopping |= (bool)dynamic_cast<THoppingWindow*>(exprAlias.second.Get());
        }
        groupBy = std::move(clause.Content());
        clause.SetFeatures("sql_features");
        legacyHoppingWindowSpec = clause.GetLegacyHoppingWindow();
        compactGroupBy = clause.IsCompactGroupBy();
        groupBySuffix = clause.GetSuffix();

        if (source->IsStream() && !hasHopping) {
            Ctx.Error() << "Streaming group by query must have a hopping window specification.";
            return nullptr;
        }
    }

    TNodePtr having;
    if (node.HasBlock12()) {
        TSqlExpression expr(Ctx, Mode);
        TColumnRefScope scope(Ctx, EColumnRefState::Allow);
        having = expr.Build(node.GetBlock12().GetRule_expr2());
        if (!having) {
            return nullptr;
        }
        Ctx.IncrementMonCounter("sql_features", "Having");
    }

    TWinSpecs windowSpec;
    if (node.HasBlock13()) {
        if (source->IsStream()) {
            Ctx.Error() << "WINDOW is not allowed in streaming queries";
            return nullptr;
        }
        if (!WindowClause(node.GetBlock13().GetRule_window_clause1(), windowSpec)) {
            return nullptr;
        }
        Ctx.IncrementMonCounter("sql_features", "WindowClause");
    }

    bool assumeSorted = false;
    TVector<TSortSpecificationPtr> orderBy;
    if (node.HasBlock14()) {
        auto& orderBlock = node.GetBlock14().GetRule_ext_order_by_clause1();
        assumeSorted = orderBlock.HasBlock1();

        Token(orderBlock.GetRule_order_by_clause2().GetToken1());

        if (source->IsStream()) {
            Ctx.Error() << "ORDER BY is not allowed in streaming queries";
            return nullptr;
        }

        if (!ValidateLimitOrderByWithSelectOp(placement, "ORDER BY")) {
            return nullptr;
        }

        if (!OrderByClause(orderBlock.GetRule_order_by_clause2(), orderBy)) {
            return nullptr;
        }
        Ctx.IncrementMonCounter("sql_features", IsColumnsOnly(orderBy)
            ? (assumeSorted ? "AssumeOrderBy" : "OrderBy")
            : (assumeSorted ? "AssumeOrderByExpr" : "OrderByExpr")
        );

        if (!NeedPassLimitOrderByToUnderlyingSelect(placement)) {
            selectOpOrderBy.swap(orderBy);
            std::swap(selectOpAssumeOrderBy, assumeSorted);
        }
    }

    TVector<TNodePtr> terms;
    {
        class TScopedWinSpecs {
        public:
            TScopedWinSpecs(TContext& ctx, TWinSpecs& specs)
                : Ctx(ctx)
            {
                Ctx.WinSpecsScopes.push_back(std::ref(specs));
            }
            ~TScopedWinSpecs() {
                Ctx.WinSpecsScopes.pop_back();
            }
        private:
            TContext& Ctx;
        };


        TScopedWinSpecs scoped(Ctx, windowSpec);
        if (!SelectTerm(terms, node.GetRule_result_column5())) {
            return nullptr;
        }
        for (auto block: node.GetBlock6()) {
            if (!SelectTerm(terms, block.GetRule_result_column2())) {
                return nullptr;
            }
        }

    }
    if (!ValidateSelectColumns(terms)) {
        return nullptr;
    }
    return BuildSelectCore(Ctx, startPos, std::move(source), groupByExpr, groupBy, compactGroupBy, groupBySuffix, assumeSorted, orderBy, having,
        std::move(windowSpec), legacyHoppingWindowSpec, std::move(terms), distinct, std::move(without), selectStream, settings, std::move(uniqueSets), std::move(distinctSets));
}

bool TSqlSelect::WindowDefinition(const TRule_window_definition& rule, TWinSpecs& winSpecs) {
    const TString windowName = Id(rule.GetRule_new_window_name1().GetRule_window_name1().GetRule_an_id_window1(), *this);
    if (winSpecs.contains(windowName)) {
        Ctx.Error() << "Unable to declare window with same name: " << windowName;
        return false;
    }
    auto windowSpec = WindowSpecification(rule.GetRule_window_specification3().GetRule_window_specification_details2());
    if (!windowSpec) {
        return false;
    }
    winSpecs.emplace(windowName, std::move(windowSpec));
    return true;
}

bool TSqlSelect::WindowClause(const TRule_window_clause& rule, TWinSpecs& winSpecs) {
    auto windowList = rule.GetRule_window_definition_list2();
    if (!WindowDefinition(windowList.GetRule_window_definition1(), winSpecs)) {
        return false;
    }
    for (auto& block: windowList.GetBlock2()) {
        if (!WindowDefinition(block.GetRule_window_definition2(), winSpecs)) {
            return false;
        }
    }
    return true;
}

bool TSqlTranslation::OrderByClause(const TRule_order_by_clause& node, TVector<TSortSpecificationPtr>& orderBy) {
    return SortSpecificationList(node.GetRule_sort_specification_list3(), orderBy);
}

bool TSqlSelect::ValidateLimitOrderByWithSelectOp(TMaybe<TSelectKindPlacement> placement, TStringBuf what) {
    if (!placement.Defined()) {
        // not in select_op chain
        return true;
    }

    if (!placement->IsLastInSelectOp) {
        Ctx.Error() << what << " within UNION ALL is only allowed after last subquery";
        return false;
    }
    return true;
}

bool TSqlSelect::NeedPassLimitOrderByToUnderlyingSelect(TMaybe<TSelectKindPlacement> placement) {
    return !placement.Defined() || !placement->IsLastInSelectOp;
}

TSqlSelect::TSelectKindResult TSqlSelect::SelectKind(const TRule_select_kind_partial& node, TPosition& selectPos,
    TMaybe<TSelectKindPlacement> placement)
{
    auto res = SelectKind(node.GetRule_select_kind1(), selectPos, placement);
    if (!res) {
        return {};
    }
    TPosition startPos(Ctx.Pos());
    /// LIMIT INTEGER block
    TNodePtr skipTake;
    if (node.HasBlock2()) {
        auto block = node.GetBlock2();

        Token(block.GetToken1());
        TPosition pos(Ctx.Pos());

        if (!ValidateLimitOrderByWithSelectOp(placement, "LIMIT")) {
            return {};
        }

        TSqlExpression takeExpr(Ctx, Mode);
        auto take = takeExpr.Build(block.GetRule_expr2());
        if (!take) {
            return{};
        }

        TNodePtr skip;
        if (block.HasBlock3()) {
            TSqlExpression skipExpr(Ctx, Mode);
            skip = skipExpr.Build(block.GetBlock3().GetRule_expr2());
            if (!skip) {
                return {};
            }
            if (Token(block.GetBlock3().GetToken1()) == ",") {
                // LIMIT skip, take
                skip.Swap(take);
                Ctx.IncrementMonCounter("sql_features", "LimitSkipTake");
            } else {
                Ctx.IncrementMonCounter("sql_features", "LimitOffset");
            }
        }

        auto st = BuildSkipTake(pos, skip, take);
        if (NeedPassLimitOrderByToUnderlyingSelect(placement)) {
            skipTake = st;
        } else {
            res.SelectOpSkipTake = st;
        }

        Ctx.IncrementMonCounter("sql_features", "Limit");
    }

    res.Source = BuildSelect(startPos, std::move(res.Source), skipTake);
    return res;
}

TSqlSelect::TSelectKindResult TSqlSelect::SelectKind(const TRule_select_kind& node, TPosition& selectPos,
    TMaybe<TSelectKindPlacement> placement)
{
    const bool discard = node.HasBlock1();
    const bool hasLabel = node.HasBlock3();
    if (hasLabel && (Mode == NSQLTranslation::ESqlMode::LIMITED_VIEW || Mode == NSQLTranslation::ESqlMode::SUBQUERY)) {
        Ctx.Error() << "INTO RESULT is not allowed in current mode";
        return {};
    }

    if (discard && hasLabel) {
        Ctx.Error() << "DISCARD and INTO RESULT cannot be used at the same time";
        return {};
    }

    if (discard && !selectPos) {
        selectPos = Ctx.TokenPosition(node.GetBlock1().GetToken1());
    }

    TWriteSettings settings;
    settings.Discard = discard;
    if (hasLabel) {
        settings.Label = PureColumnOrNamed(node.GetBlock3().GetRule_pure_column_or_named3(), *this);
    }

    TSelectKindResult res;
    if (placement.Defined()) {
        if (placement->IsFirstInSelectOp) {
            res.Settings.Discard = settings.Discard;
        } else if (settings.Discard) {
            auto discardPos = Ctx.TokenPosition(node.GetBlock1().GetToken1());
            Ctx.Error(discardPos) << "DISCARD within UNION ALL is only allowed before first subquery";
            return {};
        }

        if (placement->IsLastInSelectOp) {
            res.Settings.Label = settings.Label;
        } else if (!settings.Label.Empty()) {
            auto labelPos = Ctx.TokenPosition(node.GetBlock3().GetToken1());
            Ctx.Error(labelPos) << "INTO RESULT within UNION ALL is only allowed after last subquery";
            return {};
        }

        settings = {};
    }

    switch (node.GetBlock2().Alt_case()) {
        case TRule_select_kind_TBlock2::kAlt1:
            res.Source = ProcessCore(node.GetBlock2().GetAlt1().GetRule_process_core1(), settings, selectPos);
            break;
        case TRule_select_kind_TBlock2::kAlt2:
            res.Source = ReduceCore(node.GetBlock2().GetAlt2().GetRule_reduce_core1(), settings, selectPos);
            break;
        case TRule_select_kind_TBlock2::kAlt3: {
            res.Source = SelectCore(node.GetBlock2().GetAlt3().GetRule_select_core1(), settings, selectPos,
                placement, res.SelectOpOrderBy, res.SelectOpAssumeOrderBy);
            break;
        }
        case TRule_select_kind_TBlock2::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }

    return res;
}

TSqlSelect::TSelectKindResult TSqlSelect::SelectKind(const TRule_select_kind_parenthesis& node, TPosition& selectPos,
    TMaybe<TSelectKindPlacement> placement)
{
    if (node.Alt_case() == TRule_select_kind_parenthesis::kAltSelectKindParenthesis1) {
        return SelectKind(node.GetAlt_select_kind_parenthesis1().GetRule_select_kind_partial1(), selectPos, placement);
    } else {
        return SelectKind(node.GetAlt_select_kind_parenthesis2().GetRule_select_kind_partial2(), selectPos, {});
    }
}

template<typename TRule>
TSourcePtr TSqlSelect::Build(const TRule& node, TPosition pos, TSelectKindResult&& first) {
    if (node.GetBlock2().empty()) {
        return std::move(first.Source);
    }

    auto blocks = node.GetBlock2();

    TPosition unionPos = pos; // Position of first select
    TVector<TSortSpecificationPtr> orderBy;
    bool assumeOrderBy = false;
    TNodePtr skipTake;
    TWriteSettings outermostSettings;
    outermostSettings.Discard = first.Settings.Discard;

    TVector<TSourcePtr> sources{ std::move(first.Source)};
    bool currentQuantifier = false;

    for (int i = 0; i < blocks.size(); ++i) {
        auto& b = blocks[i];
        const bool second = (i == 0);
        const bool last = (i + 1 == blocks.size());
        TSelectKindPlacement placement;
        placement.IsLastInSelectOp = last;

        TSelectKindResult next = SelectKind(b.GetRule_select_kind_parenthesis2(), pos, placement);
        if (!next) {
            return nullptr;
        }

        if (last) {
            orderBy = next.SelectOpOrderBy;
            assumeOrderBy = next.SelectOpAssumeOrderBy;
            skipTake = next.SelectOpSkipTake;
            outermostSettings.Label = next.Settings.Label;
        }

        switch (b.GetRule_select_op1().Alt_case()) {
            case TRule_select_op::kAltSelectOp1:
                break;
            case TRule_select_op::kAltSelectOp2:
            case TRule_select_op::kAltSelectOp3:
                Ctx.Error() << "INTERSECT and EXCEPT are not implemented yet";
                return nullptr;
            case TRule_select_op::ALT_NOT_SET:
                Y_ABORT("You should change implementation according to grammar changes");
        }

        const bool quantifier = b.GetRule_select_op1().GetAlt_select_op1().HasBlock2();

        if (!second && quantifier != currentQuantifier) {
            auto source = BuildUnion(pos, std::move(sources), currentQuantifier, {});
            sources.clear();
            sources.emplace_back(std::move(source));
        }

        sources.emplace_back(std::move(next.Source));
        currentQuantifier = quantifier;
    }

    auto result = BuildUnion(pos, std::move(sources), currentQuantifier, outermostSettings);

    if (orderBy) {
        TVector<TNodePtr> groupByExpr;
        TVector<TNodePtr> groupBy;
        bool compactGroupBy = false;
        TString groupBySuffix = "";
        TNodePtr having;
        TWinSpecs winSpecs;
        TLegacyHoppingWindowSpecPtr legacyHoppingWindowSpec;
        bool distinct = false;
        TVector<TNodePtr> without;
        bool stream = false;

        TVector<TNodePtr> terms;
        terms.push_back(BuildColumn(unionPos, "*", ""));

        result = BuildSelectCore(Ctx, unionPos, std::move(result), groupByExpr, groupBy, compactGroupBy, groupBySuffix,
            assumeOrderBy, orderBy, having, std::move(winSpecs), legacyHoppingWindowSpec, std::move(terms),
            distinct, std::move(without), stream, outermostSettings, {}, {});

        result = BuildSelect(unionPos, std::move(result), skipTake);
    } else if (skipTake) {
        result = BuildSelect(unionPos, std::move(result), skipTake);
    }

    return result;
}

TSourcePtr TSqlSelect::Build(const TRule_select_stmt& node, TPosition& selectPos) {
    TMaybe<TSelectKindPlacement> placement;
    if (!node.GetBlock2().empty()) {
        placement.ConstructInPlace();
        placement->IsFirstInSelectOp = true;
    }

    auto res = SelectKind(node.GetRule_select_kind_parenthesis1(), selectPos, placement);
    if (!res) {
        return nullptr;
    }

    return Build(node, selectPos, std::move(res));
}

TSourcePtr TSqlSelect::Build(const TRule_select_unparenthesized_stmt& node, TPosition& selectPos) {
    TMaybe<TSelectKindPlacement> placement;
    if (!node.GetBlock2().empty()) {
        placement.ConstructInPlace();
        placement->IsFirstInSelectOp = true;
    }

    auto res = SelectKind(node.GetRule_select_kind_partial1(), selectPos, placement);
    if (!res) {
        return nullptr;
    }

    return Build(node, selectPos, std::move(res));
}

} // namespace NSQLTranslationV1
