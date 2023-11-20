#include "sql_group_by.h"
#include "sql_expression.h"
#include "source.h"
#include <ydb/library/yql/core/yql_expr_type_annotation.h>

namespace NSQLTranslationV1 {

using namespace NSQLv1Generated;

const TString TGroupByClause::AutogenerateNamePrefix = "group";

bool TGroupByClause::Build(const TRule_group_by_clause& node) {
    // group_by_clause: GROUP COMPACT? BY opt_set_quantifier grouping_element_list (WITH an_id)?;
    if (Ctx.CompactGroupBy.Defined()) {
        CompactGroupBy = *Ctx.CompactGroupBy;
    } else {
        CompactGroupBy = node.HasBlock2();
        if (!CompactGroupBy) {
            auto hints = Ctx.PullHintForToken(Ctx.TokenPosition(node.GetToken1()));
            CompactGroupBy = AnyOf(hints, [](const NSQLTranslation::TSQLHint& hint) { return to_lower(hint.Name) == "compact"; });
        }
    }
    TPosition distinctPos;
    if (IsDistinctOptSet(node.GetRule_opt_set_quantifier4(), distinctPos)) {
        Ctx.Error(distinctPos) << "DISTINCT is not supported in GROUP BY clause yet!";
        Ctx.IncrementMonCounter("sql_errors", "DistinctInGroupByNotSupported");
        return false;
    }
    if (!ParseList(node.GetRule_grouping_element_list5(), EGroupByFeatures::Ordinary)) {
        return false;
    }

    if (node.HasBlock6()) {
        TString mode = Id(node.GetBlock6().GetRule_an_id2(), *this);
        TMaybe<TIssue> normalizeError = NormalizeName(Ctx.Pos(), mode);
        if (!normalizeError.Empty()) {
            Error() << normalizeError->GetMessage();
            Ctx.IncrementMonCounter("sql_errors", "NormalizeGroupByModeError");
            return false;
        }

        if (mode == "combine") {
            Suffix = "Combine";
        } else if (mode == "combinestate") {
            Suffix = "CombineState";
        } else if (mode == "mergestate") {
            Suffix = "MergeState";
        } else if (mode == "finalize") {
            Suffix = "Finalize";
        } else if (mode == "mergefinalize") {
            Suffix = "MergeFinalize";
        } else if (mode == "mergemanyfinalize") {
            Suffix = "MergeManyFinalize";
        } else {
            Ctx.Error() << "Unsupported group by mode: " << mode;
            Ctx.IncrementMonCounter("sql_errors", "GroupByModeUnknown");
            return false;
        }
    }

    if (!ResolveGroupByAndGrouping()) {
        return false;
    }
    return true;
}

bool TGroupByClause::ParseList(const TRule_grouping_element_list& groupingListNode, EGroupByFeatures featureContext) {
    if (!GroupingElement(groupingListNode.GetRule_grouping_element1(), featureContext)) {
        return false;
    }
    for (auto b: groupingListNode.GetBlock2()) {
        if (!GroupingElement(b.GetRule_grouping_element2(), featureContext)) {
            return false;
        }
    }
    return true;
}

void TGroupByClause::SetFeatures(const TString& field) const {
    Ctx.IncrementMonCounter(field, "GroupBy");
    const auto& features = Features();
    if (features.Test(EGroupByFeatures::Ordinary)) {
        Ctx.IncrementMonCounter(field, "GroupByOrdinary");
    }
    if (features.Test(EGroupByFeatures::Expression)) {
        Ctx.IncrementMonCounter(field, "GroupByExpression");
    }
    if (features.Test(EGroupByFeatures::Rollup)) {
        Ctx.IncrementMonCounter(field, "GroupByRollup");
    }
    if (features.Test(EGroupByFeatures::Cube)) {
        Ctx.IncrementMonCounter(field, "GroupByCube");
    }
    if (features.Test(EGroupByFeatures::GroupingSet)) {
        Ctx.IncrementMonCounter(field, "GroupByGroupingSet");
    }
    if (features.Test(EGroupByFeatures::Empty)) {
        Ctx.IncrementMonCounter(field, "GroupByEmpty");
    }
}

TVector<TNodePtr>& TGroupByClause::Content() {
    return GroupBySet;
}

TMap<TString, TNodePtr>& TGroupByClause::Aliases() {
    return GroupSetContext->NodeAliases;
}

TLegacyHoppingWindowSpecPtr TGroupByClause::GetLegacyHoppingWindow() const {
    return LegacyHoppingWindowSpec;
}

bool TGroupByClause::IsCompactGroupBy() const {
    return CompactGroupBy;
}

TString TGroupByClause::GetSuffix() const {
    return Suffix;
}

TMaybe<TVector<TNodePtr>> TGroupByClause::MultiplyGroupingSets(const TVector<TNodePtr>& lhs, const TVector<TNodePtr>& rhs) const {
    TVector<TNodePtr> content;
    for (const auto& leftNode: lhs) {
        auto leftPtr = leftNode->ContentListPtr();
        if (!leftPtr) {
            // TODO: shouldn't happen
            Ctx.Error() << "Unable to multiply grouping sets";
            return {};
        }
        for (const auto& rightNode: rhs) {
            TVector<TNodePtr> mulItem(leftPtr->begin(), leftPtr->end());
            auto rightPtr = rightNode->ContentListPtr();
            if (!rightPtr) {
                // TODO: shouldn't happen
                Ctx.Error() << "Unable to multiply grouping sets";
                return {};
            }
            mulItem.insert(mulItem.end(), rightPtr->begin(), rightPtr->end());
            content.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(mulItem)));
        }
    }
    return content;
}

bool TGroupByClause::ResolveGroupByAndGrouping() {
    auto listPos = std::find_if(GroupBySet.begin(), GroupBySet.end(), [](const TNodePtr& node) {
        return node->ContentListPtr();
    });
    if (listPos == GroupBySet.end()) {
        return true;
    }
    auto curContent = *(*listPos)->ContentListPtr();
    if (listPos != GroupBySet.begin()) {
        TVector<TNodePtr> emulate(GroupBySet.begin(), listPos);
        TVector<TNodePtr> emulateContent(1, BuildListOfNamedNodes(Ctx.Pos(), std::move(emulate)));
        auto mult = MultiplyGroupingSets(emulateContent, curContent);
        if (!mult) {
            return false;
        }
        curContent = *mult;
    }
    for (++listPos; listPos != GroupBySet.end(); ++listPos) {
        auto newElem = (*listPos)->ContentListPtr();
        if (newElem) {
            auto mult = MultiplyGroupingSets(curContent, *newElem);
            if (!mult) {
                return false;
            }
            curContent = *mult;
        } else {
            TVector<TNodePtr> emulate(1, *listPos);
            TVector<TNodePtr> emulateContent(1, BuildListOfNamedNodes(Ctx.Pos(), std::move(emulate)));
            auto mult = MultiplyGroupingSets(curContent, emulateContent);
            if (!mult) {
                return false;
            }
            curContent = *mult;
        }
    }
    TVector<TNodePtr> result(1, BuildListOfNamedNodes(Ctx.Pos(), std::move(curContent)));
    std::swap(result, GroupBySet);
    return true;
}

bool TGroupByClause::GroupingElement(const TRule_grouping_element& node, EGroupByFeatures featureContext) {
    TSourcePtr res;
    TVector<TNodePtr> emptyContent;
    switch (node.Alt_case()) {
        case TRule_grouping_element::kAltGroupingElement1:
            if (!OrdinaryGroupingSet(node.GetAlt_grouping_element1().GetRule_ordinary_grouping_set1(), featureContext)) {
                return false;
            }
            Features().Set(EGroupByFeatures::Ordinary);
            break;
        case TRule_grouping_element::kAltGroupingElement2: {
            TGroupByClause subClause(Ctx, Mode, GroupSetContext);
            if (!subClause.OrdinaryGroupingSetList(node.GetAlt_grouping_element2().GetRule_rollup_list1().GetRule_ordinary_grouping_set_list3(),
                EGroupByFeatures::Rollup))
            {
                return false;
            }
            auto& content = subClause.Content();
            TVector<TNodePtr> collection;
            for (auto limit = content.end(), begin = content.begin(); limit != begin; --limit) {
                TVector<TNodePtr> grouping(begin, limit);
                collection.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(grouping)));
            }
            collection.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(emptyContent)));
            GroupBySet.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(collection)));
            Ctx.IncrementMonCounter("sql_features", TStringBuilder() << "GroupByRollup" << content.size());
            Features().Set(EGroupByFeatures::Rollup);
            break;
        }
        case TRule_grouping_element::kAltGroupingElement3: {
            TGroupByClause subClause(Ctx, Mode, GroupSetContext);
            if (!subClause.OrdinaryGroupingSetList(node.GetAlt_grouping_element3().GetRule_cube_list1().GetRule_ordinary_grouping_set_list3(),
                EGroupByFeatures::Cube))
            {
                return false;
            }
            auto& content = subClause.Content();
            if (content.size() > Ctx.PragmaGroupByCubeLimit) {
                Ctx.Error() << "GROUP BY CUBE is allowed only for " << Ctx.PragmaGroupByCubeLimit << " columns, but you use " << content.size();
                return false;
            }
            TVector<TNodePtr> collection;
            for (unsigned mask = (1 << content.size()) - 1; mask > 0; --mask) {
                TVector<TNodePtr> grouping;
                for (unsigned index = 0; index < content.size(); ++index) {
                    if (mask & (1 << index)) {
                        grouping.push_back(content[content.size() - index - 1]);
                    }
                }
                collection.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(grouping)));
            }
            collection.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(emptyContent)));
            GroupBySet.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(collection)));
            Ctx.IncrementMonCounter("sql_features", TStringBuilder() << "GroupByCube" << content.size());
            Features().Set(EGroupByFeatures::Cube);
            break;
        }
        case TRule_grouping_element::kAltGroupingElement4: {
            auto listNode = node.GetAlt_grouping_element4().GetRule_grouping_sets_specification1().GetRule_grouping_element_list4();
            TGroupByClause subClause(Ctx, Mode, GroupSetContext);
            if (!subClause.ParseList(listNode, EGroupByFeatures::GroupingSet)) {
                return false;
            }
            auto& content = subClause.Content();
            TVector<TNodePtr> collection;
            bool hasEmpty = false;
            for (auto& elem: content) {
                auto elemContent = elem->ContentListPtr();
                if (elemContent) {
                    if (!elemContent->empty() && elemContent->front()->ContentListPtr()) {
                        for (auto& sub: *elemContent) {
                            FeedCollection(sub, collection, hasEmpty);
                        }
                    } else {
                        FeedCollection(elem, collection, hasEmpty);
                    }
                } else {
                    TVector<TNodePtr> elemList(1, std::move(elem));
                    collection.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(elemList)));
                }
            }
            GroupBySet.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(collection)));
            Features().Set(EGroupByFeatures::GroupingSet);
            break;
        }
        case TRule_grouping_element::kAltGroupingElement5: {
            if (!HoppingWindow(node.GetAlt_grouping_element5().GetRule_hopping_window_specification1())) {
                return false;
            }
            break;
        }
        case TRule_grouping_element::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
    }
    return true;
}

void TGroupByClause::FeedCollection(const TNodePtr& elem, TVector<TNodePtr>& collection, bool& hasEmpty) const {
    auto elemContentPtr = elem->ContentListPtr();
    if (elemContentPtr && elemContentPtr->empty()) {
        if (hasEmpty) {
            return;
        }
        hasEmpty = true;
    }
    collection.push_back(elem);
}

bool TGroupByClause::OrdinaryGroupingSet(const TRule_ordinary_grouping_set& node, EGroupByFeatures featureContext) {
    TNodePtr namedExprNode;
    {
        TColumnRefScope scope(Ctx, EColumnRefState::Allow);
        namedExprNode = NamedExpr(node.GetRule_named_expr1(), EExpr::GroupBy);
    }
    if (!namedExprNode) {
        return false;
    }
    auto nodeLabel = namedExprNode->GetLabel();
    auto contentPtr = namedExprNode->ContentListPtr();
    if (contentPtr) {
        if (nodeLabel && (contentPtr->size() != 1 || contentPtr->front()->GetLabel())) {
            Ctx.Error() << "Unable to use aliases for list of named expressions";
            Ctx.IncrementMonCounter("sql_errors", "GroupByAliasForListOfExpressions");
            return false;
        }
        for (auto& content: *contentPtr) {
            auto label = content->GetLabel();
            if (!label) {
                if (content->GetColumnName()) {
                    namedExprNode->AssumeColumn();
                    continue;
                }

                if (!AllowUnnamed(content->GetPos(), featureContext)) {
                    return false;
                }

                content->SetLabel(label = GenerateGroupByExprName());
            }
            if (!AddAlias(label, content)) {
                return false;
            }
            content = BuildColumn(content->GetPos(), label);
        }
    } else {
        if (!nodeLabel && namedExprNode->GetColumnName()) {
            namedExprNode->AssumeColumn();
        }

        if (!nodeLabel && !namedExprNode->GetColumnName()) {
            if (!AllowUnnamed(namedExprNode->GetPos(), featureContext)) {
                return false;
            }
            namedExprNode->SetLabel(nodeLabel = GenerateGroupByExprName());
        }
        if (nodeLabel) {
            if (!AddAlias(nodeLabel, namedExprNode)) {
                return false;
            }
            namedExprNode = BuildColumn(namedExprNode->GetPos(), nodeLabel);
        }
    }
    GroupBySet.emplace_back(std::move(namedExprNode));
    return true;
}

bool TGroupByClause::OrdinaryGroupingSetList(const TRule_ordinary_grouping_set_list& node, EGroupByFeatures featureContext) {
    if (!OrdinaryGroupingSet(node.GetRule_ordinary_grouping_set1(), featureContext)) {
        return false;
    }
    for (auto& block: node.GetBlock2()) {
        if (!OrdinaryGroupingSet(block.GetRule_ordinary_grouping_set2(), featureContext)) {
            return false;
        }
    }
    return true;
}

bool TGroupByClause::HoppingWindow(const TRule_hopping_window_specification& node) {
    if (LegacyHoppingWindowSpec) {
        Ctx.Error() << "Duplicate hopping window specification.";
        return false;
    }
    LegacyHoppingWindowSpec = new TLegacyHoppingWindowSpec;
    {
        TColumnRefScope scope(Ctx, EColumnRefState::Allow);
        TSqlExpression expr(Ctx, Mode);
        LegacyHoppingWindowSpec->TimeExtractor = expr.Build(node.GetRule_expr3());
        if (!LegacyHoppingWindowSpec->TimeExtractor) {
            return false;
        }
    }
    auto processIntervalParam = [&] (const TRule_expr& rule) -> TNodePtr {
        TSqlExpression expr(Ctx, Mode);
        auto node = expr.Build(rule);
        if (!node) {
            return nullptr;
        }

        auto literal = node->GetLiteral("String");
        if (!literal) {
            return new TAstListNodeImpl(Ctx.Pos(), {
                new TAstAtomNodeImpl(Ctx.Pos(), "EvaluateExpr", TNodeFlags::Default),
                node
            });
        }

        const auto out = NKikimr::NMiniKQL::ValueFromString(NKikimr::NUdf::EDataSlot::Interval, *literal);
        if (!out) {
            Ctx.Error(node->GetPos()) << "Expected interval in ISO 8601 format";
            return nullptr;
        }

        if ('T' == literal->back()) {
            Ctx.Error(node->GetPos()) << "Time prefix 'T' at end of interval constant. The designator 'T' shall be absent if all of the time components are absent.";
            return nullptr;
        }

        return new TAstListNodeImpl(Ctx.Pos(), {
            new TAstAtomNodeImpl(Ctx.Pos(), "Interval", TNodeFlags::Default),
            new TAstListNodeImpl(Ctx.Pos(), {
                new TAstAtomNodeImpl(Ctx.Pos(), "quote", TNodeFlags::Default),
                new TAstAtomNodeImpl(Ctx.Pos(), ToString(out.Get<i64>()), TNodeFlags::Default)
            })
        });
    };

    LegacyHoppingWindowSpec->Hop = processIntervalParam(node.GetRule_expr5());
    if (!LegacyHoppingWindowSpec->Hop) {
        return false;
    }
    LegacyHoppingWindowSpec->Interval = processIntervalParam(node.GetRule_expr7());
    if (!LegacyHoppingWindowSpec->Interval) {
        return false;
    }
    LegacyHoppingWindowSpec->Delay = processIntervalParam(node.GetRule_expr9());
    if (!LegacyHoppingWindowSpec->Delay) {
        return false;
    }
    LegacyHoppingWindowSpec->DataWatermarks = Ctx.PragmaDataWatermarks;

    return true;
}

bool TGroupByClause::AllowUnnamed(TPosition pos, EGroupByFeatures featureContext) {
    TStringBuf feature;
    switch (featureContext) {
        case EGroupByFeatures::Ordinary:
            return true;
        case EGroupByFeatures::Rollup:
            feature = "ROLLUP";
            break;
        case EGroupByFeatures::Cube:
            feature = "CUBE";
            break;
        case EGroupByFeatures::GroupingSet:
            feature = "GROUPING SETS";
            break;
        default:
            YQL_ENSURE(false, "Unknown feature");
    }

    Ctx.Error(pos) << "Unnamed expressions are not supported in " << feature << ". Please use '<expr> AS <name>'.";
    Ctx.IncrementMonCounter("sql_errors", "GroupBySetNoAliasOrColumn");
    return false;
}

TGroupByClause::TGroupingSetFeatures& TGroupByClause::Features() {
    return GroupSetContext->GroupFeatures;
}

const TGroupByClause::TGroupingSetFeatures& TGroupByClause::Features() const {
    return GroupSetContext->GroupFeatures;
}

bool TGroupByClause::AddAlias(const TString& label, const TNodePtr& node) {
    if (Aliases().contains(label)) {
        Ctx.Error() << "Duplicated aliases not allowed";
        Ctx.IncrementMonCounter("sql_errors", "GroupByDuplicateAliases");
        return false;
    }
    Aliases().emplace(label, node);
    return true;
}

TString TGroupByClause::GenerateGroupByExprName() {
    return TStringBuilder() << AutogenerateNamePrefix << GroupSetContext->UnnamedCount++;
}

} // namespace NSQLTranslationV1
