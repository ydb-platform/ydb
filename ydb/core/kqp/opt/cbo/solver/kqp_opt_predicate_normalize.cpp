#include "kqp_opt_predicate_normalize.h"
#include "kqp_opt_stat.h"

#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql;
using namespace NYql::NNodes;

namespace NKikimr::NKqp {

namespace NPredicateTree {

/**
 * Check if a callable is an attribute of some table
 * Currently just return a boolean and cover only basic cases
 */
TMaybe<TString> IsAttribute(const TExprBase& input) {
    if (auto member = input.Maybe<TCoMember>()) {
        return TString(member.Cast().Name());
    } else if (auto cast = input.Maybe<TCoSafeCast>()) {
        return IsAttribute(cast.Cast().Value());
    } else if (auto ifPresent = input.Maybe<TCoIfPresent>()) {
        return IsAttribute(ifPresent.Cast().Optional());
    } else if (auto just = input.Maybe<TCoJust>()) {
        return IsAttribute(just.Cast().Input());
    } else if (input.Ptr()->IsCallable("PgCast")) {
        auto child = TExprBase(input.Ptr()->ChildRef(0));
        return IsAttribute(child);
    } else if (input.Ptr()->IsCallable("FromPg") || input.Ptr()->IsCallable("ToPg")) {
        auto child = TExprBase(input.Ptr()->ChildRef(0));
        return IsAttribute(child);
    } else if (auto exists = input.Maybe<TCoExists>()) {
        auto child = TExprBase(input.Ptr()->ChildRef(0));
        return IsAttribute(child);
    } else if (auto argument = input.Maybe<TCoArgument>()) {
        TString argumentName = TString(argument.Cast().Name());
        TStringBuf olapApplyMemberPrefix = "members_";
        if (argumentName.StartsWith(olapApplyMemberPrefix)) {
            return argumentName.substr(olapApplyMemberPrefix.length(), argumentName.size() - olapApplyMemberPrefix.length());
        } else {
            return argumentName;
        }
    }
    return Nothing();
}

// Returns an opposite predicate.
EInequalityPredicateType GetOppositePredicateType(EInequalityPredicateType predicate) {
    switch (predicate) {
        case EInequalityPredicateType::Less:
            return EInequalityPredicateType::Greater;
        case EInequalityPredicateType::Greater:
            return EInequalityPredicateType::Less;
        case EInequalityPredicateType::LessOrEqual:
            return EInequalityPredicateType::GreaterOrEqual;
        case EInequalityPredicateType::GreaterOrEqual:
            return EInequalityPredicateType::LessOrEqual;
        case EInequalityPredicateType::Equal:
            return EInequalityPredicateType::NotEqual;
        case EInequalityPredicateType::NotEqual:
            return EInequalityPredicateType::Equal;
        default:
            Y_ABORT();
    }
}

// Returns an opposite range direction.
EInequalityPredicateType GetOppositeRangeDirection(EInequalityPredicateType predicate) {
    switch (predicate) {
        case EInequalityPredicateType::Less:
            return EInequalityPredicateType::GreaterOrEqual;
        case EInequalityPredicateType::Greater:
            return EInequalityPredicateType::LessOrEqual;
        case EInequalityPredicateType::LessOrEqual:
            return EInequalityPredicateType::Greater;
        case EInequalityPredicateType::GreaterOrEqual:
            return EInequalityPredicateType::Less;
        case EInequalityPredicateType::Equal:
            return EInequalityPredicateType::NotEqual;
        case EInequalityPredicateType::NotEqual:
            return EInequalityPredicateType::Equal;
        default:
            Y_ABORT();
    }
}

TMaybe<TString> ExtractLiteral(NYql::NNodes::TExprBase maybeLiteral) {
    if (auto maybeJust = maybeLiteral.Maybe<NYql::NNodes::TCoJust>()) {
        maybeLiteral = maybeJust.Cast().Input();
    }
    if (maybeLiteral.Maybe<NYql::NNodes::TCoDataCtor>()) {
        auto literal = maybeLiteral.Maybe<NYql::NNodes::TCoDataCtor>().Cast();
        auto value = literal.Literal().Value();
        return TString(value);
    }
    return Nothing();
}

void CollectTableAliases(const std::shared_ptr<TTreeNode>& node, TSet<TString>& tableAliases) {
    if (node->Operator == ELogicalOperator::Leaf) {
        if (node->TableAlias.Defined()) {
            tableAliases.insert(node->TableAlias.GetRef());
        }
        return;
    }
    for (const auto& child : node->Children) {
        CollectTableAliases(child, tableAliases);
    }
}

} // namespace NPredicateTree

using namespace NPredicateTree;

namespace {

THashSet<TString> PgComparisonSigns = {"<", "<=", ">", ">=", "=", "<>", "!="};

THashMap<TString, EInequalityPredicateType> StringToComparisonOperatorMap{
    {"<", EInequalityPredicateType::Less},
    {"<=", EInequalityPredicateType::LessOrEqual},
    {">", EInequalityPredicateType::Greater},
    {">=", EInequalityPredicateType::GreaterOrEqual},
    {"=", EInequalityPredicateType::Equal},
    {"<>", EInequalityPredicateType::NotEqual},
    {"!=", EInequalityPredicateType::NotEqual}};

TMaybe<TCoMember> IsMember(const TExprBase& input) {
    if (auto member = input.Maybe<TCoMember>()) {
        return member.Cast();
    } else if (auto cast = input.Maybe<TCoSafeCast>()) {
        return IsMember(cast.Cast().Value());
    } else if (auto ifPresent = input.Maybe<TCoIfPresent>()) {
        return IsMember(ifPresent.Cast().Optional());
    } else if (auto just = input.Maybe<TCoJust>()) {
        return IsMember(just.Cast().Input());
    } else if (input.Ptr()->IsCallable("PgCast")) {
        auto child = TExprBase(input.Ptr()->ChildRef(0));
        return IsMember(child);
    } else if (input.Ptr()->IsCallable("FromPg") || input.Ptr()->IsCallable("ToPg")) {
        auto child = TExprBase(input.Ptr()->ChildRef(0));
        return IsMember(child);
    } else if (auto exists = input.Maybe<TCoExists>()) {
        auto child = TExprBase(input.Ptr()->ChildRef(0));
        return IsMember(child);
    }
    return Nothing();
}

template<typename T>
TExprNode::TPtr FindNode(const TExprBase& input) {
    for (const auto& child : input.Ptr()->Children()) {
        if (TExprBase(child).Maybe<T>()) {
            return child;
        }

        auto tmp = FindNode<T>(TExprBase(child));
        if (tmp != nullptr) {
            return tmp;
        }
    }

    return nullptr;
}

TMaybe<TString> GetAttributeType(const std::shared_ptr<NKikimr::NKqp::TOptimizerStatistics>& stats, const TString& attributeName) {
    if (!stats || !stats->ColumnStatistics) {
        return Nothing();
    }

    auto it = stats->ColumnStatistics->Data.find(attributeName);
    if (it != stats->ColumnStatistics->Data.end()) {
        return it->second.Type;
    }

    return Nothing();
}

enum class EValueParseClass {
    Bool,
    SignedInt,
    UnsignedInt,
    Float,
    Text
};

EValueParseClass GetValueParseClass(TStringBuf typeName) {
    static const THashSet<TStringBuf> unsignedTypes = {
        "Uint8", "Uint16", "Uint32", "Uint64", "Date", "Datetime", "Timestamp"};
    static const THashSet<TStringBuf> signedTypes = {
        "Int8", "Int16", "Int32", "Int64", "Date32", "Datetime64", "Interval", "Timestamp64", "Interval64"};

    if (typeName == "Bool") {
        return EValueParseClass::Bool;
    } else if (unsignedTypes.contains(typeName)) {
        return EValueParseClass::UnsignedInt;
    } else if (signedTypes.contains(typeName)) {
        return EValueParseClass::SignedInt;
    } else if (typeName == "Float" || typeName == "Double" || typeName.StartsWith("Decimal")) {
        return EValueParseClass::Float;
    }

    // other types are considered as string and compared lexicographically
    // Utf8, String, Text considered as text
    // Uuid, Bytes, DyNumber, Yson, Json, JsonDocument
    return EValueParseClass::Text;
}

TMaybe<EValueParseClass> GetValueParseClass(const TExprBase& input) {
    const TTypeAnnotationNode* type = input.Ref().GetTypeAnn();
    if (!type) {
        return Nothing();
    }
    type = RemoveAllOptionals(type);
    if (!type || type->GetKind() != ETypeAnnotationKind::Data) {
        return Nothing();
    }
    return GetValueParseClass(type->Cast<TDataExprType>()->GetName());
}

i8 CompareValuesImpl(const TString& left, const TString& right, const TString columnType) {
    if (columnType == "Bool") {
        ui8 l = FromString<bool>(left);
        ui8 r = FromString<bool>(right);
        return (l < r) ? -1 : (l > r) ? 1 : 0;
    } else if (columnType == "Uint8") {
        ui8 l = FromString<ui8>(left);
        ui8 r = FromString<ui8>(right);
        return (l < r) ? -1 : (l > r) ? 1 : 0;
    } else if (columnType == "Int8") {
        i8 l = FromString<i8>(left);
        i8 r = FromString<i8>(right);
        return (l < r) ? -1 : (l > r) ? 1 : 0;
    } else if (columnType == "Uint16") {
        ui16 l = FromString<ui16>(left);
        ui16 r = FromString<ui16>(right);
        return (l < r) ? -1 : (l > r) ? 1 : 0;
    } else if (columnType == "Int16") {
        i16 l = FromString<i16>(left);
        i16 r = FromString<i16>(right);
        return (l < r) ? -1 : (l > r) ? 1 : 0;
    } else if (columnType == "Uint32") {
        ui32 l = FromString<ui32>(left);
        ui32 r = FromString<ui32>(right);
        return (l < r) ? -1 : (l > r) ? 1 : 0;
    } else if (columnType == "Int32") {
        i32 l = FromString<i32>(left);
        i32 r = FromString<i32>(right);
        return (l < r) ? -1 : (l > r) ? 1 : 0;
    } else if (columnType == "Uint64") {
        ui64 l = FromString<ui64>(left);
        ui64 r = FromString<ui64>(right);
        return (l < r) ? -1 : (l > r) ? 1 : 0;
    } else if (columnType == "Int64") {
        i64 l = FromString<i64>(left);
        i64 r = FromString<i64>(right);
        return (l < r) ? -1 : (l > r) ? 1 : 0;
    } else if (columnType == "Float") {
        float l = FromString<float>(left);
        float r = FromString<float>(right);
        return (l < r) ? -1 : (l > r) ? 1 : 0;
    } else if (columnType == "Double" || columnType.StartsWith("Decimal")) {
        double l = FromString<double>(left);
        double r = FromString<double>(right);
        return (l < r) ? -1 : (l > r) ? 1 : 0;
    } else if (columnType == "Date") {
        ui16 l = FromString<ui16>(left);
        ui16 r = FromString<ui16>(right);
        return (l < r) ? -1 : (l > r) ? 1 : 0;
    } else if (columnType == "Date32") {
        i32 l = FromString<i32>(left);
        i32 r = FromString<i32>(right);
        return (l < r) ? -1 : (l > r) ? 1 : 0;
    } else if (columnType == "Datetime") {
        ui32 l = FromString<ui32>(left);
        ui32 r = FromString<ui32>(right);
        return (l < r) ? -1 : (l > r) ? 1 : 0;
    } else if (columnType == "Interval" || columnType == "Interval64" || columnType == "Datetime64" || columnType == "Timestamp64") {
        i64 l = FromString<i64>(left);
        i64 r = FromString<i64>(right);
        return (l < r) ? -1 : (l > r) ? 1 : 0;
    } else if (columnType == "Timestamp") {
        ui64 l = FromString<ui64>(left);
        ui64 r = FromString<ui64>(right);
        return (l < r) ? -1 : (l > r) ? 1 : 0;
    }

    // other types are considered as string and compared lexicographically
    // Utf8, String, Text, Uuid, Bytes, DyNumber, Yson, Json, JsonDocument
    return (left < right) ? -1 : (left > right) ? 1 : 0;
}

i8 CompareValues(const TString& left, const TString& right, const TString columnType) {
    try {
        return CompareValuesImpl(left, right, columnType);
    } catch (const yexception&) {
        return (left < right) ? -1 : (left > right) ? 1 : 0;
    }
}

// Input string of a Re2 UDF call: (Apply (Udf 'Re2.<Name> ...) <input>), the Udf may be wrapped in AssumeStrict.
TMaybe<TExprBase> GetRe2Input(const TExprBase& input) {
    if (!input.Ptr()->IsCallable("Apply") || input.Ptr()->ChildrenSize() < 2) {
        return Nothing();
    }
    const TExprNode* callee = input.Ptr()->Child(0);
    while (callee->IsCallable("AssumeStrict")) {
        callee = callee->Child(0);
    }
    if (!callee->IsCallable("Udf") || !callee->Head().IsAtom() || !callee->Head().Content().StartsWith("Re2.")) {
        return Nothing();
    }
    return TExprBase(input.Ptr()->ChildPtr(1));
}

// Column type for comparing range bounds. It comes from the expression, so normalization needs no statistics.
TString GetColumnTypeName(const TExprBase& attributeExpr) {
    const auto member = IsMember(attributeExpr);
    if (!member) {
        return {};
    }
    const TTypeAnnotationNode* type = member->Ref().GetTypeAnn();
    if (!type) {
        return {};
    }
    type = RemoveAllOptionals(type);
    if (!type || type->GetKind() != ETypeAnnotationKind::Data) {
        return {};
    }
    return FormatType(type);
}

// Only comparisons with a literal bound have a range; everything else is estimated on its own.
bool IsMergeable(const TTreeNode& leaf) {
    return leaf.Kind == EPredicateKind::Comparison && (leaf.Range.LeftBound.Defined() || leaf.Range.RightBound.Defined());
}

} // anonymous namespace

namespace {

TStringBuf ToString(ELogicalOperator op) {
    switch (op) {
        case ELogicalOperator::And: return "And";
        case ELogicalOperator::Or: return "Or";
        case ELogicalOperator::Leaf: return "Leaf";
    }
    return "?";
}

TStringBuf ToString(EPredicateKind kind) {
    switch (kind) {
        case EPredicateKind::Comparison: return "Comparison";
        case EPredicateKind::TwoColumns: return "TwoColumns";
        case EPredicateKind::StringMatch: return "StringMatch";
        case EPredicateKind::Regex: return "Regex";
        case EPredicateKind::BoolLiteral: return "BoolLiteral";
        case EPredicateKind::NonAttribute: return "NonAttribute";
        case EPredicateKind::NonConstant: return "NonConstant";
        case EPredicateKind::Parameter: return "Parameter";
        case EPredicateKind::MergedRanges: return "MergedRanges";
        case EPredicateKind::Unknown: return "Unknown";
    }
    return "?";
}

TStringBuf ToString(EInequalityPredicateType sign) {
    switch (sign) {
        case EInequalityPredicateType::Less: return "<";
        case EInequalityPredicateType::LessOrEqual: return "<=";
        case EInequalityPredicateType::Greater: return ">";
        case EInequalityPredicateType::GreaterOrEqual: return ">=";
        case EInequalityPredicateType::Equal: return "=";
        case EInequalityPredicateType::NotEqual: return "<>";
    }
    return "?";
}

// Column name, literal value, or the callable name for any other expression.
TString ToShortString(const TExprBase& expr) {
    if (const auto attribute = IsAttribute(expr)) {
        return *attribute;
    }
    if (const auto literal = ExtractLiteral(expr)) {
        return TStringBuilder() << "'" << *literal << "'";
    }

    const auto& node = expr.Ref();
    if (node.IsCallable()) {
        return TStringBuilder() << node.Content() << "(...)";
    }

    return TString(node.IsAtom() ? node.Content() : TStringBuf("<expr>"));
}

TString ToString(const TPredicateRange& range) {
    TStringBuilder out;
    out << (range.LeftBound.Defined() && range.LeftInclusive ? "[" : "(")
        << (range.LeftBound.Defined() ? range.LeftBound.GetRef() : TString("-inf"))
        << ", "
        << (range.RightBound.Defined() ? range.RightBound.GetRef() : TString("+inf"))
        << (range.RightBound.Defined() && range.RightInclusive ? "]" : ")");
    return out;
}

} // anonymous namespace

TString TTreeNode::ToString(ui32 indent) const {
    TStringBuilder out;
    out << TString(indent * 2, ' ') << NKikimr::NKqp::ToString(Operator);

    if (Operator != ELogicalOperator::Leaf) {
        out << " (" << Children.size() << " children)\n";
        for (const auto& child : Children) {
            out << (child ? child->ToString(indent + 1) : TString(TString((indent + 1) * 2, ' ') + "<null>\n"));
        }
        return out;
    }

    out << " " << NKikimr::NKqp::ToString(Kind);
    if (!Attribute.empty()) {
        out << " attr=" << Attribute;
        if (TableAlias.Defined()) {
            out << " alias=" << TableAlias.GetRef();
        }
        out << " column=" << Column << " type=" << (ColumnType.empty() ? TString("?") : ColumnType);
    }

    switch (Kind) {
        case EPredicateKind::BoolLiteral:
            out << " value=" << (BoolValue ? "true" : "false");
            break;
        case EPredicateKind::Regex:
        case EPredicateKind::StringMatch:
            out << " match=" << (Kind == EPredicateKind::Regex ? "regex" : (ContainString ? "contains" : "prefix/suffix"));
            if (Right.Defined()) {
                out << " value=" << ToShortString(Right.GetRef());
            }
            break;
        case EPredicateKind::MergedRanges:
            break;
        default:
            if (Left.Defined() && Right.Defined()) {
                out << " expr=" << ToShortString(Left.GetRef()) << " " << NKikimr::NKqp::ToString(Sign) << " " << ToShortString(Right.GetRef());
            }
            break;
    }
    if (UnderNot) {
        out << " NOT";
    }
    if (Range.LeftBound.Defined() || Range.RightBound.Defined()) {
        out << " range=" << NKikimr::NKqp::ToString(Range);
    }

    if (Kind == EPredicateKind::MergedRanges) {
        out << " ranges=";
        if (Ranges.empty()) {
            out << "<empty>";
        }
        for (size_t i = 0; i < Ranges.size(); ++i) {
            out << (i ? " u " : "") << NKikimr::NKqp::ToString(Ranges[i]);
        }
        out << " from " << MergedLeaves.size() << " leaves\n";
        for (const auto& leaf : MergedLeaves) {
            out << leaf->ToString(indent + 1);
        }
        return out;
    }

    out << "\n";
    return out;
}

std::shared_ptr<TTreeNode> TPredicateTreeBuilder::CreateLeafNode(
    EPredicateKind kind,
    const TExprBase* left,
    const TExprBase* right,
    EInequalityPredicateType sign,
    bool underNot
) {
    auto node = std::make_shared<TTreeNode>();
    node->Operator = ELogicalOperator::Leaf;
    node->Kind = kind;
    node->Sign = sign;
    node->UnderNot = underNot;

    if (left) {
        node->Left = *left;
    }
    if (right) {
        node->Right = *right;
    }

    const TExprBase* attributeExpr = nullptr;
    TMaybe<TString> attribute;
    if (left && (attribute = IsAttribute(*left))) {
        attributeExpr = left;
    } else if (right && (attribute = IsAttribute(*right))) {
        attributeExpr = right;
    }

    if (!attribute.Defined()) {
        return node;
    }

    TString ref = attribute.GetRef();
    node->Attribute = ref;
    size_t dotPos = ref.find('.');
    if (dotPos != TString::npos) {
        node->TableAlias = ref.substr(0, dotPos);
        node->Column = ref.substr(dotPos + 1);
    } else {
        node->Column = ref;
    }

    node->ColumnType = GetColumnTypeName(*attributeExpr);
    if (node->ColumnType.empty()) {
        TMaybe<TString> colType = GetAttributeType(Stats, ref);
        if (colType.Defined()) {
            node->ColumnType = colType.GetRef();
        }
    }

    return node;
}

void TPredicateTreeBuilder::CollectColumnStatsUsedMember(const TExprBase& attribute, bool equality) {
    // statistics to request are collected only while none are available
    if (!CollectColumnsStatUsedMembers || (Stats && Stats->ColumnStatistics)) {
        return;
    }
    if (auto maybeMember = IsMember(attribute)) {
        if (equality) {
            ColumnStatsUsedMembers.AddEquality(maybeMember.GetRef());
        } else {
            ColumnStatsUsedMembers.AddInequality(maybeMember.GetRef());
        }
    }
}

std::shared_ptr<TTreeNode> TPredicateTreeBuilder::ProcessStringPredicate(
    const TExprBase& left,
    const TExprBase& right,
    bool underNot,
    bool collectMembers,
    bool containString
) {
    Y_UNUSED(collectMembers);

    // keep the column on the left
    if (IsAttribute(right) && IsConstantExprWithParams(left.Ptr())) {
        return ProcessStringPredicate(right, left, underNot, collectMembers, containString);
    }

    std::shared_ptr<TTreeNode> node = CreateLeafNode(EPredicateKind::StringMatch, &left, &right, EInequalityPredicateType::Equal, underNot);
    node->ContainString = containString;

    return node;
}

std::shared_ptr<TTreeNode> TPredicateTreeBuilder::ProcessBoolLiteralPredicate(
    bool value,
    bool underNot,
    bool collectMembers
) {
    Y_UNUSED(collectMembers);

    std::shared_ptr<TTreeNode> node = CreateLeafNode(EPredicateKind::BoolLiteral);
    node->BoolValue = (value != underNot);

    return node;
}

std::shared_ptr<TTreeNode> TPredicateTreeBuilder::ProcessRegexPredicate(
    const TExprBase* input,
    bool underNot,
    bool collectMembers
) {
    Y_UNUSED(collectMembers);

    // input is the string the regular expression is applied to, unknown when reached through the UDF name only
    return CreateLeafNode(EPredicateKind::Regex, input, nullptr, EInequalityPredicateType::Equal, underNot);
}

std::shared_ptr<TTreeNode> TPredicateTreeBuilder::ConvertInequalityToRange(
    const TExprBase& left,
    const TExprBase& right,
    bool underNot,
    bool collectMembers,
    EInequalityPredicateType inequalitySign
) {
    Y_UNUSED(collectMembers);

    auto leftAttr = IsAttribute(left);
    auto rightAttr = IsAttribute(right);

    // the case when both are attributes
    if (leftAttr.Defined() && rightAttr.Defined()) {
        return CreateLeafNode(EPredicateKind::TwoColumns, &left, &right, inequalitySign, underNot);
    } else if (!leftAttr.Defined() && !rightAttr.Defined()) {
        return CreateLeafNode(EPredicateKind::NonAttribute, &left, &right, inequalitySign, underNot);
    }

    auto leftConstParam = IsConstantExprWithParams(left.Ptr());
    auto rightConstParam = IsConstantExprWithParams(right.Ptr());

    // the value side is not a constant expression
    if ((leftConstParam && rightConstParam) || (!leftConstParam && !rightConstParam)) {
        return CreateLeafNode(EPredicateKind::NonConstant, &left, &right, inequalitySign, underNot);
    }

    auto leftConst = IsConstantExpr(left.Ptr());
    auto rightConst = IsConstantExpr(right.Ptr());

    // the value side depends on parameters
    if ((leftConst && rightConst) || (!leftConst && !rightConst)) {
        return CreateLeafNode(EPredicateKind::Parameter, &left, &right, inequalitySign, underNot);
    }

    const TExprBase* leftPtr = &left;
    const TExprBase* rightPtr = &right;

    if (rightAttr) {
        std::swap(leftPtr, rightPtr);
        std::swap(leftAttr, rightAttr);
        inequalitySign = GetOppositePredicateType(inequalitySign);
    }

    if (underNot) {
        inequalitySign = GetOppositeRangeDirection(inequalitySign);
    }

    bool inclusive = false;
    if (inequalitySign == EInequalityPredicateType::LessOrEqual || inequalitySign == EInequalityPredicateType::GreaterOrEqual) {
        inclusive = true;
    }

    // NOT is already applied to the sign
    std::shared_ptr<TTreeNode> node = CreateLeafNode(EPredicateKind::Comparison, leftPtr, rightPtr, inequalitySign);
    CollectColumnStatsUsedMember(*leftPtr, /*equality=*/false);

    TMaybe<TString> literal = ExtractLiteral(*rightPtr);
    if (literal.Defined()) {
        if (inequalitySign == EInequalityPredicateType::Less || inequalitySign == EInequalityPredicateType::LessOrEqual) {
            TPredicateRange rangeNode(Nothing(), Nothing(), false, *rightPtr, literal.GetRef(), inclusive);
            node->Range = std::move(rangeNode);
        } else if (inequalitySign == EInequalityPredicateType::Greater || inequalitySign == EInequalityPredicateType::GreaterOrEqual) {
            TPredicateRange rangeNode(*rightPtr, literal.GetRef(), inclusive, Nothing(), Nothing(), false);
            node->Range = std::move(rangeNode);
        }
    }

    return node;
}

std::shared_ptr<TTreeNode> TPredicateTreeBuilder::ConvertEqualityToRange(
    const TExprBase& left,
    const TExprBase& right,
    bool underNot,
    bool collectMembers
) {
    auto leftAttr = IsAttribute(left);
    auto rightAttr = IsAttribute(right);

    // the case when both are attributes
    if (leftAttr.Defined() && rightAttr.Defined()) {
        if (collectMembers && CollectMemberEqualities) {
            const auto leftMember = IsMember(left);
            const auto rightMember = IsMember(right);
            if (leftMember && rightMember) {
                MemberEqualities.emplace_back(leftMember.GetRef(), rightMember.GetRef());
            }
        }
        return CreateLeafNode(EPredicateKind::TwoColumns, &left, &right, EInequalityPredicateType::Equal, underNot);
    } else if (!leftAttr.Defined() && !rightAttr.Defined()) {
        return CreateLeafNode(EPredicateKind::NonAttribute, &left, &right, EInequalityPredicateType::Equal, underNot);
    }

    auto leftConstParam = IsConstantExprWithParams(left.Ptr());
    auto rightConstParam = IsConstantExprWithParams(right.Ptr());

    // the value side is not a constant expression
    if ((leftConstParam && rightConstParam) || (!leftConstParam && !rightConstParam)) {
        return CreateLeafNode(EPredicateKind::NonConstant, &left, &right, EInequalityPredicateType::Equal, underNot);
    }

    auto leftConst = IsConstantExpr(left.Ptr());
    auto rightConst = IsConstantExpr(right.Ptr());

    // the value side depends on parameters
    if ((leftConst && rightConst) || (!leftConst && !rightConst)) {
        return CreateLeafNode(EPredicateKind::Parameter, &left, &right, EInequalityPredicateType::Equal, underNot);
    }

    const TExprBase* leftPtr = &left;
    const TExprBase* rightPtr = &right;

    if (rightAttr) {
        std::swap(leftPtr, rightPtr);
        std::swap(leftAttr, rightAttr);
    }

    if (underNot) {
        // converting not equality into two ranges (col < x) OR (col > x)
        auto node = std::make_shared<TTreeNode>();
        node->Operator = ELogicalOperator::Or;

        std::shared_ptr<TTreeNode> left = ConvertInequalityToRange(*leftPtr, *rightPtr, false, collectMembers, EInequalityPredicateType::Less);
        std::shared_ptr<TTreeNode> right = ConvertInequalityToRange(*leftPtr, *rightPtr, false, collectMembers, EInequalityPredicateType::Greater);

        if (left) {
            node->Children.push_back(left);
        }
        if (right) {
            node->Children.push_back(right);
        }

        return node;
    }

    std::shared_ptr<TTreeNode> node = CreateLeafNode(EPredicateKind::Comparison, leftPtr, rightPtr, EInequalityPredicateType::Equal);

    if (collectMembers) {
        if (auto maybeMember = IsMember(*leftPtr)) {
            ConstantMembers.push_back(maybeMember.GetRef());
        }
    }
    CollectColumnStatsUsedMember(*leftPtr, /*equality=*/true);

    TMaybe<TString> literal = ExtractLiteral(*rightPtr);
    if (literal.Defined()) {
        TPredicateRange rangeNode(*rightPtr, literal.GetRef(), true, *rightPtr, literal.GetRef(), true);
        node->Range = std::move(rangeNode);
    }

    return node;
}

std::shared_ptr<TTreeNode> TPredicateTreeBuilder::ProcessSetPredicate(
    const TExprBase& left,
    const TExprNode::TPtr list,
    bool underNot,
    bool collectMembers
) {
    auto node = std::make_shared<TTreeNode>();

    // convert set predicate into a subtree
    auto logicalOperator = underNot ? ELogicalOperator::And : ELogicalOperator::Or;
    node->Operator = logicalOperator;

    TMaybe<EValueParseClass> columnParseClass;
    if (auto member = IsMember(left)) {
        columnParseClass = GetValueParseClass(TExprBase(member.GetRef()));
    }

    bool droppedElements = false;
    for (const auto& element : list->Children()) {
        TExprBase right = TExprBase(element);

        auto elementParseClass = GetValueParseClass(right);
        if (columnParseClass.Defined() && elementParseClass.Defined() && *columnParseClass != *elementParseClass) {
            droppedElements = true;
            continue;
        }

        auto child = ConvertEqualityToRange(left, right, underNot, collectMembers);
        if (child) {
            node->Children.push_back(child);
        }
    }

    // empty set if all types do not match
    if (droppedElements && node->Children.empty()) {
        return nullptr;
    }

    return node;
}

void SortRanges(TVector<std::shared_ptr<TTreeNode>>& allRanges, TString columnType) {
    // sorting by left bound only
    std::sort(allRanges.begin(), allRanges.end(), [columnType](const std::shared_ptr<TTreeNode>& left, const std::shared_ptr<TTreeNode>& right) {

        // -inf bounds first
        if (!left->Range.LeftBound.Defined() && !right->Range.LeftBound.Defined()) {
            return false;
        }
        if (!left->Range.LeftBound.Defined()) {
            return true;
        }
        if (!right->Range.LeftBound.Defined()) {
            return false;
        }

        i8 cmp = CompareValues(left->Range.LeftBound.GetRef(), right->Range.LeftBound.GetRef(), columnType);
        if (cmp != 0) {
            return cmp < 0;
        }

        // inclusive starts earlier
        if (left->Range.LeftInclusive != right->Range.LeftInclusive) {
            return left->Range.LeftInclusive;
        }
        return false;
    });
}

TMaybe<TPredicateRange> IntersectOverlappingConjunctions(TVector<std::shared_ptr<TTreeNode>>& allRanges, TString columnType) {
    if (allRanges.empty()) {
        return Nothing();
    }

    TPredicateRange mergedRange = allRanges.front()->Range;

    for (size_t i = 1; i < allRanges.size(); ++i) {
        auto& range = allRanges[i]->Range;

        // intersect left bound
        if (range.LeftBound.Defined()) {
            if (!mergedRange.LeftBound.Defined()) {
                mergedRange.Left = range.Left;
                mergedRange.LeftBound = range.LeftBound;
                mergedRange.LeftInclusive = range.LeftInclusive;
            } else {
                // since finding overlaps, take the largest on left bound
                i8 cmp = CompareValues(range.LeftBound.GetRef(), mergedRange.LeftBound.GetRef(), columnType);
                if (cmp > 0 || (cmp == 0 && !range.LeftInclusive)) {
                    mergedRange.Left = range.Left;
                    mergedRange.LeftBound = range.LeftBound;
                    mergedRange.LeftInclusive = range.LeftInclusive;
                }
            }
        }

        // intersect right bound
        if (range.RightBound.Defined()) {
            if (!mergedRange.RightBound.Defined()) {
                mergedRange.Right = range.Right;
                mergedRange.RightBound = range.RightBound;
                mergedRange.RightInclusive = range.RightInclusive;
            } else {
                // since finding overlaps, take the smallest on right bound
                i8 cmp = CompareValues(range.RightBound.GetRef(), mergedRange.RightBound.GetRef(), columnType);
                if (cmp < 0 || (cmp == 0 && !range.RightInclusive)) {
                    mergedRange.Right = range.Right;
                    mergedRange.RightBound = range.RightBound;
                    mergedRange.RightInclusive = range.RightInclusive;
                }
            }
        }

        // check for empty intersection
        if (mergedRange.LeftBound.Defined() && mergedRange.RightBound.Defined()) {
            i8 cmp = CompareValues(mergedRange.LeftBound.GetRef(), mergedRange.RightBound.GetRef(), columnType);
            if (cmp > 0) {
                return Nothing();
            } else if (cmp == 0 && (!mergedRange.LeftInclusive || !mergedRange.RightInclusive)) {
                return Nothing();
            }
        }
    }

    return mergedRange;
}

TMaybe<TVector<TPredicateRange>> UnionOverlappingDisjunctions(TVector<std::shared_ptr<TTreeNode>>& allRanges, TString columnType) {
    if (allRanges.empty()) {
        return Nothing();
    }

    // sorting ranges by left bound only
    SortRanges(allRanges, columnType);

    TVector<TPredicateRange> mergedRanges;
    TPredicateRange current = allRanges.front()->Range;

    for (size_t i = 1; i < allRanges.size(); ++i) {
        auto& range = allRanges[i]->Range;

        // check whether the intervals do not overlap
        bool disjoint = false;
        if (current.RightBound.Defined() && range.LeftBound.Defined()) {
            i8 cmp = CompareValues(current.RightBound.GetRef(), range.LeftBound.GetRef(), columnType);
            if (cmp < 0) {
                disjoint = true;
            } else if (cmp == 0 && (!current.RightInclusive || !range.LeftInclusive)) {
                disjoint = true;
            }
        }
        if (disjoint) {
            mergedRanges.push_back(current);
            current = range;
            continue;
        }

        // expand upper bound if either side is +inf
        if (!current.RightBound.Defined() || !range.RightBound.Defined()) {
            current.Right = Nothing();
            current.RightBound = Nothing();
            current.RightInclusive = false;
        } else {
            i8 cmp = CompareValues(range.RightBound.GetRef(), current.RightBound.GetRef(), columnType);
            if (cmp > 0 || (cmp == 0 && range.RightInclusive)) {
                current.Right = range.Right;
                current.RightBound = range.RightBound;
                current.RightInclusive = range.RightInclusive;
            }
        }

        // if domain is fully covered, then exit
        if (!current.LeftBound.Defined() && !current.RightBound.Defined()) {
            mergedRanges.clear();
            mergedRanges.push_back(current);
            return mergedRanges;
        }
    }

    mergedRanges.push_back(current);
    return mergedRanges;
}

std::shared_ptr<TTreeNode> TPredicateTreeBuilder::Normalize(const std::shared_ptr<TTreeNode>& node) {
    if (!node || node->Operator == ELogicalOperator::Leaf) {
        return node;
    }

    auto result = std::make_shared<TTreeNode>();
    result->Operator = node->Operator;

    // group comparison leaves by attribute, keep the rest as they are
    TMap<TString, TVector<std::shared_ptr<TTreeNode>>> leavesByAttribute;
    for (const auto& child : node->Children) {
        if (child->Operator != ELogicalOperator::Leaf) {
            result->Children.push_back(Normalize(child));
        } else if (IsMergeable(*child)) {
            leavesByAttribute[child->Attribute].push_back(child);
        } else {
            result->Children.push_back(child);
        }
    }

    for (auto& [attribute, leaves] : leavesByAttribute) {
        if (leaves.size() == 1) {
            result->Children.push_back(leaves.front());
            continue;
        }

        auto merged = std::make_shared<TTreeNode>();
        merged->Operator = ELogicalOperator::Leaf;
        merged->Kind = EPredicateKind::MergedRanges;
        merged->Attribute = attribute;
        merged->Column = leaves.front()->Column;
        merged->ColumnType = leaves.front()->ColumnType;
        merged->TableAlias = leaves.front()->TableAlias;

        if (node->Operator == ELogicalOperator::And) {
            // an empty intersection leaves no ranges, i.e. zero selectivity
            if (auto intersection = IntersectOverlappingConjunctions(leaves, merged->ColumnType)) {
                merged->Ranges.push_back(std::move(intersection.GetRef()));
            }
        } else if (auto ranges = UnionOverlappingDisjunctions(leaves, merged->ColumnType)) {
            merged->Ranges = std::move(ranges.GetRef());
        }

        merged->MergedLeaves = std::move(leaves);
        result->Children.push_back(std::move(merged));
    }

    return result;
}

std::shared_ptr<TTreeNode> TPredicateTreeBuilder::Build(const TExprBase& input) {
    return ComputeImpl(input, false, CollectConstantMembers || CollectMemberEqualities);
}

/**
 * ComputeImpl - traverses (depth-first post-order) the predicate expression and builds the predicate tree.
 * It keeps every predicate's details and computes no selectivity.
 */
std::shared_ptr<TTreeNode> TPredicateTreeBuilder::ComputeImpl(
    const TExprBase& input,
    bool underNot,
    bool collectMembers
) {
    // Process OptionalIf, just return the predicate statistics
    if (auto optIf = input.Maybe<TCoOptionalIf>()) {
        return ComputeImpl(optIf.Cast().Predicate(), underNot, collectMembers);
    }

    // Same with Coalesce
    else if (auto coalesce = input.Maybe<TCoCoalesce>()) {
        return ComputeImpl(coalesce.Cast().Predicate(), underNot, collectMembers);
    }

    // Process regular expression UDF calls (REGEXP, MATCH, ILIKE), keeping the string they are applied to
    else if (auto re2Input = GetRe2Input(input)) {
        return ProcessRegexPredicate(&re2Input.GetRef(), underNot, collectMembers);
    }

    else if (
        input.Ptr()->IsCallable("FromPg") ||
        input.Ptr()->IsCallable("Exists") ||
        input.Ptr()->IsCallable("AssumeStrict") ||
        input.Ptr()->IsCallable("Apply") ||
        input.Ptr()->IsCallable("Udf")) {
        auto child = TExprBase(input.Ptr()->ChildRef(0));
        return ComputeImpl(child, underNot, collectMembers);
    }

    // Process AND, OR, and NOT logical operators.
    // In case of AND we multiply the selectivities, since we assume them to be independent
    // In case of OR we sum them up, again assuming independence and disjointness, but make sure it is at most 1.0
    // In case of NOT we subtract the argument's selectivity from 1.0

    else if (auto andNode = input.Maybe<TCoAnd>()) {
        auto logicalOperator = underNot ? ELogicalOperator::Or : ELogicalOperator::And;
        auto node = std::make_shared<TTreeNode>();
        node->Operator = logicalOperator;

        for (size_t i = 0; i < andNode.Cast().ArgCount(); i++) {
            // collect all conjunctions of a column
            const auto& child = ComputeImpl(andNode.Cast().Arg(i), underNot, !underNot && collectMembers);
            if (child) {
                node->Children.push_back(child);
            }
        }
        return node;
    }

    else if (auto orNode = input.Maybe<TCoOr>()) {
        auto logicalOperator = underNot ? ELogicalOperator::And : ELogicalOperator::Or;
        auto node = std::make_shared<TTreeNode>();
        node->Operator = logicalOperator;

        for (size_t i = 0; i < orNode.Cast().ArgCount(); i++) {
            // collect all disjunctions of a column
            auto child = ComputeImpl(orNode.Cast().Arg(i), underNot, underNot && collectMembers);
            if (child) {
                node->Children.push_back(child);
            }
        }
        return node;
    }

    else if (auto notNode = input.Maybe<TCoNot>()) {
        return ComputeImpl(notNode.Cast().Value(), !underNot, collectMembers);
    }

    // Process equality predicate
    else if (auto equality = input.Maybe<TCoCmpEqual>()) { // x1 = x2
        auto left = equality.Cast().Left();
        auto right = equality.Cast().Right();
        return ConvertEqualityToRange(left, right, underNot, !underNot && collectMembers);
    }

    // Process not equality predicate
    else if (auto notEquality = input.Maybe<TCoCmpNotEqual>()) {
        auto left = notEquality.Cast().Left();
        auto right = notEquality.Cast().Right();
        return ConvertEqualityToRange(left, right, !underNot, underNot && collectMembers);
    }

    // Process all inequality predicates
    else if (auto less = input.Maybe<TCoCmpLess>()) { // x < value
        auto left = less.Cast().Left();
        auto right = less.Cast().Right();
        return ConvertInequalityToRange(left, right, underNot, collectMembers, EInequalityPredicateType::Less);
    }

    else if (auto greater = input.Maybe<TCoCmpGreater>()) { // x > value
        auto left = greater.Cast().Left();
        auto right = greater.Cast().Right();
        return ConvertInequalityToRange(left, right, underNot, collectMembers, EInequalityPredicateType::Greater);
    }

    else if (auto lessOrEqual = input.Maybe<TCoCmpLessOrEqual>()) { // x <= value
        auto left = lessOrEqual.Cast().Left();
        auto right = lessOrEqual.Cast().Right();
        return ConvertInequalityToRange(left, right, underNot, collectMembers, EInequalityPredicateType::LessOrEqual);
    }

    else if (auto greaterOrEqual = input.Maybe<TCoCmpGreaterOrEqual>()) { // x >= value
        auto left = greaterOrEqual.Cast().Left();
        auto right = greaterOrEqual.Cast().Right();
        return ConvertInequalityToRange(left, right, underNot, collectMembers, EInequalityPredicateType::GreaterOrEqual);
    }

    // Process Pg operators
    else if (input.Ptr()->IsCallable("PgResolvedOp") && PgComparisonSigns.contains(input.Ptr()->ChildPtr(0)->Content())) {
        auto left = TExprBase(input.Ptr()->ChildPtr(2));
        auto right = TExprBase(input.Ptr()->ChildPtr(3));

        EInequalityPredicateType compareSign = StringToComparisonOperatorMap[input.Ptr()->ChildPtr(0)->Content()];
        if (compareSign == EInequalityPredicateType::Equal) {
            return ConvertEqualityToRange(left, right, underNot, !underNot && collectMembers);
        } else if (compareSign == EInequalityPredicateType::NotEqual) {
            return ConvertEqualityToRange(left, right, !underNot, underNot && collectMembers);
        } else {
            return ConvertInequalityToRange(left, right, underNot, collectMembers, compareSign);
        }
    }

    // Process string predicates (Find, LIKE %value%)
    else if(input.Ptr()->IsCallable("Find") || input.Ptr()->IsCallable("StringContains")) {
        auto left = TExprBase(input.Ptr()->ChildRef(0));
        auto right = TExprBase(input.Ptr()->ChildRef(1));
        return ProcessStringPredicate(left, right, underNot, collectMembers, true);
    }

    // Process string predicates (StartsWith LIKE value%, EndsWith LIKE %value)
    else if (auto comparison = input.Maybe<TCoCompare>()) {
        auto left = comparison.Cast().Left();
        auto right = comparison.Cast().Right();
        return ProcessStringPredicate(left, right, underNot, collectMembers, false);
    }

    // Process boolean literals
    else if (auto boolLiteral = input.Maybe<TCoBool>()) {
        const bool value = FromString<bool>(boolLiteral.Cast().Literal().Value());
        return ProcessBoolLiteralPredicate(value, underNot, collectMembers);
    }

    // Process regular expression and StringContainsIgnoreCase (ILIKE %value%)
    else if (input.Maybe<TCoAtom>()) {
        auto atom = input.Cast<TCoAtom>();
        // regexp (all starts with Re2)
        if (atom.StringValue().StartsWith("Re2")) {
            return ProcessRegexPredicate(nullptr, underNot, collectMembers);
        }
    }

    // Process SqlIn
    else if (input.Ptr()->IsCallable("SqlIn")) {
        auto list = input.Ptr()->ChildPtr(0);
        if (list != nullptr) {
            TExprBase left = TExprBase(input.Ptr()->ChildPtr(1)); // left is always column
            return ProcessSetPredicate(left, list, underNot, false);
        }
    }

    else if (auto maybeIfExpr = input.Maybe<TCoIf>()) {
        auto ifExpr = maybeIfExpr.Cast();
        // attr in ('a', 'b', 'c' ...)
        if (ifExpr.Predicate().Maybe<TCoExists>() && ifExpr.ThenValue().Maybe<TCoJust>() && ifExpr.ElseValue().Maybe<TCoNothing>()) {
            auto list = FindNode<TExprList>(ifExpr.ThenValue());
            if (list != nullptr) {
                TExprBase left = ifExpr.Predicate(); // left is always column
                return ProcessSetPredicate(left, list, underNot, false);
            }
        }
    }

    else {
        auto dumped = input.Raw()->Dump();
        YQL_CLOG(TRACE, CoreDq) << "ComputePredicateSelectivity NOT FOUND : " << dumped;
    }

    return CreateLeafNode(EPredicateKind::Unknown, &input);
}

} // namespace NKikimr::NKqp
