#include "collection.h"

#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/utils/log/log.h>

#include <vector>

namespace NYql::NPushdown {

using namespace NNodes;

namespace {

bool ExprHasUtf8Type(const TExprBase& expr) {
    auto typeAnn = expr.Ptr()->GetTypeAnn();
    auto itemType = GetSeqItemType(typeAnn);
    if (!itemType) {
        itemType = typeAnn;
    }
    if (itemType->GetKind() != ETypeAnnotationKind::Data) {
        return false;
    }
    auto dataTypeInfo = NUdf::GetDataTypeInfo(itemType->Cast<TDataExprType>()->GetSlot());
    return (std::string(dataTypeInfo.Name.data()) == "Utf8");
}

bool IsLikeOperator(const TCoCompare& predicate) {
    return predicate.Maybe<TCoCmpStringContains>()
        || predicate.Maybe<TCoCmpStartsWith>()
        || predicate.Maybe<TCoCmpEndsWith>();
}

bool IsSupportedLikeForUtf8(const TExprBase& left, const TExprBase& right) {
    if ((left.Maybe<TCoMember>() && ExprHasUtf8Type(left))
        || (right.Maybe<TCoMember>() && ExprHasUtf8Type(right)))
    {
        return true;
    }
    return false;
}

bool IsSupportedPredicate(const TCoCompare& predicate, const TSettings& settings) {
    if (predicate.Maybe<TCoCmpEqual>()) {
        return true;
    } else if (predicate.Maybe<TCoCmpLess>()) {
        return true;
    } else if (predicate.Maybe<TCoCmpGreater>()) {
        return true;
    } else if (predicate.Maybe<TCoCmpNotEqual>()) {
        return true;
    } else if (predicate.Maybe<TCoCmpGreaterOrEqual>()) {
        return true;
    } else if (predicate.Maybe<TCoCmpLessOrEqual>()) {
        return true;
    } else if (predicate.Maybe<TCoCmpLessOrEqual>()) {
        return true;
    } else if (settings.IsEnabled(TSettings::EFeatureFlag::LikeOperator) && IsLikeOperator(predicate)) {
        return true;
    }

    return false;
}

bool IsSupportedDataType(const TCoDataCtor& node, const TSettings& settings) {
    if (node.Maybe<TCoBool>() ||
        node.Maybe<TCoFloat>() ||
        node.Maybe<TCoDouble>() ||
        node.Maybe<TCoInt8>() ||
        node.Maybe<TCoInt16>() ||
        node.Maybe<TCoInt32>() ||
        node.Maybe<TCoInt64>() ||
        node.Maybe<TCoUint8>() ||
        node.Maybe<TCoUint16>() ||
        node.Maybe<TCoUint32>() ||
        node.Maybe<TCoUint64>())
    {
        return true;
    }

    if (settings.IsEnabled(TSettings::EFeatureFlag::TimestampCtor) && node.Maybe<TCoTimestamp>()) {
        return true;
    }

    if (settings.IsEnabled(TSettings::EFeatureFlag::StringTypes)) {
        if (node.Maybe<TCoUtf8>() || node.Maybe<TCoString>()) {
            return true;
        }
    }

    return false;
}

bool IsSupportedCast(const TCoSafeCast& cast, const TSettings& settings) {
    if (!settings.IsEnabled(TSettings::EFeatureFlag::CastExpression)) {
        return false;
    }

    auto maybeDataType = cast.Type().Maybe<TCoDataType>();
    if (!maybeDataType) {
        if (const auto maybeOptionalType = cast.Type().Maybe<TCoOptionalType>()) {
            maybeDataType = maybeOptionalType.Cast().ItemType().Maybe<TCoDataType>();
        }
    }
    YQL_ENSURE(maybeDataType.IsValid());

    const auto dataType = maybeDataType.Cast();
    if (dataType.Type().Value() == "Int32") { // TODO: Support any numeric casts.
        return cast.Value().Maybe<TCoString>() || cast.Value().Maybe<TCoUtf8>();
    }
    return false;
}

bool IsStringType(NYql::NUdf::TDataTypeId t) {
    return t == NYql::NProto::String
        || t == NYql::NProto::Utf8
        || t == NYql::NProto::Yson
        || t == NYql::NProto::Json
        || t == NYql::NProto::JsonDocument;
}

bool IsDateTimeType(NYql::NUdf::TDataTypeId t) {
    return t == NYql::NProto::Date
        || t == NYql::NProto::Datetime
        || t == NYql::NProto::Timestamp
        || t == NYql::NProto::Interval
        || t == NYql::NProto::TzDate
        || t == NYql::NProto::TzDatetime
        || t == NYql::NProto::TzTimestamp
        || t == NYql::NProto::Date32
        || t == NYql::NProto::Datetime64
        || t == NYql::NProto::Timestamp64
        || t == NYql::NProto::Interval64;
}

bool IsUuidType(NYql::NUdf::TDataTypeId t) {
    return t == NYql::NProto::Uuid;
}

bool IsDecimalType(NYql::NUdf::TDataTypeId t) {
    return t == NYql::NProto::Decimal;
}

bool IsDyNumberType(NYql::NUdf::TDataTypeId t) {
    return t == NYql::NProto::DyNumber;
}

bool IsComparableTypes(const TExprBase& leftNode, const TExprBase& rightNode, bool equality,
    const TTypeAnnotationNode* inputType, const TSettings& settings)
{
    const TExprNode::TPtr leftPtr = leftNode.Ptr();
    const TExprNode::TPtr rightPtr = rightNode.Ptr();

    auto getDataType = [inputType](const TExprNode::TPtr& node) {
        auto type = node->GetTypeAnn();

        if (type->GetKind() == ETypeAnnotationKind::Unit) {
            auto rowType = inputType->Cast<TStructExprType>();
            type = rowType->FindItemType(node->Content());
        }

        if (type->GetKind() == ETypeAnnotationKind::Optional) {
            type = type->Cast<TOptionalExprType>()->GetItemType();
        }

        return type;
    };

    auto defaultCompare = [equality](const TTypeAnnotationNode* left, const TTypeAnnotationNode* right) {
        if (equality) {
            return CanCompare<true>(left, right);
        }

        return CanCompare<false>(left, right);
    };

    auto canCompare = [&defaultCompare, &settings](const TTypeAnnotationNode* left, const TTypeAnnotationNode* right) {
        if (left->GetKind() != ETypeAnnotationKind::Data ||
            right->GetKind() != ETypeAnnotationKind::Data)
        {
            return defaultCompare(left, right);
        }

        auto leftTypeId = GetDataTypeInfo(left->Cast<TDataExprType>()->GetSlot()).TypeId;
        auto rightTypeId = GetDataTypeInfo(right->Cast<TDataExprType>()->GetSlot()).TypeId;

        if (!settings.IsEnabled(TSettings::EFeatureFlag::StringTypes) && (IsStringType(leftTypeId) || IsStringType(rightTypeId))) {
            return ECompareOptions::Uncomparable;
        }

        if (!settings.IsEnabled(TSettings::EFeatureFlag::DateTimeTypes) && (IsDateTimeType(leftTypeId) || IsDateTimeType(rightTypeId))) {
            return ECompareOptions::Uncomparable;
        }

        if (!settings.IsEnabled(TSettings::EFeatureFlag::UuidType) && (IsUuidType(leftTypeId) || IsUuidType(rightTypeId))) {
            return ECompareOptions::Uncomparable;
        }

        if (!settings.IsEnabled(TSettings::EFeatureFlag::DecimalType) && (IsDecimalType(leftTypeId) || IsDecimalType(rightTypeId))) {
            return ECompareOptions::Uncomparable;
        }

        if (!settings.IsEnabled(TSettings::EFeatureFlag::DyNumberType) && (IsDyNumberType(leftTypeId) || IsDyNumberType(rightTypeId))) {
            return ECompareOptions::Uncomparable;
        }

        if (leftTypeId == rightTypeId) {
            return ECompareOptions::Comparable;
        }

        /*
         * Check special case UInt32 <-> Datetime in case i can't put it inside switch without lot of copypaste
         */
        if (leftTypeId == NYql::NProto::Uint32 && rightTypeId == NYql::NProto::Date) {
            return ECompareOptions::Comparable;
        }

        /*
         * SSA program requires strict equality of some types, otherwise columnshard fails to execute comparison
         */
        switch (leftTypeId) {
            case NYql::NProto::Int8:
            case NYql::NProto::Int16:
            case NYql::NProto::Int32:
                // SSA program cast those values to Int32
                if (rightTypeId == NYql::NProto::Int8 ||
                    rightTypeId == NYql::NProto::Int16 ||
                    rightTypeId == NYql::NProto::Int32 ||
                    (settings.IsEnabled(TSettings::EFeatureFlag::ImplicitConversionToInt64) && rightTypeId == NYql::NProto::Int64))
                {
                    return ECompareOptions::Comparable;
                }
                break;
            case NYql::NProto::Uint16:
                if (rightTypeId == NYql::NProto::Date) {
                    return ECompareOptions::Comparable;
                }
                [[fallthrough]];
            case NYql::NProto::Uint8:
            case NYql::NProto::Uint32:
                // SSA program cast those values to Uint32
                if (rightTypeId == NYql::NProto::Uint8 ||
                    rightTypeId == NYql::NProto::Uint16 ||
                    rightTypeId == NYql::NProto::Uint32 ||
                    (settings.IsEnabled(TSettings::EFeatureFlag::ImplicitConversionToInt64) && rightTypeId == NYql::NProto::Uint64))
                {
                    return ECompareOptions::Comparable;
                }
                break;
            case NYql::NProto::Date:
                // See arcadia/ydb/library/yql/dq/runtime/dq_arrow_helpers.cpp SwitchMiniKQLDataTypeToArrowType
                if (rightTypeId == NYql::NProto::Uint16) {
                    return ECompareOptions::Comparable;
                }
                break;
            case NYql::NProto::Datetime:
                // See arcadia/ydb/library/yql/dq/runtime/dq_arrow_helpers.cpp SwitchMiniKQLDataTypeToArrowType
                if (rightTypeId == NYql::NProto::Uint32) {
                    return ECompareOptions::Comparable;
                }
                break;
            case NYql::NProto::Int64:
                if (settings.IsEnabled(TSettings::EFeatureFlag::ImplicitConversionToInt64) && (
                    rightTypeId == NYql::NProto::Int8 ||
                    rightTypeId == NYql::NProto::Int16 ||
                    rightTypeId == NYql::NProto::Int32))
                {
                    return ECompareOptions::Comparable;
                }
                break;
            case NYql::NProto::Uint64:
                if (settings.IsEnabled(TSettings::EFeatureFlag::ImplicitConversionToInt64) && (
                    rightTypeId == NYql::NProto::Uint8 ||
                    rightTypeId == NYql::NProto::Uint16 ||
                    rightTypeId == NYql::NProto::Uint32))
                {
                    return ECompareOptions::Comparable;
                }
                break;
            case NYql::NProto::Bool:
            case NYql::NProto::Float:
            case NYql::NProto::Double:
            case NYql::NProto::Decimal:
            case NYql::NProto::Timestamp:
            case NYql::NProto::Interval:
                // Obviosly here right node has not same type as left one
                break;
            default:
                return defaultCompare(left, right);
        }

        return ECompareOptions::Uncomparable;
    };

    auto leftType = getDataType(leftPtr);
    auto rightType = getDataType(rightPtr);

    if (canCompare(leftType, rightType) == ECompareOptions::Uncomparable) {
        YQL_CVLOG(NLog::ELevel::DEBUG, settings.GetLogComponent()) << "Pushdown: "
            << "Uncompatible types in compare of nodes: "
            << leftPtr->Content() << " of type " << FormatType(leftType)
            << " and "
            << rightPtr->Content() << " of type " << FormatType(rightType);

        return false;
    }

    return true;
}

std::vector<TExprBase> GetComparisonNodes(const TExprBase& node) {
    std::vector<TExprBase> res;
    if (node.Maybe<TExprList>()) {
        auto nodeList = node.Cast<TExprList>();
        res.reserve(nodeList.Size());
        for (size_t i = 0; i < nodeList.Size(); ++i) {
            res.emplace_back(nodeList.Item(i));
        }
    } else {
        res.emplace_back(node);
    }
    return res;
}

bool IsMemberColumn(const TCoMember& member, const TExprNode* lambdaArg) {
    return member.Struct().Raw() == lambdaArg;
}

bool IsMemberColumn(const TExprBase& node, const TExprNode* lambdaArg) {
    if (auto member = node.Maybe<TCoMember>()) {
        return IsMemberColumn(member.Cast(), lambdaArg);
    }
    return false;
}

bool CheckExpressionNodeForPushdown(const TExprBase& node, const TExprNode* lambdaArg, const TSettings& settings) {
    if (auto maybeSafeCast = node.Maybe<TCoSafeCast>()) {
        return IsSupportedCast(maybeSafeCast.Cast(), settings);
    } else if (auto maybeData = node.Maybe<TCoDataCtor>()) {
        return IsSupportedDataType(maybeData.Cast(), settings);
    } else if (auto maybeMember = node.Maybe<TCoMember>()) {
        return IsMemberColumn(maybeMember.Cast(), lambdaArg);
    } else if (settings.IsEnabled(TSettings::EFeatureFlag::JsonQueryOperators) && node.Maybe<TCoJsonQueryBase>()) {
        if (!node.Maybe<TCoJsonValue>()) {
            return false;
        }
        auto jsonOp = node.Cast<TCoJsonQueryBase>();
        if (!jsonOp.Json().Maybe<TCoMember>() || !jsonOp.JsonPath().Maybe<TCoUtf8>()) {
            // Currently we support only simple columns in pushdown
            return false;
        }
        return true;
    } else if (node.Maybe<TCoNull>()) {
        return true;
    } else if (settings.IsEnabled(TSettings::EFeatureFlag::ParameterExpression) && node.Maybe<TCoParameter>()) {
        return true;
    } else if (const auto op = node.Maybe<TCoUnaryArithmetic>(); op && settings.IsEnabled(TSettings::EFeatureFlag::UnaryOperators)) {
        return CheckExpressionNodeForPushdown(op.Cast().Arg(), lambdaArg, settings);
    } else if (const auto op = node.Maybe<TCoBinaryArithmetic>(); op && settings.IsEnabled(TSettings::EFeatureFlag::ArithmeticalExpressions)) {
        return CheckExpressionNodeForPushdown(op.Cast().Left(), lambdaArg, settings) && CheckExpressionNodeForPushdown(op.Cast().Right(), lambdaArg, settings);
    }
    return false;
}

bool CheckComparisonParametersForPushdown(const TCoCompare& compare, const TExprNode* lambdaArg, const TExprBase& input, const TSettings& settings) {
    const TTypeAnnotationNode* inputType = input.Ptr()->GetTypeAnn();
    switch (inputType->GetKind()) {
        case ETypeAnnotationKind::Flow:
            inputType = inputType->Cast<TFlowExprType>()->GetItemType();
            break;
        case ETypeAnnotationKind::Stream:
            inputType = inputType->Cast<TStreamExprType>()->GetItemType();
            break;
        case ETypeAnnotationKind::Struct:
            break;
        default:
            YQL_ENSURE(false, "Unsupported type of incoming data: " << (ui32)inputType->GetKind());
            // We do not know how process input that is not a sequence of elements
            return false;
    }
    YQL_ENSURE(inputType->GetKind() == ETypeAnnotationKind::Struct);

    if (inputType->GetKind() != ETypeAnnotationKind::Struct) {
        // We do not know how process input that is not a sequence of elements
        return false;
    }

    const auto leftList = GetComparisonNodes(compare.Left());
    const auto rightList = GetComparisonNodes(compare.Right());
    YQL_ENSURE(leftList.size() == rightList.size(), "Different sizes of lists in comparison!");

    for (size_t i = 0; i < leftList.size(); ++i) {
        if (!CheckExpressionNodeForPushdown(leftList[i], lambdaArg, settings) || !CheckExpressionNodeForPushdown(rightList[i], lambdaArg, settings)) {
            return false;
        }

        if (!settings.IsEnabled(TSettings::EFeatureFlag::DoNotCheckCompareArgumentsTypes)) {
            if (!IsComparableTypes(leftList[i], rightList[i], compare.Maybe<TCoCmpEqual>() || compare.Maybe<TCoCmpNotEqual>(), inputType, settings)) {
                return false;
            }
        }

        if (IsLikeOperator(compare) && settings.IsEnabled(TSettings::EFeatureFlag::LikeOperatorOnlyForUtf8) && !IsSupportedLikeForUtf8(leftList[i], rightList[i])) {
            // (KQP OLAP) If SSA_RUNTIME_VERSION == 2 Column Shard doesn't have LIKE kernel for binary strings
            return false;
        }
    }

    return true;
}

bool CompareCanBePushed(const TCoCompare& compare, const TExprNode* lambdaArg, const TExprBase& lambdaBody, const TSettings& settings) {
    if (!IsSupportedPredicate(compare, settings)) {
        return false;
    }

    if (!CheckComparisonParametersForPushdown(compare, lambdaArg, lambdaBody, settings)) {
        return false;
    }

    return true;
}

bool SafeCastCanBePushed(const TCoFlatMap& flatmap, const TExprNode* lambdaArg, const TSettings& settings) {
    /*
     * There are three ways of comparison in following format:
     *
     * FlatMap (LeftArgument, FlatMap(RightArgument(), Just(Predicate))
     *
     * Examples:
     * FlatMap (SafeCast(), FlatMap(Member(), Just(Comparison))
     * FlatMap (Member(), FlatMap(SafeCast(), Just(Comparison))
     * FlatMap (SafeCast(), FlatMap(SafeCast(), Just(Comparison))
     */
    auto maybeFlatmap = flatmap.Lambda().Body().Maybe<TCoFlatMap>();
    if (!maybeFlatmap.IsValid()) {
        return false;
    }

    auto leftList = GetComparisonNodes(flatmap.Input());
    auto rightList = GetComparisonNodes(maybeFlatmap.Cast().Input());
    YQL_ENSURE(leftList.size() == rightList.size(), "Different sizes of lists in comparison!");

    for (size_t i = 0; i < leftList.size(); ++i) {
        if (!CheckExpressionNodeForPushdown(leftList[i], lambdaArg, settings) || !CheckExpressionNodeForPushdown(rightList[i], lambdaArg, settings)) {
            return false;
        }
    }

    auto maybeJust = maybeFlatmap.Cast().Lambda().Body().Maybe<TCoJust>();
    if (!maybeJust.IsValid()) {
        return false;
    }

    auto maybePredicate = maybeJust.Cast().Input().Maybe<TCoCompare>();
    if (!maybePredicate.IsValid()) {
        return false;
    }

    auto predicate = maybePredicate.Cast();
    if (!IsSupportedPredicate(predicate, settings)) {
        return false;
    }

    return true;
}

bool JsonExistsCanBePushed(const TCoJsonExists& jsonExists, const TExprNode* lambdaArg) {
    auto maybeMember = jsonExists.Json().Maybe<TCoMember>();
    if (!maybeMember || !jsonExists.JsonPath().Maybe<TCoUtf8>()) {
        // Currently we support only simple columns in pushdown
        return false;
    }
    if (!IsMemberColumn(maybeMember.Cast(), lambdaArg)) {
        return false;
    }
    return true;
}

bool CoalesceCanBePushed(const TCoCoalesce& coalesce, const TExprNode* lambdaArg, const TExprBase& lambdaBody, const TSettings& settings) {
    if (!coalesce.Value().Maybe<TCoBool>()) {
        return false;
    }
    auto predicate = coalesce.Predicate();

    if (auto maybeCompare = predicate.Maybe<TCoCompare>()) {
        return CompareCanBePushed(maybeCompare.Cast(), lambdaArg, lambdaBody, settings);
    } else if (auto maybeFlatmap = predicate.Maybe<TCoFlatMap>()) {
        return SafeCastCanBePushed(maybeFlatmap.Cast(), lambdaArg, settings);
    } else if (settings.IsEnabled(TSettings::EFeatureFlag::JsonExistsOperator) && predicate.Maybe<TCoJsonExists>()) {
        auto jsonExists = predicate.Cast<TCoJsonExists>();
        return JsonExistsCanBePushed(jsonExists, lambdaArg);
    }

    return false;
}

bool ExistsCanBePushed(const TCoExists& exists, const TExprNode* lambdaArg) {
    return IsMemberColumn(exists.Optional(), lambdaArg);
}

void CollectChildrenPredicates(const TExprNode& opNode, TPredicateNode& predicateTree, const TExprNode* lambdaArg, const TExprBase& lambdaBody, const TSettings& settings) {
    predicateTree.Children.reserve(opNode.ChildrenSize());
    predicateTree.CanBePushed = true;
    for (const auto& childNodePtr: opNode.Children()) {
        TPredicateNode child(childNodePtr);
        const TExprBase base(childNodePtr);
        if (const auto maybeCtor = base.Maybe<TCoDataCtor>())
            child.CanBePushed = IsSupportedDataType(maybeCtor.Cast(), settings);
        else
            CollectPredicates(base, child, lambdaArg, lambdaBody, settings);
        predicateTree.Children.emplace_back(child);
        predicateTree.CanBePushed &= child.CanBePushed;
    }
}

void CollectExpressionPredicate(TPredicateNode& predicateTree, const TCoMember& member, const TExprNode* lambdaArg) {
    predicateTree.CanBePushed = IsMemberColumn(member, lambdaArg);
}

} // anonymous namespace end

void CollectPredicates(const TExprBase& predicate, TPredicateNode& predicateTree, const TExprNode* lambdaArg, const TExprBase& lambdaBody, const TSettings& settings) {
    if (predicate.Maybe<TCoCoalesce>()) {
        if (settings.IsEnabled(TSettings::EFeatureFlag::JustPassthroughOperators))
            CollectChildrenPredicates(predicate.Ref(), predicateTree, lambdaArg, lambdaBody, settings);
        else {
            auto coalesce = predicate.Cast<TCoCoalesce>();
            predicateTree.CanBePushed = CoalesceCanBePushed(coalesce, lambdaArg, lambdaBody, settings);
        }
    } else if (predicate.Maybe<TCoCompare>()) {
        auto compare = predicate.Cast<TCoCompare>();
        predicateTree.CanBePushed = CompareCanBePushed(compare, lambdaArg, lambdaBody, settings);
    } else if (predicate.Maybe<TCoExists>()) {
        auto exists = predicate.Cast<TCoExists>();
        predicateTree.CanBePushed = ExistsCanBePushed(exists, lambdaArg);
    } else if (predicate.Maybe<TCoNot>()) {
        predicateTree.Op = EBoolOp::Not;
        auto notOp = predicate.Cast<TCoNot>();
        TPredicateNode child(notOp.Value());
        CollectPredicates(notOp.Value(), child, lambdaArg, lambdaBody, settings);
        predicateTree.CanBePushed = child.CanBePushed;
        predicateTree.Children.emplace_back(child);
    } else if (predicate.Maybe<TCoAnd>()) {
        predicateTree.Op = EBoolOp::And;
        CollectChildrenPredicates(predicate.Ref(), predicateTree, lambdaArg, lambdaBody, settings);
    } else if (predicate.Maybe<TCoOr>()) {
        predicateTree.Op = EBoolOp::Or;
        CollectChildrenPredicates(predicate.Ref(), predicateTree, lambdaArg, lambdaBody, settings);
    } else if (settings.IsEnabled(TSettings::EFeatureFlag::LogicalXorOperator) && predicate.Maybe<TCoXor>()) {
        predicateTree.Op = EBoolOp::Xor;
        CollectChildrenPredicates(predicate.Ref(), predicateTree, lambdaArg, lambdaBody, settings);
    } else if (settings.IsEnabled(TSettings::EFeatureFlag::JsonExistsOperator) && predicate.Maybe<TCoJsonExists>()) {
        auto jsonExists = predicate.Cast<TCoJsonExists>();
        predicateTree.CanBePushed = JsonExistsCanBePushed(jsonExists, lambdaArg);
    } else if (settings.IsEnabled(TSettings::EFeatureFlag::ExpressionAsPredicate) && predicate.Maybe<TCoMember>()) {
        CollectExpressionPredicate(predicateTree, predicate.Cast<TCoMember>(), lambdaArg);
    } else if (settings.IsEnabled(TSettings::EFeatureFlag::JustPassthroughOperators) && (predicate.Maybe<TCoIf>() || predicate.Maybe<TCoJust>())) {
        CollectChildrenPredicates(predicate.Ref(), predicateTree, lambdaArg, lambdaBody, settings);
    } else {
        predicateTree.CanBePushed = false;
    }
}

} // namespace NYql::NPushdown
