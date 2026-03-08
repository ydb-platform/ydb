#include "kqp_query_data.h"

#include <yql/essentials/public/udf/udf_data_type.h>

#include <algorithm>

namespace NKikimr::NKqp {

using namespace NKikimr::NMiniKQL;
using namespace NYql::NUdf;

namespace {

TString BuildPath(TStringBuf parentPath, TStringBuf childSuffix) {
    return TStringBuilder() << parentPath << childSuffix;
}

TString BuildIndexPath(TStringBuf parentPath, ui32 index) {
    return TStringBuilder() << parentPath << "[" << index << "]";
}

TString DescribeType(const TType* type) {
    switch (type->GetKind()) {
        case TType::EKind::Data: {
            const auto* dataType = static_cast<const TDataType*>(type);
            if (dataType->GetSchemeType() == NUdf::TDataType<NUdf::TDecimal>::Id) {
                const auto [precision, scale] = static_cast<const TDataDecimalType*>(dataType)->GetParams();
                return TStringBuilder() << "Decimal(" << ui32(precision) << "," << ui32(scale) << ")";
            }

            if (const auto slot = dataType->GetDataSlot()) {
                return TStringBuilder() << *slot;
            }

            return TStringBuilder() << "Data(" << dataType->GetSchemeType() << ")";
        }
        case TType::EKind::Pg:
            return TStringBuilder() << "Pg(" << static_cast<const TPgType*>(type)->GetName() << ")";
        case TType::EKind::Resource:
            return TStringBuilder() << "Resource(" << static_cast<const TResourceType*>(type)->GetTag() << ")";
        case TType::EKind::Tagged:
            return TStringBuilder() << "Tagged(" << static_cast<const TTaggedType*>(type)->GetTag() << ")";
        case TType::EKind::Optional: {
            const auto* optionalType = static_cast<const TOptionalType*>(type);
            return TStringBuilder() << "Optional<" << DescribeType(optionalType->GetItemType()) << ">";
        }
        case TType::EKind::Block: {
            const auto shape = static_cast<const TBlockType*>(type)->GetShape();
            return shape == TBlockType::EShape::Scalar ? "Scalar" : "Block";
        }
        case TType::EKind::Linear:
            return static_cast<const TLinearType*>(type)->IsDynamic() ? "Linear(dynamic)" : "Linear(static)";
        default:
            return TString(type->GetKindAsStr());
    }
}

} // namespace

bool GetFirstTypeIncompatibility(const TType* expected, const TType* actual, TStringBuf path, TString& incompatibility) {
    if (expected->GetKind() != actual->GetKind()) {
        incompatibility = TStringBuilder()
            << "first incompatibility at " << path
            << ": expected " << DescribeType(expected)
            << ", actual " << DescribeType(actual);
        return true;
    }

    switch (expected->GetKind()) {
        case TType::EKind::Data: {
            const auto* expectedDataType = static_cast<const TDataType*>(expected);
            const auto* actualDataType = static_cast<const TDataType*>(actual);
            if (expectedDataType->GetSchemeType() != actualDataType->GetSchemeType()) {
                incompatibility = TStringBuilder()
                    << "first incompatibility at " << path
                    << ": expected " << DescribeType(expected)
                    << ", actual " << DescribeType(actual);
                return true;
            }

            if (expectedDataType->GetSchemeType() == NUdf::TDataType<NUdf::TDecimal>::Id) {
                const auto [expectedPrecision, expectedScale] = static_cast<const TDataDecimalType*>(expectedDataType)->GetParams();
                const auto [actualPrecision, actualScale] = static_cast<const TDataDecimalType*>(actualDataType)->GetParams();
                if (expectedPrecision != actualPrecision || expectedScale != actualScale) {
                    incompatibility = TStringBuilder()
                        << "first incompatibility at " << path
                        << ": expected " << DescribeType(expected)
                        << ", actual " << DescribeType(actual);
                    return true;
                }
            }

            return false;
        }

        case TType::EKind::Pg: {
            const auto* expectedPgType = static_cast<const TPgType*>(expected);
            const auto* actualPgType = static_cast<const TPgType*>(actual);
            if (expectedPgType->GetTypeId() != actualPgType->GetTypeId()) {
                incompatibility = TStringBuilder()
                    << "first incompatibility at " << path
                    << ": expected " << DescribeType(expected)
                    << ", actual " << DescribeType(actual);
                return true;
            }
            return false;
        }

        case TType::EKind::Struct: {
            const auto* expectedStruct = static_cast<const TStructType*>(expected);
            const auto* actualStruct = static_cast<const TStructType*>(actual);

            for (ui32 i = 0; i < expectedStruct->GetMembersCount(); ++i) {
                const auto memberName = expectedStruct->GetMemberName(i);
                const auto memberIndex = actualStruct->FindMemberIndex(memberName);
                if (!memberIndex) {
                    incompatibility = TStringBuilder()
                        << "first incompatibility at " << path
                        << ": missing member '" << memberName << "' in actual Struct";
                    return true;
                }

                const TString memberPath = TStringBuilder() << path << "." << memberName;
                if (GetFirstTypeIncompatibility(
                    expectedStruct->GetMemberType(i),
                    actualStruct->GetMemberType(*memberIndex),
                    memberPath,
                    incompatibility))
                {
                    return true;
                }
            }

            for (ui32 i = 0; i < actualStruct->GetMembersCount(); ++i) {
                const auto memberName = actualStruct->GetMemberName(i);
                if (!expectedStruct->FindMemberIndex(memberName)) {
                    incompatibility = TStringBuilder()
                        << "first incompatibility at " << path
                        << ": unexpected member '" << memberName << "' in actual Struct";
                    return true;
                }
            }

            return false;
        }

        case TType::EKind::List:
            return GetFirstTypeIncompatibility(
                static_cast<const TListType*>(expected)->GetItemType(),
                static_cast<const TListType*>(actual)->GetItemType(),
                BuildPath(path, "[]"),
                incompatibility);

        case TType::EKind::Stream:
            return GetFirstTypeIncompatibility(
                static_cast<const TStreamType*>(expected)->GetItemType(),
                static_cast<const TStreamType*>(actual)->GetItemType(),
                BuildPath(path, "[]"),
                incompatibility);

        case TType::EKind::Flow:
            return GetFirstTypeIncompatibility(
                static_cast<const TFlowType*>(expected)->GetItemType(),
                static_cast<const TFlowType*>(actual)->GetItemType(),
                BuildPath(path, "[]"),
                incompatibility);

        case TType::EKind::Optional:
            return GetFirstTypeIncompatibility(
                static_cast<const TOptionalType*>(expected)->GetItemType(),
                static_cast<const TOptionalType*>(actual)->GetItemType(),
                BuildPath(path, "?"),
                incompatibility);

        case TType::EKind::Linear: {
            const auto* expectedLinear = static_cast<const TLinearType*>(expected);
            const auto* actualLinear = static_cast<const TLinearType*>(actual);
            if (expectedLinear->IsDynamic() != actualLinear->IsDynamic()) {
                incompatibility = TStringBuilder()
                    << "first incompatibility at " << path
                    << ": expected " << DescribeType(expected)
                    << ", actual " << DescribeType(actual);
                return true;
            }

            return GetFirstTypeIncompatibility(
                expectedLinear->GetItemType(),
                actualLinear->GetItemType(),
                BuildPath(path, "[]"),
                incompatibility);
        }

        case TType::EKind::Dict: {
            const auto* expectedDict = static_cast<const TDictType*>(expected);
            const auto* actualDict = static_cast<const TDictType*>(actual);
            return GetFirstTypeIncompatibility(
                expectedDict->GetKeyType(),
                actualDict->GetKeyType(),
                BuildPath(path, "{key}"),
                incompatibility)
                || GetFirstTypeIncompatibility(
                    expectedDict->GetPayloadType(),
                    actualDict->GetPayloadType(),
                    BuildPath(path, "{value}"),
                    incompatibility);
        }

        case TType::EKind::Tuple: {
            const auto* expectedTuple = static_cast<const TTupleType*>(expected);
            const auto* actualTuple = static_cast<const TTupleType*>(actual);

            const auto expectedSize = expectedTuple->GetElementsCount();
            const auto actualSize = actualTuple->GetElementsCount();
            const auto commonSize = std::min(expectedSize, actualSize);
            for (ui32 i = 0; i < commonSize; ++i) {
                if (GetFirstTypeIncompatibility(
                    expectedTuple->GetElementType(i),
                    actualTuple->GetElementType(i),
                    BuildIndexPath(path, i),
                    incompatibility))
                {
                    return true;
                }
            }

            if (expectedSize != actualSize) {
                incompatibility = TStringBuilder()
                    << "first incompatibility at " << path
                    << ": expected Tuple size " << expectedSize
                    << ", actual size " << actualSize;
                return true;
            }

            return false;
        }

        case TType::EKind::Multi: {
            const auto* expectedMulti = static_cast<const TMultiType*>(expected);
            const auto* actualMulti = static_cast<const TMultiType*>(actual);

            const auto expectedSize = expectedMulti->GetElementsCount();
            const auto actualSize = actualMulti->GetElementsCount();
            const auto commonSize = std::min(expectedSize, actualSize);
            for (ui32 i = 0; i < commonSize; ++i) {
                if (GetFirstTypeIncompatibility(
                    expectedMulti->GetElementType(i),
                    actualMulti->GetElementType(i),
                    BuildIndexPath(path, i),
                    incompatibility))
                {
                    return true;
                }
            }

            if (expectedSize != actualSize) {
                incompatibility = TStringBuilder()
                    << "first incompatibility at " << path
                    << ": expected Multi size " << expectedSize
                    << ", actual size " << actualSize;
                return true;
            }

            return false;
        }

        case TType::EKind::Resource: {
            const auto* expectedResource = static_cast<const TResourceType*>(expected);
            const auto* actualResource = static_cast<const TResourceType*>(actual);
            if (expectedResource->GetTagStr() != actualResource->GetTagStr()) {
                incompatibility = TStringBuilder()
                    << "first incompatibility at " << path
                    << ": expected " << DescribeType(expected)
                    << ", actual " << DescribeType(actual);
                return true;
            }
            return false;
        }

        case TType::EKind::Tagged: {
            const auto* expectedTagged = static_cast<const TTaggedType*>(expected);
            const auto* actualTagged = static_cast<const TTaggedType*>(actual);
            if (expectedTagged->GetTagStr() != actualTagged->GetTagStr()) {
                incompatibility = TStringBuilder()
                    << "first incompatibility at " << path
                    << ": expected " << DescribeType(expected)
                    << ", actual " << DescribeType(actual);
                return true;
            }

            return GetFirstTypeIncompatibility(
                expectedTagged->GetBaseType(),
                actualTagged->GetBaseType(),
                path,
                incompatibility);
        }

        case TType::EKind::Variant:
            return GetFirstTypeIncompatibility(
                static_cast<const TVariantType*>(expected)->GetUnderlyingType(),
                static_cast<const TVariantType*>(actual)->GetUnderlyingType(),
                BuildPath(path, "<variant>"),
                incompatibility);

        case TType::EKind::Block: {
            const auto* expectedBlock = static_cast<const TBlockType*>(expected);
            const auto* actualBlock = static_cast<const TBlockType*>(actual);
            if (expectedBlock->GetShape() != actualBlock->GetShape()) {
                incompatibility = TStringBuilder()
                    << "first incompatibility at " << path
                    << ": expected " << DescribeType(expected)
                    << ", actual " << DescribeType(actual);
                return true;
            }

            return GetFirstTypeIncompatibility(
                expectedBlock->GetItemType(),
                actualBlock->GetItemType(),
                BuildPath(path, "[]"),
                incompatibility);
        }

        case TType::EKind::Callable: {
            const auto* expectedCallable = static_cast<const TCallableType*>(expected);
            const auto* actualCallable = static_cast<const TCallableType*>(actual);

            if (expectedCallable->GetNameStr() != actualCallable->GetNameStr()) {
                incompatibility = TStringBuilder()
                    << "first incompatibility at " << path
                    << ": expected callable '" << expectedCallable->GetName()
                    << "', actual callable '" << actualCallable->GetName() << "'";
                return true;
            }

            if (expectedCallable->GetArgumentsCount() != actualCallable->GetArgumentsCount()) {
                incompatibility = TStringBuilder()
                    << "first incompatibility at " << path
                    << ": expected callable arguments count " << expectedCallable->GetArgumentsCount()
                    << ", actual " << actualCallable->GetArgumentsCount();
                return true;
            }

            if (expectedCallable->GetOptionalArgumentsCount() != actualCallable->GetOptionalArgumentsCount()) {
                incompatibility = TStringBuilder()
                    << "first incompatibility at " << path
                    << ": expected optional arguments count " << expectedCallable->GetOptionalArgumentsCount()
                    << ", actual " << actualCallable->GetOptionalArgumentsCount();
                return true;
            }

            if (GetFirstTypeIncompatibility(
                expectedCallable->GetReturnType(),
                actualCallable->GetReturnType(),
                BuildPath(path, "->"),
                incompatibility))
            {
                return true;
            }

            for (ui32 i = 0; i < expectedCallable->GetArgumentsCount(); ++i) {
                if (GetFirstTypeIncompatibility(
                    expectedCallable->GetArgumentType(i),
                    actualCallable->GetArgumentType(i),
                    BuildIndexPath(path, i),
                    incompatibility))
                {
                    return true;
                }
            }

            return false;
        }

        default:
            break;
    }

    if (!expected->IsSameType(*actual)) {
        incompatibility = TStringBuilder()
            << "first incompatibility at " << path
            << ": expected " << DescribeType(expected)
            << ", actual " << DescribeType(actual);
        return true;
    }

    return false;
}

} // namespace NKikimr::NKqp
