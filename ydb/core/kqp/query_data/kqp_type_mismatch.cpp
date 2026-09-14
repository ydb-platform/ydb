#include "kqp_query_data.h"

#include <yql/essentials/public/udf/udf_data_type.h>

#include <algorithm>

namespace NKikimr::NKqp {

using namespace NKikimr::NMiniKQL;
using namespace NYql::NUdf;

namespace {

TString BuildPath(TStringBuf parentPath, TStringBuf childSuffix) {
    return TStringBuilder() << parentPath << "." << childSuffix;
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
        case TType::EKind::Tagged:
            return TStringBuilder() << "Tagged(" << static_cast<const TTaggedType*>(type)->GetTag() << ")";
        case TType::EKind::Optional: {
            const auto* optionalType = static_cast<const TOptionalType*>(type);
            return TStringBuilder() << "Optional<" << DescribeType(optionalType->GetItemType()) << ">";
        }
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
                BuildPath(path, "<list>"),
                incompatibility);

        case TType::EKind::Optional:
            return GetFirstTypeIncompatibility(
                static_cast<const TOptionalType*>(expected)->GetItemType(),
                static_cast<const TOptionalType*>(actual)->GetItemType(),
                BuildPath(path, "<optional>"),
                incompatibility);

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

        default:
            break;
    }

    return false;
}

} // namespace NKikimr::NKqp
