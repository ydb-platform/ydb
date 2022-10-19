#include "yql_kikimr_opt_utils.h"

namespace NYql {

using namespace NNodes;
using namespace NKikimr;
using namespace NKikimr::NUdf;

bool GetEquiJoinKeyTypes(TExprBase leftInput, const TString& leftColumnName,
                         const TKikimrTableDescription& rightTable, const TString& rightColumnName,
                         const TDataExprType*& leftData, const TDataExprType*& rightData)
{
    auto rightType = rightTable.GetColumnType(rightColumnName);
    YQL_ENSURE(rightType);
    if (rightType->GetKind() == ETypeAnnotationKind::Optional) {
        rightType = rightType->Cast<TOptionalExprType>()->GetItemType();
    }

    YQL_ENSURE(rightType->GetKind() == ETypeAnnotationKind::Data);
    rightData = rightType->Cast<TDataExprType>();

    auto leftInputType = leftInput.Ref().GetTypeAnn();
    YQL_ENSURE(leftInputType);
    YQL_ENSURE(leftInputType->GetKind() == ETypeAnnotationKind::List);
    auto itemType = leftInputType->Cast<TListExprType>()->GetItemType();
    YQL_ENSURE(itemType->GetKind() == ETypeAnnotationKind::Struct);
    auto structType = itemType->Cast<TStructExprType>();
    auto memberIndex = structType->FindItem(leftColumnName);
    YQL_ENSURE(memberIndex, "Column '" << leftColumnName << "' not found in " << *((TTypeAnnotationNode*) structType));

    auto leftType = structType->GetItems()[*memberIndex]->GetItemType();
    if (leftType->GetKind() == ETypeAnnotationKind::Optional) {
        leftType = leftType->Cast<TOptionalExprType>()->GetItemType();
    }

    if (leftType->GetKind() != ETypeAnnotationKind::Data) {
        return false;
    }

    leftData = leftType->Cast<TDataExprType>();
    return true;
}

bool CanRewriteSqlInToEquiJoin(const TTypeAnnotationNode* lookupType, const TTypeAnnotationNode* collectionType) {
    // SqlIn in Dict
    if (collectionType->GetKind() == ETypeAnnotationKind::Dict) {
        return IsDataOrOptionalOfData(lookupType);
    }

    // SqlIn in List<DataType> or List<Tuple<DataType...>>
    if (collectionType->GetKind() == ETypeAnnotationKind::List) {
        auto collectionItemType = collectionType->Cast<TListExprType>()->GetItemType();

        if (collectionItemType->GetKind() == ETypeAnnotationKind::Tuple) {
            if (lookupType->GetKind() != ETypeAnnotationKind::Tuple) {
                return false;
            }
            auto lookupItems = lookupType->Cast<TTupleExprType>()->GetItems();
            auto collectionItems = collectionItemType->Cast<TTupleExprType>()->GetItems();
            if (lookupItems.size() != collectionItems.size()) {
                return false;
            }
            return AllOf(collectionItems, [](const auto& item) { return IsDataOrOptionalOfData(item); });
        }

        return IsDataOrOptionalOfData(collectionItemType);
    }

    return false;
}

bool CanConvertSqlInToJoin(const NNodes::TCoSqlIn& sqlIn) {
    auto leftArg = sqlIn.Lookup();
    auto leftColumnType = leftArg.Ref().GetTypeAnn();

    auto rightArg = sqlIn.Collection();
    auto rightArgType = rightArg.Ref().GetTypeAnn();

    if (rightArgType->GetKind() == ETypeAnnotationKind::List) {
        auto rightListItemType = rightArgType->Cast<TListExprType>()->GetItemType();

        auto isDataOrTupleOfData = [](const TTypeAnnotationNode* type) {
            if (IsDataOrOptionalOfData(type) || type->GetKind() == ETypeAnnotationKind::Pg) {
                return true;
            }
            if (type->GetKind() == ETypeAnnotationKind::Tuple) {
                return AllOf(type->Cast<TTupleExprType>()->GetItems(), [](const auto& item) {
                    return IsDataOrOptionalOfData(item);
                });
            }
            return false;
        };

        if (rightListItemType->GetKind() == ETypeAnnotationKind::Struct) {
            auto rightStructType = rightListItemType->Cast<TStructExprType>();
            YQL_ENSURE(rightStructType->GetSize() == 1);
            auto rightColumnType = rightStructType->GetItems()[0]->GetItemType();
            return isDataOrTupleOfData(rightColumnType);
        }

        return isDataOrTupleOfData(rightListItemType);
    }

    /**
     * todo: support tuple of equal tuples
     *
     * sql expression \code{.sql} ... where (k1, k2) in ((1, 2), (2, 3), (3, 4)) \endcode
     * is equivalent to the \code{.sql} ... where (k1, k2) in AsTuple((1, 2), (2, 3), (3, 4)) \endcode
     * but not to the \code{.sql} ... where (k1, k2) in AsList((1, 2), (2, 3), (3, 4)) \endcode
     * so, it's not supported now
     */

    if (rightArgType->GetKind() == ETypeAnnotationKind::Dict) {
        auto rightDictType = rightArgType->Cast<TDictExprType>()->GetKeyType();
        return IsDataOrOptionalOfData(leftColumnType) && IsDataOrOptionalOfData(rightDictType);
    }

    return false;
}

} // namespace NYql
