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

} // namespace NYql
