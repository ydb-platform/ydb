#pragma once

#include "yql_kikimr_provider.h"

namespace NYql {

bool GetEquiJoinKeyTypes(NNodes::TExprBase leftInput, const TString& leftColumnName,
                         const TKikimrTableDescription& rightTable, const TString& rightColumnName,
                         const TDataExprType*& leftData, const TDataExprType*& rightData);

bool CanRewriteSqlInToEquiJoin(const TTypeAnnotationNode* lookupType, const TTypeAnnotationNode* collectionType);

} // namespace NYql
