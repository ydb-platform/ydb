#pragma once

#include <yql/essentials/ast/yql_ast.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NKqp {

struct TDatabaseConfigAuditSettings {
    TString Database;
    TString Changes;
    // Before SQL translation, the path is an expression whose pragmas have
    // not been applied. Do not present it as an authorized/resolved target.
    bool DatabaseIsExpression = false;
};

// Recover attempted flag changes from a successfully translated SQL AST when
// semantic compilation fails before an operation can reach Console. Ordinary
// database settings and quoted, non-executable AST fragments are excluded.
TVector<TDatabaseConfigAuditSettings> CollectDatabaseConfigAuditSettings(const NYql::TAstNode* root);

} // namespace NKikimr::NKqp
