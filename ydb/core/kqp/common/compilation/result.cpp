#include "result.h"

namespace NKikimr::NKqp {

std::shared_ptr<NYql::TAstParseResult> TKqpCompileResult::GetAst() const {
    if (QueryAst) {
        return QueryAst->Ast;
    }
    return nullptr;
}

} // namespace NKikimr::NKqp
