#include "result.h"

namespace NKikimr::NKqp {

constexpr size_t QUERY_TEXT_LIMIT = 10240;

std::shared_ptr<NYql::TAstParseResult> TKqpCompileResult::GetAst() const {
    if (QueryAst) {
        return QueryAst->Ast;
    }
    return nullptr;
}

void TKqpCompileResult::SerializeTo(NKikimrKqp::TCompileCacheQueryInfo* to) const {
    to->SetQueryId(Uid);
    to->SetAccessCount(GetAccessCount());
    to->SetCompiledQueryAt(CompiledAt.MicroSeconds());

    if (Query.Defined()) {
        if (Query->Text.size() > QUERY_TEXT_LIMIT) {
            TString truncatedText = Query->Text.substr(0, QUERY_TEXT_LIMIT);
            to->SetQuery(truncatedText);
        } else {
            to->SetQuery(Query->Text);
        }

        to->SetUserSID(Query->UserSid);
    }

}

} // namespace NKikimr::NKqp
