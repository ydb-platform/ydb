#include "result.h"
#include <ydb/core/util/proto_duration.h>

namespace NKikimr::NKqp {

constexpr size_t QUERY_TEXT_LIMIT = 10240;

std::shared_ptr<NYql::TAstParseResult> TKqpCompileResult::GetAst() const {
    if (QueryAst) {
        return QueryAst->Ast;
    }
    return nullptr;
}

void TKqpCompileResult::SerializeTo(NKikimrKqp::TCompileCacheQueryInfo* to, std::optional<ui64> lastAccessedAt) const {
    to->SetQueryId(Uid);
    to->SetAccessCount(GetAccessCount());
    to->SetCompiledQueryAt(CompiledAt.MicroSeconds());
    if (lastAccessedAt) {
        to->SetLastAccessedAt(lastAccessedAt.value());
    }
    to->SetWarnings(SerializeIssues());
    if (CompileMeta) {
        to->SetMetaInfo(NJson::WriteJson(*CompileMeta, false));
    }
    SetDuration(CompilationDuration, *to->MutableCompilationDuration());
    if (Query.Defined()) {
        if (Query->Text.size() > QUERY_TEXT_LIMIT) {
            TString truncatedText = Query->Text.substr(0, QUERY_TEXT_LIMIT);
            to->SetQuery(truncatedText);
            to->SetIsTruncated(true);
        } else {
            to->SetQuery(Query->Text);
            to->SetIsTruncated(false);
        }

        to->SetUserSID(Query->UserSid);
        to->SetDatabase(Query->Database);
    }

}

} // namespace NKikimr::NKqp
