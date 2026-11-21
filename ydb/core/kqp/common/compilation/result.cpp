#include "result.h"
#include <library/cpp/json/json_writer.h>
#include <ydb/core/util/proto_duration.h>

namespace NKikimr::NKqp {

constexpr size_t QUERY_TEXT_LIMIT = 10240;

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
        const TStringBuf text = Query->Text();
        const TStringBuf prefix = text.Head(QUERY_TEXT_LIMIT);
        to->SetQuery(prefix.data(), prefix.size());
        to->SetIsTruncated(prefix.size() < text.size());

        to->SetUserSID(TString(Query->UserSid()));
        to->SetDatabase(TString(Query->Database()));
        to->SetQueryType(NKikimrKqp::EQueryType_Name(Query->Settings().QueryType));
        to->SetSyntax(Ydb::Query::Syntax_Name(Query->Settings().Syntax));
    }

}

} // namespace NKikimr::NKqp
