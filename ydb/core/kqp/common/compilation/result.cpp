#include "result.h"
#include <library/cpp/json/json_writer.h>
#include <ydb/core/util/proto_duration.h>

namespace NKikimr::NKqp {

constexpr size_t QUERY_TEXT_LIMIT = 10240;

void TKqpCompileResult::SerializeTo(NKikimrKqp::TCompileCacheQueryInfo* to, std::optional<ui64> lastAccessedAt) const {
    SerializeCompileCacheQuery(to, Uid, Query.Get(), CompiledAt, CompilationDuration, Issues, CompileMeta, GetAccessCount(), lastAccessedAt);
}

void SerializeCompileCacheQuery(NKikimrKqp::TCompileCacheQueryInfo* to, const TString& uid,
    const TKqpQueryId* query, TInstant compiledAt, TDuration duration, const NYql::TIssues& issues,
    const TMaybe<NJson::TJsonValue>& meta, ui64 accesses, std::optional<ui64> lastAccessedAt) {
    to->SetQueryId(uid);
    to->SetAccessCount(accesses);
    to->SetCompiledQueryAt(compiledAt.MicroSeconds());
    if (lastAccessedAt) {
        to->SetLastAccessedAt(lastAccessedAt.value());
    }
    to->SetWarnings(issues.ToOneLineString());
    if (meta) {
        to->SetMetaInfo(NJson::WriteJson(*meta, false));
    }
    SetDuration(duration, *to->MutableCompilationDuration());
    if (query) {
        const TString& text = query->Text();
        if (text.size() > QUERY_TEXT_LIMIT) {
            to->SetQuery(text.data(), QUERY_TEXT_LIMIT);
            to->SetIsTruncated(true);
        } else {
            to->SetQuery(text);
            to->SetIsTruncated(false);
        }

        to->SetUserSID(TString(query->UserSid()));
        to->SetDatabase(TString(query->Database()));
        to->SetQueryType(NKikimrKqp::EQueryType_Name(query->Settings().QueryType));
        to->SetSyntax(Ydb::Query::Syntax_Name(query->Settings().Syntax));
    }

}

} // namespace NKikimr::NKqp
