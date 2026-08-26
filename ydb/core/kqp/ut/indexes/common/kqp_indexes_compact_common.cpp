#include <ydb/core/base/fulltext.h>

#include "kqp_indexes_compact_common.h"

namespace NKikimr::NKqp {

using namespace NYdb::NQuery;
using namespace NYdb;

static TString FormatTokens(THashMap<TString, TMap<ui64, ui32>> tokens, bool withRelevance) {
    TStringBuilder sb;
    sb << "[";
    bool next = false;
    for (auto& [token, ids]: tokens) {
        for (auto& [id, freq]: ids) {
            if (next) {
                sb << ";";
            }
            sb << "[[" << id << "u];";
            if (withRelevance) {
                sb << freq << "u;";
            }
            sb << "\"" << EscapeC(token) << "\"]";
            next = true;
        }
    }
    sb << "]";
    return sb;
}

TString FormatFulltextIndex(TKikimrRunner& kikimr, const TString& name, bool withRelevance) {
    bool compact = kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.GetEnableCompactFulltextIndex();
    auto db = kikimr.GetQueryClient();
    auto result = db.ExecuteQuery(Sprintf(R"(
        SELECT * FROM `%s/indexImplTable`;
    )", name.c_str()), TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    auto rs = result.GetResultSet(0);
    if (compact) {
        THashMap<TString, TMap<ui64, ui32>> tokens;
        TResultSetParser parser(rs);
        TVector<bool> addFlags;
        TVector<std::string> segments;
        std::string lastToken;
        auto flush = [&]() {
            NFulltext::TMultiDeltaReader mr;
            mr.Reset(withRelevance, false);
            for (size_t i = 0; i < segments.size(); i++) {
                mr.Add(addFlags[i], TConstArrayRef<ui8>((const ui8*)segments[i].data(), segments[i].size()));
            }
            mr.Start();
            ui64 docId;
            ui32 freq;
            auto& list = tokens[lastToken];
            while (mr.Read(docId, freq)) {
                list[docId] = freq;
            }
            segments.clear();
            addFlags.clear();
        };
        while (parser.TryNextRow()) {
            auto token = parser.ColumnParser("__ydb_token").GetString();
            if (segments.size() && token != lastToken) {
                flush();
            }
            addFlags.push_back(parser.ColumnParser("__ydb_added").GetBool());
            segments.push_back(parser.ColumnParser("__ydb_segment").GetString());
            lastToken = token;
        }
        if (segments.size()) {
            flush();
        }
        return FormatTokens(tokens, withRelevance);
    }
    return NYdb::FormatResultSetYson(rs);
}

} // namespace NKikimr::NKqp
