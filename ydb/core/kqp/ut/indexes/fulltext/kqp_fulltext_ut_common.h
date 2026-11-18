#pragma once

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <optional>

namespace NKikimr::NKqp {

using namespace NYdb;

TKikimrRunner Kikimr(NKikimrConfig::TFeatureFlags&& featureFlags);
TKikimrRunner Kikimr();
TKikimrRunner Kikimr(bool enableIndexStreamWrite);
TKikimrRunner KikimrWithCompact(bool compact, bool realThreads = true);
void CreateTexts(NQuery::TQueryClient& db, const bool utf8 = false);
void UpsertSomeTexts(NQuery::TQueryClient& db);
void UpsertTexts(NQuery::TQueryClient& db);
void AddIndex(NQuery::TQueryClient& db, const TString& indexName = "fulltext_plain", const TString& tokenizer = "standard");
void AddIndexCovered(NQuery::TQueryClient& db, const TString& indexName = "fulltext_plain");
void AddIndexSnowball(NQuery::TQueryClient& db, const TString& language, const TString& type = "fulltext_plain");
struct TFulltextNGramOptions {
    bool Lowercase = true;
    bool Stopwords = false;
    std::optional<ui32> MinTokenLength;
    std::optional<ui32> MaxTokenLength;
};

void AddIndexNGram(NQuery::TQueryClient& db, const size_t nGramMinLength = 3, const size_t nGramMaxLength = 3,
    const bool relevance = false, const bool edgeNGram = false, const bool covered = false,
    const TString& tokenizer = "standard", const TFulltextNGramOptions& options = {});

} // namespace NKikimr::NKqp
