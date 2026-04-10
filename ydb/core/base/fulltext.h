#pragma once

#include "defs.h"

#include <ydb/public/api/protos/ydb_table.pb.h>

namespace NKikimr::NFulltext {

enum class EIndexMode {
    Invalid = 0,
    Fulltext = 1,
    JsonIndexOverJson = 2,
    JsonIndexOverJsonDocument = 3,
};

void BuildNgrams(const TString& token, size_t lengthMin, size_t lengthMax, bool edge, TVector<TString>& ngrams);
Ydb::Table::FulltextIndexSettings::Analyzers GetAnalyzersForQuery(Ydb::Table::FulltextIndexSettings::Analyzers analyzers);

TVector<TString> Analyze(const TStringBuf text, const Ydb::Table::FulltextIndexSettings::Analyzers& settings, const std::unordered_set<wchar32>& ignoredDelimiters = {});
TVector<TString> BuildSearchTerms(const TString& query, const Ydb::Table::FulltextIndexSettings::Analyzers& settings);

bool ValidateColumnsMatches(const NProtoBuf::RepeatedPtrField<TString>& columns, const Ydb::Table::FulltextIndexSettings& settings, TString& error);
bool ValidateColumnsMatches(const TVector<TString>& columns, const Ydb::Table::FulltextIndexSettings& settings, TString& error);

bool ValidateSettings(const Ydb::Table::FulltextIndexSettings& settings, TString& error);
bool FillSetting(Ydb::Table::FulltextIndexSettings& settings, const TString& nameLower, const TString& value, TString& error);

struct TTermFreq {
    ui64 DocId;
    ui32 Freq;
};

struct TDeltaWriter {
    TVector<ui8> Buf;
    ui64 MinId = 0;
    ui64 MaxId = 0;
    ui64 Count = 0;
    ui64 TotalFreq = 0;
public:
    void Reset();
    void Add(ui64 DocId);
    void Add(ui64 DocId, ui32 Freq);
    size_t AddCompressed(ui64 firstId, TConstArrayRef<ui8> other, bool withFreq, size_t maxSize);
    ui64 GetMinId();
    ui64 GetMaxId();
    ui64 GetCount();
    ui64 GetTotalFreq();
    TConstArrayRef<ui8> GetBuf();
};

TVector<ui8> DeltaCompress(TConstArrayRef<ui64> ids);
TVector<ui64> DeltaDecompress(TConstArrayRef<ui8> buf);
TVector<ui8> DeltaCompressWithFreq(TConstArrayRef<TTermFreq> terms);
TVector<TTermFreq> DeltaDecompressWithFreq(TConstArrayRef<ui8> buf);

}
