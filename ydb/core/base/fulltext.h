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

class TDeltaWriter {
    TVector<ui8> Buf;
    ui64 MinId = 0;
    ui64 MaxId = 0;
    ui64 Count = 0;
    ui64 TotalFreq = 0;
    bool WithFreq = false;
public:
    void Reset(bool withFreq);
    void Add(ui64 DocId, ui32 Freq);
    size_t AddCompressed(ui64 firstId, TConstArrayRef<ui8> other, size_t maxSize);
    ui64 GetMinId() const;
    ui64 GetMaxId() const;
    ui64 GetCount() const;
    ui64 GetTotalFreq() const;
    TConstArrayRef<ui8> GetBuf() const;
};

class IDeltaReader {
public:
    virtual ~IDeltaReader() = default;
    virtual bool Read(ui64& docId, ui32& freq) = 0;
    virtual bool IsEnded() const = 0;
};

class TDeltaReader: public IDeltaReader {
    TConstArrayRef<ui8> Buf;
    size_t Pos = 0;
    ui64 LastId = 0;
    bool WithFreq = false;
    ui64 MaxId = 0;
public:
    void Reset(ui64 firstId, TConstArrayRef<ui8> buf, bool withFreq, ui64 maxId);
    bool Read(ui64& docId, ui32& freq) override;
    size_t GetPos() const;
    ui64 GetLastId() const;
    bool IsEnded() const override;
};

class TMultiDeltaReader: public IDeltaReader {
    struct TReaderRef {
        TDeltaReader Reader;
        bool Added = false;
    };
    struct TItem {
        ui64 DocId = 0;
        i32 Freq = 0;
        ui32 RdrId = 0;
    };
    TVector<TReaderRef> Readers;
    TVector<TItem> Items;
    TItem NextItem = { 0, 0, 0 };
    bool Started = false;
    bool WithFreq = false;
    bool OneLeft = false;
    static bool CompareItems(const TItem& a, const TItem& b) {
        return a.DocId > b.DocId; // min-heap
    }
    void Consume(ui32 rdrId, TReaderRef& rdr);
    void SelectNext();
public:
    void Reset(bool withFreq);
    void Add(bool added, ui64 firstId, TConstArrayRef<ui8> buf, ui64 maxId);
    void Start();
    bool Read(ui64& docId, ui32& freq);
    bool IsEnded() const;
    size_t GetPos(size_t n) const;
    ui64 GetLastId(size_t n) const;
};

}
