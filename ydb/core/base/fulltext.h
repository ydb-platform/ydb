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
    ui64 MaxId = 0;
    ui64 Count = 0;
    bool WithFreq = false;
    bool Sign = false;
public:
    void Reset(bool withFreq, bool sign);
    void Add(ui64 DocId, ui32 Freq);
    ui64 GetMaxId() const;
    ui64 GetCount() const;
    TConstArrayRef<ui8> GetBuf() const;
};

class IDeltaReader {
public:
    virtual ~IDeltaReader() = default;
    virtual bool Read(ui64& docId, ui32& freq) = 0;
};

class TDeltaReader: public IDeltaReader {
    TConstArrayRef<ui8> Buf;
    size_t Pos = 0;
    ui64 LastId = 0;
    bool WithFreq = false;
    bool Sign = false;
    ui64 MaxId = UINT64_MAX;
    size_t SavedPos = 0;
    ui64 SavedLastId = 0;
public:
    TDeltaReader(TConstArrayRef<ui8> buf, bool withFreq, bool sign);
    bool Read(ui64& docId, ui32& freq) override;
    void Save();
    void Restore();
    void SetMaxId(ui64 maxId);
};

class TMultiDeltaReader: public IDeltaReader {
    struct TReaderRef {
        TDeltaReader *Reader;
        bool Added = false;
    };
    struct TItem {
        ui64 DocId = 0;
        i32 Freq = 0;
        ui32 RdrId = 0;
    };
    TVector<std::unique_ptr<TDeltaReader>> OwnedReaders;
    TVector<TReaderRef> Readers;
    TVector<TItem> Items;
    TItem NextItem = { 0, 0, 0 };
    bool Started = false;
    bool WithFreq = false;
    bool Sign = false;
    bool OneLeft = false;
    static bool CompareItems(const TItem& a, const TItem& b) {
        return a.DocId > b.DocId; // min-heap
    }
    static bool CompareSigned(const TItem& a, const TItem& b) {
        return (i64)a.DocId > (i64)b.DocId; // min-heap
    }
    void Consume(ui32 rdrId, TReaderRef& rdr);
    void SelectNext();
public:
    void Reset(bool withFreq, bool sign);
    void Add(bool added, TDeltaReader* rdr);
    void Add(bool added, TConstArrayRef<ui8> buf);
    void Start();
    bool Read(ui64& docId, ui32& freq);
};

}
