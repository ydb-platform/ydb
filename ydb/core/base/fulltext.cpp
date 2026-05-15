#include "fulltext.h"

#include <contrib/libs/snowball/include/libstemmer.h>

#include <util/charset/utf8.h>
#include <util/generic/xrange.h>

#include <algorithm>

namespace NKikimr::NFulltext {

namespace {

    bool ValidateSettingInRange(const TString& name, i32 value, i32 minValue, i32 maxValue, TString& error) {
        if (minValue <= value && value <= maxValue) {
            return true;
        }

        error = TStringBuilder() << "Invalid " << name << ": " << value << " should be between " << minValue << " and " << maxValue;
        return false;
    };

    Ydb::Table::FulltextIndexSettings::Tokenizer ParseTokenizer(const TString& tokenizer_, TString& error) {
        const TString tokenizer = to_lower(tokenizer_);
        if (tokenizer == "whitespace")
            return Ydb::Table::FulltextIndexSettings::WHITESPACE;
        else if (tokenizer == "standard")
            return Ydb::Table::FulltextIndexSettings::STANDARD;
        else if (tokenizer == "keyword")
            return Ydb::Table::FulltextIndexSettings::KEYWORD;
        else {
            error = TStringBuilder() << "Invalid tokenizer: " << tokenizer_;
            return Ydb::Table::FulltextIndexSettings::TOKENIZER_UNSPECIFIED;
        }
    };

    i32 ParseInt32(const TString& name, const TString& value, TString& error) {
        i32 result = 0;
        if (!TryFromString(value, result) || result < 0) { // proto int32 fields with [(Ydb.value) = ">= 0"] annotation
            error = TStringBuilder() << "Invalid " << name << ": " << value;
        }
        return result;
    }

    bool ParseBool(const TString& name, const TString& value, TString& error) {
        bool result = false;
        if (!TryFromString(value, result)) {
            error = TStringBuilder() << "Invalid " << name << ": " << value;
        }
        return result;
    }

    inline bool IsNonStandard(wchar32 c) {
        return !IsAlphabetic(c) && !IsDecdigit(c);
    }

    void Tokenize(const TStringBuf text, TVector<TString>& tokens, auto isDelimiter) {
        const unsigned char* ptr = (const unsigned char*)text.data();
        const unsigned char* end = ptr + text.size();

        while (ptr < end) {
            wchar32 symbol;
            size_t symbolBytes = 0;

            while (ptr < end) { // skip delimiters
                if (SafeReadUTF8Char(symbol, symbolBytes, ptr, end) != RECODE_OK) {
                    tokens.clear();
                    return;
                }
                if (!isDelimiter(symbol)) {
                    break;
                }
                ptr += symbolBytes;
            }
            if (ptr >= end) {
                break;
            }

            const unsigned char* tokenPtr = ptr;
            while (ptr < end) { // read token
                if (SafeReadUTF8Char(symbol, symbolBytes, ptr, end) != RECODE_OK) {
                    tokens.clear();
                    return;
                }
                if (isDelimiter(symbol)) {
                    break;
                }
                ptr += symbolBytes;
            }
            tokens.emplace_back((const char*)tokenPtr, ptr - tokenPtr);
        }
    }

    TVector<TString> Tokenize(const TStringBuf text, const Ydb::Table::FulltextIndexSettings::Tokenizer& tokenizer, const std::unordered_set<wchar32> ignoredDelimiter) {
        TVector<TString> tokens;
        switch (tokenizer) {
            case Ydb::Table::FulltextIndexSettings::WHITESPACE:
                Tokenize(text, tokens, [&ignoredDelimiter](const wchar32 c) {
                    return !ignoredDelimiter.empty() && ignoredDelimiter.contains(c)
                        ? false
                        : IsWhitespace(c);
                });
                break;
            case Ydb::Table::FulltextIndexSettings::STANDARD:
                Tokenize(text, tokens, [&ignoredDelimiter](const wchar32 c) {
                    return !ignoredDelimiter.empty() && ignoredDelimiter.contains(c)
                        ? false
                        : IsNonStandard(c);
                });
                break;
            case Ydb::Table::FulltextIndexSettings::KEYWORD:
                if (UTF8Detect(text) != NotUTF8) {
                    tokens.push_back(TString(text));
                }
                break;
            default:
                Y_ENSURE(false, TStringBuilder() << "Invalid tokenizer: " << static_cast<int>(tokenizer));
        }
        return tokens;
    }

    size_t GetLengthUTF8(const TString& token) {
        const unsigned char* ptr = (const unsigned char*)token.data();
        const unsigned char* end = ptr + token.size();
        size_t length = 0;
        wchar32 symbol;
        size_t symbolBytes = 0;

        while (ptr < end) {
            if (SafeReadUTF8Char(symbol, symbolBytes, ptr, end) != RECODE_OK) {
                Y_ASSERT(false); // should be dropped during tokenize
                return 0;
            }
            length++;
            ptr += symbolBytes;
        }

        return length;
    }

    bool ValidateSettings(const Ydb::Table::FulltextIndexSettings::Analyzers& settings, TString& error) {
        if (!settings.has_tokenizer() || settings.tokenizer() == Ydb::Table::FulltextIndexSettings::TOKENIZER_UNSPECIFIED) {
            error = "tokenizer should be set";
            return false;
        }

        if (settings.use_filter_snowball()) {
            if (settings.use_filter_ngram() || settings.use_filter_edge_ngram()) {
                error = "cannot set use_filter_snowball with use_filter_ngram or use_filter_edge_ngram at the same time";
                return false;
            }

            if (!settings.has_language()) {
                error = "language required when use_filter_snowball is set";
                return false;
            }

            bool supportedLanguage = false;
            for (auto ptr = sb_stemmer_list(); *ptr != nullptr; ++ptr) {
                if (settings.language() == *ptr) {
                    supportedLanguage = true;
                    break;
                }
            }
            if (!supportedLanguage) {
                error = "language is not supported by snowball";
                return false;
            }
        } else if (settings.has_language()) {
            // Currently, language is only used for stemming (use_filter_snowball).
            // In the future, it may be used for other language-sensitive operations (e.g., stopword filtering).
            error = "language setting is only supported with use_filter_snowball at present; other uses may be supported in the future";
            return false;
        }

        if (settings.use_filter_stopwords()) {
            error = "Unsupported use_filter_stopwords setting";
            return false;
        }

        if (settings.use_filter_ngram() || settings.use_filter_edge_ngram()) {
            if (settings.use_filter_ngram() && settings.use_filter_edge_ngram()) {
                error = "only one of use_filter_ngram or use_filter_edge_ngram should be set, not both";
                return false;
            }
            if (!settings.has_filter_ngram_min_length()) {
                error = "filter_ngram_min_length should be set with use_filter_ngram/use_filter_edge_ngram";
                return false;
            }
            if (!settings.has_filter_ngram_max_length()) {
                error = "filter_ngram_max_length should be set with use_filter_ngram/use_filter_edge_ngram";
                return false;
            }
            if (!ValidateSettingInRange("filter_ngram_min_length", settings.filter_ngram_min_length(), 1, 20, error)) {
                return false;
            }
            if (!ValidateSettingInRange("filter_ngram_max_length", settings.filter_ngram_max_length(), 1, 20, error)) {
                return false;
            }
            if (settings.filter_ngram_min_length() > settings.filter_ngram_max_length()) {
                error = "Invalid filter_ngram_min_length: should be less than or equal to filter_ngram_max_length";
                return false;
            }
        } else {
            if (settings.has_filter_ngram_min_length()) {
                error = "use_filter_ngram or use_filter_edge_ngram should be set with filter_ngram_min_length";
                return false;
            }
            if (settings.has_filter_ngram_max_length()) {
                error = "use_filter_ngram or use_filter_edge_ngram should be set with filter_ngram_max_length";
                return false;
            }
        }

        if (settings.use_filter_length()) {
            if (!settings.has_filter_length_min() && !settings.has_filter_length_max()) {
                error = "either filter_length_min or filter_length_max should be set with use_filter_length";
                return false;
            }
            if (settings.has_filter_length_min() && !ValidateSettingInRange("filter_length_min", settings.filter_length_min(), 1, 1000, error)) {
                return false;
            }
            if (settings.has_filter_length_max() && !ValidateSettingInRange("filter_length_max", settings.filter_length_max(), 1, 1000, error)) {
                return false;
            }
            if (settings.has_filter_length_min() && settings.has_filter_length_max() && settings.filter_length_min() > settings.filter_length_max()) {
                error = "Invalid filter_length_min: should be less than or equal to filter_length_max";
                return false;
            }
        } else {
            if (settings.has_filter_length_min()) {
                error = "use_filter_length should be set with filter_length_min";
                return false;
            }
            if (settings.has_filter_length_max()) {
                error = "use_filter_length should be set with filter_length_max";
                return false;
            }
        }

        return true;
    }
}

void BuildNgrams(const TString& token, size_t lengthMin, size_t lengthMax, bool edge, TVector<TString>& ngrams) {
    const unsigned char* ngram_begin_ptr = (const unsigned char*)token.data();
    const unsigned char* end = ngram_begin_ptr + token.size();
    wchar32 symbol;
    size_t symbolBytes;

    while (ngram_begin_ptr < end) {
        const unsigned char* ngram_end_ptr = ngram_begin_ptr;
        size_t ngram_length = 0;
        while (ngram_end_ptr < end) {
            if (SafeReadUTF8Char(symbol, symbolBytes, ngram_end_ptr, end) != RECODE_OK) {
                Y_ASSERT(false); // should already be validated during tokenization
                return;
            }
            ngram_length++;
            ngram_end_ptr += symbolBytes;
            if (lengthMin <= ngram_length && ngram_length <= lengthMax) {
                ngrams.emplace_back((const char*)ngram_begin_ptr, ngram_end_ptr - ngram_begin_ptr);
            }
        }
        if (edge) {
            break; // only prefixes
        }
        if (SafeReadUTF8Char(symbol, symbolBytes, ngram_begin_ptr, end) != RECODE_OK) {
            Y_ASSERT(false); // should already be validated during tokenization
            return;
        }
        ngram_begin_ptr += symbolBytes;
    }
}

Ydb::Table::FulltextIndexSettings::Analyzers GetAnalyzersForQuery(Ydb::Table::FulltextIndexSettings::Analyzers analyzers) {
    // Prevent splitting tokens into ngrams
    analyzers.set_use_filter_ngram(false);
    analyzers.set_use_filter_edge_ngram(false);
    // Prevent dropping patterns by length
    analyzers.set_use_filter_length(false);

    return analyzers;
}

TVector<TString> Analyze(const TStringBuf text, const Ydb::Table::FulltextIndexSettings::Analyzers& settings, const std::unordered_set<wchar32>& ignoredDelimiters) {
    TVector<TString> tokens = Tokenize(text, settings.tokenizer(), ignoredDelimiters);

    if (settings.use_filter_lowercase()) {
        for (auto i : xrange(tokens.size())) {
            tokens[i] = ToLowerUTF8(tokens[i]);
        }
    }

    if (settings.use_filter_length() && (settings.has_filter_length_min() || settings.has_filter_length_max())) {
        tokens.erase(std::remove_if(tokens.begin(), tokens.end(), [&](const TString& token){
            auto length = GetLengthUTF8(token);
            if (settings.has_filter_length_min() && length < static_cast<size_t>(settings.filter_length_min())) {
                return true;
            }
            if (settings.has_filter_length_max() && length > static_cast<size_t>(settings.filter_length_max())) {
                return true;
            }
            return false;
        }), tokens.end());
    }

    if (settings.use_filter_snowball()) {
        struct sb_stemmer* stemmer = sb_stemmer_new(settings.language().c_str(), nullptr);
        if (Y_UNLIKELY(stemmer == nullptr)) {
            ythrow yexception() << "sb_stemmer_new returned nullptr";
        }
        Y_DEFER { sb_stemmer_delete(stemmer); };

        for (auto& token : tokens) {
            const sb_symbol* stemmed = sb_stemmer_stem(
                stemmer,
                reinterpret_cast<const sb_symbol*>(token.data()),
                token.size()
            );
            if (Y_UNLIKELY(stemmed == nullptr)) {
                ythrow yexception() << "unable to allocate memory for sb_stemmer_stem result";
            }

            const size_t resultLength = sb_stemmer_length(stemmer);
            token = std::string(reinterpret_cast<const char*>(stemmed), resultLength);
        }
    }

    if (settings.use_filter_ngram() || settings.use_filter_edge_ngram()) {
        TVector<TString> ngrams;
        for (const auto& token : tokens) {
            BuildNgrams(token, settings.filter_ngram_min_length(), settings.filter_ngram_max_length(), settings.use_filter_edge_ngram(), ngrams);
        }
        tokens.swap(ngrams);
    }

    return tokens;
}

TVector<TString> BuildSearchTerms(const TString& query, const Ydb::Table::FulltextIndexSettings::Analyzers& settings) {
    const bool expectWildcard = settings.use_filter_ngram() || settings.use_filter_edge_ngram();
    const bool edge = settings.use_filter_edge_ngram();

    if (!expectWildcard) {
        return Analyze(query, settings);
    }

    const Ydb::Table::FulltextIndexSettings::Analyzers analyzersForQuery = GetAnalyzersForQuery(settings);

    TVector<TString> searchTerms;
    for (const TString& pattern : Analyze(query, analyzersForQuery, std::unordered_set<wchar32>{'%', '_'})) {
        for (const auto& term : StringSplitter(pattern).SplitBySet("%_")) {
            const TString token(term.Token());
            const i64 tokenLength = GetLengthUTF8(token);

            if (tokenLength != 0 && analyzersForQuery.filter_ngram_min_length() <= tokenLength) {
                const size_t upper = std::min(static_cast<i64>(analyzersForQuery.filter_ngram_max_length()), tokenLength);
                BuildNgrams(token, upper, upper, edge, searchTerms);
            }

            if (edge) {
                break;
            }
        }
    }
    return searchTerms;
}

bool ValidateColumnsMatches(const NProtoBuf::RepeatedPtrField<TString>& columns, const Ydb::Table::FulltextIndexSettings& settings, TString& error) {
    return ValidateColumnsMatches(TVector<TString>{columns.begin(), columns.end()}, settings, error);
}

bool ValidateColumnsMatches(const TVector<TString>& columns, const Ydb::Table::FulltextIndexSettings& settings, TString& error) {
    TVector<TString> settingsColumns(::Reserve(settings.columns().size()));
    for (auto column : settings.columns()) {
        settingsColumns.push_back(column.column());
    }

    if (columns != settingsColumns) {
        error = TStringBuilder() << "columns " << settingsColumns << " should be " << columns;
        return false;
    }

    error = "";
    return true;
}

bool ValidateSettings(const Ydb::Table::FulltextIndexSettings& settings, TString& error) {
    // layout is set automatically based on index type (fulltext_plain vs fulltext_relevance)

    if (settings.columns().empty()) {
        error = "columns should be set";
        return false;
    }

    // current implementation limitation:
    if (settings.columns().size() != 1) {
        error = "columns should have a single value";
        return false;
    }

    for (auto column : settings.columns()) {
        if (!column.has_column()) {
            error = "column name should be set";
            return false;
        }

        // current implementation limitation:
        if (!settings.columns().at(0).has_analyzers()) {
            error = "column analyzers should be set";
            return false;
        }
        if (!ValidateSettings(column.analyzers(), error)) {
            return false;
        }
    }

    error = "";
    return true;
}

bool FillSetting(Ydb::Table::FulltextIndexSettings& settings, const TString& nameLower, const TString& value, TString& error) {
    error = "";

    Ydb::Table::FulltextIndexSettings::Analyzers* analyzers = settings.columns().empty()
        ? settings.add_columns()->mutable_analyzers()
        : settings.mutable_columns()->rbegin()->mutable_analyzers();

    if (nameLower == "tokenizer") {
        analyzers->set_tokenizer(ParseTokenizer(value, error));
    } else if (nameLower == "language") {
        analyzers->set_language(value);
    } else if (nameLower == "use_filter_lowercase") {
        analyzers->set_use_filter_lowercase(ParseBool(nameLower, value, error));
    } else if (nameLower == "use_filter_stopwords") {
        analyzers->set_use_filter_stopwords(ParseBool(nameLower, value, error));
    } else if (nameLower == "use_filter_ngram") {
        analyzers->set_use_filter_ngram(ParseBool(nameLower, value, error));
    } else if (nameLower == "use_filter_edge_ngram") {
        analyzers->set_use_filter_edge_ngram(ParseBool(nameLower, value, error));
    } else if (nameLower == "filter_ngram_min_length") {
        analyzers->set_filter_ngram_min_length(ParseInt32(nameLower, value, error));
    } else if (nameLower == "filter_ngram_max_length") {
        analyzers->set_filter_ngram_max_length(ParseInt32(nameLower, value, error));
    } else if (nameLower == "use_filter_length") {
        analyzers->set_use_filter_length(ParseBool(nameLower, value, error));
    } else if (nameLower == "filter_length_min") {
        analyzers->set_filter_length_min(ParseInt32(nameLower, value, error));
    } else if (nameLower == "filter_length_max") {
        analyzers->set_filter_length_max(ParseInt32(nameLower, value, error));
    } else if (nameLower == "use_filter_snowball") {
        analyzers->set_use_filter_snowball(ParseBool(nameLower, value, error));
    } else {
        error = TStringBuilder() << "Unknown index setting: " << nameLower;
        return false;
    }

    return !error;
}

void AddVarint(TVector<ui8>& buf, ui64 num) {
    while (true) {
        if (num < 0x80) {
            buf.push_back((ui8)num);
            break;
        } else {
            buf.push_back(0x80 | (ui8)(num & 0x7F));
            num >>= 7;
        }
    }
}

// regular varint but with an additional flag in the second most-significant bit
void AddVarintWithFlag(TVector<ui8>& buf, ui64 num, bool flag) {
    if (num < 0x40) {
        buf.push_back(((ui8)num | (flag ? 0x40 : 0)));
        return;
    } else {
        buf.push_back(0x80 | (flag ? 0x40 : 0) | (ui8)(num & 0x3F));
        num >>= 6;
        AddVarint(buf, num);
    }
}

ui64 ReadVarint(TConstArrayRef<ui8> buf, size_t& pos) {
    ui64 r = 0;
    ui32 o = 0;
    while (pos < buf.size()) {
        ui8 c = buf[pos++];
        r |= ((c & 0x7F) << o);
        if (!(c & 0x80)) {
            break;
        }
        o += 7;
    }
    return r;
}

ui64 ReadVarintWithFlag(TConstArrayRef<ui8> buf, size_t& pos, bool& flag) {
    flag = false;
    if (pos >= buf.size()) {
        return 0;
    }
    ui8 c = buf[pos++];
    flag = !!(c & 0x40);
    ui64 r = c & 0x3F;
    if (c & 0x80) {
        r |= ReadVarint(buf, pos) << 6;
    }
    return r;
}

void TDeltaReader::Reset(ui64 firstId, TConstArrayRef<ui8> buf)
{
    Buf = buf;
    Pos = 0;
    LastId = firstId;
}

bool TDeltaReader::Read(ui64& docId)
{
    if (Pos >= Buf.size())
        return false;
    docId = LastId + ReadVarint(Buf, Pos);
    LastId = docId;
    return true;
}

bool TDeltaReader::Read(ui64& docId, ui32& freq)
{
    if (Pos >= Buf.size())
        return false;
    bool hasFreq = false;
    docId = LastId + ReadVarintWithFlag(Buf, Pos, hasFreq);
    freq = hasFreq ? ReadVarint(Buf, Pos) : 1;
    LastId = docId;
    return true;
}

size_t TDeltaReader::GetPos() const
{
    return Pos;
}

bool TDeltaReader::IsEnded() const
{
    return Pos >= Buf.size();
}

void TDeltaWriter::Reset()
{
    Buf.clear();
    MinId = 0;
    MaxId = 0;
    Count = 0;
    TotalFreq = 0;
}

void TDeltaWriter::Add(ui64 DocId)
{
    Y_ENSURE(DocId > MaxId || !Count);
    if (!Count) {
        MinId = DocId;
    }
    AddVarint(Buf, DocId - MaxId);
    MaxId = DocId;
    Count++;
    TotalFreq++;
}

void TDeltaWriter::Add(ui64 DocId, ui32 Freq)
{
    Y_ENSURE(DocId > MaxId || !Count);
    if (!Count) {
        MinId = DocId;
    }
    AddVarintWithFlag(Buf, DocId - MaxId, Freq > 1);
    if (Freq > 1) {
        AddVarint(Buf, Freq);
    }
    MaxId = DocId;
    Count++;
    TotalFreq += Freq;
}

size_t TDeltaWriter::AddCompressed(ui64 firstId, TConstArrayRef<ui8> other, bool withFreq, ui64 maxCount)
{
    if (!other.size() || maxCount > 0 && Count >= maxCount) {
        return 0;
    }
    size_t pos = 0;
    if (withFreq) {
        bool hasFreq = false;
        ui64 docId = firstId+ReadVarintWithFlag(other, pos, hasFreq);
        ui64 freq = hasFreq ? ReadVarint(other, pos) : 1;
        Add(docId, freq);
        while (pos < other.size() && (!maxCount || Count < maxCount)) {
            ui64 deltaId = ReadVarintWithFlag(other, pos, hasFreq);
            ui64 freq = hasFreq ? ReadVarint(other, pos) : 1;
            Add(MaxId+deltaId, freq);
        }
    } else {
        ui64 docId = firstId+ReadVarint(other, pos);
        Add(docId);
        while (pos < other.size() && (!maxCount || Count < maxCount)) {
            ui64 deltaId = ReadVarint(other, pos);
            Add(MaxId+deltaId);
        }
    }
    return pos;
}

ui64 TDeltaWriter::GetMinId() const
{
    return MinId;
}

ui64 TDeltaWriter::GetMaxId() const
{
    return MaxId;
}

ui64 TDeltaWriter::GetCount() const
{
    return Count;
}

ui64 TDeltaWriter::GetTotalFreq() const
{
    return TotalFreq;
}

TConstArrayRef<ui8> TDeltaWriter::GetBuf() const
{
    return Buf;
}

void TMultiDeltaReader::Reset(bool withFreq)
{
    Readers.clear();
    Items.clear();
    WithFreq = withFreq;
    OneLeft = false;
    Started = false;
}

void TMultiDeltaReader::Add(TConstArrayRef<ui8> buf, bool added)
{
    Y_ENSURE(!Started);
    if (!buf.size()) {
        return;
    }
    Readers.emplace_back();
    auto& rdr = Readers.back();
    rdr.Added = added;
    rdr.Reader.Reset(0, buf);
}

void TMultiDeltaReader::Start()
{
    Started = true;
    if (Readers.size() == 1) {
        OneLeft = true;
    } else {
        for (size_t i = 0; i < Readers.size(); i++) {
            Consume(i+1, Readers[i]);
        }
        SelectNext();
    }
}

void TMultiDeltaReader::SelectNext()
{
    std::pop_heap(Items.begin(), Items.end(), CompareItems);
    NextItem = Items.back();
    Items.pop_back();
    auto& rdr = Readers[NextItem.RdrId-1];
    if (!rdr.Reader.IsEnded()) {
        Consume(NextItem.RdrId, rdr);
    }
}

void TMultiDeltaReader::Consume(ui32 rdrId, TReaderRef& rdr)
{
    ui64 docId = 0;
    ui32 freq = 1;
    if (WithFreq) {
        rdr.Reader.Read(docId, freq);
    } else {
        rdr.Reader.Read(docId);
    }
    Items.push_back(TItem{docId, (rdr.Added ? (i32)freq : -(i32)freq), rdrId});
    std::push_heap(Items.begin(), Items.end(), CompareItems);
}

bool TMultiDeltaReader::Read(ui64& docId)
{
    ui32 freq = 0;
    return Read(docId, freq);
}

bool TMultiDeltaReader::Read(ui64& docId, ui32& freq)
{
    Y_ENSURE(Started);
    TItem cur = NextItem;
    if (OneLeft) {
        auto& rdr = Readers[0];
        freq = 1;
        return WithFreq ? rdr.Reader.Read(docId, freq) : rdr.Reader.Read(docId);
    }
    if (Items.size() == 1) {
        // Bypass heap :)
        if (Items[0].RdrId == NextItem.RdrId) {
            if (NextItem.Freq > 0) {
                docId = NextItem.DocId;
                freq = NextItem.Freq;
                NextItem = {};
                return true;
            }
            NextItem = {};
            return Read(docId, freq);
        } else if (!NextItem.RdrId) {
            auto& rdr = Readers[Items[0].RdrId-1];
            if (!rdr.Added) {
                // This is the last reader and it's "deleted", finish.
                Readers.clear();
                Items.clear();
                return false;
            } else {
                docId = Items[0].DocId;
                freq = Items[0].Freq;
                Readers = {std::move(rdr)};
                OneLeft = true;
                Items.clear();
                return true;
            }
        }
    }
    while (Items.size() > 0) {
        SelectNext();
        if (cur.DocId == NextItem.DocId) {
            // Add frequencies
            cur.Freq += NextItem.Freq;
        } else {
            if (cur.Freq > 0) {
                // Finished, item has positive frequency (not canceled by updates)
                // Leave NextItem as is
                docId = cur.DocId;
                freq = cur.Freq;
                return true;
            } else {
                // Scan the next item
                cur = NextItem;
            }
        }
    }
    NextItem = {};
    if (cur.Freq > 0) {
        // Finished, item has positive frequency (not canceled by updates)
        docId = cur.DocId;
        freq = cur.Freq;
        return true;
    }
    return false;
}

bool TMultiDeltaReader::IsEnded() const
{
    return !Items.size() && !NextItem.RdrId;
}

}

template<> inline
void Out<TVector<TString>>(IOutputStream& o, const TVector<TString> &vec) {
    o << "[ ";
    for (const auto &x : vec)
        o << x << ' ';
    o << "]";
}
