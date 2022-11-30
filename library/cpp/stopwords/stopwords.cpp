#include <algorithm>

#include <library/cpp/charset/wide.h>
#include <util/memory/tempbuf.h>
#include <util/string/vector.h>
#include <util/generic/yexception.h>
#include <util/digest/murmur.h>
#include <util/string/split.h>

#include "stopwords.h"

const EStickySide DefaultStickiness = STICK_RIGHT;
const TWordFilter TWordFilter::EmptyFilter;

struct TToLower {
    wchar16 operator()(wchar16 c) {
        return (wchar16)ToLower(c);
    }
};

size_t TTCharStrIHashImpl(const wchar16* ptr) {
    const size_t len = ptr ? std::char_traits<wchar16>::length(ptr) : 0;
    TCharTemp buf(len);
    std::transform(ptr, ptr + len, buf.Data(), TToLower());
    return MurmurHash<size_t>((void*)buf.Data(), len * sizeof(wchar16));
}

bool TTCharStrIEqualToImpl(const wchar16* s1, const wchar16* s2) {
    if (!s1 || !s2)
        return !s1 == !s2;
    for (; *s1 && *s2; ++s1, ++s2)
        if (ToLower(*s1) != ToLower(*s2))
            return false;
    return *s1 == *s2;
}

namespace {
    struct TReaderImpl: public TWordListReader {
        TWordFilter::TWideStopWordsHash* Res;
        ELanguage CurrentLanguage;
        EStickySide CurrentStickiness;
        TReaderImpl(TWordFilter::TWideStopWordsHash* res)
            : Res(res)
            , CurrentLanguage(LANG_UNK)
            , CurrentStickiness(DefaultStickiness)
        {
        }
        void ParseLine(const TUtf16String& line, ELanguage langcode, int version) override;
        void ReadDataFile(const char* s) {
            TWordListReader::ReadDataFile(s);
        }
        void ReadDataFile(IInputStream& in) {
            TWordListReader::ReadDataFile(in);
        }
    };
}

bool TWordFilter::InitStopWordsList(const char* filename) {
    if (!filename || !*filename)
        return true;
    TBuffered<TUnbufferedFileInput> src(4096, filename);
    return InitStopWordsList(src);
}

bool TWordFilter::InitStopWordsList(IInputStream& instream) {
    TermStopWordsList();
    WordFilter.Reset(new TStopWordsHash);
    WideWordFilter.Reset(new TWideStopWordsHash);
    PlainWordFilter.Reset(new HashSet);
    TReaderImpl reader(WideWordFilter.Get());
    reader.ReadDataFile(instream);
    InitNarrowFilter();
    return true;
}

// Deprecated initializer - no language data, default stickiness
bool TWordFilter::InitStopWordsList(const char** s, size_t n) {
    TermStopWordsList();
    if (!s)
        return false;
    WordFilter.Reset(new TStopWordsHash);
    WideWordFilter.Reset(new TWideStopWordsHash);
    PlainWordFilter.Reset(new HashSet);
    for (size_t i = 0; i < n; i++) {
        if (s[i])
            WordFilter->Add(s[i], TStopWordInfo(LI_ALL_LANGUAGES, DefaultStickiness));
    }
    InitWideFilter();
    return true;
}

void TWordFilter::InitWideFilter() {
    for (TStopWordsHash::const_iterator it = WordFilter->begin(); it != WordFilter->end(); ++it) {
        TUtf16String tmp = UTF8ToWide(it->first);
        PlainWordFilter->Add(WideToChar(tmp.data(), tmp.size(), CODES_YANDEX).c_str());
        WideWordFilter->insert_copy(tmp.c_str(), tmp.size() + 1, it->second);
    }
}

void TWordFilter::InitNarrowFilter() {
    TString tmp;
    for (TWideStopWordsHash::const_iterator it = WideWordFilter->begin(); it != WideWordFilter->end(); ++it) {
        const wchar16* const str = it->first;
        const size_t len = std::char_traits<wchar16>::length(str);
        tmp.resize(len);
        WideToChar(str, len, tmp.begin(), CODES_YANDEX);
        PlainWordFilter->Add(tmp.c_str());
        WordFilter->Add(WideToUTF8(it->first).c_str(), it->second);
    }
}

void TReaderImpl::ParseLine(const TUtf16String& line, ELanguage langcode, int version) {
    static const TUtf16String delimiters = u" \t\r\n,;";
    static const TUtf16String strNone = u"NONE:";
    static const TUtf16String strLeft = u"LEFT:";
    static const TUtf16String strRight = u"RIGHT:";
    static const TUtf16String strBoth = u"BOTH:";

    if (langcode != CurrentLanguage) {
        CurrentStickiness = DefaultStickiness; // reset stickiness at the beginning of each zone
        CurrentLanguage = langcode;
    }
    TLangMask langCode = langcode != LANG_UNK ? TLangMask(langcode) : LI_ALL_LANGUAGES;

    TVector<TUtf16String> tokens;
    StringSplitter(line).SplitBySet(delimiters.c_str()).SkipEmpty().Collect(&tokens);
    TVector<TUtf16String>::const_iterator it;
    for (it = tokens.begin(); it != tokens.end(); it++) {
        if (it->empty())
            continue;      // due diligence
        if (version > 1) { // support for stickiness comes from version 2
            if (*it == strNone) {
                CurrentStickiness = STICK_NONE;
                continue;
            } else if (*it == strLeft) {
                CurrentStickiness = STICK_LEFT;
                continue;
            } else if (*it == strRight) {
                CurrentStickiness = STICK_RIGHT;
                continue;
            } else if (*it == strBoth) {
                CurrentStickiness = STICK_BOTH;
                continue;
            }
        }
        TWordFilter::TWideStopWordsHash::iterator fit = Res->find(it->c_str());
        if (fit == Res->end())
            Res->insert_copy(it->c_str(), it->length() + 1, TWordFilter::TStopWordInfo(langCode, CurrentStickiness));
        else
            fit->second.Language.SafeSet(langcode);
    }
}
