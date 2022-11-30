#pragma once

#include <library/cpp/charset/wide.h>
#include <library/cpp/containers/str_map/str_map.h>
#include <library/cpp/containers/str_hash/str_hash.h>
#include <library/cpp/wordlistreader/wordlistreader.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/charset/wide.h>
#include <util/memory/tempbuf.h>

#include <type_traits>

enum EStickySide {
    STICK_NONE = 0,
    STICK_LEFT = 1,
    STICK_RIGHT = 2,
    STICK_BOTH = 3,
};

size_t TTCharStrIHashImpl(const wchar16* ptr);
bool TTCharStrIEqualToImpl(const wchar16* s1, const wchar16* s2);

struct TTCharStrIHasher {
    size_t operator()(const wchar16* s) const {
        return TTCharStrIHashImpl(s);
    }
};

struct TTCharStrIEqualTo {
    bool operator()(const wchar16* s1, const wchar16* s2) {
        return TTCharStrIEqualToImpl(s1, s2);
    }
};

// Hash of stop words, plus facilities to load it from a file
class TWordFilter {
public:
    struct TStopWordInfo {
        ::TLangMask Language;
        EStickySide Stickiness;
        TStopWordInfo(::TLangMask lang = LI_ALL_LANGUAGES, EStickySide side = STICK_NONE)
            : Language(lang)
            , Stickiness(side)
        {
        }
    };
    typedef Hash<TStopWordInfo> TStopWordsHash;
    typedef THashWithSegmentedPoolForKeys<wchar16, TStopWordInfo, TTCharStrIHasher, TTCharStrIEqualTo> TWideStopWordsHash;
    template <class TTChar>
    struct THashType;

    inline TWordFilter() {
    }

    // Recommended initialization - from a config file
    bool InitStopWordsList(const char* filename);
    bool InitStopWordsList(IInputStream& instream);

    // Deprecated initialization - just words in single-byte encoding, no language data, no i18n
    bool InitStopWordsList(const char** s, size_t n);

    void TermStopWordsList() {
        WordFilter = nullptr;
        WideWordFilter = nullptr;
        PlainWordFilter = nullptr;
    }

    //in case TTChar == char, assumes csYandex
    //see MORPH-74
    template <class TTChar>
    bool IsStopWord(const TTChar* word, ::TLangMask lang = ::TLangMask(), EStickySide* side = nullptr) const {
        if (!word || !*word)
            return false;
        typedef typename THashType<TTChar>::Type THash;
        const TAtomicSharedPtr<THash>& wordFilter = GetHashPtr<TTChar>();
        if (!wordFilter)
            return false;

        typename THash::const_iterator it = wordFilter->find(word);
        if (it == wordFilter->end())
            return false;
        if (lang.none() || (it->second.Language & lang).any()) {
            if (side)
                *side = it->second.Stickiness;
            return true;
        }
        return false;
    }

    // assumes word is in UTF8
    bool IsStopWord(const TString& word, ::TLangMask lang = ::TLangMask(), EStickySide* side = nullptr) const {
        return IsStopWord(word.c_str(), lang, side);
    }

    bool IsStopWord(const TUtf16String& word, ::TLangMask lang = ::TLangMask(), EStickySide* side = nullptr) const {
        return IsStopWord(word.c_str(), lang, side);
    }

    template <class TTChar>
    bool IsStopWord(const TTChar* word, size_t len, ::TLangMask lang = ::TLangMask(), EStickySide* side = nullptr) const {
        TTempArray<TTChar> str(len + 1);
        memcpy((void*)str.Data(), word, len * sizeof(TTChar));
        str.Data()[len] = 0;
        return IsStopWord(str.Data(), lang, side);
    }

    // Deprecated interface - get a plain list of single-byte strings
    const HashSet* GetWordFilter() const {
        return PlainWordFilter.Get();
    }

    static const TWordFilter EmptyFilter;

private:
    //in csYandex
    TAtomicSharedPtr<HashSet> PlainWordFilter; // compatibility: will be gone when no one uses GetWordFilter()
    //in UTF8
    TAtomicSharedPtr<TStopWordsHash> WordFilter;
    //in UTF16
    TAtomicSharedPtr<TWideStopWordsHash> WideWordFilter;
    void InitWideFilter();
    void InitNarrowFilter();

    template <class TTChar>
    inline const TAtomicSharedPtr<typename THashType<TTChar>::Type>& GetHashPtr() const;
};
template <>
struct TWordFilter::THashType<char> {
    typedef TStopWordsHash Type;
};
template <>
struct TWordFilter::THashType<wchar16> {
    typedef TWideStopWordsHash Type;
};
template <>
inline const TAtomicSharedPtr<TWordFilter::TStopWordsHash>& TWordFilter::GetHashPtr<char>() const {
    return WordFilter;
}
template <>
inline const TAtomicSharedPtr<TWordFilter::TWideStopWordsHash>& TWordFilter::GetHashPtr<wchar16>() const {
    return WideWordFilter;
}
