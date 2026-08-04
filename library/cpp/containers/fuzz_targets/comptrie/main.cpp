#include <library/cpp/containers/comptrie/comptrie.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/stream/str.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>

namespace {

using TTrie = TCompactTrie<char, ui32>;
using TBuilder = TCompactTrieBuilder<char, ui32>;

struct TUnsignedStringLess {
    bool operator()(const TString& lhs, const TString& rhs) const {
        const size_t common = Min(lhs.size(), rhs.size());
        for (size_t i = 0; i < common; ++i) {
            const ui8 left = static_cast<ui8>(lhs[i]);
            const ui8 right = static_cast<ui8>(rhs[i]);
            if (left != right) {
                return left < right;
            }
        }
        return lhs.size() < rhs.size();
    }
};

using TModel = TMap<TString, ui32, TUnsignedStringLess>;

constexpr size_t MaxInitialEntries = 48;
constexpr size_t MaxOperations = 96;
constexpr size_t MaxKeySize = 32;

TString MakeKey(FuzzedDataProvider& fdp) {
    auto bytes = fdp.ConsumeRandomLengthString(MaxKeySize);
    TString key(bytes.data(), bytes.size());

    // char comptrie intentionally cannot distinguish "" from "\0"; avoid
    // model false positives while still exercising byte-oriented keys.
    for (char& ch : key) {
        if (ch == '\0') {
            ch = '\1';
        }
    }
    return key;
}

bool HasPrefix(const TString& key, const TString& prefix) {
    return key.size() >= prefix.size() && std::equal(prefix.begin(), prefix.end(), key.begin());
}

TString Tail(const TString& key, const TString& prefix) {
    return TString(key.data() + prefix.size(), key.size() - prefix.size());
}

bool FindLongestPrefixModel(const TModel& model, const TString& key, size_t& prefixLen, ui32& value) {
    bool found = false;
    prefixLen = 0;
    value = 0;

    for (const auto& [candidate, candidateValue] : model) {
        if (candidate.size() >= prefixLen && HasPrefix(key, candidate)) {
            found = true;
            prefixLen = candidate.size();
            value = candidateValue;
        }
    }

    return found;
}

template <class TLookup>
void ValidateLookup(const TLookup& lookup, const TModel& model, const TVector<TString>& queries) {
    for (const auto& [key, expectedValue] : model) {
        ui32 value = 0;
        Y_ENSURE(lookup.Find(key.data(), key.size(), &value));
        Y_ENSURE(value == expectedValue);
    }

    for (const TString& query : queries) {
        const auto it = model.find(query);

        ui32 value = 0xdeadbeefu;
        const bool found = lookup.Find(query.data(), query.size(), &value);
        Y_ENSURE(found == (it != model.end()));
        if (it != model.end()) {
            Y_ENSURE(value == it->second);
        } else {
            Y_ENSURE(value == 0xdeadbeefu);
        }

        size_t actualPrefixLen = Max<size_t>();
        ui32 actualPrefixValue = 0xbaadf00du;
        const bool actualPrefix = lookup.FindLongestPrefix(
            query.data(),
            query.size(),
            &actualPrefixLen,
            &actualPrefixValue
        );

        size_t expectedPrefixLen = 0;
        ui32 expectedPrefixValue = 0;
        const bool expectedPrefix = FindLongestPrefixModel(model, query, expectedPrefixLen, expectedPrefixValue);

        Y_ENSURE(actualPrefix == expectedPrefix);
        if (expectedPrefix) {
            Y_ENSURE(actualPrefixLen == expectedPrefixLen);
            Y_ENSURE(actualPrefixValue == expectedPrefixValue);
        }
    }
}

TModel CollectTrieEntries(const TTrie& trie) {
    TModel actual;
    for (auto it = trie.Begin(), end = trie.End(); it != end; ++it) {
        const auto value = *it;
        actual[value.first] = value.second;
    }
    return actual;
}

void ValidateTrie(const TTrie& trie, const TModel& model, const TVector<TString>& queries) {
    Y_ENSURE(trie.Size() == model.size());
    Y_ENSURE(CollectTrieEntries(trie) == model);
    ValidateLookup(trie, model, queries);

    for (const TString& query : queries) {
        if (!model.empty()) {
            auto trieIt = trie.UpperBound(query);
            const auto exactIt = model.find(query);
            if (exactIt != model.end()) {
                Y_ENSURE(trieIt != trie.End());
                Y_ENSURE(trieIt.GetKey() == exactIt->first);
                Y_ENSURE(trieIt.GetValue() == exactIt->second);
            } else if (trieIt != trie.End()) {
                const auto returnedIt = model.find(trieIt.GetKey());
                Y_ENSURE(returnedIt != model.end());
                Y_ENSURE(returnedIt->second == trieIt.GetValue());
            }
        }

        TModel expectedTails;
        for (const auto& [key, value] : model) {
            if (HasPrefix(key, query)) {
                expectedTails[Tail(key, query)] = value;
            }
        }

        TTrie tails;
        const bool hasTails = trie.FindTails(query.data(), query.size(), tails);
        Y_ENSURE(hasTails == !expectedTails.empty());
        if (hasTails) {
            Y_ENSURE(CollectTrieEntries(tails) == expectedTails);
        }
    }
}

TBuffer SaveBuilder(const TBuilder& builder) {
    TBufferOutput out;
    builder.Save(out);
    return out.Buffer();
}

void ValidateSerialized(const TBuffer& data, const TModel& model, const TVector<TString>& queries) {
    TTrie trie(data.Data(), data.Size());
    ValidateTrie(trie, model, queries);
}

TBuffer Minimize(const TBuffer& data) {
    TBufferOutput out;
    CompactTrieMinimize<TTrie::TPacker>(out, data.Data(), data.Size(), false);
    return out.Buffer();
}

TBuffer MakeFastLayout(const TBuffer& data) {
    TBufferOutput out;
    CompactTrieMakeFastLayout<TTrie::TPacker>(out, data.Data(), data.Size(), false);
    return out.Buffer();
}

TBuilder BuildFromModel(const TModel& model, TCompactTrieBuilderFlags flags = CTBF_NONE) {
    TBuilder builder(flags);
    for (const auto& [key, value] : model) {
        Y_ENSURE(builder.Add(key.data(), key.size(), value));
    }
    return builder;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider fdp(data, size);

    TBuilder builder;
    TModel model;
    TVector<TString> knownKeys;
    TVector<TString> queries;

    const size_t initialEntries = fdp.ConsumeIntegralInRange<size_t>(0, MaxInitialEntries);
    for (size_t i = 0; i < initialEntries; ++i) {
        TString key = MakeKey(fdp);
        const ui32 value = fdp.ConsumeIntegral<ui32>();
        const bool expectedNew = model.find(key) == model.end();
        const bool added = builder.Add(key.data(), key.size(), value);
        Y_ENSURE(added == expectedNew);
        if (added) {
            model.emplace(key, value);
            knownKeys.push_back(key);
        }
        queries.push_back(key);
    }

    const size_t operations = fdp.ConsumeIntegralInRange<size_t>(0, MaxOperations);
    for (size_t i = 0; i < operations; ++i) {
        const ui8 op = fdp.ConsumeIntegral<ui8>() % 7;
        TString key = (op == 1 && !knownKeys.empty())
            ? knownKeys[fdp.ConsumeIntegralInRange<size_t>(0, knownKeys.size() - 1)]
            : MakeKey(fdp);

        if (op <= 2) {
            const ui32 value = fdp.ConsumeIntegral<ui32>();
            const bool expectedNew = model.find(key) == model.end();
            const bool added = builder.Add(key.data(), key.size(), value);
            Y_ENSURE(added == expectedNew);
            if (added) {
                model.emplace(key, value);
                knownKeys.push_back(key);
            }
        }

        if (op == 3 && !key.empty()) {
            key = key.substr(0, fdp.ConsumeIntegralInRange<size_t>(0, key.size()));
        } else if (op == 4 && !knownKeys.empty()) {
            const TString& base = knownKeys[fdp.ConsumeIntegralInRange<size_t>(0, knownKeys.size() - 1)];
            key = base + MakeKey(fdp);
        }

        queries.push_back(key);
    }

    ValidateLookup(builder, model, queries);

    const TBuffer serialized = SaveBuilder(builder);
    ValidateSerialized(serialized, model, queries);

    const TBuilder prefixGrouped = BuildFromModel(model, CTBF_PREFIX_GROUPED);
    ValidateLookup(prefixGrouped, model, queries);
    ValidateSerialized(SaveBuilder(prefixGrouped), model, queries);

    TBuilder unique(CTBF_UNIQUE);
    for (const auto& [key, value] : model) {
        Y_ENSURE(unique.Add(key.data(), key.size(), value));
        try {
            Y_ENSURE(!unique.Add(key.data(), key.size(), value + 1));
        } catch (const yexception&) {
        }
    }
    ValidateLookup(unique, model, queries);

    {
        TBuilder destructive = BuildFromModel(model);
        TBufferOutput out;
        destructive.SaveAndDestroy(out);
        ValidateSerialized(out.Buffer(), model, queries);
    }

    if (!serialized.Empty()) {
        const TBuffer minimized = Minimize(serialized);
        ValidateSerialized(minimized, model, queries);

        const TBuffer fast = MakeFastLayout(serialized);
        ValidateSerialized(fast, model, queries);

        const TBuffer minFast = MakeFastLayout(minimized);
        ValidateSerialized(minFast, model, queries);
    }

    return 0;
}
