#pragma once

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <random>

namespace NYdbWorkload {

    class TMarkovModelEvaluator {
    public:
        static constexpr TStringBuf START_TOKEN = "__START__";
        static constexpr TStringBuf END_TOKEN = "__END__";

        struct TWeightedSuccessor {
            TString Word;
            int Weight = 0;
        };

        struct TTransitions {
            TVector<TWeightedSuccessor> Successors;
            int TotalWeight = 0;
        };
    private:
        THashMap<TString, TTransitions> Chain;
        int Order = 1;
    public:
        static TMarkovModelEvaluator LoadFromFile(const TString& path);
        TString NextWord(const TString& contextKey, std::mt19937& rng) const;
        static TString MakeKey(const TDeque<TString>& context);
        TString GenerateSentence(size_t targetLen, std::mt19937& rng) const;
    };

} // namespace NYdbWorkload
