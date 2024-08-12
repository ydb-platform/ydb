#pragma once
#include <queue>
#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NViewer {

inline ui32 LevenshteinDistance(TString word1, TString word2) {
    word1 = to_lower(word1);
    word2 = to_lower(word2);
    ui32 size1 = word1.size();
    ui32 size2 = word2.size();
    ui32 dist[size1 + 1][size2 + 1]; // distance matrix

    if (size1 == 0)
        return size2;
    if (size2 == 0)
        return size1;

    for (ui32 i = 0; i <= size1; i++)
        dist[i][0] = i;
    for (ui32 j = 0; j <= size2; j++)
        dist[0][j] = j;

    for (ui32 i = 1; i <= size1; i++) {
        for (ui32 j = 1; j <= size2; j++) {
            ui32 cost = (word2[j - 1] == word1[i - 1]) ? 0 : 1;
            dist[i][j] = std::min(std::min(dist[i - 1][j] + 1, dist[i][j - 1] + 1),dist[i - 1][j - 1] + cost);
        }
    }

    return dist[size1][size2];
}

template<typename Type>
class FuzzySearcher {
    struct WordHit {
        bool Contains;
        ui32 LengthDifference;
        ui32 LevenshteinDistance;
        Type Data;

        WordHit(bool contains, ui32 lengthDifference, ui32 levenshteinDistance, Type data)
            : Contains(contains)
            , LengthDifference(lengthDifference)
            , LevenshteinDistance(levenshteinDistance)
            , Data(data)
        {}

        bool operator<(const WordHit& other) const {
            if (this->Contains && !other.Contains) {
                return true;
            }
            if (this->Contains && other.Contains) {
                return this->LengthDifference < other.LengthDifference;
            }
            return this->LevenshteinDistance < other.LevenshteinDistance;
        }

        bool operator>(const WordHit& other) const {
            if (!this->Contains && other.Contains) {
                return true;
            }
            if (this->Contains && other.Contains) {
                return this->LengthDifference > other.LengthDifference;
            }
            return this->LevenshteinDistance > other.LevenshteinDistance;
        }
    };

    static WordHit CalculateWordHit(TString searchWord, TString testWord, Type testData) {
        searchWord = to_lower(searchWord);
        testWord = to_lower(testWord);
        if (testWord.Contains(searchWord)) {
            return {1, static_cast<ui32>(testWord.length() - searchWord.length()), 0, testData};
        } else {
            ui32 levenshteinDistance = LevenshteinDistance(searchWord, testWord);
            return {0, 0, levenshteinDistance, testData};
        }
    }

public:
    THashMap<TString, Type> Dictionary;

    FuzzySearcher(const THashMap<TString, Type>& dictionary)
        : Dictionary(dictionary) {}

    FuzzySearcher(const TVector<TString>& words) {
        for (const auto& word : words) {
            Dictionary[word] = word;
        }
    }

    TVector<Type> Search(const TString& searchWord, ui32 limit = 10) {
        auto cmp = [](const WordHit& left, const WordHit& right) {
            return left < right;
        };
        std::priority_queue<WordHit, TVector<WordHit>, decltype(cmp)> queue(cmp);

        for (const auto& [word, data]: Dictionary) {
            auto wordHit = CalculateWordHit(searchWord, word, data);
            if (queue.size() < limit) {
                queue.emplace(wordHit);
            } else if (queue.size() > 0 && wordHit < queue.top()) {
                queue.pop();
                queue.emplace(wordHit);
            }
        }

        TVector<Type> results;
        while (!queue.empty()) {
            results.emplace_back(queue.top().Data);
            queue.pop();
        }

        std::reverse(results.begin(), results.end());
        return results;
    }
};

}
