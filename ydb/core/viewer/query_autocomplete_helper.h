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

class FuzzySearcher {
    static size_t CalculateWordHit(const TString& searchWord, const TString& testWord) {
        size_t findPos = testWord.find(searchWord);
        if (findPos != TString::npos) {
            return testWord.size() - searchWord.size() + findPos;
        } else {
            return 1000 * LevenshteinDistance(searchWord, testWord);
        }
    }

public:
    template<typename Type>
    static std::vector<const Type*> Search(const std::vector<Type>& dictionary, const TString& searchWord, ui32 limit = 10) {
        TString search = to_lower(searchWord);
        std::vector<std::pair<size_t, size_t>> hits; // {distance, index}
        hits.reserve(dictionary.size());
        for (size_t index = 0; index < dictionary.size(); ++index) {
            hits.emplace_back(CalculateWordHit(search, to_lower(TString(dictionary[index]))), index);
        }
        std::sort(hits.begin(), hits.end());
        if (hits.size() > limit) {
            hits.resize(limit);
        }
        std::vector<const Type*> result;
        result.reserve(hits.size());
        for (const auto& hit : hits) {
            result.emplace_back(&dictionary[hit.second]);
        }
        return result;
    }
};

}
