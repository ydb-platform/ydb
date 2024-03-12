#pragma once

#include <util/generic/algorithm.h>

namespace NKikimr::NViewer {

inline ui32 LevenshteinDistance(TString word1, TString word2) {
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
    struct WordHit {
        ui32 Distance;
        TString Word;

        WordHit(ui32 dist, TString word)
            : Distance(dist)
            , Word(word)
        {}

        bool operator<(const WordHit& other) const {
            return Distance < other.Distance;
        }

        bool operator>(const WordHit& other) const {
            return Distance > other.Distance;
        }
    };

public:
    TVector<TString> Tenants;
    FuzzySearcher(const TVector<TString>& tenants)
        : Tenants(tenants) {}

    TVector<TString> Search(const TString& prefix, ui32 limit = 10) {
        auto cmp = [](const WordHit& left, const WordHit& right) {
            return left.Distance > right.Distance;
        };
        std::priority_queue<WordHit, TVector<WordHit>, decltype(cmp)> queue(cmp);

        for (const auto& word : Tenants) {
            auto wordHit = WordHit(LevenshteinDistance(prefix, word), word);
            if (queue.size() < limit) {
                queue.emplace(wordHit);
            } else if (wordHit.Distance < queue.top().Distance) {
                queue.pop();
                queue.emplace(wordHit);
            }
        }

        TVector<TString> results;
        while (!queue.empty()) {
            results.emplace_back(queue.top().Word);
            queue.pop();
        }

        std::reverse(results.begin(), results.end());
        return results;
    }
};

}
