#pragma once

#include <iostream>
#include <vector>
#include <string>
#include <climits>

class LoserTree {
private:
    std::vector<int> tree; // This will store indices of the losers
    std::vector<std::vector<char*>>& arrays;
    std::vector<int> indices; // Current indices of each sorted array

    void build() {
        size_t k = arrays.size() - 1;
        for (size_t i = 0; i <= k; ++i) {
            indices[i] = 0;
        }
        for (int i = 1; i <= k; ++i) {
            tree[i] = k;
        }
        for (int i = 0; i < k; ++i) {
            adjust(i);
        }
    }

    void adjust(int index) {
        size_t t = (tree.size() + index - 1) / 2; // Start from the leaf node corresponding to index
        while (t > 0) {
            if (indices[tree[t]] < arrays[tree[t]].size() && (indices[index] >= arrays[index].size() || strcmp(arrays[tree[t]][indices[tree[t]]], arrays[index][indices[index]]) < 0)) {
                std::swap(index, tree[t]);
            }
            t /= 2;
        }
        tree[0] = index; // Winner goes to the root
    }

public:
    LoserTree(std::vector<std::vector<char*>>& input) : arrays(input) {
        size_t k = input.size() + 1;
        std::string str;
        char* c = &str[0];
        arrays.push_back({ c });

        tree = std::vector<int>(k, 0);
        indices = std::vector<int>(k, 0);

        build();
    }

    void merge(std::vector<char*>& result) {
        while (true) {
            int winnerIndex = tree[0];
            if (winnerIndex == -1) {
                break;
            }

            if (indices[winnerIndex] >= arrays[winnerIndex].size()) {
                break;
            }

            result.push_back(arrays[winnerIndex][indices[winnerIndex]]);

            indices[winnerIndex]++;
            adjust(winnerIndex);
        }
    }
};


/* Min heap implementation (actually not better than the first one) */

/*
#include <iostream>
#include <vector>
#include <queue>
#include <tuple>
#include <cstring>

class LoserTree {
private:
    std::vector<std::vector<char*>>& arrays;
    using Element = std::tuple<char*, int, int>;

    struct Compare {
        bool operator()(const Element& a, const Element& b) {
            return strcmp(std::get<0>(a), std::get<0>(b)) > 0;
        }
    };

    std::priority_queue<Element, std::vector<Element>, Compare> minHeap;

    void initialize() {
        for (int i = 0; i < arrays.size(); ++i) {
            if (!arrays[i].empty()) {
                minHeap.push(std::make_tuple(arrays[i][0], i, 0));
            }
        }
    }

public:
    LoserTree(std::vector<std::vector<char*>>& input) : arrays(input) {
        initialize();
    }

    void merge(std::vector<char*>& result) {
        while (!minHeap.empty()) {
            auto [value, arrayIndex, elementIndex] = minHeap.top();
            minHeap.pop();

            result.push_back(value);

            if (elementIndex + 1 < arrays[arrayIndex].size()) {
                minHeap.push(std::make_tuple(arrays[arrayIndex][elementIndex + 1], arrayIndex, elementIndex + 1));
            }
        }
    }
};
*/