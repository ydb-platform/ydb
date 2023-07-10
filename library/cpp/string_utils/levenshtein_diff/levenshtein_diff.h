#pragma once

#include <util/draft/matrix.h>
#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <type_traits>
#include <utility>

namespace NLevenshtein {
    enum EEditMoveType {
        EMT_SPECIAL,
        EMT_PRESERVE,
        EMT_REPLACE,
        EMT_DELETE,
        EMT_INSERT,
        EMT_TRANSPOSE
    };

    inline bool IsImportantEditMove(EEditMoveType p) {
        return (p != EMT_SPECIAL && p != EMT_PRESERVE);
    }

    inline void MakeMove(EEditMoveType t, int& p1, int& p2) {
        switch (t) {
            case EMT_TRANSPOSE:
                p1 += 2;
                p2 += 2;
                break;
            case EMT_PRESERVE:
            case EMT_REPLACE:
                p1++;
                p2++;
                break;
            case EMT_DELETE:
                p1++;
                break;
            case EMT_INSERT:
                p2++;
                break;
            default:
                break;
        }
    }

    using TEditChain = TVector<EEditMoveType>;

    template <typename TArgType, typename TWeightType = int>
    struct TWeightOneUnaryGetter {
        TWeightType operator()(const TArgType&) const {
            return 1;
        }
    };

    template <typename TArgType, typename TWeightType = int>
    struct TWeightOneBinaryGetter {
        TWeightType operator()(const TArgType&, const TArgType&) const {
            return 1;
        }
    };

    template <typename TArgType, typename TWeightType = int>
    struct TWeightInfBinaryGetter {
        TWeightType operator()(const TArgType&, const TArgType&) const {
            return std::numeric_limits<TWeightType>::max();
        }
    };

    template <typename TStringType>
    using TCharType = typename std::decay_t<decltype(std::add_const_t<TStringType>()[0])>;

    /// Finds sequence of "edit moves" for two strings
    template <class TStringType, class TWeightType = int,
        class TReplaceWeigher = TWeightOneBinaryGetter<TCharType<TStringType>, TWeightType>,
        class TDeleteWeigher = TWeightOneUnaryGetter<TCharType<TStringType>, TWeightType>,
        class TInsertWeigher = TWeightOneUnaryGetter<TCharType<TStringType>, TWeightType>,
        class TTransposeWeigher = TWeightInfBinaryGetter<TCharType<TStringType>, TWeightType>
    >
    void GetEditChain(const TStringType& str1, const TStringType& str2, TEditChain& res, TWeightType* weight = nullptr,
        const TReplaceWeigher& replaceWeigher = TReplaceWeigher(),
        const TDeleteWeigher& deleteWeigher = TDeleteWeigher(),
        const TInsertWeigher& insertWeigher = TInsertWeigher(),
        const TTransposeWeigher& transposeWeigher = TTransposeWeigher())
    {
        int l1 = (int)str1.size();
        int l2 = (int)str2.size();

        TMatrix<std::pair<TWeightType, EEditMoveType>> ma(l1 + 1, l2 + 1); /// ma[i][j].first = diff(str1[0..i-1], str2[0..j-1])
        ma[0][0] = std::make_pair(0, EMT_SPECIAL);                 // starting point
        for (int i = 1; i <= l1; i++) {
            ma[i][0] = std::make_pair(ma[i - 1][0].first + deleteWeigher(str1[i - 1]), EMT_DELETE);
        }
        for (int i = 1; i <= l2; i++) {
            ma[0][i] = std::make_pair(ma[0][i - 1].first + insertWeigher(str2[i - 1]), EMT_INSERT);
        }
        const TWeightType maxWeight = std::numeric_limits<TWeightType>::max();
        // Here goes basic Damerau-Levenshtein's algorithm
        for (int i = 1; i <= l1; i++) {
            for (int j = 1; j <= l2; j++) {
                if (str1[i - 1] == str2[j - 1]) {
                    ma[i][j] = std::make_pair(ma[i - 1][j - 1].first, EMT_PRESERVE);
                } else {
                    const TWeightType replaceWeight = replaceWeigher(str1[i - 1], str2[j - 1]);
                    Y_ASSERT(replaceWeight >= 0);
                    if (replaceWeight < maxWeight) {
                        ma[i][j] = std::make_pair(ma[i - 1][j - 1].first + replaceWeight, EMT_REPLACE);
                    } else {
                        ma[i][j] = std::make_pair(replaceWeight, EMT_REPLACE);
                    }
                }

                if (ma[i][j].first > ma[i - 1][j].first) {
                    const TWeightType deleteWeight = deleteWeigher(str1[i - 1]);
                    Y_ASSERT(deleteWeight >= 0);
                    if (deleteWeight < maxWeight) {
                        const TWeightType deletePathWeight = ma[i - 1][j].first + deleteWeight;
                        if (deletePathWeight <= ma[i][j].first) {
                            ma[i][j] = std::make_pair(deletePathWeight, EMT_DELETE);
                        }
                    }
                }

                if (ma[i][j].first > ma[i][j - 1].first) {
                    const TWeightType insertWeight = insertWeigher(str2[j - 1]);
                    Y_ASSERT(insertWeight >= 0);
                    if (insertWeight < maxWeight) {
                        const TWeightType insertPathWeight = ma[i][j - 1].first + insertWeight;
                        if (insertPathWeight <= ma[i][j].first) {
                            ma[i][j] = std::make_pair(insertPathWeight, EMT_INSERT);
                        }
                    }
                }

                if (i > 1 && j > 1 && str1[i - 1] == str2[j - 2] && str1[i - 2] == str2[j - 1]) {
                    const TWeightType transposeWeight = transposeWeigher(str1[i - 2], str2[j - 2]);
                    Y_ASSERT(transposeWeight >= 0);
                    if (transposeWeight < maxWeight) {
                        const TWeightType transposePathWeight = ma[i - 2][j - 2].first + transposeWeight;
                        if (transposePathWeight <= ma[i][j].first) {
                            ma[i][j] = std::make_pair(transposePathWeight, EMT_TRANSPOSE);
                        }
                    }
                }
            }
        }
        // Tracing the path from final point
        res.clear();
        res.reserve(Max<size_t>(l1, l2));
        for (int i = l1, j = l2; ma[i][j].second != EMT_SPECIAL;) {
            res.push_back(ma[i][j].second);
            switch (ma[i][j].second) {
                case EMT_TRANSPOSE:
                    i -= 2;
                    j -= 2;
                    break;
                case EMT_PRESERVE:
                case EMT_REPLACE:
                    --i;
                    --j;
                    break;
                case EMT_DELETE:
                    --i;
                    break;
                case EMT_INSERT:
                    --j;
                    break;
                default:
                    // TODO: throw exception
                    break;
            }
        }
        std::reverse(res.begin(), res.end());

        if (weight != nullptr) {
            *weight = ma[l1][l2].first;
        }
    }

    template <class TStringType, bool Damerau = false, class TWeightType = int>
    void GetEditChainGeneric(const TStringType& str1, const TStringType& str2, TEditChain& res, TWeightType* weight = nullptr) {
        typedef TCharType<TStringType> TArgType;
        GetEditChain<
            TStringType, TWeightType,
            TWeightOneBinaryGetter<TArgType, TWeightType>,
            TWeightOneUnaryGetter<TArgType, TWeightType>,
            TWeightOneUnaryGetter<TArgType, TWeightType>,
            std::conditional_t<
                Damerau,
                TWeightOneBinaryGetter<TArgType, TWeightType>,
                TWeightInfBinaryGetter<TArgType, TWeightType>
            >
        >(str1, str2, res, weight);
    }

    template <class TStringType, bool Damerau = false>
    size_t DistanceImpl(const TStringType& str1, const TStringType& str2) {
        if (str1.size() > str2.size()) {
            return DistanceImpl<TStringType, Damerau>(str2, str1);
        }

        size_t size1 = str1.size();
        size_t size2 = str2.size();

        TVector<size_t> currentRow(size1 + 1);
        TVector<size_t> previousRow(size1 + 1);
        TVector<size_t> transpositionRow(size1 + 1);

        for (size_t i = 0; i <= size1; ++i) {
            previousRow[i] = i;
        }

        for (size_t i = 1; i <= size2; ++i) {
            currentRow[0] = i;

            for (size_t j = 1; j <= size1; ++j) {
                size_t cost = str1[j - 1] == str2[i - 1] ? 0 : 1;

                currentRow[j] = std::min(previousRow[j - 1] + cost, std::min(currentRow[j - 1], previousRow[j]) + 1);

                if (Damerau && i > 1 && j > 1 && str1[j - 2] == str2[i - 1] && str1[j - 1] == str2[i - 2]) {
                    currentRow[j] = std::min(currentRow[j], transpositionRow[j - 2] + cost);
                }
            }

            if (Damerau) {
                std::swap(transpositionRow, previousRow);
            }

            std::swap(previousRow, currentRow);
        }

        return previousRow[size1];
    }

    template <class TStringType>
    size_t Distance(const TStringType& str1, const TStringType& str2) {
        return DistanceImpl<TStringType, false>(str1, str2);
    }

    template <class TStringType>
    size_t DamerauDistance(const TStringType& str1, const TStringType& str2) {
        return DistanceImpl<TStringType, true>(str1, str2);
    }

    /// Calculates substrings to be replaced for str1->str2 transformation
    struct TReplacement {
        int CorrectOffset, CorrectLength, MisspelledOffset, MisspelledLength;
        TReplacement()
            : CorrectOffset(0)
            , CorrectLength(0)
            , MisspelledOffset(0)
            , MisspelledLength(0)
        {
        }
        TReplacement(int correctOffset, int correctLength, int misspelledOffset, int misspelledLength)
            : CorrectOffset(correctOffset)
            , CorrectLength(correctLength)
            , MisspelledOffset(misspelledOffset)
            , MisspelledLength(misspelledLength)
        {
        }
    };

    template <class TStringType>
    void GetStringReplacements(const TStringType& str1, const TStringType& str2, TVector<TReplacement>& res) {
        TEditChain editChain;
        GetEditChain(str1, str2, editChain);
        editChain.push_back(EMT_SPECIAL);
        int c1 = 0, c2 = 0;
        res.clear();
        for (TEditChain::const_iterator it = editChain.begin(); it != editChain.end(); it++) {
            if (IsImportantEditMove(*it)) {
                int sc1 = c1, sc2 = c2;
                do {
                    MakeMove(*it, c1, c2);
                    ++it;
                } while (IsImportantEditMove(*it));
                res.push_back(TReplacement(sc1, c1 - sc1, sc2, c2 - sc2));
            }
            MakeMove(*it, c1, c2);
        }
    }
}
