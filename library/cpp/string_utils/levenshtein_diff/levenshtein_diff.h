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
        EMT_INSERT
    };

    inline bool IsImportantEditMove(EEditMoveType p) {
        return (p != EMT_SPECIAL && p != EMT_PRESERVE);
    }

    inline void MakeMove(EEditMoveType t, int& p1, int& p2) {
        switch (t) {
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

    template <typename TArgType>
    struct TWeightOneUnaryGetter {
        int operator()(const TArgType&) const {
            return 1;
        }
    };

    template <typename TArgType>
    struct TWeightOneBinaryGetter {
        int operator()(const TArgType&, const TArgType&) const {
            return 1;
        }
    };

    template <typename TStringType>
    using TCharType = typename std::decay_t<decltype(std::add_const_t<TStringType>()[0])>;

    /// Finds sequence of "edit moves" for two strings
    template <class TStringType, class TWeightType = int,
        class TReplaceWeigher = TWeightOneBinaryGetter<TCharType<TStringType>>,
        class TDeleteWeigher = TWeightOneUnaryGetter<TCharType<TStringType>>,
        class TInsertWeigher = TWeightOneUnaryGetter<TCharType<TStringType>>
    >
    void GetEditChain(const TStringType& str1, const TStringType& str2, TEditChain& res, TWeightType* weight = nullptr,
        const TReplaceWeigher& replaceWeigher = TReplaceWeigher(),
        const TDeleteWeigher& deleteWeigher = TDeleteWeigher(),
        const TInsertWeigher& insertWeigher = TInsertWeigher())
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
        // Here goes basic Levestein's algorithm
        for (int i = 1; i <= l1; i++) {
            for (int j = 1; j <= l2; j++) {
                if (str1[i - 1] == str2[j - 1]) {
                    ma[i][j] = std::make_pair(ma[i - 1][j - 1].first, EMT_PRESERVE);
                } else {
                    const TWeightType replaceWeight = replaceWeigher(str1[i - 1], str2[j - 1]);
                    Y_ASSERT(replaceWeight >= 0);
                    ma[i][j] = std::make_pair(ma[i - 1][j - 1].first + replaceWeight, EMT_REPLACE);
                }

                if (ma[i][j].first > ma[i - 1][j].first) {
                    const TWeightType deleteWeight = deleteWeigher(str1[i - 1]);
                    Y_ASSERT(deleteWeight >= 0);
                    const TWeightType deletePathWeight = ma[i - 1][j].first + deleteWeight;
                    if (deletePathWeight <= ma[i][j].first) {
                        ma[i][j] = std::make_pair(deletePathWeight, EMT_DELETE);
                    }
                }

                if (ma[i][j].first > ma[i][j - 1].first) {
                    const TWeightType insertWeight = insertWeigher(str2[j - 1]);
                    Y_ASSERT(insertWeight >= 0);
                    const TWeightType insertPathWeight = ma[i][j - 1].first + insertWeight;
                    if (insertPathWeight <= ma[i][j].first) {
                        ma[i][j] = std::make_pair(insertPathWeight, EMT_INSERT);
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

    template <class TStringType>
    size_t Distance(const TStringType& str1, const TStringType& str2) {
        TEditChain editChain;
        GetEditChain(str1, str2, editChain);
        size_t result = 0;
        for (auto edit : editChain) {
            if (IsImportantEditMove(edit))
                result++;
        }
        return result;
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
