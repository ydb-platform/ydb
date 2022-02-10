#pragma once

#include "defs.h"

namespace NDiffUtils {

    template<typename T>
    struct TKeyLess;

    template<typename TKey, typename TValue>
    struct TKeyLess<TMap<TKey, TValue>> {
        using T = typename TMap<TKey, TValue>::value_type;
        bool operator ()(const T &x, const T &y) const { return x.first < y.first; }
    };

    template<typename TItem>
    struct TKeyLess<TSet<TItem>> {
        using T = typename TSet<TItem>::value_type;
        bool operator ()(const T &x, const T &y) const { return x < y; }
    };

    template<typename TCont, typename TContIterator>
    class TDiffCalculator {
        class TIterator {
            const TDiffCalculator *Calc;
            TContIterator Left;
            TContIterator Right;
            enum class EState {
                LEFT,
                RIGHT,
                BOTH
            } State;

            using TValueRef = typename std::remove_reference<decltype(*std::declval<TContIterator>())>::type;

        public:
            TIterator(const TDiffCalculator *calc, TContIterator left, TContIterator right)
                : Calc(calc)
                , Left(left)
                , Right(right)
            {
                UpdateState();
            }

            std::pair<TValueRef*, TValueRef*> operator *() const {
                switch (State) {
                    case EState::LEFT:
                        return {&*Left, nullptr};

                    case EState::BOTH:
                        return {&*Left, &*Right};

                    case EState::RIGHT:
                        return {nullptr, &*Right};
                }
            }

            TIterator& operator ++() {
                if (State == EState::LEFT || State == EState::BOTH) {
                    ++Left;
                }
                if (State == EState::RIGHT || State == EState::BOTH) {
                    ++Right;
                }
                UpdateState();
                return *this;
            }

            bool operator !=(const TIterator& other) const {
                return Left != other.Left || Right != other.Right;
            }

            void UpdateState() {
                static constexpr TKeyLess<typename std::remove_const<TCont>::type> less;
                State =
                    Left == Calc->Left->end()   ? EState::RIGHT :
                    Right == Calc->Right->end() ? EState::LEFT  :
                    less(*Left, *Right)         ? EState::LEFT  :
                    less(*Right, *Left)         ? EState::RIGHT :
                                                  EState::BOTH;
            }
        };

        TCont LeftCont;
        TCont RightCont;
        TCont *Left;
        TCont *Right;

    public:
        TDiffCalculator(TCont *left, TCont *right)
            : Left(left ? left : &LeftCont)
            , Right(right ? right : &RightCont)
        {}

        TDiffCalculator(TCont&& left, TCont&& right)
            : LeftCont(std::move(left))
            , RightCont(std::move(right))
            , Left(&LeftCont)
            , Right(&RightCont)
        {}

        TIterator begin() const {
            return TIterator(this, Left->begin(), Right->begin());
        }

        TIterator end() const {
            return TIterator(this, Left->end(), Right->end());
        }
    };

} // NDiffUtils

template<typename TCont>
NDiffUtils::TDiffCalculator<TCont, typename TCont::iterator> Diff(TCont *left, TCont *right) {
    if (left != right) {
        return {left, right};
    } else {
        return {nullptr, nullptr};
    }
}

template<typename TCont>
NDiffUtils::TDiffCalculator<const TCont, typename TCont::const_iterator> Diff(const TCont *left, const TCont *right) {
    if (left != right) {
        return {left, right};
    } else {
        return {nullptr, nullptr};
    }
}

template<typename TCont>
NDiffUtils::TDiffCalculator<TCont, typename TCont::iterator> Diff(TCont&& left, TCont&& right) {
    return {std::forward<TCont>(left), std::forward<TCont>(right)};
}
