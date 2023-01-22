#pragma once

#include "defs.h"

namespace NKikimr {

    template<typename T>
    class TClosedIntervalSet {
        struct TByLeft {
            const T& Value;
        };

        struct TByRight {
            const T& Value;
        };

        struct TInterval {
            T Left;
            T Right;

            TInterval(T&& left, T&& right)
                : Left(std::move(left))
                , Right(std::move(right))
            {}

            struct TCompare {
                using is_transparent = void;

                bool operator ()(const TInterval& x, const TInterval& y) const { return x.Left < y.Left; }
                bool operator ()(const TByLeft& x, const TInterval& y) const { return x.Value < y.Left; }
                bool operator ()(const TInterval& x, const TByLeft& y) const { return x.Left < y.Value; }
                bool operator ()(const TByRight& x, const TInterval& y) const { return x.Value < y.Right; }
                bool operator ()(const TInterval& x, const TByRight& y) const { return x.Right < y.Value; }
            };
        };
        std::set<TInterval, typename TInterval::TCompare> Intervals;

    public:
        TClosedIntervalSet() = default;
        TClosedIntervalSet(const TClosedIntervalSet&) = default;
        TClosedIntervalSet(TClosedIntervalSet&&) = default;

        TClosedIntervalSet& operator =(const TClosedIntervalSet&) = default;
        TClosedIntervalSet& operator =(TClosedIntervalSet&&) = default;

        TClosedIntervalSet& operator |=(const std::pair<T, T>& range) { AddRange(range); return *this; }
        TClosedIntervalSet& operator |=(std::pair<T, T>&& range) { AddRange(std::move(range)); return *this; }

        template<typename TRange>
        void AddRange(TRange&& range) {
            auto&& [left, right] = range;
            const auto leftIt = Intervals.lower_bound(TByRight{left});
            const auto rightIt = Intervals.upper_bound(TByLeft{right});
            if (leftIt == rightIt) {
                Intervals.emplace(std::move(left), std::move(right));
            } else {
                auto& current = const_cast<TInterval&>(*leftIt);
                auto& last = const_cast<TInterval&>(*std::prev(rightIt));
                if (left < current.Left) {
                    current.Left = std::move(left);
                }
                if (current.Right < right || current.Right < last.Right) {
                    current.Right = right < last.Right ? std::move(last.Right) : std::move(right);
                }
                Intervals.erase(std::next(leftIt), rightIt);
            }
        }

        TClosedIntervalSet& operator -=(const TClosedIntervalSet& other) {
            auto myIt = Intervals.begin();
            auto otherIt = other.Intervals.begin();
            while (myIt != Intervals.end() && otherIt != other.Intervals.end()) {
                auto& my = const_cast<TInterval&>(*myIt);

                if (my.Right < otherIt->Left) {
                    ++myIt;
                    if (myIt != Intervals.end() && myIt->Right < otherIt->Left) {
                        myIt = Intervals.lower_bound(TByRight{otherIt->Left});
                    }
                } else if (otherIt->Right < my.Left) {
                    ++otherIt;
                    if (otherIt != other.Intervals.end() && otherIt->Right < my.Left) {
                        otherIt = other.Intervals.lower_bound(TByRight{my.Left});
                    }
                } else if (otherIt->Left <= my.Left) {
                    if (my.Right <= otherIt->Right) {
                        myIt = Intervals.erase(myIt);
                    } else {
                        my.Left = otherIt->Right;
                        ++otherIt;
                    }
                } else if (my.Right <= otherIt->Right) {
                    my.Right = otherIt->Left;
                    ++myIt;
                } else {
                    if (otherIt->Left < otherIt->Right) {
                        myIt = Intervals.emplace_hint(std::next(myIt), T(otherIt->Right), std::exchange(my.Right, otherIt->Left));
                    }
                    ++otherIt;
                }
            }
            return *this;
        }

        // returns the first subrange of the full subtraction result
        std::optional<std::pair<T, T>> PartialSubtract(const TClosedIntervalSet& other) const {
            if (auto myIt = Intervals.begin(); myIt != Intervals.end()) {
                const T *myLeft = &myIt->Left;
                const T *myRight = &myIt->Right;

                for (auto otherIt = other.Intervals.begin(); otherIt != other.Intervals.end(); ) {
                    if (*myRight < otherIt->Left) {
                        return std::make_pair(*myLeft, *myRight);
                    } else if (otherIt->Right < *myLeft) {
                        ++otherIt;
                        if (otherIt != other.Intervals.end() && otherIt->Right < *myLeft) {
                            otherIt = other.Intervals.lower_bound(TByRight{*myLeft});
                        }
                    } else if (otherIt->Left <= *myLeft) {
                        if (*myRight <= otherIt->Right) {
                            ++myIt;
                            if (myIt == Intervals.end()) {
                                return std::nullopt;
                            }
                            std::tie(myLeft, myRight) = std::make_pair(&myIt->Left, &myIt->Right);
                        } else {
                            myLeft = &otherIt->Right;
                            ++otherIt;
                        }
                    } else if (*myRight <= otherIt->Right) {
                        return std::make_pair(*myLeft, otherIt->Left);
                    } else {
                        if (otherIt->Left < otherIt->Right) {
                            return std::make_pair(*myLeft, otherIt->Left);
                        }
                        ++otherIt;
                    }
                }

                return std::make_pair(*myLeft, *myRight);
            } else {
                return std::nullopt;
            }
        }

        operator bool() const {
            return !Intervals.empty();
        }

        bool operator [](const T& pt) const {
            const auto it = Intervals.lower_bound(TByRight{pt});
            return it != Intervals.end() && it->Left <= pt;
        }

        template<typename TCallback>
        bool operator ()(TCallback&& callback) const {
            for (const auto& i : Intervals) {
                if (!callback(i.Left, i.Right)) {
                    return false;
                }
            }
            return true;
        }
    };

} // NKikimr
