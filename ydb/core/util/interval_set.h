#pragma once
#include "defs.h"

#include <variant>
#include <utility>
#include <util/generic/map.h>
#include <util/stream/str.h>
#include <util/system/yassert.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>

namespace NKikimr {

//
// A set of non-intersecting non-adjoint non-empty intervals in form [begin, end) (end > begin)
//
template <class T> struct TIntervalMap; // implemented as TMap
template <class T> struct TIntervalVec; // implemented as sorted TStackVec
template <class T, size_t ThresholdSize = 8> struct TIntervalSet; // implement as either of the above

template <class T>
struct TIntervalMap {
    TMap<T, T> EndForBegin;

    using iterator = typename decltype(EndForBegin)::iterator;
    using const_iterator = typename decltype(EndForBegin)::const_iterator;

    TIntervalMap() = default;
    TIntervalMap(const TIntervalMap<T>&) = default;
    TIntervalMap(TIntervalMap<T>&&) = default;

    TIntervalMap(T begin, T end) {
        Y_VERIFY_DEBUG_S(begin < end, "begin# " << begin << " end# " << end);
        EndForBegin[begin] = end;
    }

    TIntervalMap& operator=(const TIntervalMap<T>&) = default;
    TIntervalMap& operator=(TIntervalMap<T>&&) = default;

    void Add(const TIntervalMap<T>& b) {
       *this |= b;
    }

    void Add(T begin, T end) {
        *this |= TIntervalMap<T>(begin, end);
    }

    void Clear() {
        EndForBegin.clear();
    }

    void Assign(T begin, T end) {
        Y_VERIFY_DEBUG_S(begin < end, "begin# " << begin << " end# " << end);
        EndForBegin.clear();
        EndForBegin[begin] = end;
    }

    bool IsEmpty() const {
        return EndForBegin.empty();
    }

    auto UpperBound(const T& key) const {
        return EndForBegin.upper_bound(key);
    }

    bool IsSubsetOf(const TIntervalSet<T>& b) const {
        return std::visit([this](auto& var) -> bool { return this->IsSubsetOf(var); }, b.Var);
    }

    template <class B>
    bool IsSubsetOf(const B& b) const {
        auto aIt = EndForBegin.begin();
        if (aIt == EndForBegin.end()) {
            return true;
        }
        auto bIt = b.UpperBound(aIt->first);
        if (bIt != b.EndForBegin.begin()) {
            --bIt;
        }
        if (bIt == b.EndForBegin.end()) {
            return false;
        }
        while (true) {
            //     a---a
            // b---b 1
            //  b---b 2
            //     b---b 3
            //       b---b 4
            //         b---b 5
            //           b-b 6
            if (bIt->second <= aIt->first) {
                // 1
                ++bIt;
                if (bIt == b.EndForBegin.end()) {
                    return false;
                }
            } else if (bIt->first <= aIt->first) {
                // 2,3
                if (bIt->second < aIt->second) {
                    // 2
                    return false;
                }
                // 3
                ++aIt;
                if (aIt == EndForBegin.end()) {
                    return true;
                }
            } else {
                // 4, 5, 6
                return false;
            }
        }
    }

    void Subtract(const TIntervalMap<T>& b) {
        *this -= b;
    }

    void Subtract(T begin, T end) {
        *this -= TIntervalMap<T>(begin, end);
    }

    TString ToString() const {
        TStringStream str;
        str << "{";
        for (auto it = EndForBegin.begin(); it != EndForBegin.end(); ++it) {
            if (it != EndForBegin.begin()) {
                str << " U ";
            }
            str << "[" << it->first << ", " << it->second << ")";
        }
        str << "}";
        return str.Str();
    }

    void Verify() const {
        T lastEnd = Min<T>();
        for (auto it = EndForBegin.begin(); it != EndForBegin.end(); ++it) {
            Y_VERIFY_S(it->first < it->second, "begin# " << it->first << " end# " << it->second);
            if (it != EndForBegin.begin()) {
                Y_VERIFY_S(it->first > lastEnd, "[" << it->first << ", " << it->second << ") vs " << lastEnd);
            }
            lastEnd = it->second;
        }
    }

    size_t Size() const {
        return EndForBegin.size();
    }

    template <class Y>
    friend bool operator ==(const TIntervalMap<T>& x, const Y& y) {
        if (x.EndForBegin.size() != y.EndForBegin.size()) {
            return false;
        }
        auto xi = x.EndForBegin.begin(), xe = x.EndForBegin.end();
        auto yi = y.EndForBegin.begin();
        for (; xi != xe; ++xi, ++yi) {
            if (xi->first != yi->first || xi->second != yi->second) {
                return false;
            }
        }
        return true;
    }

    explicit operator bool() const {
        return !IsEmpty();
    }

    template <class Y>
    TIntervalMap<T> operator |(const Y& y) const {
        TIntervalMap<T> res;
        auto xIt = EndForBegin.begin();
        auto yIt = y.EndForBegin.begin();
        while (xIt != EndForBegin.end() || yIt != y.EndForBegin.end()) {
            if (yIt == y.EndForBegin.end() || (xIt != EndForBegin.end() && xIt->second < yIt->first)) {
                res.EndForBegin.insert(res.EndForBegin.end(), *xIt++);
            } else if (xIt == EndForBegin.end() || yIt->second < xIt->first) {
                res.EndForBegin.insert(res.EndForBegin.end(), *yIt++);
            } else { // intervals do intersect
                T begin = Min(xIt->first, yIt->first), end = Max(xIt->second, yIt->second);
                ++xIt, ++yIt;
                for (;;) { // consume any other intervals intersecting with [begin, end)
                    if (xIt != EndForBegin.end() && xIt->first <= end) {
                        end = Max(end, xIt->second);
                        ++xIt;
                    } else if (yIt != y.EndForBegin.end() && yIt->first <= end) {
                        end = Max(end, yIt->second);
                        ++yIt;
                    } else {
                        break;
                    }
                }
                res.EndForBegin.emplace_hint(res.EndForBegin.end(), begin, end);
            }
        }
        return res;
    }

    template <class Y>
    TIntervalMap<T>& operator |=(const Y& y) {
        auto yIt = y.EndForBegin.begin();
        auto ye = y.EndForBegin.end();
        if (yIt == ye) {
            return *this;
        }
        auto xIt = EndForBegin.upper_bound(yIt->first);
        if (xIt != EndForBegin.begin()) {
            --xIt;
        }
        while (xIt != EndForBegin.end() && yIt != ye) {
            if (xIt->second < yIt->first) {
                ++xIt;
            } else if (yIt->second < xIt->first) {
                EndForBegin.insert(xIt, *yIt++);
            } else { // intervals do intersect
                T begin = Min(xIt->first, yIt->first), end = Max(xIt->second, yIt->second);
                auto baseIt = xIt++;
                ++yIt;
                for (;;) { // consume any other intervals intersecting with [begin, end)
                    if (xIt != EndForBegin.end() && xIt->first <= end) {
                        end = Max(end, xIt->second);
                        ++xIt;
                    } else if (yIt != ye && yIt->first <= end) {
                        end = Max(end, yIt->second);
                        ++yIt;
                    } else {
                        break;
                    }
                }
                if (baseIt->first != begin) {
                    EndForBegin.emplace_hint(EndForBegin.erase(baseIt, xIt), begin, end);
                } else {
                    baseIt->second = end;
                    EndForBegin.erase(++baseIt, xIt);
                }
            }
        }
        EndForBegin.insert(yIt, ye);
        return *this;
    }

    template <class Y>
    TIntervalMap<T> operator &(const Y& y) const {
        TIntervalMap<T> res;
        auto xIt = EndForBegin.begin();
        auto yIt = y.EndForBegin.begin();
        while (xIt != EndForBegin.end() && yIt != y.EndForBegin.end()) {
            const auto [xf, xs] = *xIt;
            const auto [yf, ys] = *yIt;
            if (xs <= yf) {
                ++xIt;
            } else if (ys <= xf) {
                ++yIt;
            } else { // intervals do intersect
                const T begin = Max(xf, yf), end = Min(xs, ys);
                if (xs <= ys) {
                    ++xIt;
                }
                if (ys <= xs) {
                    ++yIt;
                }
                res.EndForBegin.emplace_hint(res.EndForBegin.end(), begin, end);
            }
        }
        return res;
    }

    template <class Y>
    TIntervalMap<T>& operator &=(const Y& y) {
        auto xIt = EndForBegin.begin();
        auto yIt = y.EndForBegin.begin();
        std::optional<typename TMap<T, T>::value_type> carry;
        auto dropFront = [&] {
            if (carry) {
                carry.reset();
            } else {
                xIt = EndForBegin.erase(xIt);
            }
            return xIt;
        };
        while ((carry || xIt != EndForBegin.end()) && yIt != y.EndForBegin.end()) {
            const auto [xf, xs] = carry ? *carry : *xIt;
            const auto [yf, ys] = *yIt;
            if (xs <= yf) {
                dropFront();
            } else if (ys <= xf) {
                ++yIt;
            } else { // intervals do intersect
                const T begin = Max(xf, yf), end = Min(xs, ys);
                if (!carry && xs <= ys && xf == begin) { // just replace interval's end
                    xIt->second = end;
                } else { // or replace the whole entry
                    xIt = EndForBegin.emplace_hint(dropFront(), begin, end);
                }
                ++xIt;
                if (ys <= xs) {
                    ++yIt;
                }
                if (end != xs) { // there is remaining part of the interval -- keep it
                    carry.emplace(end, xs);
                }
            }
        }
        EndForBegin.erase(xIt, EndForBegin.end());
        return *this;
    }

    template <class Y>
    TIntervalMap<T> operator -(const Y& y) const {
        return TIntervalMap<T>(*this) -= y;
    }

    template <class Y>
    TIntervalMap<T>& operator -=(const Y& y) {
        if constexpr (std::is_same_v<TIntervalMap<T>, Y>) {
            if (this == &y) {
                EndForBegin.clear();
                return *this;
            }
        }

        TIntervalMap<T> res;
        auto xIt = EndForBegin.begin();
        if (y.EndForBegin.size() == 1) {
            xIt = EndForBegin.upper_bound(y.EndForBegin.begin()->first);
            if (xIt != EndForBegin.begin()) {
                --xIt;
            }
        }
        auto yIt = y.EndForBegin.begin();
        while (xIt != EndForBegin.end() && yIt != y.EndForBegin.end()) {
            const auto [xf, xs] = *xIt;
            const auto [yf, ys] = *yIt;
            if (xs <= yf) {
                ++xIt;
            } else if (ys <= xf) {
                ++yIt;
            } else {
                xIt = EndForBegin.erase(xIt);
                if (xf < yf) {
                    xIt = EndForBegin.emplace_hint(xIt, xf, yf);
                }
                if (ys < xs) {
                    xIt = EndForBegin.emplace_hint(xf < yf ? std::next(xIt) : xIt, ys, xs);
                }
            }
        }
        return *this;
    }
};

template <class T>
struct TIntervalVec {
    static constexpr size_t StackSize = 1;

    TStackVec<std::pair<T, T>, StackSize> EndForBegin; // <begin, end>

    using iterator = typename decltype(EndForBegin)::iterator;
    using const_iterator = typename decltype(EndForBegin)::const_iterator;

    TIntervalVec() = default;
    TIntervalVec(const TIntervalVec<T>&) = default;
    TIntervalVec(TIntervalVec<T>&&) = default;

    TIntervalVec(T begin, T end) {
        Y_VERIFY_DEBUG_S(begin < end, "begin# " << begin << " end# " << end);
        EndForBegin.emplace_back(begin, end);
    }

    TIntervalVec& operator=(const TIntervalVec<T>&) = default;
    TIntervalVec& operator=(TIntervalVec<T>&&) = default;

    void Add(const TIntervalVec<T>& b) {
       *this |= b;
    }

    void Add(T begin, T end) {
        *this |= TIntervalVec<T>(begin, end);
    }

    void Clear() {
        EndForBegin.clear();
    }

    void Assign(T begin, T end) {
        Y_VERIFY_DEBUG_S(begin < end, "begin# " << begin << " end# " << end);
        EndForBegin.assign(1, {begin, end});
    }

    bool IsEmpty() const {
        return EndForBegin.empty();
    }

    auto UpperBound(const T& key) const {
        // Do not use binary search to optimize for small vectors
        for (auto i = EndForBegin.begin(), e = EndForBegin.end(); i != e; ++i) {
            if (key < i->first) {
                return i;
            }
        }
        return EndForBegin.end();
    }

    bool IsSubsetOf(const TIntervalSet<T>& b) const {
        return std::visit([this](auto& var) -> bool { return this->IsSubsetOf(var); }, b.Var);
    }

    template <class B>
    bool IsSubsetOf(const B& b) const {
        auto aIt = EndForBegin.begin();
        if (aIt == EndForBegin.end()) {
            return true;
        }
        // Do not use binary search, optimized for small interval vecs, anyway worst-case is O(n) + O(m)
        auto bIt = b.EndForBegin.begin();
        if (bIt == b.EndForBegin.end()) {
            return false;
        }
        while (true) {
            //     a---a
            // b---b 1
            //  b---b 2
            //     b---b 3
            //       b---b 4
            //         b---b 5
            //            b-b 6
            if (bIt->second <= aIt->first) {
                // 1
                ++bIt;
                if (bIt == b.EndForBegin.end()) {
                    return false;
                }
            } else if (bIt->first <= aIt->first) {
                // 2,3
                if (bIt->second < aIt->second) {
                    // 2
                    return false;
                }
                // 3
                ++aIt;
                if (aIt == EndForBegin.end()) {
                    return true;
                }
            } else {
                // 4, 5, 6
                return false;
            }
        }
    }

    void Subtract(const TIntervalVec<T>& b) {
        *this -= b;
    }

    void Subtract(T begin, T end) {
        *this -= TIntervalVec<T>(begin, end);
    }

    TString ToString() const {
        TStringStream str;
        str << "{";
        for (auto it = EndForBegin.begin(); it != EndForBegin.end(); ++it) {
            if (it != EndForBegin.begin()) {
                str << " U ";
            }
            str << "[" << it->first << ", " << it->second << ")";
        }
        str << "}";
        return str.Str();
    }

    void Verify() const {
        T lastEnd = Min<T>();
        for (auto it = EndForBegin.begin(); it != EndForBegin.end(); ++it) {
            Y_VERIFY_S(it->first < it->second, "begin# " << it->first << " end# " << it->second);
            if (it != EndForBegin.begin()) {
                Y_VERIFY_S(it->first > lastEnd, "[" << it->first << ", " << it->second << ") vs " << lastEnd);
            }
            lastEnd = it->second;
        }
    }

    size_t Size() const {
        return EndForBegin.size();
    }

    template <class Y>
    friend bool operator ==(const TIntervalVec<T>& x, const Y& y) {
        if (x.EndForBegin.size() != y.EndForBegin.size()) {
            return false;
        }
        auto xi = x.EndForBegin.begin(), xe = x.EndForBegin.end();
        auto yi = y.EndForBegin.begin();
        for (; xi != xe; ++xi, ++yi) {
            if (xi->first != yi->first || xi->second != yi->second) {
                return false;
            }
        }
        return true;
    }

    explicit operator bool() const {
        return !IsEmpty();
    }

    template <class Y>
    TIntervalVec<T> operator |(const Y& y) const {
        TIntervalVec<T> res;
        auto xIt = EndForBegin.begin();
        auto yIt = y.EndForBegin.begin();
        while (xIt != EndForBegin.end() || yIt != y.EndForBegin.end()) {
            if (yIt == y.EndForBegin.end() || (xIt != EndForBegin.end() && xIt->second < yIt->first)) {
                res.EndForBegin.emplace_back(*xIt++);
            } else if (xIt == EndForBegin.end() || yIt->second < xIt->first) {
                res.EndForBegin.emplace_back(*yIt++);
            } else { // intervals do intersect
                T begin = Min(xIt->first, yIt->first), end = Max(xIt->second, yIt->second);
                ++xIt, ++yIt;
                for (;;) { // consume any other intervals intersecting with [begin, end)
                    if (xIt != EndForBegin.end() && xIt->first <= end) {
                        end = Max(end, xIt->second);
                        ++xIt;
                    } else if (yIt != y.EndForBegin.end() && yIt->first <= end) {
                        end = Max(end, yIt->second);
                        ++yIt;
                    } else {
                        break;
                    }
                }
                res.EndForBegin.emplace_back(begin, end);
            }
        }
        return res;
    }

    template <class Y>
    TIntervalVec<T>& operator |=(const Y& y) {
        size_t xi = 0;
        size_t xo = 0;
        auto yi = y.EndForBegin.begin();
        auto ye = y.EndForBegin.end();
        while (yi != ye && xi != EndForBegin.size()) {
            if (EndForBegin[xi].second < yi->first) {
                if (xi != xo) {
                    EndForBegin[xo] = EndForBegin[xi];
                }
                ++xi;
                ++xo;
            } else if (yi->second < EndForBegin[xi].first) {
                if (xo == xi) {
                    EndForBegin.insert(EndForBegin.begin() + xo, *yi++);
                    ++xi;
                    ++xo;
                } else {
                    EndForBegin[xo++] = *yi++;
                }
            } else { // intervals do intersect
                T begin = Min(EndForBegin[xi].first, yi->first);
                T end = Max(EndForBegin[xi].second, yi->second);
                ++xi;
                ++yi;
                for (;;) { // consume any other intervals intersecting with [begin, end)
                    if (xi != EndForBegin.size() && EndForBegin[xi].first <= end) {
                        end = Max(end, EndForBegin[xi].second);
                        ++xi;
                    } else if (yi != ye && yi->first <= end) {
                        end = Max(end, yi->second);
                        ++yi;
                    } else {
                        break;
                    }
                }
                EndForBegin[xo++] = {begin, end};
            }
        }
        EndForBegin.erase(EndForBegin.begin() + xo, EndForBegin.begin() + xi);
        EndForBegin.insert(EndForBegin.end(), yi, ye);
        return *this;
    }

    template <class Y>
    TIntervalVec<T> operator &(const Y& y) const {
        TIntervalVec<T> res;
        auto xIt = EndForBegin.begin();
        auto yIt = y.EndForBegin.begin();
        while (xIt != EndForBegin.end() && yIt != y.EndForBegin.end()) {
            const auto [xf, xs] = *xIt;
            const auto [yf, ys] = *yIt;
            if (xs <= yf) {
                ++xIt;
            } else if (ys <= xf) {
                ++yIt;
            } else { // intervals do intersect
                const T begin = Max(xf, yf), end = Min(xs, ys);
                if (xs <= ys) {
                    ++xIt;
                }
                if (ys <= xs) {
                    ++yIt;
                }
                res.EndForBegin.emplace_back(begin, end);
            }
        }
        return res;
    }

    template <class Y>
    TIntervalVec<T>& operator &=(const Y& y) {
        size_t xi = 0;
        auto yIt = y.EndForBegin.begin();
        std::optional<std::pair<T, T>> carry;
        while ((carry || xi != EndForBegin.size()) && yIt != y.EndForBegin.end()) {
            const auto [xf, xs] = carry ? *carry : EndForBegin[xi];
            const auto [yf, ys] = *yIt;
            if (xs <= yf) {
                if (carry) {
                    carry.reset();
                } else {
                    EndForBegin.erase(EndForBegin.begin() + xi);
                }
            } else if (ys <= xf) {
                ++yIt;
            } else { // intervals do intersect
                const T begin = Max(xf, yf), end = Min(xs, ys);
                if (!carry) {
                    EndForBegin[xi] = {begin, end};
                } else {
                    carry.reset();
                    EndForBegin.emplace(EndForBegin.begin() + xi, begin, end);
                }
                ++xi;
                if (ys <= xs) {
                    ++yIt;
                }
                if (end != xs) { // there is remaining part of the interval -- keep it
                    carry.emplace(end, xs);
                }
            }
        }
        EndForBegin.resize(xi);
        return *this;
    }

    template <class Y>
    TIntervalVec<T> operator -(const Y& y) const {
        return TIntervalVec<T>(*this) -= y;
    }

    template <class Y>
    TIntervalVec<T>& operator -=(const Y& y) {
        if constexpr (std::is_same_v<TIntervalVec<T>, Y>) {
            if (this == &y) {
                EndForBegin.clear();
                return *this;
            }
        }

        TIntervalVec<T> res;
        size_t xi = 0;
        auto yIt = y.EndForBegin.begin();
        while (xi != EndForBegin.size() && yIt != y.EndForBegin.end()) {
            const auto [xf, xs] = EndForBegin[xi];
            const auto [yf, ys] = *yIt;
            if (xs <= yf) {
                ++xi;
            } else if (ys <= xf) {
                ++yIt;
            } else {
                if (xf < yf) {
                    EndForBegin[xi++].second = yf;
                    if (ys < xs) {
                        EndForBegin.emplace(EndForBegin.begin() + xi, ys, xs);
                    }
                } else if (ys < xs) {
                    EndForBegin[xi].first = ys;
                } else {
                    EndForBegin.erase(EndForBegin.begin() + xi);
                }
            }
        }
        return *this;
    }
};

// helper type for the visitor
template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
// explicit deduction guide (not needed as of C++20)
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

template <class T, size_t ThresholdSize>
struct TIntervalSet {
    // Vec is used for small size, up to ThresholdSize intervals
    // If vector become larger, then data is moved into Map
    using TVec = TIntervalVec<T>;
    using TMap = TIntervalMap<T>;
    using TVar = std::variant<TVec, TMap>;
    TVar Var;

    class TConstIterator {
        using TVecIter = typename TIntervalVec<T>::const_iterator;
        using TMapIter = typename TIntervalMap<T>::const_iterator;
        using TValue = std::pair<T, T>;

        std::variant<TVecIter, TMapIter> Iter;

    public:
        TConstIterator() {}

        explicit TConstIterator(TVecIter vecIter)
            : Iter(vecIter)
        {}

        explicit TConstIterator(TMapIter mapIter)
            : Iter(mapIter)
        {}

        TConstIterator(const TConstIterator&) = default;
        TConstIterator& operator=(const TConstIterator& other) = default;

        TValue operator*() const {
            return std::visit([](auto& iter) -> TValue { return *iter; }, Iter);
        }

        TConstIterator& operator++() {
            std::visit([](auto& iter) { iter++; }, Iter);
            return *this;
        }

        friend bool operator==(const TConstIterator& x, const TConstIterator& y) {
            return x.Iter == y.Iter;
        }

        friend bool operator!=(const TConstIterator& x, const TConstIterator& y) {
            return !(x == y);
        }
    };

    using const_iterator = TConstIterator;

    TIntervalSet() = default;
    TIntervalSet(const TIntervalSet&) = default;
    TIntervalSet(TIntervalSet&&) = default;

    TIntervalSet(T begin, T end)
        : Var(std::in_place_type<TVec>, begin, end)
    {}

    TIntervalSet(const TIntervalVec<T>& vec) : Var(vec) {}
    TIntervalSet(TIntervalVec<T>&& vec) : Var(std::move(vec)) {}
    TIntervalSet(const TIntervalMap<T>& map) : Var(map) {}
    TIntervalSet(TIntervalMap<T>&& map) : Var(std::move(map)) {}

    TIntervalSet& operator=(const TIntervalSet&) = default;
    TIntervalSet& operator=(TIntervalSet&&) = default;

    TIntervalSet& operator=(const TIntervalVec<T>& vec) {
        Var = vec;
        return *this;
    }

    TIntervalSet& operator=(TIntervalVec<T>&& vec) {
        Var = std::move(vec);
        return *this;
    }

    TIntervalSet& operator=(const TIntervalMap<T>& map) {
        Var = map;
        return *this;
    }

    TIntervalSet& operator=(TIntervalMap<T>&& map) {
        Var = std::move(map);
        return *this;
    }

    TConstIterator begin() const {
        return std::visit([](auto& var) -> TConstIterator { return TConstIterator(var.EndForBegin.begin()); }, Var);
    }

    TConstIterator end() const {
        return std::visit([](auto& var) -> TConstIterator { return TConstIterator(var.EndForBegin.end()); }, Var);
    }

    bool IsVec() const {
        return Var.index() == 0;
    }

    template <class Y>
    void Add(const Y& y) {
       *this |= y;
    }

    void Add(T begin, T end) {
        *this |= TIntervalVec<T>(begin, end);
    }

    void Clear() {
        std::visit([this](auto&& var) {
            if constexpr (std::is_same_v<std::decay_t<decltype(var)>, TVec>) {
                var.Clear(); // just clear vector (do not reset storage)
            } else {
                Var = TVec(); // use empty vector instead of map
            }
        }, Var);
    }

    void Assign(T begin, T end) {
        std::visit(overloaded {
            [=](TVec& vec) {
                vec.Assign(begin, end); // just clear vector (do not reset storage)
            },
            [=](TMap&) {
                Var = TVec(begin, end); // use vector instead of map
            }
        }, Var);
    }

    bool IsEmpty() const {
        return std::visit([](auto& var) -> bool { return var.IsEmpty(); }, Var);
    }

    template <class Y>
    bool IsSubsetOf(const Y& y) const {
        return std::visit([&y](auto& var) -> bool { return var.IsSubsetOf(y); }, Var);
    }

    bool IsSubsetOf(const TIntervalSet& y) const {
        return std::visit([this](auto& var) -> bool { return this->IsSubsetOf(var); }, y.Var);
    }

    template <class Y>
    void Subtract(const Y& y) {
        *this -= y;
    }

    void Subtract(T begin, T end) {
        *this -= TIntervalVec<T>(begin, end);
    }

    TString ToString() const {
        return std::visit([](auto& var) -> TString { return var.ToString(); }, Var);
    }

    void Verify() const {
        std::visit([](auto& var) { var.Verify(); }, Var);
    }

    size_t Size() const {
        return std::visit([](auto& var) -> size_t { return var.Size(); }, Var);
    }

    template <class Y>
    friend bool operator ==(const TIntervalSet& x, const Y& y) {
        return std::visit([&y](auto& var) -> bool { return var == y; }, x.Var);
    }

    template <class X>
    friend bool operator ==(const X& x, const TIntervalSet& y) {
        return std::visit([&x](auto& var) -> bool { return x == var; }, y.Var);
    }

    friend bool operator ==(const TIntervalSet& x, const TIntervalSet& y) {
        return std::visit([&x](auto& var) -> bool { return x == var; }, y.Var);
    }

    explicit operator bool() const {
        return !IsEmpty();
    }

    template <class Y>
    TIntervalSet operator |(const Y& y) const {
        return std::visit([&y](auto& var) { return TIntervalSet(var | y); } , Var);
    }

    TIntervalSet operator |(const TIntervalSet& y) const {
        return std::visit([this](auto&& var) { return TIntervalSet(*this | var); }, y.Var);
    }

    template <class Y>
    TIntervalSet& operator |=(const Y& y) {
        std::visit(overloaded {
            [this, &y](TVec& vec) {
                vec |= y;
                if (vec.Size() > ThresholdSize) {
                    Mapify(vec);
                }
            },
            [&y](TMap& map) {
                map |= y;
            }
        }, Var);
        return *this;
    }

    TIntervalSet& operator |=(const TIntervalSet& y) {
        return std::visit([this](auto& var) -> TIntervalSet& { return *this |= var; }, y.Var);
    }

    template <class Y>
    TIntervalSet operator &(const Y& y) const {
        return std::visit([&y](auto& var) { return TIntervalSet(var & y); }, Var);
    }

    TIntervalSet operator &(const TIntervalSet& y) const {
        return std::visit([this](auto& var) -> TIntervalSet { return *this & var; }, y.Var);
    }

    template <class Y>
    TIntervalSet& operator &=(const Y& y) {
        std::visit(overloaded {
            [this, &y](TVec& vec) {
                vec &= y;
                if (vec.Size() > ThresholdSize) {
                    Mapify(vec);
                }
            },
            [&y](TMap& map) {
                map &= y;
            }
        }, Var);
        return *this;
    }

    TIntervalSet& operator &=(const TIntervalSet& y) {
        return std::visit([this](auto& var) -> TIntervalSet& { return *this &= var; }, y.Var);
    }

    template <class Y>
    TIntervalSet operator -(const Y& y) const {
        return std::visit([&y](auto& var) { return TIntervalSet(var - y); }, Var);
    }

    TIntervalSet operator -(const TIntervalSet& y) const {
        return std::visit([this](auto& var) -> TIntervalSet { return *this - var; }, y.Var);
    }

    template <class Y>
    TIntervalSet& operator -=(const Y& y) {
        std::visit(overloaded {
            [this, &y](TVec& vec) {
                vec -= y;
                if (vec.Size() > ThresholdSize) {
                    Mapify(vec);
                }
            },
            [&y](TMap& map) {
                map -= y;
            }
        }, Var);
        return *this;
    }

    TIntervalSet& operator -=(const TIntervalSet& y) {
        return std::visit([this](auto& var) -> TIntervalSet& { return *this -= var; }, y.Var);
    }

private:
    void Mapify(TVec& vec) {
        TMap map;
        // move data from vec into map
        for (auto [l, r] : vec.EndForBegin) {
            map.EndForBegin.emplace_hint(map.EndForBegin.end(), l, r);
        }
        Var = std::move(map);
    }

};

} // NKikimr
