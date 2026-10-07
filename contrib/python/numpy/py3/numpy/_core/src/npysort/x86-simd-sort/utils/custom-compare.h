#ifndef UTILS_CUSTOM_COMPARE
#define UTILS_CUSTOM_COMPARE

#include <limits>
#include <cmath>
#include "xss-custom-float.h"

/*
 * Custom comparator class to handle NAN's: treats NAN  > INF
 */
template <typename T, typename Comparator>
struct compare {
    static constexpr auto op = Comparator {};
    bool operator()(const T a, const T b)
    {
        if constexpr (xss::fp::is_floating_point_v<T>) {
            T inf = xss::fp::infinity<T>();
            T one = (T)1.0;
            if (!xss::fp::isunordered(a, b)) { return op(a, b); }
            else if ((xss::fp::isnan(a)) && (!xss::fp::isnan(b))) {
                return b == inf ? op(inf, one) : op(inf, b);
            }
            else if ((!xss::fp::isnan(a)) && (xss::fp::isnan(b))) {
                return a == inf ? op(one, inf) : op(a, inf);
            }
            else {
                return op(one, one);
            }
        }
        else {
            return op(a, b);
        }
    }
};

template <typename T, typename Comparator>
struct compare_arg {
    compare_arg(const T *arr)
    {
        this->arr = arr;
    }
    bool operator()(const int64_t a, const int64_t b)
    {
        return compare<T, Comparator>()(arr[a], arr[b]);
    }
    const T *arr;
};

#endif // UTILS_CUSTOM_COMPARE