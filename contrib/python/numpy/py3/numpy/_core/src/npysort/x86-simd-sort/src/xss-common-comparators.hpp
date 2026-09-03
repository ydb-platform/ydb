#ifndef XSS_COMMON_COMPARATORS
#define XSS_COMMON_COMPARATORS

template <typename type_t>
type_t prev_value(type_t value)
{
    // TODO this probably handles non-native float16 wrong
    if constexpr (std::is_floating_point<type_t>::value) {
        return std::nextafter(value, -std::numeric_limits<type_t>::infinity());
    }
    else {
        if (value > std::numeric_limits<type_t>::min()) { return value - 1; }
        else {
            return value;
        }
    }
}

template <typename type_t>
type_t next_value(type_t value)
{
    // TODO this probably handles non-native float16 wrong
    if constexpr (std::is_floating_point<type_t>::value) {
        return std::nextafter(value, std::numeric_limits<type_t>::infinity());
    }
    else {
        if (value < std::numeric_limits<type_t>::max()) { return value + 1; }
        else {
            return value;
        }
    }
}

template <typename vtype, typename mm_t>
X86_SIMD_SORT_INLINE void COEX(mm_t &a, mm_t &b);

template <typename vtype, bool descend>
struct Comparator {
    using reg_t = typename vtype::reg_t;
    using opmask_t = typename vtype::opmask_t;
    using type_t = typename vtype::type_t;

    X86_SIMD_SORT_FINLINE bool STDSortComparator(const type_t &a,
                                                 const type_t &b)
    {
        if constexpr (descend) { return comparison_func<vtype>(b, a); }
        else {
            return comparison_func<vtype>(a, b);
        }
    }

    X86_SIMD_SORT_FINLINE opmask_t PartitionComparator(reg_t a, reg_t b)
    {
        if constexpr (descend) { return vtype::ge(b, a); }
        else {
            return vtype::ge(a, b);
        }
    }

    X86_SIMD_SORT_FINLINE void COEX(reg_t &a, reg_t &b)
    {
        if constexpr (descend) { ::COEX<vtype, reg_t>(b, a); }
        else {
            ::COEX<vtype, reg_t>(a, b);
        }
    }

    // Returns a vector of values that would be sorted as far right as possible
    // For ascending order, this is the maximum possible value
    X86_SIMD_SORT_FINLINE reg_t rightmostPossibleVec()
    {
        if constexpr (descend) { return vtype::zmm_min(); }
        else {
            return vtype::zmm_max();
        }
    }

    // Returns the value that would be leftmost of the two when sorted
    // For ascending order, that is the smaller value
    X86_SIMD_SORT_FINLINE type_t leftmost(type_t smaller, type_t larger)
    {
        if constexpr (descend) {
            UNUSED(smaller);
            return larger;
        }
        else {
            UNUSED(larger);
            return smaller;
        }
    }

    // Returns the value that would be rightmost of the two when sorted
    // For ascending order, that is the larger value
    X86_SIMD_SORT_FINLINE type_t rightmost(type_t smaller, type_t larger)
    {
        if constexpr (descend) {
            UNUSED(larger);
            return smaller;
        }
        else {
            UNUSED(smaller);
            return larger;
        }
    }

    // If median == smallest, that implies approximately half the array is equal to smallest, unless we were very unlucky with our sample
    // Try just doing the next largest value greater than this seemingly very common value to seperate them out
    X86_SIMD_SORT_FINLINE type_t choosePivotMedianIsSmallest(type_t median)
    {
        if constexpr (descend) { return median; }
        else {
            return next_value<type_t>(median);
        }
    }

    // If median == largest, that implies approximately half the array is equal to largest, unless we were very unlucky with our sample
    // Thus, median probably is a fine pivot, since it will move all of this common value into its own partition
    X86_SIMD_SORT_FINLINE type_t choosePivotMedianIsLargest(type_t median)
    {
        if constexpr (descend) { return prev_value<type_t>(median); }
        else {
            return median;
        }
    }
};

#endif // XSS_COMMON_COMPARATORS
