/*******************************************************************************
 * tlx/string/levenshtein.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_LEVENSHTEIN_HEADER
#define TLX_STRING_LEVENSHTEIN_HEADER

#include <tlx/simple_vector.hpp>
#include <tlx/string/to_lower.hpp>

#include <algorithm>
#include <cstring>
#include <string>

namespace tlx {

//! \addtogroup tlx_string
//! \{

// *** Parameter Struct and Algorithm ***

/*!
 * Standard parameters to levenshtein distance function. Costs are all 1 and
 * characters are compared directly.
 */
struct LevenshteinStandardParameters {
    static const unsigned int cost_insert_delete = 1;
    static const unsigned int cost_replace = 1;

    static inline bool char_equal(const char& a, const char& b)
    { return (a == b); }
};

/*!
 * Standard parameters to Levenshtein distance function. Costs are all 1 and
 * characters are compared case-insensitively.
 */
struct LevenshteinStandardICaseParameters {
    static const unsigned int cost_insert_delete = 1;
    static const unsigned int cost_replace = 1;

    static inline bool char_equal(const char& a, const char& b)
    { return to_lower(a) == to_lower(b); }
};

/*!
 * Computes the Levenshtein string distance also called edit distance between
 * two strings. The distance is the minimum number of
 * replacements/inserts/deletes needed to change one string into the
 * other. Implemented with time complexity O(|n|+|m|) and memory complexity
 * O(2*max(|n|,|m|))
 *
 * \param a      first string
 * \param a_size size of first string
 * \param b      second string
 * \param b_size size of second string
 * \return       Levenshtein distance
 */
template <typename Param>
static inline
size_t levenshtein_algorithm(const char* a, size_t a_size,
                             const char* b, size_t b_size) {
    // if one of the strings is zero, then all characters of the other must
    // be inserted.
    if (a_size == 0) return b_size * Param::cost_insert_delete;
    if (b_size == 0) return a_size * Param::cost_insert_delete;

    // make "as" the longer string and "bs" the shorter.
    if (a_size < b_size) {
        std::swap(a, b);
        std::swap(a_size, b_size);
    }

    // only allocate two rows of the needed matrix.
    simple_vector<size_t> lastrow(a_size + 1);
    simple_vector<size_t> thisrow(a_size + 1);

    // fill this row with ascending ordinals.
    for (size_t i = 0; i < a_size + 1; i++) {
        thisrow[i] = i;
    }

    // compute distance
    for (size_t j = 1; j < b_size + 1; j++)
    {
        // switch rows
        std::swap(lastrow, thisrow);

        // compute new row
        thisrow[0] = j;

        for (size_t i = 1; i < a_size + 1; i++)
        {
            // three-way mimimum of
            thisrow[i] = std::min(
                std::min(
                    // left plus insert cost
                    thisrow[i - 1] + Param::cost_insert_delete,
                    // top plus delete cost
                    lastrow[i] + Param::cost_insert_delete),
                // top left plus replacement cost
                lastrow[i - 1] + (
                    Param::char_equal(a[i - 1], b[j - 1])
                    ? 0 : Param::cost_replace)
                );
        }
    }

    // result is in the last cell of the last computed row
    return thisrow[a_size];
}

/*!
 * Computes the Levenshtein string distance between two strings. The distance
 * is the minimum number of replacements/inserts/deletes needed to change one
 * string into the other.
 *
 * \param a     first string
 * \param b     second string
 * \return      Levenshtein distance
 */
static inline size_t levenshtein(const char* a, const char* b) {
    return levenshtein_algorithm<LevenshteinStandardParameters>(
        a, std::strlen(a), b, std::strlen(b));
}

/*!
 * Computes the Levenshtein string distance between two strings. The distance
 * is the minimum number of replacements/inserts/deletes needed to change one
 * string into the other.
 *
 * \param a     first string
 * \param b     second string
 * \return      Levenshtein distance
 */
static inline size_t levenshtein_icase(const char* a, const char* b) {
    return levenshtein_algorithm<LevenshteinStandardICaseParameters>(
        a, std::strlen(a), b, std::strlen(b));
}

/*!
 * Computes the Levenshtein string distance between two strings. The distance
 * is the minimum number of replacements/inserts/deletes needed to change one
 * string into the other.
 *
 * \param a     first string
 * \param b     second string
 * \return      Levenshtein distance
 */
static inline size_t levenshtein(const std::string& a, const std::string& b) {
    return levenshtein_algorithm<LevenshteinStandardParameters>(
        a.data(), a.size(), b.data(), b.size());
}

/*!
 * Computes the Levenshtein string distance between two strings. The distance
 * is the minimum number of replacements/inserts/deletes needed to change one
 * string into the other. Character comparison is done case-insensitively.
 *
 * \param a     first string
 * \param b     second string
 * \return      Levenshtein distance
 */
static inline
size_t levenshtein_icase(const std::string& a, const std::string& b) {
    return levenshtein_algorithm<LevenshteinStandardICaseParameters>(
        a.data(), a.size(), b.data(), b.size());
}

//! \}

} // namespace tlx

#endif // !TLX_STRING_LEVENSHTEIN_HEADER

/******************************************************************************/
