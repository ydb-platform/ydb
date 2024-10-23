/*******************************************************************************
 * tlx/string/join_generic.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_JOIN_GENERIC_HEADER
#define TLX_STRING_JOIN_GENERIC_HEADER

#include <sstream>
#include <string>

namespace tlx {

//! \addtogroup tlx_string
//! \{
//! \name Split and Join
//! \{

/*!
 * Join a sequence of strings by some glue string between each pair from the
 * sequence. The sequence in given as a range between two iterators.
 *
 * \param glue     string to glue
 * \param first    the beginning iterator of the range to join
 * \param last     the ending iterator of the range to join
 * \return string  constructed from the range with the glue between two strings.
 */
template <typename Glue, typename Iterator>
static inline
std::string join(Glue glue, Iterator first, Iterator last) {
    std::ostringstream out;
    if (first == last) return out.str();

    out << *first;
    ++first;

    while (first != last)
    {
        out << glue;
        out << *first;
        ++first;
    }

    return out.str();
}

/*!
 * Join a Container of strings by some glue character between each pair from the
 * sequence.
 *
 * \param glue     character for glue
 * \param parts    the vector of strings to join
 * \return string  constructed from the vector with the glue between two strings
 */
template <typename Container>
static inline
std::string join(char glue, const Container& parts) {
    return join(glue, std::begin(parts), std::end(parts));
}

/*!
 * Join a Container of strings by some glue string between each pair from the
 * sequence.
 *
 * \param glue     string to glue
 * \param parts    the vector of strings to join
 * \return string  constructed from the vector with the glue between two strings
 */
template <typename Container>
static inline
std::string join(const char* glue, const Container& parts) {
    return join(glue, std::begin(parts), std::end(parts));
}

/*!
 * Join a Container of strings by some glue string between each pair from the
 * sequence.
 *
 * \param glue     string to glue
 * \param parts    the vector of strings to join
 * \return string  constructed from the vector with the glue between two strings
 */
template <typename Container>
static inline
std::string join(const std::string& glue, const Container& parts) {
    return join(glue, std::begin(parts), std::end(parts));
}

//! \}
//! \}

} // namespace tlx

#endif // !TLX_STRING_JOIN_GENERIC_HEADER

/******************************************************************************/
