/*******************************************************************************
 * tlx/sort/networks/bose_nelson.hpp
 *
 * Recursively called Bose-Nelson sorting networks.
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018-2020 Jasper Marianczuk <jasper.marianczuk@gmail.com>
 * Copyright (C) 2020 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_SORT_NETWORKS_BOSE_NELSON_HEADER
#define TLX_SORT_NETWORKS_BOSE_NELSON_HEADER

#include <tlx/sort/networks/cswap.hpp>

#include <functional>

namespace tlx {

//! Implementations of sorting networks for up to sixteen elements.
namespace sort_networks {

//! \addtogroup tlx_sort
//! \{
//! \name Implementations of Sorting Networks
//! \{

//! Implementation of Bose-Nelson sorting networks for up to sixteen elements.
namespace bose_nelson {

//! default conditional swap implementation
template <typename Iterator>
using DefaultCSwap = CS_IfSwap<
    std::less<typename std::iterator_traits<Iterator>::value_type> >;

/*----------------------------------------------------------------------------*/

//! merge network for element arrays length one and one
template <typename Iterator, typename CSwap>
static inline void merge1_1(Iterator a, Iterator b, CSwap cswap) {
    cswap(a[0], b[0]);
}

//! merge network for element arrays length one and two
template <typename Iterator, typename CSwap>
static inline void merge1_2(Iterator a, Iterator b, CSwap cswap) {
    cswap(a[0], b[1]);
    cswap(a[0], b[0]);
}

//! merge network for element arrays length two and one
template <typename Iterator, typename CSwap>
static inline void merge2_1(Iterator a, Iterator b, CSwap cswap) {
    cswap(a[0], b[0]);
    cswap(a[1], b[0]);
}

//! merge network for element arrays length two and two
template <typename Iterator, typename CSwap>
static inline void merge2_2(Iterator a, Iterator b, CSwap cswap) {
    merge1_1(a, b, cswap);
    merge1_1(a + 1, b + 1, cswap);
    merge1_1(a + 1, b, cswap);
}

//! merge network for element arrays length two and three
template <typename Iterator, typename CSwap>
static inline void merge2_3(Iterator a, Iterator b, CSwap cswap) {
    merge1_2(a, b, cswap);
    merge1_1(a + 1, b + 2, cswap);
    merge1_2(a + 1, b, cswap);
}

//! merge network for element arrays length three and two
template <typename Iterator, typename CSwap>
static inline void merge3_2(Iterator a, Iterator b, CSwap cswap) {
    merge1_1(a, b, cswap);
    merge2_1(a + 1, b + 1, cswap);
    merge2_1(a + 1, b, cswap);
}

//! merge network for element arrays length three and three
template <typename Iterator, typename CSwap>
static inline void merge3_3(Iterator a, Iterator b, CSwap cswap) {
    merge1_1(a, b, cswap);
    merge2_2(a + 1, b + 1, cswap);
    merge2_1(a + 1, b, cswap);
}

//! merge network for element arrays length three and four
template <typename Iterator, typename CSwap>
static inline void merge3_4(Iterator a, Iterator b, CSwap cswap) {
    merge1_2(a, b, cswap);
    merge2_2(a + 1, b + 2, cswap);
    merge2_2(a + 1, b, cswap);
}

//! merge network for element arrays length four and three
template <typename Iterator, typename CSwap>
static inline void merge4_3(Iterator a, Iterator b, CSwap cswap) {
    merge2_2(a, b, cswap);
    merge2_1(a + 2, b + 2, cswap);
    merge2_2(a + 2, b, cswap);
}

//! merge network for element arrays length four and four
template <typename Iterator, typename CSwap>
static inline void merge4_4(Iterator a, Iterator b, CSwap cswap) {
    merge2_2(a, b, cswap);
    merge2_2(a + 2, b + 2, cswap);
    merge2_2(a + 2, b, cswap);
}

//! merge network for element arrays length four and five
template <typename Iterator, typename CSwap>
static inline void merge4_5(Iterator a, Iterator b, CSwap cswap) {
    merge2_3(a, b, cswap);
    merge2_2(a + 2, b + 3, cswap);
    merge2_3(a + 2, b, cswap);
}

//! merge network for element arrays length five and five
template <typename Iterator, typename CSwap>
static inline void merge5_5(Iterator a, Iterator b, CSwap cswap) {
    merge2_2(a, b, cswap);
    merge3_3(a + 2, b + 2, cswap);
    merge3_2(a + 2, b, cswap);
}

//! merge network for element arrays length five and six
template <typename Iterator, typename CSwap>
static inline void merge5_6(Iterator a, Iterator b, CSwap cswap) {
    merge2_3(a, b, cswap);
    merge3_3(a + 2, b + 3, cswap);
    merge3_3(a + 2, b, cswap);
}

//! merge network for element arrays length six and six
template <typename Iterator, typename CSwap>
static inline void merge6_6(Iterator a, Iterator b, CSwap cswap) {
    merge3_3(a, b, cswap);
    merge3_3(a + 3, b + 3, cswap);
    merge3_3(a + 3, b, cswap);
}

//! merge network for element arrays length six and seven
template <typename Iterator, typename CSwap>
static inline void merge6_7(Iterator a, Iterator b, CSwap cswap) {
    merge3_4(a, b, cswap);
    merge3_3(a + 3, b + 4, cswap);
    merge3_4(a + 3, b, cswap);
}

//! merge network for element arrays length seven and seven
template <typename Iterator, typename CSwap>
static inline void merge7_7(Iterator a, Iterator b, CSwap cswap) {
    merge3_3(a, b, cswap);
    merge4_4(a + 3, b + 3, cswap);
    merge4_3(a + 3, b, cswap);
}

//! merge network for element arrays length seven and eight
template <typename Iterator, typename CSwap>
static inline void merge7_8(Iterator a, Iterator b, CSwap cswap) {
    merge3_4(a, b, cswap);
    merge4_4(a + 3, b + 4, cswap);
    merge4_4(a + 3, b, cswap);
}

//! merge network for element arrays length eight and eight
template <typename Iterator, typename CSwap>
static inline void merge8_8(Iterator a, Iterator b, CSwap cswap) {
    merge4_4(a, b, cswap);
    merge4_4(a + 4, b + 4, cswap);
    merge4_4(a + 4, b, cswap);
}

/*----------------------------------------------------------------------------*/

//! Bose-Nelson sorting network for two elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static inline void sort2(Iterator a, CSwap cswap = CSwap()) {
    merge1_1(a, a + 1, cswap);
}

//! Bose-Nelson sorting network for three elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static inline void sort3(Iterator a, CSwap cswap = CSwap()) {
    sort2(a + 1, cswap);
    merge1_2(a, a + 1, cswap);
}

//! Bose-Nelson sorting network for four elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort4(Iterator a, CSwap cswap = CSwap()) {
    sort2(a, cswap);
    sort2(a + 2, cswap);
    merge2_2(a, a + 2, cswap);
}

//! Bose-Nelson sorting network for five elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort5(Iterator a, CSwap cswap = CSwap()) {
    sort2(a, cswap);
    sort3(a + 2, cswap);
    merge2_3(a, a + 2, cswap);
}

//! Bose-Nelson sorting network for six elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort6(Iterator a, CSwap cswap = CSwap()) {
    sort3(a, cswap);
    sort3(a + 3, cswap);
    merge3_3(a, a + 3, cswap);
}

//! Bose-Nelson sorting network for seven elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort7(Iterator a, CSwap cswap = CSwap()) {
    sort3(a, cswap);
    sort4(a + 3, cswap);
    merge3_4(a, a + 3, cswap);
}

//! Bose-Nelson sorting network for eight elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort8(Iterator a, CSwap cswap = CSwap()) {
    sort4(a, cswap);
    sort4(a + 4, cswap);
    merge4_4(a, a + 4, cswap);
}

//! Bose-Nelson sorting network for nine elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort9(Iterator a, CSwap cswap = CSwap()) {
    sort4(a, cswap);
    sort5(a + 4, cswap);
    merge4_5(a, a + 4, cswap);
}

//! Bose-Nelson sorting network for ten elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort10(Iterator a, CSwap cswap = CSwap()) {
    sort5(a, cswap);
    sort5(a + 5, cswap);
    merge5_5(a, a + 5, cswap);
}

//! Bose-Nelson sorting network for eleven elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort11(Iterator a, CSwap cswap = CSwap()) {
    sort5(a, cswap);
    sort6(a + 5, cswap);
    merge5_6(a, a + 5, cswap);
}

//! Bose-Nelson sorting network for twelve elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort12(Iterator a, CSwap cswap = CSwap()) {
    sort6(a, cswap);
    sort6(a + 6, cswap);
    merge6_6(a, a + 6, cswap);
}

//! Bose-Nelson sorting network for thirteen elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort13(Iterator a, CSwap cswap = CSwap()) {
    sort6(a, cswap);
    sort7(a + 6, cswap);
    merge6_7(a, a + 6, cswap);
}

//! Bose-Nelson sorting network for fourteen elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort14(Iterator a, CSwap cswap = CSwap()) {
    sort7(a, cswap);
    sort7(a + 7, cswap);
    merge7_7(a, a + 7, cswap);
}

//! Bose-Nelson sorting network for fifteen elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort15(Iterator a, CSwap cswap = CSwap()) {
    sort7(a, cswap);
    sort8(a + 7, cswap);
    merge7_8(a, a + 7, cswap);
}

//! Bose-Nelson sorting network for sixteen elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort16(Iterator a, CSwap cswap = CSwap()) {
    sort8(a, cswap);
    sort8(a + 8, cswap);
    merge8_8(a, a + 8, cswap);
}

/*----------------------------------------------------------------------------*/

//! Call Bose-Network sorting network for up to sixteen elements with given
//! comparison method
template <typename Iterator, typename Comparator =
              std::less<typename std::iterator_traits<Iterator>::value_type> >
static void sort(Iterator begin, Iterator end, Comparator cmp = Comparator()) {
    CS_IfSwap<Comparator> cswap(cmp);

    switch (end - begin) {
    case 0:
        break;
    case 1:
        break;
    case 2:
        sort2(begin, cswap);
        break;
    case 3:
        sort3(begin, cswap);
        break;
    case 4:
        sort4(begin, cswap);
        break;
    case 5:
        sort5(begin, cswap);
        break;
    case 6:
        sort6(begin, cswap);
        break;
    case 7:
        sort7(begin, cswap);
        break;
    case 8:
        sort8(begin, cswap);
        break;
    case 9:
        sort9(begin, cswap);
        break;
    case 10:
        sort10(begin, cswap);
        break;
    case 11:
        sort11(begin, cswap);
        break;
    case 12:
        sort12(begin, cswap);
        break;
    case 13:
        sort13(begin, cswap);
        break;
    case 14:
        sort14(begin, cswap);
        break;
    case 15:
        sort15(begin, cswap);
        break;
    case 16:
        sort16(begin, cswap);
        break;
    default:
        abort();
        break;
    }
}

} // namespace bose_nelson

/******************************************************************************/

//! \}
//! \}

} // namespace sort_networks
} // namespace tlx

#endif // !TLX_SORT_NETWORKS_BOSE_NELSON_HEADER

/******************************************************************************/
