/*******************************************************************************
 * tlx/sort/networks/bose_nelson_parameter.hpp
 *
 * Recursively called Bose-Nelson sorting networks processing parameters instead
 * of an array.
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018-2020 Jasper Marianczuk <jasper.marianczuk@gmail.com>
 * Copyright (C) 2020 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_SORT_NETWORKS_BOSE_NELSON_PARAMETER_HEADER
#define TLX_SORT_NETWORKS_BOSE_NELSON_PARAMETER_HEADER

#include <tlx/sort/networks/cswap.hpp>

#include <functional>

namespace tlx {

//! Implementations of sorting networks for up to sixteen elements.
namespace sort_networks {

//! \addtogroup tlx_sort
//! \{
//! \name Implementations of Sorting Networks
//! \{

//! Implementation of Bose-Nelson sorting networks for up to sixteen elements
//! processing parameters instead of an array.
namespace bose_nelson_parameter {

//! default conditional swap implementation
template <typename ValueType>
using DefaultCSwap = CS_IfSwap<std::less<ValueType> >;

/*----------------------------------------------------------------------------*/

//! merge network for element arrays length one and one
template <typename ValueType, typename CSwap>
static inline
void merge_1_1(ValueType& a0, ValueType& b0, CSwap cswap) {
    cswap(a0, b0);
}

//! merge network for element arrays length one and two
template <typename ValueType, typename CSwap>
static inline
void merge_1_2(ValueType& a0, ValueType& b0, ValueType& b1, CSwap cswap) {
    cswap(a0, b1);
    cswap(a0, b0);
}

//! merge network for element arrays length two and one
template <typename ValueType, typename CSwap>
static inline
void merge_2_1(ValueType& a0, ValueType& a1, ValueType& b0, CSwap cswap) {
    cswap(a0, b0);
    cswap(a1, b0);
}

//! merge network for element arrays length two and two
template <typename ValueType, typename CSwap>
static inline
void merge_2_2(ValueType& a0, ValueType& a1, ValueType& b0, ValueType& b1,
               CSwap cswap) {
    merge_1_1(a0, b0, cswap);
    merge_1_1(a1, b1, cswap);
    merge_1_1(a1, b0, cswap);
}

//! merge network for element arrays length two and three
template <typename ValueType, typename CSwap>
static inline
void merge_2_3(ValueType& a0, ValueType& a1, ValueType& b0, ValueType& b1,
               ValueType& b2, CSwap cswap) {
    merge_1_2(a0, b0, b1, cswap);
    merge_1_1(a1, b2, cswap);
    merge_1_2(a1, b0, b1, cswap);
}

//! merge network for element arrays length three and two
template <typename ValueType, typename CSwap>
static inline
void merge_3_2(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& b0,
               ValueType& b1, CSwap cswap) {
    merge_1_1(a0, b0, cswap);
    merge_2_1(a1, a2, b1, cswap);
    merge_2_1(a1, a2, b0, cswap);
}

//! merge network for element arrays length three and three
template <typename ValueType, typename CSwap>
static inline
void merge_3_3(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& b0,
               ValueType& b1, ValueType& b2, CSwap cswap) {
    merge_1_1(a0, b0, cswap);
    merge_2_2(a1, a2, b1, b2, cswap);
    merge_2_1(a1, a2, b0, cswap);
}

//! merge network for element arrays length three and four
template <typename ValueType, typename CSwap>
static inline
void merge_3_4(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& b0,
               ValueType& b1, ValueType& b2, ValueType& b3, CSwap cswap) {
    merge_1_2(a0, b0, b1, cswap);
    merge_2_2(a1, a2, b2, b3, cswap);
    merge_2_2(a1, a2, b0, b1, cswap);
}

//! merge network for element arrays length four and three
template <typename ValueType, typename CSwap>
static inline
void merge_4_3(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3,
               ValueType& b0, ValueType& b1, ValueType& b2, CSwap cswap) {
    merge_2_2(a0, a1, b0, b1, cswap);
    merge_2_1(a2, a3, b2, cswap);
    merge_2_2(a2, a3, b0, b1, cswap);
}

//! merge network for element arrays length four and four
template <typename ValueType, typename CSwap>
static inline
void merge_4_4(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3,
               ValueType& b0, ValueType& b1, ValueType& b2, ValueType& b3,
               CSwap cswap) {
    merge_2_2(a0, a1, b0, b1, cswap);
    merge_2_2(a2, a3, b2, b3, cswap);
    merge_2_2(a2, a3, b0, b1, cswap);
}

//! merge network for element arrays length four and five
template <typename ValueType, typename CSwap>
static inline
void merge_4_5(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3,
               ValueType& b0, ValueType& b1, ValueType& b2, ValueType& b3,
               ValueType& b4, CSwap cswap) {
    merge_2_3(a0, a1, b0, b1, b2, cswap);
    merge_2_2(a2, a3, b3, b4, cswap);
    merge_2_3(a2, a3, b0, b1, b2, cswap);
}

//! merge network for element arrays length five and five
template <typename ValueType, typename CSwap>
static inline
void merge_5_5(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3,
               ValueType& a4, ValueType& b0, ValueType& b1, ValueType& b2,
               ValueType& b3, ValueType& b4, CSwap cswap) {
    merge_2_2(a0, a1, b0, b1, cswap);
    merge_3_3(a2, a3, a4, b2, b3, b4, cswap);
    merge_3_2(a2, a3, a4, b0, b1, cswap);
}

//! merge network for element arrays length five and six
template <typename ValueType, typename CSwap>
static inline
void merge_5_6(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3,
               ValueType& a4, ValueType& b0, ValueType& b1, ValueType& b2,
               ValueType& b3, ValueType& b4, ValueType& b5, CSwap cswap) {
    merge_2_3(a0, a1, b0, b1, b2, cswap);
    merge_3_3(a2, a3, a4, b3, b4, b5, cswap);
    merge_3_3(a2, a3, a4, b0, b1, b2, cswap);
}

//! merge network for element arrays length six and six
template <typename ValueType, typename CSwap>
static inline
void merge_6_6(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3,
               ValueType& a4, ValueType& a5, ValueType& b0, ValueType& b1,
               ValueType& b2, ValueType& b3, ValueType& b4, ValueType& b5,
               CSwap cswap) {
    merge_3_3(a0, a1, a2, b0, b1, b2, cswap);
    merge_3_3(a3, a4, a5, b3, b4, b5, cswap);
    merge_3_3(a3, a4, a5, b0, b1, b2, cswap);
}

//! merge network for element arrays length six and seven
template <typename ValueType, typename CSwap>
static inline
void merge_6_7(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3,
               ValueType& a4, ValueType& a5, ValueType& b0, ValueType& b1,
               ValueType& b2, ValueType& b3, ValueType& b4, ValueType& b5,
               ValueType& b6, CSwap cswap) {
    merge_3_4(a0, a1, a2, b0, b1, b2, b3, cswap);
    merge_3_3(a3, a4, a5, b4, b5, b6, cswap);
    merge_3_4(a3, a4, a5, b0, b1, b2, b3, cswap);
}

//! merge network for element arrays length seven and seven
template <typename ValueType, typename CSwap>
static inline
void merge_7_7(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3,
               ValueType& a4, ValueType& a5, ValueType& a6, ValueType& b0,
               ValueType& b1, ValueType& b2, ValueType& b3, ValueType& b4,
               ValueType& b5, ValueType& b6, CSwap cswap) {
    merge_3_3(a0, a1, a2, b0, b1, b2, cswap);
    merge_4_4(a3, a4, a5, a6, b3, b4, b5, b6, cswap);
    merge_4_3(a3, a4, a5, a6, b0, b1, b2, cswap);
}

//! merge network for element arrays length seven and eight
template <typename ValueType, typename CSwap>
static inline
void merge_7_8(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3,
               ValueType& a4, ValueType& a5, ValueType& a6, ValueType& b0,
               ValueType& b1, ValueType& b2, ValueType& b3, ValueType& b4,
               ValueType& b5, ValueType& b6, ValueType& b7, CSwap cswap) {
    merge_3_4(a0, a1, a2, b0, b1, b2, b3, cswap);
    merge_4_4(a3, a4, a5, a6, b4, b5, b6, b7, cswap);
    merge_4_4(a3, a4, a5, a6, b0, b1, b2, b3, cswap);
}

//! merge network for element arrays length eight and eight
template <typename ValueType, typename CSwap>
static inline
void merge_8_8(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3,
               ValueType& a4, ValueType& a5, ValueType& a6, ValueType& a7,
               ValueType& b0, ValueType& b1, ValueType& b2, ValueType& b3,
               ValueType& b4, ValueType& b5, ValueType& b6, ValueType& b7,
               CSwap cswap) {
    merge_4_4(a0, a1, a2, a3, b0, b1, b2, b3, cswap);
    merge_4_4(a4, a5, a6, a7, b4, b5, b6, b7, cswap);
    merge_4_4(a4, a5, a6, a7, b0, b1, b2, b3, cswap);
}

/*----------------------------------------------------------------------------*/

//! Bose-Nelson sorting network for two elements
template <typename ValueType, typename CSwap = DefaultCSwap<ValueType> >
static inline
void sort2(ValueType& x0, ValueType& x1, CSwap cswap = CSwap()) {
    merge_1_1(x0, x1, cswap);
}

//! Bose-Nelson sorting network for three elements
template <typename ValueType, typename CSwap = DefaultCSwap<ValueType> >
static inline
void sort3(ValueType& x0, ValueType& x1, ValueType& x2, CSwap cswap = CSwap()) {
    sort2(x1, x2, cswap);
    merge_1_2(x0, x1, x2, cswap);
}

//! Bose-Nelson sorting network for four elements
template <typename ValueType, typename CSwap = DefaultCSwap<ValueType> >
static inline
void sort4(ValueType& x0, ValueType& x1, ValueType& x2, ValueType& x3,
           CSwap cswap = CSwap()) {
    sort2(x0, x1, cswap);
    sort2(x2, x3, cswap);
    merge_2_2(x0, x1, x2, x3, cswap);
}

//! Bose-Nelson sorting network for five elements
template <typename ValueType, typename CSwap = DefaultCSwap<ValueType> >
static inline
void sort5(ValueType& x0, ValueType& x1, ValueType& x2, ValueType& x3,
           ValueType& x4, CSwap cswap = CSwap()) {
    sort2(x0, x1, cswap);
    sort3(x2, x3, x4, cswap);
    merge_2_3(x0, x1, x2, x3, x4, cswap);
}

//! Bose-Nelson sorting network for six elements
template <typename ValueType, typename CSwap = DefaultCSwap<ValueType> >
static inline
void sort6(ValueType& x0, ValueType& x1, ValueType& x2, ValueType& x3,
           ValueType& x4, ValueType& x5, CSwap cswap = CSwap()) {
    sort3(x0, x1, x2, cswap);
    sort3(x3, x4, x5, cswap);
    merge_3_3(x0, x1, x2, x3, x4, x5, cswap);
}

//! Bose-Nelson sorting network for seven elements
template <typename ValueType, typename CSwap = DefaultCSwap<ValueType> >
static inline
void sort7(ValueType& x0, ValueType& x1, ValueType& x2, ValueType& x3,
           ValueType& x4, ValueType& x5, ValueType& x6, CSwap cswap = CSwap()) {
    sort3(x0, x1, x2, cswap);
    sort4(x3, x4, x5, x6, cswap);
    merge_3_4(x0, x1, x2,
              x3, x4, x5, x6, cswap);
}

//! Bose-Nelson sorting network for eight elements
template <typename ValueType, typename CSwap = DefaultCSwap<ValueType> >
static inline
void sort8(ValueType& x0, ValueType& x1, ValueType& x2, ValueType& x3,
           ValueType& x4, ValueType& x5, ValueType& x6, ValueType& x7,
           CSwap cswap = CSwap()) {
    sort4(x0, x1, x2, x3, cswap);
    sort4(x4, x5, x6, x7, cswap);
    merge_4_4(x0, x1, x2, x3,
              x4, x5, x6, x7, cswap);
}

//! Bose-Nelson sorting network for nine elements
template <typename ValueType, typename CSwap = DefaultCSwap<ValueType> >
static inline
void sort9(ValueType& x0, ValueType& x1, ValueType& x2, ValueType& x3,
           ValueType& x4, ValueType& x5, ValueType& x6, ValueType& x7,
           ValueType& x8, CSwap cswap = CSwap()) {
    sort4(x0, x1, x2, x3, cswap);
    sort5(x4, x5, x6, x7, x8, cswap);
    merge_4_5(x0, x1, x2, x3,
              x4, x5, x6, x7, x8, cswap);
}

//! Bose-Nelson sorting network for ten elements
template <typename ValueType, typename CSwap = DefaultCSwap<ValueType> >
static inline
void sort10(ValueType& x0, ValueType& x1, ValueType& x2, ValueType& x3,
            ValueType& x4, ValueType& x5, ValueType& x6, ValueType& x7,
            ValueType& x8, ValueType& x9, CSwap cswap = CSwap()) {
    sort5(x0, x1, x2, x3, x4, cswap);
    sort5(x5, x6, x7, x8, x9, cswap);
    merge_5_5(x0, x1, x2, x3, x4,
              x5, x6, x7, x8, x9, cswap);
}

//! Bose-Nelson sorting network for eleven elements
template <typename ValueType, typename CSwap = DefaultCSwap<ValueType> >
static inline
void sort11(ValueType& x0, ValueType& x1, ValueType& x2, ValueType& x3,
            ValueType& x4, ValueType& x5, ValueType& x6, ValueType& x7,
            ValueType& x8, ValueType& x9, ValueType& x10,
            CSwap cswap = CSwap()) {
    sort5(x0, x1, x2, x3, x4, cswap);
    sort6(x5, x6, x7, x8, x9, x10, cswap);
    merge_5_6(x0, x1, x2, x3, x4,
              x5, x6, x7, x8, x9, x10, cswap);
}

//! Bose-Nelson sorting network for twelve elements
template <typename ValueType, typename CSwap = DefaultCSwap<ValueType> >
static inline
void sort12(ValueType& x0, ValueType& x1, ValueType& x2, ValueType& x3,
            ValueType& x4, ValueType& x5, ValueType& x6, ValueType& x7,
            ValueType& x8, ValueType& x9, ValueType& x10, ValueType& x11,
            CSwap cswap = CSwap()) {
    sort6(x0, x1, x2, x3, x4, x5, cswap);
    sort6(x6, x7, x8, x9, x10, x11, cswap);
    merge_6_6(x0, x1, x2, x3, x4, x5,
              x6, x7, x8, x9, x10, x11, cswap);
}

//! Bose-Nelson sorting network for thirteen elements
template <typename ValueType, typename CSwap = DefaultCSwap<ValueType> >
static inline
void sort13(ValueType& x0, ValueType& x1, ValueType& x2, ValueType& x3,
            ValueType& x4, ValueType& x5, ValueType& x6, ValueType& x7,
            ValueType& x8, ValueType& x9, ValueType& x10, ValueType& x11,
            ValueType& x12, CSwap cswap = CSwap()) {
    sort6(x0, x1, x2, x3, x4, x5, cswap);
    sort7(x6, x7, x8, x9, x10, x11, x12, cswap);
    merge_6_7(x0, x1, x2, x3, x4, x5,
              x6, x7, x8, x9, x10, x11, x12, cswap);
}

//! Bose-Nelson sorting network for fourteen elements
template <typename ValueType, typename CSwap = DefaultCSwap<ValueType> >
static inline
void sort14(ValueType& x0, ValueType& x1, ValueType& x2, ValueType& x3,
            ValueType& x4, ValueType& x5, ValueType& x6, ValueType& x7,
            ValueType& x8, ValueType& x9, ValueType& x10, ValueType& x11,
            ValueType& x12, ValueType& x13, CSwap cswap = CSwap()) {
    sort7(x0, x1, x2, x3, x4, x5, x6, cswap);
    sort7(x7, x8, x9, x10, x11, x12, x13, cswap);
    merge_7_7(x0, x1, x2, x3, x4, x5, x6,
              x7, x8, x9, x10, x11, x12, x13, cswap);
}

//! Bose-Nelson sorting network for fifteen elements
template <typename ValueType, typename CSwap = DefaultCSwap<ValueType> >
static inline
void sort15(ValueType& x0, ValueType& x1, ValueType& x2, ValueType& x3,
            ValueType& x4, ValueType& x5, ValueType& x6, ValueType& x7,
            ValueType& x8, ValueType& x9, ValueType& x10, ValueType& x11,
            ValueType& x12, ValueType& x13, ValueType& x14,
            CSwap cswap = CSwap()) {
    sort7(x0, x1, x2, x3, x4, x5, x6, cswap);
    sort8(x7, x8, x9, x10, x11, x12, x13, x14, cswap);
    merge_7_8(x0, x1, x2, x3, x4, x5, x6,
              x7, x8, x9, x10, x11, x12, x13, x14, cswap);
}

//! Bose-Nelson sorting network for sixteen elements
template <typename ValueType, typename CSwap = DefaultCSwap<ValueType> >
static inline
void sort16(ValueType& x0, ValueType& x1, ValueType& x2, ValueType& x3,
            ValueType& x4, ValueType& x5, ValueType& x6, ValueType& x7,
            ValueType& x8, ValueType& x9, ValueType& x10, ValueType& x11,
            ValueType& x12, ValueType& x13, ValueType& x14, ValueType& x15,
            CSwap cswap = CSwap()) {
    sort8(x0, x1, x2, x3, x4, x5, x6, x7, cswap);
    sort8(x8, x9, x10, x11, x12, x13, x14, x15, cswap);
    merge_8_8(x0, x1, x2, x3, x4, x5, x6, x7,
              x8, x9, x10, x11, x12, x13, x14, x15, cswap);
}

/*----------------------------------------------------------------------------*/

//! Call Bose-Network sorting network for up to sixteen elements with given
//! comparison method
template <typename Iterator, typename Comparator =
              std::less<typename std::iterator_traits<Iterator>::value_type> >
static void sort(Iterator a, Iterator b, Comparator cmp = Comparator()) {
    CS_IfSwap<Comparator> cswap(cmp);

    switch (b - a) {
    case 0:
        break;
    case 1:
        break;
    case 2:
        sort2(a[0], a[1], cswap);
        break;
    case 3:
        sort3(a[0], a[1], a[2], cswap);
        break;
    case 4:
        sort4(a[0], a[1], a[2], a[3], cswap);
        break;
    case 5:
        sort5(a[0], a[1], a[2], a[3], a[4], cswap);
        break;
    case 6:
        sort6(a[0], a[1], a[2], a[3], a[4], a[5], cswap);
        break;
    case 7:
        sort7(a[0], a[1], a[2], a[3], a[4], a[5], a[6], cswap);
        break;
    case 8:
        sort8(a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], cswap);
        break;
    case 9:
        sort9(a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], a[8], cswap);
        break;
    case 10:
        sort10(a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], a[8], a[9],
               cswap);
        break;
    case 11:
        sort11(a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], a[8], a[9],
               a[10], cswap);
        break;
    case 12:
        sort12(a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], a[8], a[9],
               a[10], a[11], cswap);
        break;
    case 13:
        sort13(a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], a[8], a[9],
               a[10], a[11], a[12], cswap);
        break;
    case 14:
        sort14(a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], a[8], a[9],
               a[10], a[11], a[12], a[13], cswap);
        break;
    case 15:
        sort15(a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], a[8], a[9],
               a[10], a[11], a[12], a[13], a[14], cswap);
        break;
    case 16:
        sort16(a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], a[8], a[9],
               a[10], a[11], a[12], a[13], a[14], a[15], cswap);
        break;
    default:
        abort();
        break;
    }
}

} // namespace bose_nelson_parameter

/******************************************************************************/

//! \}
//! \}

} // namespace sort_networks
} // namespace tlx

#endif // !TLX_SORT_NETWORKS_BOSE_NELSON_PARAMETER_HEADER

/******************************************************************************/
