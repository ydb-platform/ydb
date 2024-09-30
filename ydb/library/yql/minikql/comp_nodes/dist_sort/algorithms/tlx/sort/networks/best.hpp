/*******************************************************************************
 * tlx/sort/networks/best.hpp
 *
 * Implementation of best known sorting networks for up to sixteen elements.
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018-2020 Jasper Marianczuk <jasper.marianczuk@gmail.com>
 * Copyright (C) 2020 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_SORT_NETWORKS_BEST_HEADER
#define TLX_SORT_NETWORKS_BEST_HEADER

#include <tlx/sort/networks/cswap.hpp>

#include <functional>

namespace tlx {

//! Implementations of sorting networks for up to sixteen elements.
namespace sort_networks {

//! \addtogroup tlx_sort
//! \{
//! \name Implementations of Sorting Networks
//! \{

//! Implementation of best known sorting networks for up to sixteen elements.
namespace best {

//! default conditional swap implementation
template <typename Iterator>
using DefaultCSwap = CS_IfSwap<
    std::less<typename std::iterator_traits<Iterator>::value_type> >;

//! sorting network for two elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort2(Iterator a, CSwap cswap = CSwap()) {
    cswap(a[0], a[1]);
}

//! sorting network for three elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort3(Iterator a, CSwap cswap = CSwap()) {
    cswap(a[1], a[2]);
    cswap(a[0], a[2]);
    cswap(a[0], a[1]);
}

//! sorting network for four elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort4(Iterator a, CSwap cswap = CSwap()) {
    cswap(a[0], a[1]);
    cswap(a[2], a[3]);
    cswap(a[0], a[2]);
    cswap(a[1], a[3]);
    cswap(a[1], a[2]);
}

//! sorting network for five elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort5(Iterator a, CSwap cswap = CSwap()) {
    cswap(a[0], a[1]);
    cswap(a[3], a[4]);
    cswap(a[2], a[4]);
    cswap(a[2], a[3]);
    cswap(a[0], a[3]);
    cswap(a[0], a[2]);
    cswap(a[1], a[4]);
    cswap(a[1], a[3]);
    cswap(a[1], a[2]);
}

//! sorting network for six elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort6(Iterator a, CSwap cswap = CSwap()) {
    cswap(a[1], a[2]);
    cswap(a[0], a[2]);
    cswap(a[0], a[1]);
    cswap(a[4], a[5]);
    cswap(a[3], a[5]);
    cswap(a[3], a[4]);
    cswap(a[0], a[3]);
    cswap(a[1], a[4]);
    cswap(a[2], a[5]);
    cswap(a[2], a[4]);
    cswap(a[1], a[3]);
    cswap(a[2], a[3]);
}

//! sorting network for seven elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort7(Iterator a, CSwap cswap = CSwap()) {
    cswap(a[1], a[2]);
    cswap(a[0], a[2]);
    cswap(a[0], a[1]);
    cswap(a[3], a[4]);
    cswap(a[5], a[6]);
    cswap(a[3], a[5]);
    cswap(a[4], a[6]);
    cswap(a[4], a[5]);
    cswap(a[0], a[4]);
    cswap(a[0], a[3]);
    cswap(a[1], a[5]);
    cswap(a[2], a[6]);
    cswap(a[2], a[5]);
    cswap(a[1], a[3]);
    cswap(a[2], a[4]);
    cswap(a[2], a[3]);
}

//! sorting network for eight elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort8(Iterator a, CSwap cswap = CSwap()) {
    cswap(a[0], a[1]);
    cswap(a[2], a[3]);
    cswap(a[0], a[2]);
    cswap(a[1], a[3]);
    cswap(a[1], a[2]);
    cswap(a[4], a[5]);
    cswap(a[6], a[7]);
    cswap(a[4], a[6]);
    cswap(a[5], a[7]);
    cswap(a[5], a[6]);
    cswap(a[0], a[4]);
    cswap(a[1], a[5]);
    cswap(a[1], a[4]);
    cswap(a[2], a[6]);
    cswap(a[3], a[7]);
    cswap(a[3], a[6]);
    cswap(a[2], a[4]);
    cswap(a[3], a[5]);
    cswap(a[3], a[4]);
}

//! sorting network for nine elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort9(Iterator a, CSwap cswap = CSwap()) {
    cswap(a[0], a[1]);
    cswap(a[3], a[4]);
    cswap(a[6], a[7]);
    cswap(a[1], a[2]);
    cswap(a[4], a[5]);
    cswap(a[7], a[8]);
    cswap(a[0], a[1]);
    cswap(a[3], a[4]);
    cswap(a[6], a[7]);
    cswap(a[0], a[3]);
    cswap(a[3], a[6]);
    cswap(a[0], a[3]);
    cswap(a[1], a[4]);
    cswap(a[4], a[7]);
    cswap(a[1], a[4]);
    cswap(a[2], a[5]);
    cswap(a[5], a[8]);
    cswap(a[2], a[5]);
    cswap(a[1], a[3]);
    cswap(a[5], a[7]);
    cswap(a[2], a[6]);
    cswap(a[4], a[6]);
    cswap(a[2], a[4]);
    cswap(a[2], a[3]);
    cswap(a[5], a[6]);
}

//! sorting network for ten elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort10(Iterator a, CSwap cswap = CSwap()) {
    cswap(a[4], a[9]);
    cswap(a[3], a[8]);
    cswap(a[2], a[7]);
    cswap(a[1], a[6]);
    cswap(a[0], a[5]);
    cswap(a[1], a[4]);
    cswap(a[6], a[9]);
    cswap(a[0], a[3]);
    cswap(a[5], a[8]);
    cswap(a[0], a[2]);
    cswap(a[3], a[6]);
    cswap(a[7], a[9]);
    cswap(a[0], a[1]);
    cswap(a[2], a[4]);
    cswap(a[5], a[7]);
    cswap(a[8], a[9]);
    cswap(a[1], a[2]);
    cswap(a[4], a[6]);
    cswap(a[7], a[8]);
    cswap(a[3], a[5]);
    cswap(a[2], a[5]);
    cswap(a[6], a[8]);
    cswap(a[1], a[3]);
    cswap(a[4], a[7]);
    cswap(a[2], a[3]);
    cswap(a[6], a[7]);
    cswap(a[3], a[4]);
    cswap(a[5], a[6]);
    cswap(a[4], a[5]);
}

//! sorting network for eleven elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort11(Iterator a, CSwap cswap = CSwap()) {
    cswap(a[0], a[1]);
    cswap(a[2], a[3]);
    cswap(a[4], a[5]);
    cswap(a[6], a[7]);
    cswap(a[8], a[9]);
    cswap(a[1], a[3]);
    cswap(a[5], a[7]);
    cswap(a[0], a[2]);
    cswap(a[4], a[6]);
    cswap(a[8], a[10]);
    cswap(a[1], a[2]);
    cswap(a[5], a[6]);
    cswap(a[9], a[10]);
    cswap(a[1], a[5]);
    cswap(a[6], a[10]);
    cswap(a[5], a[9]);
    cswap(a[2], a[6]);
    cswap(a[1], a[5]);
    cswap(a[6], a[10]);
    cswap(a[0], a[4]);
    cswap(a[3], a[7]);
    cswap(a[4], a[8]);
    cswap(a[0], a[4]);
    cswap(a[1], a[4]);
    cswap(a[7], a[10]);
    cswap(a[3], a[8]);
    cswap(a[2], a[3]);
    cswap(a[8], a[9]);
    cswap(a[2], a[4]);
    cswap(a[7], a[9]);
    cswap(a[3], a[5]);
    cswap(a[6], a[8]);
    cswap(a[3], a[4]);
    cswap(a[5], a[6]);
    cswap(a[7], a[8]);
}

//! sorting network for twelve elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort12(Iterator a, CSwap cswap = CSwap()) {
    cswap(a[0], a[1]);
    cswap(a[2], a[3]);
    cswap(a[4], a[5]);
    cswap(a[6], a[7]);
    cswap(a[8], a[9]);
    cswap(a[10], a[11]);
    cswap(a[1], a[3]);
    cswap(a[5], a[7]);
    cswap(a[9], a[11]);
    cswap(a[0], a[2]);
    cswap(a[4], a[6]);
    cswap(a[8], a[10]);
    cswap(a[1], a[2]);
    cswap(a[5], a[6]);
    cswap(a[9], a[10]);
    cswap(a[1], a[5]);
    cswap(a[6], a[10]);
    cswap(a[5], a[9]);
    cswap(a[2], a[6]);
    cswap(a[1], a[5]);
    cswap(a[6], a[10]);
    cswap(a[0], a[4]);
    cswap(a[7], a[11]);
    cswap(a[3], a[7]);
    cswap(a[4], a[8]);
    cswap(a[0], a[4]);
    cswap(a[7], a[11]);
    cswap(a[1], a[4]);
    cswap(a[7], a[10]);
    cswap(a[3], a[8]);
    cswap(a[2], a[3]);
    cswap(a[8], a[9]);
    cswap(a[2], a[4]);
    cswap(a[7], a[9]);
    cswap(a[3], a[5]);
    cswap(a[6], a[8]);
    cswap(a[3], a[4]);
    cswap(a[5], a[6]);
    cswap(a[7], a[8]);
}

//! sorting network for thirteen elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort13(Iterator a, CSwap cswap = CSwap()) {
    cswap(a[1], a[7]);
    cswap(a[9], a[11]);
    cswap(a[3], a[4]);
    cswap(a[5], a[8]);
    cswap(a[0], a[12]);
    cswap(a[2], a[6]);
    cswap(a[0], a[1]);
    cswap(a[2], a[3]);
    cswap(a[4], a[6]);
    cswap(a[8], a[11]);
    cswap(a[7], a[12]);
    cswap(a[5], a[9]);
    cswap(a[0], a[2]);
    cswap(a[3], a[7]);
    cswap(a[10], a[11]);
    cswap(a[1], a[4]);
    cswap(a[6], a[12]);
    cswap(a[7], a[8]);
    cswap(a[11], a[12]);
    cswap(a[4], a[9]);
    cswap(a[6], a[10]);
    cswap(a[3], a[4]);
    cswap(a[5], a[6]);
    cswap(a[8], a[9]);
    cswap(a[10], a[11]);
    cswap(a[1], a[7]);
    cswap(a[2], a[6]);
    cswap(a[9], a[11]);
    cswap(a[1], a[3]);
    cswap(a[4], a[7]);
    cswap(a[8], a[10]);
    cswap(a[0], a[5]);
    cswap(a[2], a[5]);
    cswap(a[6], a[8]);
    cswap(a[9], a[10]);
    cswap(a[1], a[2]);
    cswap(a[3], a[5]);
    cswap(a[7], a[8]);
    cswap(a[4], a[6]);
    cswap(a[2], a[3]);
    cswap(a[4], a[5]);
    cswap(a[6], a[7]);
    cswap(a[8], a[9]);
    cswap(a[3], a[4]);
    cswap(a[5], a[6]);
}

//! sorting network for fourteen elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort14(Iterator a, CSwap cswap = CSwap()) {
    cswap(a[0], a[1]);
    cswap(a[2], a[3]);
    cswap(a[4], a[5]);
    cswap(a[6], a[7]);
    cswap(a[8], a[9]);
    cswap(a[10], a[11]);
    cswap(a[12], a[13]);
    cswap(a[0], a[2]);
    cswap(a[4], a[6]);
    cswap(a[8], a[10]);
    cswap(a[1], a[3]);
    cswap(a[5], a[7]);
    cswap(a[9], a[11]);
    cswap(a[0], a[4]);
    cswap(a[8], a[12]);
    cswap(a[1], a[5]);
    cswap(a[9], a[13]);
    cswap(a[2], a[6]);
    cswap(a[3], a[7]);
    cswap(a[0], a[8]);
    cswap(a[1], a[9]);
    cswap(a[2], a[10]);
    cswap(a[3], a[11]);
    cswap(a[4], a[12]);
    cswap(a[5], a[13]);
    cswap(a[5], a[10]);
    cswap(a[6], a[9]);
    cswap(a[3], a[12]);
    cswap(a[7], a[11]);
    cswap(a[1], a[2]);
    cswap(a[4], a[8]);
    cswap(a[1], a[4]);
    cswap(a[7], a[13]);
    cswap(a[2], a[8]);
    cswap(a[2], a[4]);
    cswap(a[5], a[6]);
    cswap(a[9], a[10]);
    cswap(a[11], a[13]);
    cswap(a[3], a[8]);
    cswap(a[7], a[12]);
    cswap(a[6], a[8]);
    cswap(a[10], a[12]);
    cswap(a[3], a[5]);
    cswap(a[7], a[9]);
    cswap(a[3], a[4]);
    cswap(a[5], a[6]);
    cswap(a[7], a[8]);
    cswap(a[9], a[10]);
    cswap(a[11], a[12]);
    cswap(a[6], a[7]);
    cswap(a[8], a[9]);
}

//! sorting network for fifteen elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort15(Iterator a, CSwap cswap = CSwap()) {
    cswap(a[0], a[1]);
    cswap(a[2], a[3]);
    cswap(a[4], a[5]);
    cswap(a[6], a[7]);
    cswap(a[8], a[9]);
    cswap(a[10], a[11]);
    cswap(a[12], a[13]);
    cswap(a[0], a[2]);
    cswap(a[4], a[6]);
    cswap(a[8], a[10]);
    cswap(a[12], a[14]);
    cswap(a[1], a[3]);
    cswap(a[5], a[7]);
    cswap(a[9], a[11]);
    cswap(a[0], a[4]);
    cswap(a[8], a[12]);
    cswap(a[1], a[5]);
    cswap(a[9], a[13]);
    cswap(a[2], a[6]);
    cswap(a[10], a[14]);
    cswap(a[3], a[7]);
    cswap(a[0], a[8]);
    cswap(a[1], a[9]);
    cswap(a[2], a[10]);
    cswap(a[3], a[11]);
    cswap(a[4], a[12]);
    cswap(a[5], a[13]);
    cswap(a[6], a[14]);
    cswap(a[5], a[10]);
    cswap(a[6], a[9]);
    cswap(a[3], a[12]);
    cswap(a[13], a[14]);
    cswap(a[7], a[11]);
    cswap(a[1], a[2]);
    cswap(a[4], a[8]);
    cswap(a[1], a[4]);
    cswap(a[7], a[13]);
    cswap(a[2], a[8]);
    cswap(a[11], a[14]);
    cswap(a[2], a[4]);
    cswap(a[5], a[6]);
    cswap(a[9], a[10]);
    cswap(a[11], a[13]);
    cswap(a[3], a[8]);
    cswap(a[7], a[12]);
    cswap(a[6], a[8]);
    cswap(a[10], a[12]);
    cswap(a[3], a[5]);
    cswap(a[7], a[9]);
    cswap(a[3], a[4]);
    cswap(a[5], a[6]);
    cswap(a[7], a[8]);
    cswap(a[9], a[10]);
    cswap(a[11], a[12]);
    cswap(a[6], a[7]);
    cswap(a[8], a[9]);
}

//! sorting network for sixteen elements
template <typename Iterator, typename CSwap = DefaultCSwap<Iterator> >
static void sort16(Iterator a, CSwap cswap = CSwap()) {
    cswap(a[0], a[1]);
    cswap(a[2], a[3]);
    cswap(a[4], a[5]);
    cswap(a[6], a[7]);
    cswap(a[8], a[9]);
    cswap(a[10], a[11]);
    cswap(a[12], a[13]);
    cswap(a[14], a[15]);
    cswap(a[0], a[2]);
    cswap(a[4], a[6]);
    cswap(a[8], a[10]);
    cswap(a[12], a[14]);
    cswap(a[1], a[3]);
    cswap(a[5], a[7]);
    cswap(a[9], a[11]);
    cswap(a[13], a[15]);
    cswap(a[0], a[4]);
    cswap(a[8], a[12]);
    cswap(a[1], a[5]);
    cswap(a[9], a[13]);
    cswap(a[2], a[6]);
    cswap(a[10], a[14]);
    cswap(a[3], a[7]);
    cswap(a[11], a[15]);
    cswap(a[0], a[8]);
    cswap(a[1], a[9]);
    cswap(a[2], a[10]);
    cswap(a[3], a[11]);
    cswap(a[4], a[12]);
    cswap(a[5], a[13]);
    cswap(a[6], a[14]);
    cswap(a[7], a[15]);
    cswap(a[5], a[10]);
    cswap(a[6], a[9]);
    cswap(a[3], a[12]);
    cswap(a[13], a[14]);
    cswap(a[7], a[11]);
    cswap(a[1], a[2]);
    cswap(a[4], a[8]);
    cswap(a[1], a[4]);
    cswap(a[7], a[13]);
    cswap(a[2], a[8]);
    cswap(a[11], a[14]);
    cswap(a[2], a[4]);
    cswap(a[5], a[6]);
    cswap(a[9], a[10]);
    cswap(a[11], a[13]);
    cswap(a[3], a[8]);
    cswap(a[7], a[12]);
    cswap(a[6], a[8]);
    cswap(a[10], a[12]);
    cswap(a[3], a[5]);
    cswap(a[7], a[9]);
    cswap(a[3], a[4]);
    cswap(a[5], a[6]);
    cswap(a[7], a[8]);
    cswap(a[9], a[10]);
    cswap(a[11], a[12]);
    cswap(a[6], a[7]);
    cswap(a[8], a[9]);
}

//! Call best known sorting network for up to sixteen elements with given
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

} // namespace best

/******************************************************************************/

//! \}
//! \}

} // namespace sort_networks
} // namespace tlx

#endif // !TLX_SORT_NETWORKS_BEST_HEADER

/******************************************************************************/
