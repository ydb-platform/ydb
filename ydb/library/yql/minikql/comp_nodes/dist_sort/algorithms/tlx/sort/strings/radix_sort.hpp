/*******************************************************************************
 * tlx/sort/strings/radix_sort.hpp
 *
 * Out-of-place and in-place radix sort for strings. This is an internal
 * implementation header, see tlx/sort/strings.hpp for public front-end
 * functions.
 *
 * These are explicit stack-based most-significant-digit radix sort
 * implementations. All implementations were written by Timo Bingmann and are
 * based on work by Juha Kärkkäinen and Tommi Rantala. "Engineering Radix Sort
 * for Strings."  International Symposium on String Processing and Information
 * Retrieval (SPIRE). Springer, 2008.
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2015-2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_SORT_STRINGS_RADIX_SORT_HEADER
#define TLX_SORT_STRINGS_RADIX_SORT_HEADER

#include "../../container/simple_vector.hpp"
#include "../../define/likely.hpp"
#include "../../sort/strings/multikey_quicksort.hpp"
#include "../../sort/strings/string_ptr.hpp"

#include <cstdint>
#include <stack>
#include <utility>
#include <vector>

namespace tlx {

//! \addtogroup tlx_sort
//! \{

namespace sort_strings_detail {

/******************************************************************************/

static const size_t g_inssort_threshold = 32;

/******************************************************************************/
// Out-of-place 8-bit radix-sort WITHOUT character caching.

template <typename StringShadowPtr>
struct RadixStep_CE0 {
    StringShadowPtr strptr;
    size_t idx, pos, bkt_size[256];

    typedef typename StringShadowPtr::StringSet StringSet;
    typedef typename StringSet::Iterator Iterator;

    RadixStep_CE0(const StringShadowPtr& in_strptr, size_t depth)
        : strptr(in_strptr) {

        const StringSet& ss = strptr.active();

        // count character occurrences
        std::fill(bkt_size, bkt_size + 256, 0);
        for (Iterator i = ss.begin(); i != ss.end(); ++i)
            ++bkt_size[ss.get_uint8(ss[i], depth)];

        // prefix sum
        Iterator bkt_index[256];
        bkt_index[0] = strptr.shadow().begin();
        for (size_t i = 1; i < 256; ++i)
            bkt_index[i] = bkt_index[i - 1] + bkt_size[i - 1];

        // distribute
        for (Iterator i = ss.begin(); i != ss.end(); ++i)
            *(bkt_index[ss.get_uint8(ss[i], depth)]++) = std::move(ss[i]);

        // will increment to 1 on first process
        idx = 0;
        pos = bkt_size[0];

        // copy back finished strings in zeroth bucket
        strptr.flip(0, pos).copy_back();

        // store lcps
        if (strptr.with_lcp) {
            size_t size = ss.size();

            // set lcps of zero-terminated strings
            for (size_t i = 1; i < pos; ++i)
                strptr.set_lcp(i, depth);

            // set lcps between non-empty bucket boundaries
            size_t bkt = bkt_size[0], i = 1;
            if (bkt > 0 && bkt < size)
                strptr.set_lcp(bkt, depth);
            while (i < 256) {
                while (i < 256 && bkt_size[i] == 0)
                    ++i;
                bkt += bkt_size[i];
                if (bkt >= size)
                    break;
                strptr.set_lcp(bkt, depth);
                ++i;
            }
        }
    }
};

/*
 * Out-of-place 8-bit radix-sort WITHOUT character caching.
 */
template <typename StringShadowPtr>
static inline void
radixsort_CE0_loop(const StringShadowPtr& strptr, size_t depth, size_t memory) {

    typedef RadixStep_CE0<StringShadowPtr> RadixStep;

    std::stack<RadixStep, std::vector<RadixStep> > radixstack;
    radixstack.emplace(strptr, depth);

    while (radixstack.size())
    {
        while (radixstack.top().idx < 255)
        {
            RadixStep& rs = radixstack.top();

            // process the bucket rs.idx
            size_t bkt_size = rs.bkt_size[++rs.idx];

            if (bkt_size == 0) {
                // done
            }
            else if (bkt_size < g_inssort_threshold) {
                insertion_sort(
                    rs.strptr.flip(rs.pos, bkt_size).copy_back(),
                    depth + radixstack.size(),
                    memory - sizeof(RadixStep) * radixstack.size());
                rs.pos += bkt_size;
            }
            else if (
                TLX_UNLIKELY(
                    memory != 0 &&
                    memory < sizeof(RadixStep) * (radixstack.size() + 1)))
            {
                multikey_quicksort(
                    rs.strptr.flip(rs.pos, bkt_size).copy_back(),
                    depth + radixstack.size(),
                    memory - sizeof(RadixStep) * radixstack.size());
                rs.pos += bkt_size;
            }
            else
            {
                rs.pos += bkt_size;
                radixstack.emplace(rs.strptr.flip(rs.pos - bkt_size, bkt_size),
                                   depth + radixstack.size());
                // cannot add here, because rs may have invalidated
            }
        }
        radixstack.pop();
    }
}

/*!
 * Adapter method to allocate shadow array if needed.
 */
template <typename StringPtr>
static inline void
radixsort_CE0(const StringPtr& strptr, size_t depth, size_t memory) {

    typedef typename StringPtr::StringSet StringSet;
    const StringSet& ss = strptr.active();
    if (ss.size() < g_inssort_threshold)
        return insertion_sort(strptr, depth, memory);

    typedef RadixStep_CE0<typename StringPtr::WithShadow> RadixStep;

    // try to estimate the amount of memory used
    size_t memory_use =
        2 * sizeof(size_t) + sizeof(StringSet)
        + ss.size() * sizeof(typename StringSet::String);
    size_t memory_slack = 3 * sizeof(RadixStep);

    if (memory != 0 && memory < memory_use + memory_slack + 1)
        return multikey_quicksort(strptr, depth, memory);

    typename StringSet::Container shadow = ss.allocate(ss.size());
    radixsort_CE0_loop(
        strptr.add_shadow(StringSet(shadow)), depth, memory - memory_use);
    StringSet::deallocate(shadow);
}

/******************************************************************************/
// Out-of-place 8-bit radix-sort with character caching.

template <typename StringShadowPtr>
struct RadixStep_CE2 {
    StringShadowPtr strptr;
    size_t idx, pos, bkt_size[256];

    typedef typename StringShadowPtr::StringSet StringSet;
    typedef typename StringSet::Iterator Iterator;

    RadixStep_CE2(const StringShadowPtr& in_strptr, size_t depth,
                  std::uint8_t* charcache) : strptr(in_strptr) {

        const StringSet& ss = strptr.active();
        const size_t n = ss.size();

        // read characters and count character occurrences
        std::fill(bkt_size, bkt_size + 256, 0);
        std::uint8_t* cc = charcache;
        for (Iterator i = ss.begin(); i != ss.end(); ++i, ++cc)
            *cc = ss.get_uint8(ss[i], depth);
        for (cc = charcache; cc != charcache + n; ++cc)
            ++bkt_size[static_cast<std::uint8_t>(*cc)];

        // prefix sum
        Iterator bkt_index[256];
        bkt_index[0] = strptr.shadow().begin();
        for (size_t i = 1; i < 256; ++i)
            bkt_index[i] = bkt_index[i - 1] + bkt_size[i - 1];

        // distribute
        cc = charcache;
        for (Iterator i = ss.begin(); i != ss.end(); ++i, ++cc)
            *(bkt_index[static_cast<std::uint8_t>(*cc)]++) = std::move(ss[i]);

        idx = 0; // will increment to 1 on first process
        pos = bkt_size[0];

        // copy back finished strings in zeroth bucket
        strptr.flip(0, pos).copy_back();

        // store lcps
        if (strptr.with_lcp) {
            size_t size = ss.size();

            // set lcps of zero-terminated strings
            for (size_t i = 1; i < pos; ++i)
                strptr.set_lcp(i, depth);

            // set lcps between non-empty bucket boundaries
            size_t bkt = bkt_size[0], i = 1;
            if (bkt > 0 && bkt < size)
                strptr.set_lcp(bkt, depth);
            while (i < 256) {
                while (i < 256 && bkt_size[i] == 0)
                    ++i;
                bkt += bkt_size[i];
                if (bkt >= size)
                    break;
                strptr.set_lcp(bkt, depth);
                ++i;
            }
        }
    }
};

template <typename StringPtr>
static inline void
radixsort_CI3(const StringPtr& strptr, std::uint16_t* charcache,
              size_t depth, size_t memory);

/*
 * Out-of-place 8-bit radix-sort with character caching.
 */
template <typename StringShadowPtr>
static inline void
radixsort_CE2_loop(const StringShadowPtr& strptr,
                   std::uint8_t* charcache, size_t depth, size_t memory) {

    typedef RadixStep_CE2<StringShadowPtr> RadixStep;

    std::stack<RadixStep, std::vector<RadixStep> > radixstack;
    radixstack.emplace(strptr, depth, charcache);

    while (TLX_LIKELY(!radixstack.empty()))
    {
        while (TLX_LIKELY(radixstack.top().idx < 255))
        {
            RadixStep& rs = radixstack.top();

            // process the bucket rs.idx
            size_t bkt_size = rs.bkt_size[++rs.idx];

            if (TLX_UNLIKELY(bkt_size == 0)) {
                // done
            }
            else if (bkt_size < g_inssort_threshold) {
                insertion_sort(
                    rs.strptr.flip(rs.pos, bkt_size).copy_back(),
                    depth + radixstack.size(),
                    memory - sizeof(RadixStep) * radixstack.size());
                rs.pos += bkt_size;
            }
            else if (
                TLX_UNLIKELY(
                    memory != 0 &&
                    memory < sizeof(RadixStep) * (radixstack.size() + 1)))
            {
                multikey_quicksort(
                    rs.strptr.flip(rs.pos, bkt_size).copy_back(),
                    depth + radixstack.size(),
                    memory - sizeof(RadixStep) * radixstack.size());
                rs.pos += bkt_size;
            }
            else
            {
                // have to increment first, as rs may be invalidated
                rs.pos += bkt_size;
                radixstack.emplace(rs.strptr.flip(rs.pos - bkt_size, bkt_size),
                                   depth + radixstack.size(), charcache);
            }
        }
        radixstack.pop();
    }
}

template <typename StringPtr>
static inline void
radixsort_CE2(const StringPtr& strptr, size_t depth, size_t memory) {

    typedef typename StringPtr::StringSet StringSet;
    const StringSet& ss = strptr.active();
    if (ss.size() < g_inssort_threshold)
        return insertion_sort(strptr, depth, memory);

    typedef RadixStep_CE2<typename StringPtr::WithShadow> RadixStep;

    // try to estimate the amount of memory used
    size_t memory_use =
        2 * sizeof(size_t) + sizeof(StringSet)
        + ss.size() * sizeof(std::uint8_t)
        + ss.size() * sizeof(typename StringSet::String);
    size_t memory_slack = 3 * sizeof(RadixStep);

    if (memory != 0 && memory < memory_use + memory_slack + 1)
        return radixsort_CI3(strptr, depth, memory);

    typename StringSet::Container shadow = ss.allocate(ss.size());
    std::uint8_t* charcache = new std::uint8_t[ss.size()];

    radixsort_CE2_loop(strptr.add_shadow(StringSet(shadow)),
                       charcache, depth, memory - memory_use);

    delete[] charcache;
    StringSet::deallocate(shadow);
}

/******************************************************************************/
// Out-of-place adaptive radix-sort with character caching

template <typename StringShadowPtr>
struct RadixStep_CE3 {
    enum { RADIX = 0x10000 };

    StringShadowPtr strptr;
    size_t idx, pos, bkt_size[RADIX];

    typedef typename StringShadowPtr::StringSet StringSet;
    typedef typename StringSet::Iterator Iterator;

    RadixStep_CE3(const StringShadowPtr& in_strptr, size_t depth,
                  std::uint16_t* charcache) : strptr(in_strptr) {

        const StringSet& ss = strptr.active();
        const size_t n = ss.size();

        // read characters and count character occurrences
        std::fill(bkt_size, bkt_size + RADIX, 0);
        std::uint16_t* cc = charcache;
        for (Iterator i = ss.begin(); i != ss.end(); ++i, ++cc)
            *cc = ss.get_uint16(ss[i], depth);
        for (cc = charcache; cc != charcache + n; ++cc)
            ++bkt_size[static_cast<std::uint16_t>(*cc)];

        // prefix sum
        simple_vector<Iterator> bkt_index(RADIX);
        bkt_index[0] = strptr.shadow().begin();
        for (size_t i = 1; i < RADIX; ++i)
            bkt_index[i] = bkt_index[i - 1] + bkt_size[i - 1];

        // store lcps
        if (strptr.with_lcp) {
            // set lcps of zero-terminated strings
            for (size_t i = 1; i < bkt_size[0]; ++i)
                strptr.set_lcp(i, depth);

            // set lcps between non-empty bucket boundaries
            size_t first = get_next_non_empty_bkt_index(0);
            size_t bkt = bkt_index[first] + bkt_size[first] - bkt_index[0];

            size_t second = get_next_non_empty_bkt_index(first + 1);
            while (second < RADIX)
            {
                size_t partial_equal = (first >> 8) == (second >> 8);
                strptr.set_lcp(bkt, depth + partial_equal);
                bkt += bkt_size[second];
                first = second;
                second = get_next_non_empty_bkt_index(second + 1);
            }
        }

        // distribute
        cc = charcache;
        for (Iterator i = ss.begin(); i != ss.end(); ++i, ++cc)
            *(bkt_index[static_cast<std::uint16_t>(*cc)]++) = std::move(ss[i]);

        // will increment to 1 on first process
        idx = 0;
        pos = bkt_size[0];

        // copy back finished strings in zeroth bucket
        strptr.flip(0, pos).copy_back();
    }

    size_t get_next_non_empty_bkt_index(size_t start) {
        if (start >= RADIX)
            return RADIX;
        while (start < RADIX && bkt_size[start] == 0)
            ++start;
        return start;
    }
};

/*
 * Out-of-place adaptive radix-sort with character caching which starts with
 * 16-bit radix sort and then switching to 8-bit for smaller string sets.
 */
template <typename StringShadowPtr>
static inline void
radixsort_CE3_loop(const StringShadowPtr& strptr,
                   std::uint16_t* charcache, size_t depth, size_t memory) {

    enum { RADIX = 0x10000 };

    typedef RadixStep_CE3<StringShadowPtr> RadixStep;

    std::stack<RadixStep, std::vector<RadixStep> > radixstack;
    radixstack.emplace(strptr, depth, charcache);

    while (TLX_LIKELY(!radixstack.empty()))
    {
        while (TLX_LIKELY(radixstack.top().idx < RADIX - 1))
        {
            RadixStep& rs = radixstack.top();

            // process the bucket rs.idx
            size_t bkt_size = rs.bkt_size[++rs.idx];

            if (TLX_UNLIKELY(bkt_size == 0)) {
                // done
            }
            else if (TLX_UNLIKELY((rs.idx & 0xFF) == 0)) {
                // zero-termination
                rs.strptr.flip(rs.pos, bkt_size).copy_back();
                for (size_t i = rs.pos + 1; i < rs.pos + bkt_size; ++i)
                    rs.strptr.set_lcp(i, depth + 2 * radixstack.size() - 1);
                rs.pos += bkt_size;
            }
            else if (TLX_UNLIKELY(bkt_size < g_inssort_threshold))
            {
                insertion_sort(
                    rs.strptr.flip(rs.pos, bkt_size).copy_back(),
                    depth + 2 * radixstack.size(),
                    memory - sizeof(RadixStep) * radixstack.size());
                rs.pos += bkt_size;
            }
            else if (bkt_size < RADIX)
            {
                radixsort_CE2_loop(
                    rs.strptr.flip(rs.pos, bkt_size),
                    reinterpret_cast<std::uint8_t*>(charcache),
                    depth + 2 * radixstack.size(),
                    memory - sizeof(RadixStep) * radixstack.size());
                rs.pos += bkt_size;
            }
            else if (
                TLX_UNLIKELY(
                    memory != 0 &&
                    memory < sizeof(RadixStep) * (radixstack.size() + 1)))
            {
                multikey_quicksort(
                    rs.strptr.flip(rs.pos, bkt_size).copy_back(),
                    depth + 2 * radixstack.size(),
                    memory - sizeof(RadixStep) * radixstack.size());
                rs.pos += bkt_size;
            }
            else
            {
                // have to increment first, as rs may be invalidated
                rs.pos += bkt_size;
                radixstack.emplace(
                    rs.strptr.flip(rs.pos - bkt_size, bkt_size),
                    depth + 2 * radixstack.size(), charcache);
            }
        }
        radixstack.pop();
    }
}

template <typename StringPtr>
static inline void
radixsort_CE3(const StringPtr& strptr, size_t depth, size_t memory) {
    enum { RADIX = 0x10000 };
    typedef typename StringPtr::StringSet StringSet;

    const StringSet& ss = strptr.active();
    if (ss.size() < g_inssort_threshold)
        return insertion_sort(strptr, depth, memory);

    if (ss.size() < RADIX)
        return radixsort_CE2(strptr, depth, memory);

    typedef RadixStep_CE3<typename StringPtr::WithShadow> RadixStep;

    // try to estimate the amount of memory used
    size_t memory_use =
        2 * sizeof(size_t) + sizeof(StringSet)
        + ss.size() * sizeof(std::uint16_t)
        + ss.size() * sizeof(typename StringSet::String);
    size_t memory_slack = 3 * sizeof(RadixStep);

    if (memory != 0 && memory < memory_use + memory_slack + 1)
        return radixsort_CE2(strptr, depth, memory);

    typename StringSet::Container shadow = ss.allocate(ss.size());
    std::uint16_t* charcache = new std::uint16_t[ss.size()];

    radixsort_CE3_loop(strptr.add_shadow(StringSet(shadow)),
                       charcache, depth, memory - memory_use);

    delete[] charcache;
    StringSet::deallocate(shadow);
}

/******************************************************************************/
// In-place 8-bit radix-sort with character caching.

template <typename StringPtr>
struct RadixStep_CI2 {
    typedef typename StringPtr::StringSet StringSet;
    typedef typename StringSet::Iterator Iterator;
    typedef typename StringSet::String String;

    size_t idx, pos;
    size_t bkt_size[256];

    RadixStep_CI2(const StringPtr& strptr,
                  size_t base, size_t depth, std::uint8_t* charcache) {

        const StringSet& ss = strptr.active();
        size_t size = ss.size();

        // read characters and count character occurrences
        std::fill(bkt_size, bkt_size + 256, 0);
        std::uint8_t* cc = charcache;
        for (Iterator i = ss.begin(); i != ss.end(); ++i, ++cc)
            *cc = ss.get_uint8(ss[i], depth);
        for (cc = charcache; cc != charcache + size; ++cc)
            ++bkt_size[static_cast<std::uint8_t>(*cc)];

        // inclusive prefix sum
        size_t bkt[256];
        bkt[0] = bkt_size[0];
        size_t last_bkt_size = bkt_size[0];
        for (size_t i = 1; i < 256; ++i) {
            bkt[i] = bkt[i - 1] + bkt_size[i];
            if (bkt_size[i] != 0) last_bkt_size = bkt_size[i];
        }

        // premute in-place
        for (size_t i = 0, j; i < size - last_bkt_size; )
        {
            String perm = std::move(ss[ss.begin() + i]);
            std::uint8_t permch = charcache[i];
            while ((j = --bkt[permch]) > i)
            {
                std::swap(perm, ss[ss.begin() + j]);
                std::swap(permch, charcache[j]);
            }
            ss[ss.begin() + i] = std::move(perm);
            i += bkt_size[permch];
        }

        // will increment idx to 1 in first step, bkt 0 is not sorted further
        idx = 0;
        pos = base + bkt_size[0];

        // store lcps
        if (strptr.with_lcp) {
            // set lcps of zero-terminated strings
            for (size_t i = 1; i < bkt_size[0]; ++i)
                strptr.set_lcp(i, depth);

            // set lcps between non-empty bucket boundaries
            size_t lbkt = bkt_size[0], i = 1;
            if (lbkt > 0 && lbkt < size)
                strptr.set_lcp(lbkt, depth);
            while (i < 256) {
                while (i < 256 && bkt_size[i] == 0)
                    ++i;
                lbkt += bkt_size[i];
                if (lbkt >= size)
                    break;
                strptr.set_lcp(lbkt, depth);
                ++i;
            }
        }
    }
};

/*
 * In-place 8-bit radix-sort with character caching.
 */
template <typename StringPtr>
static inline void
radixsort_CI2(const StringPtr& strptr, std::uint8_t* charcache,
              size_t depth, size_t memory) {

    typedef RadixStep_CI2<StringPtr> RadixStep;

    std::stack<RadixStep, std::vector<RadixStep> > radixstack;
    radixstack.emplace(strptr, /* base */ 0, depth, charcache);

    while (TLX_LIKELY(!radixstack.empty()))
    {
        while (TLX_LIKELY(radixstack.top().idx < 255))
        {
            RadixStep& rs = radixstack.top();

            // process the bucket rs.idx
            size_t bkt_size = rs.bkt_size[++rs.idx];

            if (TLX_UNLIKELY(bkt_size <= 1)) {
                // done
                rs.pos += bkt_size;
            }
            else if (bkt_size < g_inssort_threshold) {
                insertion_sort(
                    strptr.sub(rs.pos, bkt_size),
                    depth + radixstack.size(),
                    memory - sizeof(RadixStep) * radixstack.size());
                rs.pos += bkt_size;
            }
            else if (
                TLX_UNLIKELY(
                    memory != 0 &&
                    memory < sizeof(RadixStep) * (radixstack.size() + 1)))
            {
                multikey_quicksort(
                    strptr.sub(rs.pos, bkt_size),
                    depth + radixstack.size(),
                    memory - sizeof(RadixStep) * radixstack.size());
                rs.pos += bkt_size;
            }
            else
            {
                // have to increment first, as rs may be invalidated
                rs.pos += bkt_size;
                radixstack.emplace(
                    strptr.sub(rs.pos - bkt_size, bkt_size),
                    rs.pos - bkt_size, depth + radixstack.size(), charcache);
            }
        }
        radixstack.pop();
    }
}

/*
 * In-place 8-bit radix-sort with character caching.
 */
template <typename StringPtr>
static inline void
radixsort_CI2(const StringPtr& strptr, size_t depth, size_t memory) {
    typedef typename StringPtr::StringSet StringSet;

    if (strptr.size() < g_inssort_threshold)
        return insertion_sort(strptr, depth, memory);

    typedef RadixStep_CI2<StringPtr> RadixStep;

    // try to estimate the amount of memory used
    size_t memory_use =
        2 * sizeof(size_t) + sizeof(StringSet)
        + strptr.size() * sizeof(std::uint8_t);
    size_t memory_slack = 3 * sizeof(RadixStep);

    if (memory != 0 && memory < memory_use + memory_slack + 1)
        return multikey_quicksort(strptr, depth, memory);

    std::uint8_t* charcache = new         std::uint8_t[strptr.size()];

    radixsort_CI2(strptr, charcache, depth, memory - memory_use);

    delete[] charcache;
}

/******************************************************************************/
// In-place adaptive radix-sort with character caching

template <typename StringPtr>
struct RadixStep_CI3 {
    enum { RADIX = 0x10000 };

    typedef typename StringPtr::StringSet StringSet;
    typedef typename StringSet::Iterator Iterator;
    typedef typename StringSet::String String;

    size_t idx, pos;
    size_t bkt_size[RADIX];

    RadixStep_CI3(const StringPtr& strptr, size_t base, size_t depth,
                  std::uint16_t* charcache) {

        const StringSet& ss = strptr.active();
        const size_t n = ss.size();
        // read characters and count character occurrences
        std::fill(bkt_size, bkt_size + RADIX, 0);
        std::uint16_t* cc = charcache;
        for (Iterator i = ss.begin(); i != ss.end(); ++i, ++cc)
            *cc = ss.get_uint16(ss[i], depth);
        for (cc = charcache; cc != charcache + n; ++cc)
            ++bkt_size[static_cast<std::uint16_t>(*cc)];

        // inclusive prefix sum
        simple_vector<size_t> bkt_index(RADIX);
        bkt_index[0] = bkt_size[0];
        size_t last_bkt_size = bkt_size[0];
        for (size_t i = 1; i < RADIX; ++i) {
            bkt_index[i] = bkt_index[i - 1] + bkt_size[i];
            if (bkt_size[i]) last_bkt_size = bkt_size[i];
        }

        // store lcps
        if (strptr.with_lcp) {
            // set lcps of zero-terminated strings
            for (size_t i = 1; i < bkt_size[0]; ++i)
                strptr.set_lcp(i, depth);

            // set lcps between non-empty bucket boundaries
            size_t first = get_next_non_empty_bkt_index(0);
            size_t bkt = bkt_index[first];

            size_t second = get_next_non_empty_bkt_index(first + 1);
            while (second < RADIX)
            {
                size_t partial_equal = (first >> 8) == (second >> 8);
                strptr.set_lcp(bkt, depth + partial_equal);
                bkt += bkt_size[second];
                first = second;
                second = get_next_non_empty_bkt_index(second + 1);
            }
        }

        // premute in-place
        for (size_t i = 0, j; i < n - last_bkt_size; )
        {
            String perm = std::move(ss[ss.begin() + i]);
            std::uint16_t permch = charcache[i];
            while ((j = --bkt_index[permch]) > i)
            {
                std::swap(perm, ss[ss.begin() + j]);
                std::swap(permch, charcache[j]);
            }
            ss[ss.begin() + i] = std::move(perm);
            i += bkt_size[permch];
        }

        // will increment to 1 on first process, bkt 0 is not sorted further
        idx = 0;
        pos = base + bkt_size[0];
    }

    size_t get_next_non_empty_bkt_index(size_t start) {
        if (start >= RADIX)
            return RADIX;
        while (start < RADIX && bkt_size[start] == 0)
            ++start;
        return start;
    }
};

/*
 * In-place adaptive radix-sort with character caching which starts with 16-bit
 * radix sort and then switching to 8-bit for smaller string sets.
 */
template <typename StringPtr>
static inline void
radixsort_CI3(const StringPtr& strptr, std::uint16_t* charcache,
              size_t depth, size_t memory) {
    enum { RADIX = 0x10000 };

    typedef RadixStep_CI3<StringPtr> RadixStep;

    std::stack<RadixStep, std::vector<RadixStep> > radixstack;
    radixstack.emplace(strptr, /* base */ 0, depth, charcache);

    while (TLX_LIKELY(!radixstack.empty()))
    {
        while (TLX_LIKELY(radixstack.top().idx < RADIX - 1))
        {
            RadixStep& rs = radixstack.top();

            // process the bucket rs.idx
            size_t bkt_size = rs.bkt_size[++rs.idx];

            if (TLX_UNLIKELY(bkt_size <= 1)) {
                // done
                rs.pos += bkt_size;
            }
            else if (TLX_UNLIKELY((rs.idx & 0xFF) == 0)) {
                // zero-termination
                for (size_t i = rs.pos + 1; i < rs.pos + bkt_size; ++i)
                    strptr.set_lcp(i, depth + 2 * radixstack.size() - 1);

                rs.pos += bkt_size;
            }
            else if (TLX_UNLIKELY(bkt_size < g_inssort_threshold))
            {
                insertion_sort(
                    strptr.sub(rs.pos, bkt_size),
                    depth + 2 * radixstack.size(),
                    memory - sizeof(RadixStep) * radixstack.size());
                rs.pos += bkt_size;
            }
            else if (bkt_size < RADIX)
            {
                radixsort_CI2(
                    strptr.sub(rs.pos, bkt_size),
                    reinterpret_cast<std::uint8_t*>(charcache),
                    depth + 2 * radixstack.size(),
                    memory - sizeof(RadixStep) * radixstack.size());
                rs.pos += bkt_size;
            }
            else if (
                TLX_UNLIKELY(
                    memory != 0 &&
                    memory < sizeof(RadixStep) * (radixstack.size() + 1)))
            {
                multikey_quicksort(
                    strptr.sub(rs.pos, bkt_size),
                    depth + 2 * radixstack.size(),
                    memory - sizeof(RadixStep) * radixstack.size());
                rs.pos += bkt_size;
            }
            else
            {
                // have to increment first, as rs may be invalidated
                rs.pos += bkt_size;
                radixstack.emplace(
                    strptr.sub(rs.pos - bkt_size, bkt_size),
                    /* base */ rs.pos - bkt_size,
                    depth + 2 * radixstack.size(), charcache);
            }
        }
        radixstack.pop();
    }
}

template <typename StringPtr>
static inline void
radixsort_CI3(const StringPtr& strptr, size_t depth, size_t memory) {
    enum { RADIX = 0x10000 };
    typedef typename StringPtr::StringSet StringSet;

    if (strptr.size() < g_inssort_threshold)
        return insertion_sort(strptr, depth, memory);

    if (strptr.size() < RADIX)
        return radixsort_CI2(strptr, depth, memory);

    typedef RadixStep_CI3<StringPtr> RadixStep;

    // try to estimate the amount of memory used
    size_t memory_use =
        2 * sizeof(size_t) + sizeof(StringSet)
        + strptr.size() * sizeof(std::uint16_t);
    size_t memory_slack = 3 * sizeof(RadixStep);

    if (memory != 0 && memory < memory_use + memory_slack + 1)
        return radixsort_CI2(strptr, depth, memory);

    std::uint16_t* charcache = new std::uint16_t[strptr.size()];
    radixsort_CI3(strptr, charcache, depth, memory - memory_use);
    delete[] charcache;
}

/******************************************************************************/

} // namespace sort_strings_detail

//! \}

} // namespace tlx

#endif // !TLX_SORT_STRINGS_RADIX_SORT_HEADER

/******************************************************************************/
