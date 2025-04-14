/*******************************************************************************
 * tlx/sort/strings/sample_sort_tools.hpp
 *
 * Helpers for (Parallel) Super Scalar String Sample Sort
 *
 * See also Timo Bingmann, Andreas Eberle, and Peter Sanders. "Engineering
 * parallel string sorting." Algorithmica 77.1 (2017): 235-286.
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2013-2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_SORT_STRINGS_SAMPLE_SORT_TOOLS_HEADER
#define TLX_SORT_STRINGS_SAMPLE_SORT_TOOLS_HEADER

#include <tlx/sort/strings/string_set.hpp>

#include <tlx/define/attribute_fallthrough.hpp>
#include <tlx/die/core.hpp>
#include <tlx/logger/core.hpp>
#include <tlx/math/clz.hpp>
#include <tlx/math/ctz.hpp>
#include <tlx/string/hexdump.hpp>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>

namespace tlx {
namespace sort_strings_detail {

/******************************************************************************/

//! represent binary digits of large integer datatypes
template <typename Type>
static inline
std::string to_binary(Type v, const size_t width = (8 * sizeof(Type))) {
    std::string str(width, ' ');
    for (size_t i = 0; i < width; i++) {
        str[width - i - 1] = (v & 1) ? '1' : '0';
        v /= 2;
    }
    return str;
}

//! Class to transform in-order to level-order indexes in a perfect binary tree
template <size_t TreeBits>
struct PerfectTreeCalculations {
    static const bool debug = false;

    static const size_t treebits = TreeBits;
    static const size_t num_nodes = (1 << treebits) - 1;

    static inline unsigned int level_to_preorder(unsigned int id) {
        assert(id > 0);
        TLX_LOG << "index: " << id << " = " << to_binary(id);

        static const int bitmask = num_nodes;

        int hi = treebits - 32 + clz<std::uint32_t>(id);
        TLX_LOG << "high zero: " << hi;

        unsigned int bkt = ((id << (hi + 1)) & bitmask) | (1 << hi);

        TLX_LOG << "bkt: " << bkt << " = " << to_binary(bkt);
        return bkt;
    }

    static inline unsigned int pre_to_levelorder(unsigned int id) {
        assert(id > 0);
        TLX_LOG << "index: " << id << " = " << to_binary(id);

        static const int bitmask = num_nodes;

        int lo = ctz<std::uint32_t>(id) + 1;
        TLX_LOG << "low zero: " << lo;

        unsigned int bkt = ((id >> lo) & bitmask) | (1 << (treebits - lo));

        TLX_LOG << "bkt: " << bkt << " = " << to_binary(bkt);
        return bkt;
    }

    static inline void self_verify() {
        for (size_t i = 1; i <= num_nodes; ++i) {
            TLX_LOG << to_binary(i, treebits) << " -> ";

            size_t id = level_to_preorder(i);
            TLX_LOG << to_binary(id, treebits) << " -> ";

            id = pre_to_levelorder(id);
            TLX_LOG << to_binary(id, treebits);

            TLX_LOG << "";
            tlx_die_unequal(id, i);
        }
    }
};

static inline void perfect_tree_calculations_self_verify() {
    PerfectTreeCalculations<4>::self_verify();
    PerfectTreeCalculations<5>::self_verify();
    PerfectTreeCalculations<6>::self_verify();
    PerfectTreeCalculations<10>::self_verify();
    PerfectTreeCalculations<11>::self_verify();
    PerfectTreeCalculations<12>::self_verify();
    PerfectTreeCalculations<13>::self_verify();
    PerfectTreeCalculations<14>::self_verify();
    PerfectTreeCalculations<15>::self_verify();
    PerfectTreeCalculations<16>::self_verify();
}

/******************************************************************************/

//! Recursive TreeBuilder for full-descent and unrolled variants, constructs a
//! both a pre-order and level-order array of splitters and the corresponding
//! LCPs.
template <typename key_type, size_t num_splitters>
class SSTreeBuilderPreAndLevelOrder
{
    static const bool debug_splitter = false;

public:
    // build tree: splitter[num_splitters], splitter_tree[num_splitters + 1],
    // and splitter_lcp[num_splitters + 1].
    SSTreeBuilderPreAndLevelOrder(key_type splitter[num_splitters],
                                  key_type tree[num_splitters + 1],
                                  unsigned char splitter_lcp[num_splitters + 1],
                                  const key_type* samples, size_t samplesize)
        : splitter_(splitter), tree_(tree),
          lcp_iter_(splitter_lcp), samples_(samples) {
        key_type sentinel = 0;
        recurse(samples, samples + samplesize, 1, sentinel);

        assert(splitter_ == splitter + num_splitters);
        assert(lcp_iter_ == splitter_lcp + num_splitters);
        // overwrite sentinel lcp for first < everything bucket
        splitter_lcp[0] &= 0x80;
        // sentinel for > everything bucket
        splitter_lcp[num_splitters] = 0;
    }

    ptrdiff_t snum(const key_type* s) const {
        return static_cast<ptrdiff_t>(s - samples_);
    }

    key_type recurse(const key_type* lo, const key_type* hi,
                     unsigned int treeidx, key_type& rec_prevkey) {
        TLX_LOGC(debug_splitter)
            << "rec_buildtree(" << snum(lo) << "," << snum(hi)
            << ", treeidx=" << treeidx << ")";

        // pick middle element as splitter
        const key_type* mid = lo + static_cast<ptrdiff_t>(hi - lo) / 2;

        TLX_LOGC(debug_splitter)
            << "tree[" << treeidx << "] = samples[" << snum(mid) << "] = "
            << hexdump_type(*mid);

        key_type mykey = tree_[treeidx] = *mid;
#if 1
        const key_type* midlo = mid;
        while (lo < midlo && *(midlo - 1) == mykey) midlo--;

        const key_type* midhi = mid;
        while (midhi + 1 < hi && *midhi == mykey) midhi++;

        if (midhi - midlo > 1)
            TLX_LOG0 << "key range = [" << snum(midlo) << "," << snum(midhi) << ")";
#else
        const key_type* midlo = mid, * midhi = mid + 1;
#endif
        if (2 * treeidx < num_splitters)
        {
            key_type prevkey = recurse(lo, midlo, 2 * treeidx + 0, rec_prevkey);

            key_type xorSplit = prevkey ^ mykey;

            TLX_LOGC(debug_splitter)
                << "    lcp: " << hexdump_type(prevkey)
                << " XOR " << hexdump_type(mykey)
                << " = " << hexdump_type(xorSplit)
                << " - " << clz(xorSplit)
                << " bits = " << clz(xorSplit) / 8
                << " chars lcp";

            *splitter_++ = mykey;

            *lcp_iter_++ =
                (clz(xorSplit) / 8) |
                // marker for done splitters
                ((mykey & 0xFF) ? 0 : 0x80);

            return recurse(midhi, hi, 2 * treeidx + 1, mykey);
        }
        else
        {
            key_type xorSplit = rec_prevkey ^ mykey;

            TLX_LOGC(debug_splitter)
                << "    lcp: " << hexdump_type(rec_prevkey)
                << " XOR " << hexdump_type(mykey)
                << " = " << hexdump_type(xorSplit)
                << " - " << clz(xorSplit)
                << " bits = " << clz(xorSplit) / 8
                << " chars lcp";

            *splitter_++ = mykey;

            *lcp_iter_++ =
                (clz(xorSplit) / 8) |
                // marker for done splitters
                ((mykey & 0xFF) ? 0 : 0x80);

            return mykey;
        }
    }

private:
    key_type* splitter_;
    key_type* tree_;
    unsigned char* lcp_iter_;
    const key_type* samples_;
};

//! Recursive TreeBuilder for full-descent and unrolled variants, constructs
//! only a level-order binary tree of splitters
template <typename key_type, size_t num_splitters>
class SSTreeBuilderLevelOrder
{
    static const bool debug_splitter = false;

public:
    //! build tree, sizes: splitter_tree[num_splitters + 1] and
    SSTreeBuilderLevelOrder(key_type tree[num_splitters],
                            unsigned char splitter_lcp[num_splitters + 1],
                            const key_type* samples, size_t samplesize)
        : tree_(tree),
          lcp_iter_(splitter_lcp),
          samples_(samples) {
        key_type sentinel = 0;
        recurse(samples, samples + samplesize, 1, sentinel);

        assert(lcp_iter_ == splitter_lcp + num_splitters);
        // overwrite sentinel lcp for first < everything bucket
        splitter_lcp[0] &= 0x80;
        // sentinel for > everything bucket
        splitter_lcp[num_splitters] = 0;
    }

    ptrdiff_t snum(const key_type* s) const {
        return static_cast<ptrdiff_t>(s - samples_);
    }

    key_type recurse(const key_type* lo, const key_type* hi,
                     unsigned int treeidx, key_type& rec_prevkey) {
        TLX_LOGC(debug_splitter)
            << "rec_buildtree(" << snum(lo) << "," << snum(hi)
            << ", treeidx=" << treeidx << ")";

        // pick middle element as splitter
        const key_type* mid = lo + static_cast<ptrdiff_t>(hi - lo) / 2;

        TLX_LOGC(debug_splitter)
            << "tree[" << treeidx << "] = samples[" << snum(mid) << "] = "
            << hexdump_type(*mid);

        key_type mykey = tree_[treeidx] = *mid;
#if 1
        const key_type* midlo = mid;
        while (lo < midlo && *(midlo - 1) == mykey) midlo--;

        const key_type* midhi = mid;
        while (midhi + 1 < hi && *midhi == mykey) midhi++;

        if (midhi - midlo > 1)
            TLX_LOG0 << "key range = [" << snum(midlo) << "," << snum(midhi) << ")";
#else
        const key_type* midlo = mid, * midhi = mid + 1;
#endif
        if (2 * treeidx < num_splitters)
        {
            key_type prevkey = recurse(lo, midlo, 2 * treeidx + 0, rec_prevkey);

            key_type xorSplit = prevkey ^ mykey;

            TLX_LOGC(debug_splitter)
                << "    lcp: " << hexdump_type(prevkey)
                << " XOR " << hexdump_type(mykey)
                << " = " << hexdump_type(xorSplit)
                << " - " << clz(xorSplit)
                << " bits = " << clz(xorSplit) / 8
                << " chars lcp";

            *lcp_iter_++ =
                (clz(xorSplit) / 8) |
                // marker for done splitters
                ((mykey & 0xFF) ? 0 : 0x80);

            return recurse(midhi, hi, 2 * treeidx + 1, mykey);
        }
        else
        {
            key_type xorSplit = rec_prevkey ^ mykey;

            TLX_LOGC(debug_splitter)
                << "    lcp: " << hexdump_type(rec_prevkey)
                << " XOR " << hexdump_type(mykey)
                << " = " << hexdump_type(xorSplit)
                << " - " << clz(xorSplit)
                << " bits = " << clz(xorSplit) / 8
                << " chars lcp";

            *lcp_iter_++ =
                (clz(xorSplit) / 8) |
                // marker for done splitters
                ((mykey & 0xFF) ? 0 : 0x80);

            return mykey;
        }
    }

private:
    key_type* tree_;
    unsigned char* lcp_iter_;
    const key_type* samples_;
};

/******************************************************************************/

//! Sample Sort Classification Tree Unrolled and Interleaved
template <typename key_type, size_t TreeBits, size_t Rollout = 4>
class SSClassifyTreeUnrollInterleave
{
public:
    static const size_t treebits = TreeBits;
    static const size_t num_splitters = (1 << treebits) - 1;

    //! build tree and splitter array from sample
    void build(key_type* samples, size_t samplesize,
               unsigned char* splitter_lcp) {
        SSTreeBuilderPreAndLevelOrder<key_type, num_splitters>(
            splitter_, splitter_tree_, splitter_lcp,
            samples, samplesize);
    }

    //! binary search on splitter array for bucket number
    unsigned int find_bkt(const key_type& key) const {
        // binary tree traversal without left branch

        unsigned int i = 1;
        while (i <= num_splitters) {
            // in gcc-4.6.3 this produces a SETA, LEA sequence
            i = 2 * i + (key <= splitter_tree_[i] ? 0 : 1);
        }
        i -= num_splitters + 1;

        size_t b = i * 2;                                      // < bucket
        if (i < num_splitters && splitter_[i] == key) b += 1;  // equal bucket

        return b;
    }

    // in gcc-4.6.3 this produces a SETA, LEA sequence
#define TLX_CLASSIFY_TREE_STEP                                      \
    for (size_t u = 0; u < Rollout; ++u) {                          \
        i[u] = 2 * i[u] + (key[u] <= splitter_tree_[i[u]] ? 0 : 1); \
    }                                                               \
    TLX_ATTRIBUTE_FALLTHROUGH;

    //! search in splitter tree for bucket number, unrolled for Rollout keys at
    //! once.
    void find_bkt_unroll(
        const key_type key[Rollout], std::uint16_t obkt[Rollout]) const {
        // binary tree traversal without left branch

        unsigned int i[Rollout];
        std::fill(i, i + Rollout, 1u);

        switch (treebits)
        {
        default:
            abort();
        case 15:
            TLX_CLASSIFY_TREE_STEP;
        case 14:
            TLX_CLASSIFY_TREE_STEP;
        case 13:
            TLX_CLASSIFY_TREE_STEP;
        case 12:
            TLX_CLASSIFY_TREE_STEP;
        case 11:
            TLX_CLASSIFY_TREE_STEP;

        case 10:
            TLX_CLASSIFY_TREE_STEP;
        case 9:
            TLX_CLASSIFY_TREE_STEP;
        case 8:
            TLX_CLASSIFY_TREE_STEP;
        case 7:
            TLX_CLASSIFY_TREE_STEP;
        case 6:
            TLX_CLASSIFY_TREE_STEP;

        case 5:
            TLX_CLASSIFY_TREE_STEP;
        case 4:
            TLX_CLASSIFY_TREE_STEP;
        case 3:
            TLX_CLASSIFY_TREE_STEP;
        case 2:
            TLX_CLASSIFY_TREE_STEP;
        case 1:
            TLX_CLASSIFY_TREE_STEP;
        }

        for (size_t u = 0; u < Rollout; ++u)
            i[u] -= num_splitters + 1;

        for (size_t u = 0; u < Rollout; ++u) {
            // < bucket
            obkt[u] = i[u] * 2;
        }

        for (size_t u = 0; u < Rollout; ++u) {
            // equal bucket
            if (i[u] < num_splitters && splitter_[i[u]] == key[u])
                obkt[u] += 1;
        }
    }

#undef TLX_CLASSIFY_TREE_STEP

    //! classify all strings in area by walking tree and saving bucket id
    template <typename StringSet>
    // __attribute__ ((optimize("unroll-all-loops")))
    void classify(
        const StringSet& strset,
        typename StringSet::Iterator begin, typename StringSet::Iterator end,
        std::uint16_t* bktout, size_t depth) const {
        while (begin != end)
        {
            if (begin + Rollout <= end)
            {
                key_type key[Rollout];
                for (size_t u = 0; u < Rollout; ++u)
                    key[u] = get_key<key_type>(strset, begin[u], depth);

                find_bkt_unroll(key, bktout);

                begin += Rollout, bktout += Rollout;
            }
            else
            {
                key_type key = get_key<key_type>(strset, *begin++, depth);
                *bktout++ = this->find_bkt(key);
            }
        }
    }

    //! return a splitter
    key_type get_splitter(unsigned int i) const
    { return splitter_[i]; }

private:
    key_type splitter_[num_splitters];
    key_type splitter_tree_[num_splitters + 1];
};

/******************************************************************************/

//! Sample Sort Classification Tree Unrolled with Equal Comparisons
template <typename key_type, size_t TreeBits>
class SSClassifyEqualUnroll
{
public:
    static const size_t treebits = TreeBits;
    static const size_t num_splitters = (1 << treebits) - 1;

    //! build tree and splitter array from sample
    void build(key_type* samples, size_t samplesize, unsigned char* splitter_lcp) {
        SSTreeBuilderLevelOrder<key_type, num_splitters>(
            splitter_tree_, splitter_lcp, samples, samplesize);
    }

#define TLX_CLASSIFY_TREE_STEP                                               \
    if (TLX_UNLIKELY(key == splitter_tree_[i])) {                            \
        return                                                               \
            2 * PerfectTreeCalculations<treebits>::level_to_preorder(i) - 1; \
    }                                                                        \
    i = 2 * i + (key < splitter_tree_[i] ? 0 : 1);                           \
    TLX_ATTRIBUTE_FALLTHROUGH;

    //! binary search on splitter array for bucket number
    unsigned int find_bkt(const key_type& key) const {
        // binary tree traversal without left branch

        unsigned int i = 1;

        switch (treebits)
        {
        default:
            abort();
        case 15:
            TLX_CLASSIFY_TREE_STEP;
        case 14:
            TLX_CLASSIFY_TREE_STEP;
        case 13:
            TLX_CLASSIFY_TREE_STEP;
        case 12:
            TLX_CLASSIFY_TREE_STEP;
        case 11:
            TLX_CLASSIFY_TREE_STEP;

        case 10:
            TLX_CLASSIFY_TREE_STEP;
        case 9:
            TLX_CLASSIFY_TREE_STEP;
        case 8:
            TLX_CLASSIFY_TREE_STEP;
        case 7:
            TLX_CLASSIFY_TREE_STEP;
        case 6:
            TLX_CLASSIFY_TREE_STEP;

        case 5:
            TLX_CLASSIFY_TREE_STEP;
        case 4:
            TLX_CLASSIFY_TREE_STEP;
        case 3:
            TLX_CLASSIFY_TREE_STEP;
        case 2:
            TLX_CLASSIFY_TREE_STEP;
        case 1:
            TLX_CLASSIFY_TREE_STEP;
        }

        i -= num_splitters + 1;
        return 2 * i; // < or > bucket
    }

#undef TLX_CLASSIFY_TREE_STEP

    //! classify all strings in area by walking tree and saving bucket id
    template <typename StringSet>
    void classify(
        const StringSet& strset,
        typename StringSet::Iterator begin, typename StringSet::Iterator end,
        std::uint16_t* bktout, size_t depth) const {
        while (begin != end)
        {
            key_type key = get_key<key_type>(strset, *begin++, depth);
            *bktout++ = find_bkt(key);
        }
    }

    //! return a splitter
    key_type get_splitter(unsigned int i) const {
        return splitter_tree_[
            PerfectTreeCalculations<treebits>::pre_to_levelorder(i)];
    }

private:
    key_type splitter_tree_[num_splitters + 1];
};

/******************************************************************************/

//! Sample Sort Classification Tree Unrolled, Interleaved, and with Perfect Tree
//! Index Calculations
template <typename key_type, size_t TreeBits, unsigned Rollout = 4>
class SSClassifyTreeCalcUnrollInterleave
{
public:
    static const size_t treebits = TreeBits;
    static const size_t num_splitters = (1 << treebits) - 1;

    //! build tree and splitter array from sample
    void build(key_type* samples, size_t samplesize,
               unsigned char* splitter_lcp) {
        SSTreeBuilderLevelOrder<key_type, num_splitters>(
            splitter_tree_, splitter_lcp, samples, samplesize);
    }

    //! binary search on splitter array for bucket number
    unsigned int find_bkt(const key_type& key) const {
        // binary tree traversal without left branch

        unsigned int i = 1;

        while (i <= num_splitters) {
            // in gcc-4.6.3 this produces a SETA, LEA sequence
            i = 2 * i + (key <= splitter_tree_[i] ? 0 : 1);
        }

        i -= num_splitters + 1;

        // < bucket
        size_t b = i * 2;
        if (i < num_splitters && get_splitter(i) == key) {
            // equal bucket
            b += 1;
        }

        return b;
    }

    // in gcc-4.6.3 this produces a SETA, LEA sequence
#define TLX_CLASSIFY_TREE_STEP                                      \
    for (size_t u = 0; u < Rollout; ++u) {                          \
        i[u] = 2 * i[u] + (key[u] <= splitter_tree_[i[u]] ? 0 : 1); \
    }                                                               \
    TLX_ATTRIBUTE_FALLTHROUGH;

    //! search in splitter tree for bucket number, unrolled for Rollout keys at
    //! once.
    void find_bkt_unroll(
        const key_type key[Rollout], std::uint16_t obkt[Rollout]) const {
        // binary tree traversal without left branch

        unsigned int i[Rollout];
        std::fill(i + 0, i + Rollout, 1);

        switch (treebits)
        {
        default:
            abort();

        case 15:
            TLX_CLASSIFY_TREE_STEP;
        case 14:
            TLX_CLASSIFY_TREE_STEP;
        case 13:
            TLX_CLASSIFY_TREE_STEP;
        case 12:
            TLX_CLASSIFY_TREE_STEP;
        case 11:
            TLX_CLASSIFY_TREE_STEP;

        case 10:
            TLX_CLASSIFY_TREE_STEP;
        case 9:
            TLX_CLASSIFY_TREE_STEP;
        case 8:
            TLX_CLASSIFY_TREE_STEP;
        case 7:
            TLX_CLASSIFY_TREE_STEP;
        case 6:
            TLX_CLASSIFY_TREE_STEP;

        case 5:
            TLX_CLASSIFY_TREE_STEP;
        case 4:
            TLX_CLASSIFY_TREE_STEP;
        case 3:
            TLX_CLASSIFY_TREE_STEP;
        case 2:
            TLX_CLASSIFY_TREE_STEP;
        case 1:
            TLX_CLASSIFY_TREE_STEP;
        }

        for (unsigned u = 0; u < Rollout; ++u)
            i[u] -= num_splitters + 1;

        for (unsigned u = 0; u < Rollout; ++u) {
            // < bucket
            obkt[u] = i[u] * 2;
        }

        for (unsigned u = 0; u < Rollout; ++u) {
            // equal bucket
            if (i[u] < num_splitters && get_splitter(i[u]) == key[u])
                obkt[u] += 1;
        }
    }

#undef TLX_CLASSIFY_TREE_STEP

    //! classify all strings in area by walking tree and saving bucket id
    template <typename StringSet>
    // __attribute__ ((optimize("unroll-all-loops")))
    void classify(
        const StringSet& strset,
        typename StringSet::Iterator begin, typename StringSet::Iterator end,
        std::uint16_t* bktout, size_t depth) const {
        while (begin != end)
        {
            if (begin + Rollout <= end)
            {
                key_type key[Rollout];
                for (size_t u = 0; u < Rollout; ++u)
                    key[u] = get_key<key_type>(strset, begin[u], depth);

                find_bkt_unroll(key, bktout);

                begin += Rollout, bktout += Rollout;
            }
            else
            {
                key_type key = get_key<key_type>(strset, *begin++, depth);
                *bktout++ = this->find_bkt(key);
            }
        }
    }

    //! return a splitter
    key_type get_splitter(unsigned int i) const {
        return splitter_tree_[
            PerfectTreeCalculations<treebits>::pre_to_levelorder(i + 1)];
    }

private:
    key_type splitter_tree_[num_splitters + 1];
};

/******************************************************************************/

} // namespace sort_strings_detail
} // namespace tlx

#endif // !TLX_SORT_STRINGS_SAMPLE_SORT_TOOLS_HEADER

/******************************************************************************/
