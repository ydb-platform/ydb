/*******************************************************************************
 * tlx/sort/strings/parallel_sample_sort.hpp
 *
 * Parallel Super Scalar String Sample Sort (pS5)
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

#ifndef TLX_SORT_STRINGS_PARALLEL_SAMPLE_SORT_HEADER
#define TLX_SORT_STRINGS_PARALLEL_SAMPLE_SORT_HEADER

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <random>
#include <vector>

#include <tlx/sort/strings/insertion_sort.hpp>
#include <tlx/sort/strings/sample_sort_tools.hpp>
#include <tlx/sort/strings/string_ptr.hpp>

#include <tlx/logger/core.hpp>
#include <tlx/math/clz.hpp>
#include <tlx/math/ctz.hpp>
#include <tlx/meta/enable_if.hpp>
#include <tlx/multi_timer.hpp>
#include <tlx/simple_vector.hpp>
#include <tlx/thread_pool.hpp>
#include <tlx/unused.hpp>

namespace tlx {
namespace sort_strings_detail {

class PS5SortStep;

/******************************************************************************/
//! Parallel Super Scalar String Sample Sort Parameter Struct

class PS5ParametersDefault
{
public:
    static const bool debug_steps = false;
    static const bool debug_jobs = false;

    static const bool debug_bucket_size = false;
    static const bool debug_recursion = false;
    static const bool debug_lcp = false;

    static const bool debug_result = false;

    //! enable/disable various sorting levels
    static const bool enable_parallel_sample_sort = true;
    static const bool enable_sequential_sample_sort = true;
    static const bool enable_sequential_mkqs = true;

    //! terminate sort after first parallel sample sort step
    static const bool use_only_first_sortstep = false;

    //! enable work freeing
    static const bool enable_work_sharing = true;

    //! whether the base sequential_threshold() on the remaining unsorted string
    //! set or on the whole string set.
    static const bool enable_rest_size = false;

    //! key type for sample sort: 32-bit or 64-bit
    typedef size_t key_type;

    //! depth of classification tree used in sample sorts
    static const unsigned TreeBits = 10;

    //! classification tree variant for sample sorts
    using Classify = SSClassifyTreeCalcUnrollInterleave<key_type, TreeBits>;

    //! threshold to run sequential small sorts
    static const size_t smallsort_threshold = 1024 * 1024;
    //! threshold to switch to insertion sort
    static const size_t inssort_threshold = 32;
};

/******************************************************************************/
//! Parallel Super Scalar String Sample Sort Context

template <typename Parameters>
class PS5Context : public Parameters
{
public:
    //! total size of input
    size_t total_size;

    //! number of remaining strings to sort
    std::atomic<size_t> rest_size;

    //! counters
    std::atomic<size_t> para_ss_steps, sequ_ss_steps, base_sort_steps;

    //! timers for individual sorting steps
    MultiTimer mtimer;

    //! number of threads overall
    size_t num_threads;

    //! thread pool
    ThreadPool threads_;

    //! context constructor
    PS5Context(size_t _thread_num)
        : para_ss_steps(0), sequ_ss_steps(0), base_sort_steps(0),
          num_threads(_thread_num),
          threads_(_thread_num)
    { }

    //! enqueue a new job in the thread pool
    template <typename StringPtr>
    void enqueue(PS5SortStep* sstep, const StringPtr& strptr, size_t depth);

    //! return sequential sorting threshold
    size_t sequential_threshold() {
        size_t threshold = this->smallsort_threshold;
        if (this->enable_rest_size) {
            return std::max(threshold, rest_size / num_threads);
        }
        else {
            return std::max(threshold, total_size / num_threads);
        }
    }

    //! decrement number of unordered strings
    void donesize(size_t n) {
        if (this->enable_rest_size)
            rest_size -= n;
    }
};

/******************************************************************************/
//! LCP calculation of Splitter Strings

template <typename KeyType>
static inline unsigned char
lcpKeyType(const KeyType& a, const KeyType& b) {
    // XOR both values and count the number of zero bytes
    return clz(a ^ b) / 8;
}

template <typename KeyType>
static inline unsigned char
lcpKeyDepth(const KeyType& a) {
    // count number of non-zero bytes
    return sizeof(KeyType) - (ctz(a) / 8);
}

//! return the d-th character in the (swapped) key
template <typename KeyType>
static inline unsigned char
getCharAtDepth(const KeyType& a, unsigned char d) {
    return static_cast<unsigned char>(a >> (8 * (sizeof(KeyType) - 1 - d)));
}

/******************************************************************************/
//! PS5SortStep Top-Level Class to Keep Track of Substeps

class PS5SortStep
{
private:
    //! Number of substeps still running
    std::atomic<size_t> substep_working_;

    //! Pure virtual function called by substep when all substeps are done.
    virtual void substep_all_done() = 0;

protected:
    PS5SortStep() : substep_working_(0) { }

    virtual ~PS5SortStep() {
        assert(substep_working_ == 0);
    }

    //! Register new substep
    void substep_add() {
        ++substep_working_;
    }

public:
    //! Notify superstep that the currently substep is done.
    void substep_notify_done() {
        assert(substep_working_ > 0);
        if (--substep_working_ == 0)
            substep_all_done();
    }
};

/******************************************************************************/
//! LCP Calculation for Finished Sample Sort Steps

template <size_t bktnum, typename Context, typename Classify,
          typename StringPtr, typename BktSizeType>
void ps5_sample_sort_lcp(const Context& ctx, const Classify& classifier,
                         const StringPtr& strptr, size_t depth,
                         const BktSizeType* bkt) {
    assert(!strptr.flipped());

    const typename StringPtr::StringSet& strset = strptr.active();
    typedef typename Context::key_type key_type;

    size_t b = 0;         // current bucket number
    key_type prevkey = 0; // previous key

    // the following while loops only check b < bktnum when b is odd,
    // because bktnum is always odd. We need a goto to jump into the loop,
    // as b == 0 start even.
    goto even_first;

    // find first non-empty bucket
    while (b < bktnum)
    {
        // odd bucket: = bkt
        if (bkt[b] != bkt[b + 1]) {
            prevkey = classifier.get_splitter(b / 2);
            assert(prevkey == get_key_at<key_type>(strset, bkt[b + 1] - 1, depth));
            break;
        }
        ++b;
even_first:
        // even bucket: <, << or > bkt
        if (bkt[b] != bkt[b + 1]) {
            prevkey = get_key_at<key_type>(strset, bkt[b + 1] - 1, depth);
            break;
        }
        ++b;
    }
    ++b;

    // goto depends on whether the first non-empty bucket was odd or
    // even. the while loop below encodes this in the program counter.
    if (b < bktnum && b % 2 == 0) goto even_bucket;

    // find next non-empty bucket
    while (b < bktnum)
    {
        // odd bucket: = bkt
        if (bkt[b] != bkt[b + 1]) {
            key_type thiskey = classifier.get_splitter(b / 2);
            assert(thiskey == get_key_at<key_type>(strset, bkt[b], depth));

            int rlcp = lcpKeyType(prevkey, thiskey);
            strptr.set_lcp(bkt[b], depth + rlcp);
            // strptr.set_cache(bkt[b], getCharAtDepth(thiskey, rlcp));

            TLX_LOGC(ctx.debug_lcp)
                << "LCP at odd-bucket " << b
                << " [" << bkt[b] << "," << bkt[b + 1] << ")"
                << " is " << depth + rlcp;

            prevkey = thiskey;
            assert(prevkey == get_key_at<key_type>(strset, bkt[b + 1] - 1, depth));
        }
        ++b;
even_bucket:
        // even bucket: <, << or > bkt
        if (bkt[b] != bkt[b + 1]) {
            key_type thiskey = get_key_at<key_type>(strset, bkt[b], depth);

            int rlcp = lcpKeyType(prevkey, thiskey);
            strptr.set_lcp(bkt[b], depth + rlcp);
            // strptr.set_cache(bkt[b], getCharAtDepth(thiskey, rlcp));

            TLX_LOGC(ctx.debug_lcp)
                << "LCP at even-bucket " << b
                << " [" << bkt[b] << "," << bkt[b + 1] << ")"
                << " is " << depth + rlcp;

            prevkey = get_key_at<key_type>(strset, bkt[b + 1] - 1, depth);
        }
        ++b;
    }
}

/******************************************************************************/
//! SampleSort: Non-Recursive In-Place Sequential Sample Sort for Small Sorts

template <typename Context, typename StringPtr, typename BktSizeType>
class PS5SmallsortJob : public PS5SortStep
{
public:
    Context& ctx_;

    //! parent sort step
    PS5SortStep* pstep_;

    StringPtr strptr_;
    size_t depth_;
    MultiTimer mtimer_;

    typedef typename Context::key_type key_type;
    typedef typename StringPtr::StringSet StringSet;
    typedef BktSizeType bktsize_type;

    PS5SmallsortJob(Context& ctx, PS5SortStep* pstep,
                    const StringPtr& strptr, size_t depth)
        : ctx_(ctx), pstep_(pstep), strptr_(strptr), depth_(depth) {
        TLX_LOGC(ctx_.debug_steps)
            << "enqueue depth=" << depth_
            << " size=" << strptr_.size() << " flip=" << strptr_.flipped();
    }

    ~PS5SmallsortJob() {
        mtimer_.stop();
        ctx_.mtimer.add(mtimer_);
    }

    simple_vector<std::uint8_t> bktcache_;
    size_t bktcache_size_ = 0;

    void run() {
        mtimer_.start("sequ_ss");

        size_t n = strptr_.size();

        TLX_LOGC(ctx_.debug_jobs)
            << "Process PS5SmallsortJob " << this << " of size " << n;

        // create anonymous wrapper job
        this->substep_add();

        if (ctx_.enable_sequential_sample_sort && n >= ctx_.smallsort_threshold)
        {
            bktcache_.resize(n * sizeof(std::uint16_t));
            sort_sample_sort(strptr_, depth_);
        }
        else
        {
            mtimer_.start("mkqs");
            sort_mkqs_cache(strptr_, depth_);
        }

        // finish wrapper job, handler delete's this
        this->substep_notify_done();
    }

    /*------------------------------------------------------------------------*/
    //! Stack of Recursive Sample Sort Steps

    class SeqSampleSortStep
    {
    public:
        StringPtr strptr_;
        size_t idx_;
        size_t depth_;

        using StringSet = typename StringPtr::StringSet;
        using bktsize_type = BktSizeType;

        typename Context::Classify classifier;

        static const size_t num_splitters = Context::Classify::num_splitters;
        static const size_t bktnum = 2 * num_splitters + 1;

        unsigned char splitter_lcp[num_splitters + 1];
        bktsize_type bkt[bktnum + 1];

        SeqSampleSortStep(Context& ctx, const StringPtr& strptr, size_t depth,
                          std::uint16_t* bktcache)
            : strptr_(strptr), idx_(0), depth_(depth) {
            size_t n = strptr_.size();

            // step 1: select splitters with oversampling

            const size_t oversample_factor = 2;
            const size_t sample_size = oversample_factor * num_splitters;

            simple_vector<key_type> samples(sample_size);

            const StringSet& strset = strptr_.active();

            std::minstd_rand rng(reinterpret_cast<uintptr_t>(samples.data()));

            for (size_t i = 0; i < sample_size; ++i)
                samples[i] = get_key_at<key_type>(strset, rng() % n, depth_);

            std::sort(samples.begin(), samples.end());

            classifier.build(samples.data(), sample_size, splitter_lcp);

            // step 2: classify all strings

            classifier.classify(
                strset, strset.begin(), strset.end(), bktcache, depth_);

            // step 2.5: count bucket sizes

            bktsize_type bktsize[bktnum];
            memset(bktsize, 0, bktnum * sizeof(bktsize_type));

            for (size_t si = 0; si < n; ++si)
                ++bktsize[bktcache[si]];

            // step 3: inclusive prefix sum

            bkt[0] = bktsize[0];
            for (unsigned int i = 1; i < bktnum; ++i) {
                bkt[i] = bkt[i - 1] + bktsize[i];
            }
            assert(bkt[bktnum - 1] == n);
            bkt[bktnum] = n;

            // step 4: premute out-of-place

            const StringSet& strB = strptr_.active();
            // get alternative shadow pointer array
            const StringSet& sorted = strptr_.shadow();
            typename StringSet::Iterator sbegin = sorted.begin();

            for (typename StringSet::Iterator str = strB.begin();
                 str != strB.end(); ++str, ++bktcache)
                *(sbegin + --bkt[*bktcache]) = std::move(*str);

            // bkt is afterwards the exclusive prefix sum of bktsize

            // statistics

            ++ctx.sequ_ss_steps;
        }

        void calculate_lcp(Context& ctx) {
            TLX_LOGC(ctx.debug_lcp) << "Calculate LCP after sample sort step";
            if (strptr_.with_lcp) {
                ps5_sample_sort_lcp<bktnum>(ctx, classifier, strptr_, depth_, bkt);
            }
        }
    };

    size_t ss_front_ = 0;
    std::vector<SeqSampleSortStep> ss_stack_;

    void sort_sample_sort(const StringPtr& strptr, size_t depth) {
        typedef SeqSampleSortStep Step;

        assert(ss_front_ == 0);
        assert(ss_stack_.size() == 0);

        std::uint16_t* bktcache = reinterpret_cast<std::uint16_t*>(bktcache_.data());

        // sort first level
        ss_stack_.emplace_back(ctx_, strptr, depth, bktcache);

        // step 5: "recursion"

        while (ss_stack_.size() > ss_front_)
        {
            Step& s = ss_stack_.back();
            size_t i = s.idx_++; // process the bucket s.idx_

            if (i < Step::bktnum)
            {
                size_t bktsize = s.bkt[i + 1] - s.bkt[i];

                StringPtr sp = s.strptr_.flip(s.bkt[i], bktsize);

                // i is even -> bkt[i] is less-than bucket
                if (i % 2 == 0)
                {
                    if (bktsize == 0) {
                        // empty bucket
                    }
                    else if (bktsize < ctx_.smallsort_threshold)
                    {
                        assert(i / 2 <= Step::num_splitters);
                        if (i == Step::bktnum - 1)
                            TLX_LOGC(ctx_.debug_recursion)
                                << "Recurse[" << s.depth_ << "]: > bkt "
                                << i << " size " << bktsize << " no lcp";
                        else
                            TLX_LOGC(ctx_.debug_recursion)
                                << "Recurse[" << s.depth_ << "]: < bkt "
                                << i << " size " << bktsize << " lcp "
                                << int(s.splitter_lcp[i / 2] & 0x7F);

                        ScopedMultiTimerSwitch sts_inssort(mtimer_, "mkqs");
                        sort_mkqs_cache(
                            sp, s.depth_ + (s.splitter_lcp[i / 2] & 0x7F));
                    }
                    else
                    {
                        if (i == Step::bktnum - 1)
                            TLX_LOGC(ctx_.debug_recursion)
                                << "Recurse[" << s.depth_ << "]: > bkt "
                                << i << " size " << bktsize << " no lcp";
                        else
                            TLX_LOGC(ctx_.debug_recursion)
                                << "Recurse[" << s.depth_ << "]: < bkt "
                                << i << " size " << bktsize << " lcp "
                                << int(s.splitter_lcp[i / 2] & 0x7F);

                        ss_stack_.emplace_back(
                            ctx_, sp, s.depth_ + (s.splitter_lcp[i / 2] & 0x7F), bktcache);
                    }
                }
                // i is odd -> bkt[i] is equal bucket
                else
                {
                    if (bktsize == 0) {
                        // empty bucket
                    }
                    else if (s.splitter_lcp[i / 2] & 0x80) {
                        // equal-bucket has nullptr-terminated key, done.
                        TLX_LOGC(ctx_.debug_recursion)
                            << "Recurse[" << s.depth_ << "]: = bkt "
                            << i << " size " << bktsize << " is done!";
                        StringPtr spb = sp.copy_back();

                        if (sp.with_lcp) {
                            spb.fill_lcp(
                                s.depth_ + lcpKeyDepth(s.classifier.get_splitter(i / 2)));
                        }
                        ctx_.donesize(bktsize);
                    }
                    else if (bktsize < ctx_.smallsort_threshold)
                    {
                        TLX_LOGC(ctx_.debug_recursion)
                            << "Recurse[" << s.depth_ << "]: = bkt "
                            << i << " size " << bktsize << " lcp keydepth!";

                        ScopedMultiTimerSwitch sts_inssort(mtimer_, "mkqs");
                        sort_mkqs_cache(sp, s.depth_ + sizeof(key_type));
                    }
                    else
                    {
                        TLX_LOGC(ctx_.debug_recursion)
                            << "Recurse[" << s.depth_ << "]: = bkt "
                            << i << " size " << bktsize << " lcp keydepth!";

                        ss_stack_.emplace_back(
                            ctx_, sp, s.depth_ + sizeof(key_type), bktcache);
                    }
                }
            }
            else
            {
                // finished sort
                assert(ss_stack_.size() > ss_front_);

                // after full sort: calculate LCPs at this level
                ss_stack_.back().calculate_lcp(ctx_);

                ss_stack_.pop_back();
            }

            if (ctx_.enable_work_sharing && ctx_.threads_.has_idle()) {
                sample_sort_free_work();
            }
        }
    }

    void sample_sort_free_work() {
        assert(ss_stack_.size() >= ss_front_);

        if (ss_stack_.size() == ss_front_) {
            // ss_stack_ is empty, check other stack
            return mkqs_free_work();
        }

        // convert top level of stack into independent jobs
        TLX_LOGC(ctx_.debug_jobs)
            << "Freeing top level of PS5SmallsortJob's sample_sort stack";

        typedef SeqSampleSortStep Step;
        Step& s = ss_stack_[ss_front_];

        while (s.idx_ < Step::bktnum)
        {
            size_t i = s.idx_++; // process the bucket s.idx_

            size_t bktsize = s.bkt[i + 1] - s.bkt[i];

            StringPtr sp = s.strptr_.flip(s.bkt[i], bktsize);

            // i is even -> bkt[i] is less-than bucket
            if (i % 2 == 0)
            {
                if (bktsize == 0) {
                    // empty bucket
                }
                else
                {
                    if (i == Step::bktnum - 1)
                        TLX_LOGC(ctx_.debug_recursion)
                            << "Recurse[" << s.depth_ << "]: > bkt "
                            << i << " size " << bktsize << " no lcp";
                    else
                        TLX_LOGC(ctx_.debug_recursion)
                            << "Recurse[" << s.depth_ << "]: < bkt "
                            << i << " size " << bktsize << " lcp "
                            << int(s.splitter_lcp[i / 2] & 0x7F);

                    this->substep_add();
                    ctx_.enqueue(this, sp,
                                 s.depth_ + (s.splitter_lcp[i / 2] & 0x7F));
                }
            }
            // i is odd -> bkt[i] is equal bucket
            else
            {
                if (bktsize == 0) {
                    // empty bucket
                }
                else if (s.splitter_lcp[i / 2] & 0x80) {
                    // equal-bucket has nullptr-terminated key, done.
                    TLX_LOGC(ctx_.debug_recursion)
                        << "Recurse[" << s.depth_ << "]: = bkt "
                        << i << " size " << bktsize << " is done!";
                    StringPtr spb = sp.copy_back();

                    if (sp.with_lcp) {
                        spb.fill_lcp(s.depth_ + lcpKeyDepth(
                                         s.classifier.get_splitter(i / 2)));
                    }
                    ctx_.donesize(bktsize);
                }
                else
                {
                    TLX_LOGC(ctx_.debug_recursion)
                        << "Recurse[" << s.depth_ << "]: = bkt "
                        << i << " size " << bktsize << " lcp keydepth!";

                    this->substep_add();
                    ctx_.enqueue(this, sp, s.depth_ + sizeof(key_type));
                }
            }
        }

        // shorten the current stack
        ++ss_front_;
    }

    /*------------------------------------------------------------------------*/
    //! Stack of Recursive MKQS Steps

    static inline int cmp(const key_type& a, const key_type& b) {
        return (a > b) ? 1 : (a < b) ? -1 : 0;
    }

    template <typename Type>
    static inline size_t
    med3(Type* A, size_t i, size_t j, size_t k) {
        if (A[i] == A[j]) return i;
        if (A[k] == A[i] || A[k] == A[j]) return k;
        if (A[i] < A[j]) {
            if (A[j] < A[k]) return j;
            if (A[i] < A[k]) return k;
            return i;
        }
        else {
            if (A[j] > A[k]) return j;
            if (A[i] < A[k]) return i;
            return k;
        }
    }

    //! Insertion sort the strings only based on the cached characters.
    static inline void
    insertion_sort_cache_block(const StringPtr& strptr, key_type* cache) {
        const StringSet& strings = strptr.active();
        size_t n = strptr.size();
        size_t pi, pj;
        for (pi = 1; --n > 0; ++pi) {
            typename StringSet::String tmps = std::move(strings.at(pi));
            key_type tmpc = cache[pi];
            for (pj = pi; pj > 0; --pj) {
                if (cache[pj - 1] <= tmpc)
                    break;
                strings.at(pj) = std::move(strings.at(pj - 1));
                cache[pj] = cache[pj - 1];
            }
            strings.at(pj) = std::move(tmps);
            cache[pj] = tmpc;
        }
    }

    //! Insertion sort, but use cached characters if possible.
    template <bool CacheDirty>
    static inline void
    insertion_sort_cache(const StringPtr& _strptr, key_type* cache, size_t depth) {
        StringPtr strptr = _strptr.copy_back();

        if (strptr.size() <= 1) return;
        if (CacheDirty)
            return insertion_sort(strptr, depth, /* memory */ 0);

        insertion_sort_cache_block(strptr, cache);

        size_t start = 0, bktsize = 1;
        for (size_t i = 0; i < strptr.size() - 1; ++i) {
            // group areas with equal cache values
            if (cache[i] == cache[i + 1]) {
                ++bktsize;
                continue;
            }
            // calculate LCP between group areas
            if (start != 0 && strptr.with_lcp) {
                int rlcp = lcpKeyType(cache[start - 1], cache[start]);
                strptr.set_lcp(start, depth + rlcp);
                // strptr.set_cache(start, getCharAtDepth(cache[start], rlcp));
            }
            // sort group areas deeper if needed
            if (bktsize > 1) {
                if (cache[start] & 0xFF) {
                    // need deeper sort
                    insertion_sort(
                        strptr.sub(start, bktsize), depth + sizeof(key_type),
                        /* memory */ 0);
                }
                else {
                    // cache contains nullptr-termination
                    strptr.sub(start, bktsize).fill_lcp(depth + lcpKeyDepth(cache[start]));
                }
            }
            bktsize = 1;
            start = i + 1;
        }
        // tail of loop for last item
        if (start != 0 && strptr.with_lcp) {
            int rlcp = lcpKeyType(cache[start - 1], cache[start]);
            strptr.set_lcp(start, depth + rlcp);
            // strptr.set_cache(start, getCharAtDepth(cache[start], rlcp));
        }
        if (bktsize > 1) {
            if (cache[start] & 0xFF) {
                // need deeper sort
                insertion_sort(
                    strptr.sub(start, bktsize), depth + sizeof(key_type),
                    /* memory */ 0);
            }
            else {
                // cache contains nullptr-termination
                strptr.sub(start, bktsize).fill_lcp(depth + lcpKeyDepth(cache[start]));
            }
        }
    }

    class MKQSStep
    {
    public:
        StringPtr strptr_;
        key_type* cache_;
        size_t num_lt_, num_eq_, num_gt_, depth_;
        size_t idx_;
        unsigned char eq_recurse_;
        // typename StringPtr::StringSet::Char dchar_eq_, dchar_gt_;
        std::uint8_t lcp_lt_, lcp_eq_, lcp_gt_;

        MKQSStep(Context& ctx, const StringPtr& strptr,
                 key_type* cache, size_t depth, bool CacheDirty)
            : strptr_(strptr), cache_(cache), depth_(depth), idx_(0) {
            size_t n = strptr_.size();

            const StringSet& strset = strptr_.active();

            if (CacheDirty) {
                typename StringSet::Iterator it = strset.begin();
                for (size_t i = 0; i < n; ++i, ++it) {
                    cache_[i] = get_key<key_type>(strset, *it, depth);
                }
            }
            // select median of 9
            size_t p = med3(
                cache_,
                med3(cache_, 0, n / 8, n / 4),
                med3(cache_, n / 2 - n / 8, n / 2, n / 2 + n / 8),
                med3(cache_, n - 1 - n / 4, n - 1 - n / 8, n - 3));
            // swap pivot to first position
            std::swap(strset.at(0), strset.at(p));
            std::swap(cache_[0], cache_[p]);
            // save the pivot value
            key_type pivot = cache_[0];
            // for immediate LCP calculation
            key_type max_lt = 0, min_gt = std::numeric_limits<key_type>::max();

            // indexes into array:
            // 0 [pivot] 1 [===] leq [<<<] llt [???] rgt [>>>] req [===] n-1
            size_t leq = 1, llt = 1, rgt = n - 1, req = n - 1;
            while (true)
            {
                while (llt <= rgt)
                {
                    int r = cmp(cache[llt], pivot);
                    if (r > 0) {
                        min_gt = std::min(min_gt, cache[llt]);
                        break;
                    }
                    else if (r == 0) {
                        std::swap(strset.at(leq), strset.at(llt));
                        std::swap(cache[leq], cache[llt]);
                        leq++;
                    }
                    else {
                        max_lt = std::max(max_lt, cache[llt]);
                    }
                    ++llt;
                }
                while (llt <= rgt)
                {
                    int r = cmp(cache[rgt], pivot);
                    if (r < 0) {
                        max_lt = std::max(max_lt, cache[rgt]);
                        break;
                    }
                    else if (r == 0) {
                        std::swap(strset.at(req), strset.at(rgt));
                        std::swap(cache[req], cache[rgt]);
                        req--;
                    }
                    else {
                        min_gt = std::min(min_gt, cache[rgt]);
                    }
                    --rgt;
                }
                if (llt > rgt)
                    break;
                std::swap(strset.at(llt), strset.at(rgt));
                std::swap(cache[llt], cache[rgt]);
                ++llt;
                --rgt;
            }
            // calculate size of areas = < and >, save into struct
            size_t num_leq = leq, num_req = n - 1 - req;
            num_eq_ = num_leq + num_req;
            num_lt_ = llt - leq;
            num_gt_ = req - rgt;
            assert(num_eq_ > 0);
            assert(num_lt_ + num_eq_ + num_gt_ == n);

            // swap equal values from left to center
            const size_t size1 = std::min(num_leq, num_lt_);
            std::swap_ranges(strset.begin(), strset.begin() + size1,
                             strset.begin() + llt - size1);
            std::swap_ranges(cache, cache + size1, cache + llt - size1);

            // swap equal values from right to center
            const size_t size2 = std::min(num_req, num_gt_);
            std::swap_ranges(strset.begin() + llt, strset.begin() + llt + size2,
                             strset.begin() + n - size2);
            std::swap_ranges(cache + llt, cache + llt + size2,
                             cache + n - size2);

            // No recursive sorting if pivot has a zero byte
            eq_recurse_ = (pivot & 0xFF);

            // save LCP values for writing into LCP array after sorting further
            if (strptr_.with_lcp && num_lt_ > 0) {
                assert(max_lt == *std::max_element(
                           cache_ + 0, cache + num_lt_));

                lcp_lt_ = lcpKeyType(max_lt, pivot);
                // dchar_eq_ = getCharAtDepth(pivot, lcp_lt_);
                TLX_LOGC(ctx.debug_lcp) << "LCP lt with pivot: " << depth_ + lcp_lt_;
            }

            // calculate equal area lcp: +1 for the equal zero termination byte
            lcp_eq_ = lcpKeyDepth(pivot);

            if (strptr_.with_lcp && num_gt_ > 0) {
                assert(min_gt == *std::min_element(
                           cache_ + num_lt_ + num_eq_, cache_ + n));

                lcp_gt_ = lcpKeyType(pivot, min_gt);
                // dchar_gt_ = getCharAtDepth(min_gt, lcp_gt_);
                TLX_LOGC(ctx.debug_lcp) << "LCP pivot with gt: " << depth_ + lcp_gt_;
            }

            ++ctx.base_sort_steps;
        }

        void calculate_lcp() {
            if (strptr_.with_lcp && num_lt_ > 0) {
                strptr_.set_lcp(num_lt_, depth_ + lcp_lt_);
                // strptr_.set_cache(num_lt_, dchar_eq_);
            }

            if (strptr_.with_lcp && num_gt_ > 0) {
                strptr_.set_lcp(num_lt_ + num_eq_, depth_ + lcp_gt_);
                // strptr_.set_cache(num_lt_ + num_eq_, dchar_gt_);
            }
        }
    };

    size_t ms_front_ = 0;
    std::vector<MKQSStep> ms_stack_;

    void sort_mkqs_cache(const StringPtr& strptr, size_t depth) {
        assert(strcmp(mtimer_.running(), "mkqs") == 0);

        if (!ctx_.enable_sequential_mkqs ||
            strptr.size() < ctx_.inssort_threshold) {
            TLX_LOGC(ctx_.debug_jobs)
                << "insertion_sort() size "
                << strptr.size() << " depth " << depth;

            ScopedMultiTimerSwitch sts_inssort(mtimer_, "inssort");
            insertion_sort(strptr.copy_back(), depth, /* memory */ 0);
            ctx_.donesize(strptr.size());
            return;
        }

        TLX_LOGC(ctx_.debug_jobs)
            << "sort_mkqs_cache() size " << strptr.size() << " depth " << depth;

        if (bktcache_.size() < strptr.size() * sizeof(key_type)) {
            bktcache_.destroy();
            bktcache_.resize(strptr.size() * sizeof(key_type));
        }

        // reuse bktcache as keycache
        key_type* cache = reinterpret_cast<key_type*>(bktcache_.data());

        assert(ms_front_ == 0);
        assert(ms_stack_.size() == 0);

        // std::deque is much slower than std::vector, so we use an artificial
        // pop_front variable.
        ms_stack_.emplace_back(ctx_, strptr, cache, depth, true);

        while (ms_stack_.size() > ms_front_)
        {
            MKQSStep& ms = ms_stack_.back();
            ++ms.idx_; // increment here, because stack may change

            // process the lt-subsequence
            if (ms.idx_ == 1) {
                if (ms.num_lt_ == 0) {
                    // empty subsequence
                }
                else if (ms.num_lt_ < ctx_.inssort_threshold) {
                    ScopedMultiTimerSwitch sts_inssort(mtimer_, "inssort");
                    insertion_sort_cache<false>(ms.strptr_.sub(0, ms.num_lt_),
                                                ms.cache_, ms.depth_);
                    ctx_.donesize(ms.num_lt_);
                }
                else {
                    ms_stack_.emplace_back(
                        ctx_,
                        ms.strptr_.sub(0, ms.num_lt_),
                        ms.cache_, ms.depth_, false);
                }
            }
            // process the eq-subsequence
            else if (ms.idx_ == 2) {
                StringPtr sp = ms.strptr_.sub(ms.num_lt_, ms.num_eq_);

                assert(ms.num_eq_ > 0);

                if (!ms.eq_recurse_) {
                    StringPtr spb = sp.copy_back();
                    spb.fill_lcp(ms.depth_ + ms.lcp_eq_);
                    ctx_.donesize(spb.size());
                }
                else if (ms.num_eq_ < ctx_.inssort_threshold) {
                    ScopedMultiTimerSwitch sts_inssort(mtimer_, "inssort");
                    insertion_sort_cache<true>(sp, ms.cache_ + ms.num_lt_,
                                               ms.depth_ + sizeof(key_type));
                    ctx_.donesize(ms.num_eq_);
                }
                else {
                    ms_stack_.emplace_back(
                        ctx_, sp,
                        ms.cache_ + ms.num_lt_,
                        ms.depth_ + sizeof(key_type), true);
                }
            }
            // process the gt-subsequence
            else if (ms.idx_ == 3) {
                StringPtr sp = ms.strptr_.sub(
                    ms.num_lt_ + ms.num_eq_, ms.num_gt_);

                if (ms.num_gt_ == 0) {
                    // empty subsequence
                }
                else if (ms.num_gt_ < ctx_.inssort_threshold) {
                    ScopedMultiTimerSwitch sts_inssort(mtimer_, "inssort");
                    insertion_sort_cache<false>(
                        sp, ms.cache_ + ms.num_lt_ + ms.num_eq_, ms.depth_);
                    ctx_.donesize(ms.num_gt_);
                }
                else {
                    ms_stack_.emplace_back(
                        ctx_, sp,
                        ms.cache_ + ms.num_lt_ + ms.num_eq_,
                        ms.depth_, false);
                }
            }
            // calculate lcps
            else {
                // finished sort
                assert(ms_stack_.size() > ms_front_);

                // calculate LCP after the three parts are sorted
                ms_stack_.back().calculate_lcp();

                ms_stack_.pop_back();
            }

            if (ctx_.enable_work_sharing && ctx_.threads_.has_idle()) {
                sample_sort_free_work();
            }
        }
    }

    void mkqs_free_work() {
        assert(ms_stack_.size() >= ms_front_);

        for (unsigned int fl = 0; fl < 8; ++fl)
        {
            if (ms_stack_.size() == ms_front_) {
                return;
            }

            TLX_LOGC(ctx_.debug_jobs)
                << "Freeing top level of PS5SmallsortJob's mkqs stack - size "
                << ms_stack_.size();

            // convert top level of stack into independent jobs

            MKQSStep& ms = ms_stack_[ms_front_];

            if (ms.idx_ == 0 && ms.num_lt_ != 0)
            {
                this->substep_add();
                ctx_.enqueue(this, ms.strptr_.sub(0, ms.num_lt_), ms.depth_);
            }
            if (ms.idx_ <= 1) // st.num_eq > 0 always
            {
                assert(ms.num_eq_ > 0);

                StringPtr sp = ms.strptr_.sub(ms.num_lt_, ms.num_eq_);

                if (ms.eq_recurse_) {
                    this->substep_add();
                    ctx_.enqueue(this, sp, ms.depth_ + sizeof(key_type));
                }
                else {
                    StringPtr spb = sp.copy_back();
                    spb.fill_lcp(ms.depth_ + ms.lcp_eq_);
                    ctx_.donesize(ms.num_eq_);
                }
            }
            if (ms.idx_ <= 2 && ms.num_gt_ != 0)
            {
                this->substep_add();
                ctx_.enqueue(
                    this, ms.strptr_.sub(ms.num_lt_ + ms.num_eq_, ms.num_gt_),
                    ms.depth_);
            }

            // shorten the current stack
            ++ms_front_;
        }
    }

    /*------------------------------------------------------------------------*/
    // Called When PS5SmallsortJob is Finished

    void substep_all_done() final {
        TLX_LOGC(ctx_.debug_recursion)
            << "SmallSort[" << depth_ << "] "
            << "all substeps done -> LCP calculation";

        while (ms_front_ > 0) {
            TLX_LOGC(ctx_.debug_lcp)
                << "SmallSort[" << depth_ << "] ms_front_: " << ms_front_;
            ms_stack_[--ms_front_].calculate_lcp();
        }

        while (ss_front_ > 0) {
            TLX_LOGC(ctx_.debug_lcp)
                << "SmallSort[" << depth_ << "] ss_front_: " << ss_front_;
            ss_stack_[--ss_front_].calculate_lcp(ctx_);
        }

        if (pstep_) pstep_->substep_notify_done();
        delete this;
    }
};

/******************************************************************************/
//! PS5BigSortStep Out-of-Place Parallel Sample Sort with Separate Jobs

template <typename Context, typename StringPtr>
class PS5BigSortStep : public PS5SortStep
{
public:
    typedef typename StringPtr::StringSet StringSet;
    typedef typename StringSet::Iterator StrIterator;
    typedef typename Context::key_type key_type;

    //! context
    Context& ctx_;
    //! parent sort step for notification
    PS5SortStep* pstep_;

    //! string pointers, size, and current sorting depth
    StringPtr strptr_;
    size_t depth_;

    //! number of parts into which the strings were split
    size_t parts_;
    //! size of all parts except the last
    size_t psize_;
    //! number of threads still working
    std::atomic<size_t> pwork_;

    //! classifier instance and variables (contains splitter tree
    typename Context::Classify classifier_;

    static const size_t treebits_ = Context::Classify::treebits;
    static const size_t num_splitters_ = Context::Classify::num_splitters;
    static const size_t bktnum_ = 2 * num_splitters_ + 1;

    //! LCPs of splitters, needed for recursive calls
    unsigned char splitter_lcp_[num_splitters_ + 1];

    //! individual bucket array of threads, keep bkt[0] for DistributeJob
    simple_vector<simple_vector<size_t> > bkt_;
    //! bucket ids cache, created by classifier and later counted
    simple_vector<simple_vector<std::uint16_t> > bktcache_;

    /*------------------------------------------------------------------------*/
    // Constructor

    PS5BigSortStep(Context& ctx, PS5SortStep* pstep,
                   const StringPtr& strptr, size_t depth)
        : ctx_(ctx), pstep_(pstep), strptr_(strptr), depth_(depth) {
        // calculate number of parts
        parts_ = strptr_.size() / ctx.sequential_threshold() * 2;
        if (parts_ == 0) parts_ = 1;

        bkt_.resize(parts_);
        bktcache_.resize(parts_);

        psize_ = (strptr.size() + parts_ - 1) / parts_;

        TLX_LOGC(ctx_.debug_steps)
            << "enqueue depth=" << depth_
            << " size=" << strptr_.size()
            << " parts=" << parts_
            << " psize=" << psize_
            << " flip=" << strptr_.flipped();

        ctx.threads_.enqueue([this]() { sample(); });
        ++ctx.para_ss_steps;
    }

    virtual ~PS5BigSortStep() { }

    /*------------------------------------------------------------------------*/
    // Sample Step

    void sample() {
        ScopedMultiTimer smt(ctx_.mtimer, "para_ss");
        TLX_LOGC(ctx_.debug_jobs) << "Process SampleJob @ " << this;

        const size_t oversample_factor = 2;
        size_t sample_size = oversample_factor * num_splitters_;

        const StringSet& strset = strptr_.active();
        size_t n = strset.size();

        simple_vector<key_type> samples(sample_size);

        std::minstd_rand rng(reinterpret_cast<uintptr_t>(samples.data()));

        for (size_t i = 0; i < sample_size; ++i)
            samples[i] = get_key_at<key_type>(strset, rng() % n, depth_);

        std::sort(samples.begin(), samples.end());

        classifier_.build(samples.data(), sample_size, splitter_lcp_);

        // create new jobs
        pwork_ = parts_;
        for (unsigned int p = 0; p < parts_; ++p) {
            ctx_.threads_.enqueue([this, p]() { count(p); });
        }
    }

    /*------------------------------------------------------------------------*/
    // Counting Step

    void count(unsigned int p) {
        ScopedMultiTimer smt(ctx_.mtimer, "para_ss");
        TLX_LOGC(ctx_.debug_jobs) << "Process CountJob " << p << " @ " << this;

        const StringSet& strset = strptr_.active();

        StrIterator strB = strset.begin() + p * psize_;
        StrIterator strE = strset.begin() + std::min((p + 1) * psize_, strptr_.size());
        if (strE < strB) strE = strB;

        bktcache_[p].resize(strE - strB);
        std::uint16_t* bktcache = bktcache_[p].data();
        classifier_.classify(strset, strB, strE, bktcache, depth_);

        bkt_[p].resize(bktnum_ + (p == 0 ? 1 : 0));
        size_t* bkt = bkt_[p].data();
        memset(bkt, 0, bktnum_ * sizeof(size_t));

        for (std::uint16_t* bc = bktcache; bc != bktcache + (strE - strB); ++bc)
            ++bkt[*bc];

        if (--pwork_ == 0)
            count_finished();
    }

    void count_finished() {
        ScopedMultiTimer smt(ctx_.mtimer, "para_ss");
        TLX_LOGC(ctx_.debug_jobs) << "Finishing CountJob " << this << " with prefixsum";

        // abort sorting if we're measuring only the top level
        if (ctx_.use_only_first_sortstep)
            return;

        // inclusive prefix sum over bkt
        size_t sum = 0;
        for (unsigned int i = 0; i < bktnum_; ++i) {
            for (unsigned int p = 0; p < parts_; ++p) {
                bkt_[p][i] = (sum += bkt_[p][i]);
            }
        }
        assert(sum == strptr_.size());

        // create new jobs
        pwork_ = parts_;
        for (unsigned int p = 0; p < parts_; ++p) {
            ctx_.threads_.enqueue([this, p]() { distribute(p); });
        }
    }

    /*------------------------------------------------------------------------*/
    // Distribute Step

    void distribute(unsigned int p) {
        ScopedMultiTimer smt(ctx_.mtimer, "para_ss");
        TLX_LOGC(ctx_.debug_jobs) << "Process DistributeJob " << p << " @ " << this;

        const StringSet& strset = strptr_.active();

        StrIterator strB = strset.begin() + p * psize_;
        StrIterator strE = strset.begin() + std::min((p + 1) * psize_, strptr_.size());
        if (strE < strB) strE = strB;

        // get alternative shadow pointer array
        const StringSet& sorted = strptr_.shadow();
        typename StringSet::Iterator sbegin = sorted.begin();

        std::uint16_t* bktcache = bktcache_[p].data();
        size_t* bkt = bkt_[p].data();

        for (StrIterator str = strB; str != strE; ++str, ++bktcache)
            *(sbegin + --bkt[*bktcache]) = std::move(*str);

        if (p != 0) // p = 0 is needed for recursion into bkts
            bkt_[p].destroy();

        bktcache_[p].destroy();

        if (--pwork_ == 0)
            distribute_finished();
    }

    void distribute_finished() {
        TLX_LOGC(ctx_.debug_jobs)
            << "Finishing DistributeJob " << this << " with enqueuing subjobs";

        size_t* bkt = bkt_[0].data();
        assert(bkt);

        // first processor's bkt pointers are boundaries between bkts, just add sentinel:
        assert(bkt[0] == 0);
        bkt[bktnum_] = strptr_.size();

        // keep anonymous subjob handle while creating subjobs
        this->substep_add();

        size_t i = 0;
        while (i < bktnum_ - 1)
        {
            // i is even -> bkt[i] is less-than bucket
            size_t bktsize = bkt[i + 1] - bkt[i];
            if (bktsize == 0) {
                // empty bucket
            }
            else if (bktsize == 1) { // just one string pointer, copyback
                strptr_.flip(bkt[i], 1).copy_back();
                ctx_.donesize(1);
            }
            else
            {
                TLX_LOGC(ctx_.debug_recursion)
                    << "Recurse[" << depth_ << "]: < bkt " << bkt[i]
                    << " size " << bktsize << " lcp "
                    << int(splitter_lcp_[i / 2] & 0x7F);
                this->substep_add();
                ctx_.enqueue(this, strptr_.flip(bkt[i], bktsize),
                             depth_ + (splitter_lcp_[i / 2] & 0x7F));
            }
            ++i;
            // i is odd -> bkt[i] is equal bucket
            bktsize = bkt[i + 1] - bkt[i];
            if (bktsize == 0) {
                // empty bucket
            }
            else if (bktsize == 1) { // just one string pointer, copyback
                strptr_.flip(bkt[i], 1).copy_back();
                ctx_.donesize(1);
            }
            else
            {
                if (splitter_lcp_[i / 2] & 0x80) {
                    // equal-bucket has nullptr-terminated key, done.
                    TLX_LOGC(ctx_.debug_recursion)
                        << "Recurse[" << depth_ << "]: = bkt " << bkt[i]
                        << " size " << bktsize << " is done!";
                    StringPtr sp = strptr_.flip(bkt[i], bktsize).copy_back();
                    sp.fill_lcp(
                        depth_ + lcpKeyDepth(classifier_.get_splitter(i / 2)));
                    ctx_.donesize(bktsize);
                }
                else {
                    TLX_LOGC(ctx_.debug_recursion)
                        << "Recurse[" << depth_ << "]: = bkt " << bkt[i]
                        << " size " << bktsize << " lcp keydepth!";
                    this->substep_add();
                    ctx_.enqueue(this, strptr_.flip(bkt[i], bktsize),
                                 depth_ + sizeof(key_type));
                }
            }
            ++i;
        }

        size_t bktsize = bkt[i + 1] - bkt[i];

        if (bktsize == 0) {
            // empty bucket
        }
        else if (bktsize == 1) { // just one string pointer, copyback
            strptr_.flip(bkt[i], 1).copy_back();
            ctx_.donesize(1);
        }
        else {
            TLX_LOGC(ctx_.debug_recursion)
                << "Recurse[" << depth_ << "]: > bkt " << bkt[i]
                << " size " << bktsize << " no lcp";
            this->substep_add();
            ctx_.enqueue(this, strptr_.flip(bkt[i], bktsize), depth_);
        }

        this->substep_notify_done(); // release anonymous subjob handle

        if (!strptr_.with_lcp)
            bkt_[0].destroy();
    }

    /*------------------------------------------------------------------------*/
    // After Recursive Sorting

    void substep_all_done() final {
        ScopedMultiTimer smt(ctx_.mtimer, "para_ss");
        if (strptr_.with_lcp) {
            TLX_LOGC(ctx_.debug_steps)
                << "pSampleSortStep[" << depth_ << "]: all substeps done.";

            ps5_sample_sort_lcp<bktnum_>(
                ctx_, classifier_, strptr_, depth_, bkt_[0].data());
            bkt_[0].destroy();
        }

        if (pstep_) pstep_->substep_notify_done();
        delete this;
    }
};

/******************************************************************************/
// PS5Context::enqueue()

template <typename Parameters>
template <typename StringPtr>
void PS5Context<Parameters>::enqueue(
    PS5SortStep* pstep, const StringPtr& strptr, size_t depth) {
    if (this->enable_parallel_sample_sort &&
        (strptr.size() > sequential_threshold() ||
         this->use_only_first_sortstep)) {
        new PS5BigSortStep<PS5Context, StringPtr>(*this, pstep, strptr, depth);
    }
    else {
        if (strptr.size() < (1LLU << 32)) {
            auto j = new PS5SmallsortJob<PS5Context, StringPtr, std::uint32_t>(
                *this, pstep, strptr, depth);
            threads_.enqueue([j]() { j->run(); });
        }
        else {
            auto j = new PS5SmallsortJob<PS5Context, StringPtr, std::uint64_t>(
                *this, pstep, strptr, depth);
            threads_.enqueue([j]() { j->run(); });
        }
    }
}

/******************************************************************************/
// Externally Callable Sorting Methods

//! Main Parallel Sample Sort Function. See below for more convenient wrappers.
template <typename PS5Parameters, typename StringPtr>
void parallel_sample_sort_base(const StringPtr& strptr, size_t depth) {

    using Context = PS5Context<PS5Parameters>;
    Context ctx(std::thread::hardware_concurrency());
    ctx.total_size = strptr.size();
    ctx.rest_size = strptr.size();
    ctx.num_threads = ctx.threads_.size();

    MultiTimer timer;
    timer.start("sort");

    ctx.enqueue(/* pstep */ nullptr, strptr, depth);
    ctx.threads_.loop_until_empty();

    timer.stop();

    assert(!ctx.enable_rest_size || ctx.rest_size == 0);

    using BigSortStep = PS5BigSortStep<Context, StringPtr>;

    TLX_LOGC(ctx.debug_result)
        << "RESULT"
        << " sizeof(key_type)=" << sizeof(typename PS5Parameters::key_type)
        << " splitter_treebits=" << size_t(BigSortStep::treebits_)
        << " num_splitters=" << size_t(BigSortStep::num_splitters_)
        << " num_threads=" << ctx.num_threads
        << " enable_work_sharing=" << size_t(ctx.enable_work_sharing)
        << " use_restsize=" << size_t(ctx.enable_rest_size)
        << " tm_para_ss=" << ctx.mtimer.get("para_ss")
        << " tm_seq_ss=" << ctx.mtimer.get("sequ_ss")
        << " tm_mkqs=" << ctx.mtimer.get("mkqs")
        << " tm_inssort=" << ctx.mtimer.get("inssort")
        << " tm_total=" << ctx.mtimer.total()
        << " tm_idle="
        << (ctx.num_threads * timer.total()) - ctx.mtimer.total()
        << " steps_para_sample_sort=" << ctx.para_ss_steps
        << " steps_seq_sample_sort=" << ctx.sequ_ss_steps
        << " steps_base_sort=" << ctx.base_sort_steps;
}

//! Parallel Sample Sort Function for a generic StringSet, this allocates the
//! shadow array for flipping.
template <typename PS5Parameters, typename StringPtr>
typename enable_if<!StringPtr::with_lcp, void>::type
parallel_sample_sort_params(
    const StringPtr& strptr, size_t depth, size_t memory = 0) {
    tlx::unused(memory);

    typedef typename StringPtr::StringSet StringSet;
    const StringSet& strset = strptr.active();

    typedef StringShadowPtr<StringSet> StringShadowPtr;
    typedef typename StringSet::Container Container;

    // allocate shadow pointer array
    Container shadow = strset.allocate(strset.size());
    StringShadowPtr new_strptr(strset, StringSet(shadow));

    parallel_sample_sort_base<PS5Parameters>(new_strptr, depth);

    StringSet::deallocate(shadow);
}

//! Parallel Sample Sort Function for a generic StringSet with LCPs, this
//! allocates the shadow array for flipping.
template <typename PS5Parameters, typename StringPtr>
typename enable_if<StringPtr::with_lcp, void>::type
parallel_sample_sort_params(
    const StringPtr& strptr, size_t depth, size_t memory = 0) {
    tlx::unused(memory);

    typedef typename StringPtr::StringSet StringSet;
    typedef typename StringPtr::LcpType LcpType;
    const StringSet& strset = strptr.active();

    typedef StringShadowLcpPtr<StringSet, LcpType> StringShadowLcpPtr;
    typedef typename StringSet::Container Container;

    // allocate shadow pointer array
    Container shadow = strset.allocate(strset.size());
    StringShadowLcpPtr new_strptr(strset, StringSet(shadow), strptr.lcp());

    parallel_sample_sort_base<PS5Parameters>(new_strptr, depth);

    StringSet::deallocate(shadow);
}

//! Parallel Sample Sort Function with default parameter size for a generic
//! StringSet.
template <typename StringPtr>
void parallel_sample_sort(
    const StringPtr& strptr, size_t depth, size_t memory) {
    return parallel_sample_sort_params<PS5ParametersDefault>(
        strptr, depth, memory);
}

} // namespace sort_strings_detail
} // namespace tlx

#endif // !TLX_SORT_STRINGS_PARALLEL_SAMPLE_SORT_HEADER

/******************************************************************************/
