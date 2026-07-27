/**
 * Non-metric Space Library
 *
 * Main developers: Bilegsaikhan Naidan, Leonid Boytsov, Yury Malkov, Ben Frederickson, David Novak
 *
 * For the complete list of contributors and further details see:
 * https://github.com/nmslib/nmslib
 *
 * Copyright (c) 2013-2018
 *
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 */
/*
*
* A Hierarchical Navigable Small World (HNSW) approach.
*
* The main publication is (available on arxiv: http://arxiv.org/abs/1603.09320):
* "Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs" by Yu. A. Malkov, D. A. Yashunin
* This code was contributed by Yu. A. Malkov. It also was used in tests from the paper.
*
*
*/

#include "method/hnsw.h"
#include "method/hnsw_distfunc_opt_impl_inline.h"
#include "knnquery.h"
#include "ported_boost_progress.h"
#include "rangequery.h"

#include "portable_prefetch.h"
#include "space.h"

#include "sort_arr_bi.h"
#define MERGE_BUFFER_ALGO_SWITCH_THRESHOLD 100

#include <algorithm> // std::min
#include <limits>
#include <vector>

//#define DIST_CALC
namespace similarity {


    template <typename dist_t>
    void
    Hnsw<dist_t>::SearchOld(KNNQuery<dist_t> *query, bool normalize)
    {
        float *pVectq = (float *)((char *)query->QueryObject()->data());
        TMP_RES_ARRAY(TmpRes);
        size_t qty = query->QueryObject()->datalength() >> 2;

        if (normalize) {
            NormalizeVect(pVectq, qty);
        }

        VisitedList *vl = visitedlistpool->getFreeVisitedList();
        vl_type *massVisited = vl->mass;
        vl_type currentV = vl->curV;

        int maxlevel1 = maxlevel_;
        int curNodeNum = enterpointId_;
        dist_t curdist = (fstdistfunc_(
            pVectq, (float *)(data_level0_memory_ + enterpointId_ * memoryPerObject_ + offsetData_ + 16), qty, TmpRes));

        for (int i = maxlevel1; i > 0; i--) {
            bool changed = true;
            while (changed) {
                changed = false;
                int *data = (int *)(linkLists_[curNodeNum] + (maxM_ + 1) * (i - 1) * sizeof(int));
                int size = *data;
                for (int j = 1; j <= size; j++) {
                    PREFETCH(data_level0_memory_ + (*(data + j)) * memoryPerObject_ + offsetData_, _MM_HINT_T0);
                }
#ifdef DIST_CALC
                query->distance_computations_ += size;
#endif

                for (int j = 1; j <= size; j++) {
                    int tnum = *(data + j);

                    dist_t d = (fstdistfunc_(
                        pVectq, (float *)(data_level0_memory_ + tnum * memoryPerObject_ + offsetData_ + 16), qty, TmpRes));
                    if (d < curdist) {
                        curdist = d;
                        curNodeNum = tnum;
                        changed = true;
                    }
                }
            }
        }

        priority_queue<EvaluatedMSWNodeInt<dist_t>> candidateQueuei; // the set of elements which we can use to evaluate

        priority_queue<EvaluatedMSWNodeInt<dist_t>> closestDistQueuei; // The set of closest found elements
        // EvaluatedMSWNodeInt<dist_t> evi(curdist, curNodeNum);
        candidateQueuei.emplace(-curdist, curNodeNum);

        closestDistQueuei.emplace(curdist, curNodeNum);

        // query->CheckAndAddToResult(curdist, new Object(data_level0_memory_ + (curNodeNum)*memoryPerObject_ + offsetData_));
        query->CheckAndAddToResult(curdist, data_rearranged_[curNodeNum]);
        massVisited[curNodeNum] = currentV;

        while (!candidateQueuei.empty()) {
            EvaluatedMSWNodeInt<dist_t> currEv = candidateQueuei.top(); // This one was already compared to the query

            dist_t lowerBound = closestDistQueuei.top().getDistance();
            if ((-currEv.getDistance()) > lowerBound) {
                break;
            }

            candidateQueuei.pop();
            curNodeNum = currEv.element;
            int *data = (int *)(data_level0_memory_ + curNodeNum * memoryPerObject_ + offsetLevel0_);
            int size = *data;
            PREFETCH((char *)(massVisited + *(data + 1)), _MM_HINT_T0);
            PREFETCH((char *)(massVisited + *(data + 1) + 64), _MM_HINT_T0);
            PREFETCH(data_level0_memory_ + (*(data + 1)) * memoryPerObject_ + offsetData_, _MM_HINT_T0);
            PREFETCH((char *)(data + 2), _MM_HINT_T0);

            for (int j = 1; j <= size; j++) {
                int tnum = *(data + j);
                PREFETCH((char *)(massVisited + *(data + j + 1)), _MM_HINT_T0);
                PREFETCH(data_level0_memory_ + (*(data + j + 1)) * memoryPerObject_ + offsetData_, _MM_HINT_T0);
                if (!(massVisited[tnum] == currentV)) {
#ifdef DIST_CALC
                    query->distance_computations_++;
#endif
                    massVisited[tnum] = currentV;
                    char *currObj1 = (data_level0_memory_ + tnum * memoryPerObject_ + offsetData_);
                    dist_t d = (fstdistfunc_(pVectq, (float *)(currObj1 + 16), qty, TmpRes));
                    if (closestDistQueuei.top().getDistance() > d || closestDistQueuei.size() < ef_) {
                        candidateQueuei.emplace(-d, tnum);
                        PREFETCH(data_level0_memory_ + candidateQueuei.top().element * memoryPerObject_ + offsetLevel0_,
                                     _MM_HINT_T0);
                        // query->CheckAndAddToResult(d, new Object(currObj1));
                        query->CheckAndAddToResult(d, data_rearranged_[tnum]);
                        closestDistQueuei.emplace(d, tnum);

                        if (closestDistQueuei.size() > ef_) {
                            closestDistQueuei.pop();
                        }
                    }
                }
            }
        }
        visitedlistpool->releaseVisitedList(vl);
    }

    template <typename dist_t>
    void
    Hnsw<dist_t>::SearchV1Merge(KNNQuery<dist_t> *query, bool normalize)
    {
        float *pVectq = (float *)((char *)query->QueryObject()->data());
        TMP_RES_ARRAY(TmpRes);
        size_t qty = query->QueryObject()->datalength() >> 2;

        if (normalize) {
            NormalizeVect(pVectq, qty);
        }

        VisitedList *vl = visitedlistpool->getFreeVisitedList();
        vl_type *massVisited = vl->mass;
        vl_type currentV = vl->curV;

        int maxlevel1 = maxlevel_;
        int curNodeNum = enterpointId_;
        dist_t curdist = (fstdistfunc_(
            pVectq, (float *)(data_level0_memory_ + enterpointId_ * memoryPerObject_ + offsetData_ + 16), qty, TmpRes));

        for (int i = maxlevel1; i > 0; i--) {
            bool changed = true;
            while (changed) {
                changed = false;
                int *data = (int *)(linkLists_[curNodeNum] + (maxM_ + 1) * (i - 1) * sizeof(int));
                int size = *data;
                for (int j = 1; j <= size; j++) {
                    PREFETCH(data_level0_memory_ + (*(data + j)) * memoryPerObject_ + offsetData_, _MM_HINT_T0);
                }
#ifdef DIST_CALC
                query->distance_computations_ += size;
#endif

                for (int j = 1; j <= size; j++) {
                    int tnum = *(data + j);

                    dist_t d = (fstdistfunc_(
                        pVectq, (float *)(data_level0_memory_ + tnum * memoryPerObject_ + offsetData_ + 16), qty, TmpRes));
                    if (d < curdist) {
                        curdist = d;
                        curNodeNum = tnum;
                        changed = true;
                    }
                }
            }
        }

        SortArrBI<dist_t, int> sortedArr(max<size_t>(ef_, query->GetK()));
        sortedArr.push_unsorted_grow(curdist, curNodeNum);

        int_fast32_t currElem = 0;

        typedef typename SortArrBI<dist_t, int>::Item QueueItem;
        vector<QueueItem> &queueData = sortedArr.get_data();
        vector<QueueItem> itemBuff(1 + max(maxM_, maxM0_));

        massVisited[curNodeNum] = currentV;

        while (currElem < min(sortedArr.size(), ef_)) {
            auto &e = queueData[currElem];
            CHECK(!e.used);
            e.used = true;
            curNodeNum = e.data;
            ++currElem;

            size_t itemQty = 0;
            dist_t topKey = sortedArr.top_key();

            int *data = (int *)(data_level0_memory_ + curNodeNum * memoryPerObject_ + offsetLevel0_);
            int size = *data;
            PREFETCH((char *)(massVisited + *(data + 1)), _MM_HINT_T0);
            PREFETCH((char *)(massVisited + *(data + 1) + 64), _MM_HINT_T0);
            PREFETCH(data_level0_memory_ + (*(data + 1)) * memoryPerObject_ + offsetData_, _MM_HINT_T0);
            PREFETCH((char *)(data + 2), _MM_HINT_T0);

            for (int j = 1; j <= size; j++) {
                int tnum = *(data + j);
                PREFETCH((char *)(massVisited + *(data + j + 1)), _MM_HINT_T0);
                PREFETCH(data_level0_memory_ + (*(data + j + 1)) * memoryPerObject_ + offsetData_, _MM_HINT_T0);
                if (!(massVisited[tnum] == currentV)) {
#ifdef DIST_CALC
                    query->distance_computations_++;
#endif
                    massVisited[tnum] = currentV;
                    char *currObj1 = (data_level0_memory_ + tnum * memoryPerObject_ + offsetData_);
                    dist_t d = (fstdistfunc_(pVectq, (float *)(currObj1 + 16), qty, TmpRes));

                    if (d < topKey || sortedArr.size() < ef_) {
                        CHECK_MSG(itemBuff.size() > itemQty,
                                  "Perhaps a bug: buffer size is not enough " + 
                                  ConvertToString(itemQty) + " >= " + ConvertToString(itemBuff.size()));
                        itemBuff[itemQty++] = QueueItem(d, tnum);
                    }
                }
            }

            if (itemQty) {
                PREFETCH(const_cast<const char *>(reinterpret_cast<char *>(&itemBuff[0])), _MM_HINT_T0);
                std::sort(itemBuff.begin(), itemBuff.begin() + itemQty);

                size_t insIndex = 0;
                if (itemQty > MERGE_BUFFER_ALGO_SWITCH_THRESHOLD) {
                    insIndex = sortedArr.merge_with_sorted_items(&itemBuff[0], itemQty);

                    if (insIndex < currElem) {
                        currElem = insIndex;
                    }
                } else {
                    for (size_t ii = 0; ii < itemQty; ++ii) {
                        size_t insIndex = sortedArr.push_or_replace_non_empty_exp(itemBuff[ii].key, itemBuff[ii].data);
                        if (insIndex < currElem) {
                            currElem = insIndex;
                        }
                    }
                }
                // because itemQty > 1, there would be at least item in sortedArr
                PREFETCH(data_level0_memory_ + sortedArr.top_item().data * memoryPerObject_ + offsetLevel0_, _MM_HINT_T0);
            }
            // To ensure that we either reach the end of the unexplored queue or currElem points to the first unused element
            while (currElem < sortedArr.size() && queueData[currElem].used == true)
                ++currElem;
        }

        for (int_fast32_t i = 0; i < query->GetK() && i < sortedArr.size(); ++i) {
            int tnum = queueData[i].data;
            // char *currObj = (data_level0_memory_ + tnum*memoryPerObject_ + offsetData_);
            // query->CheckAndAddToResult(queueData[i].key, new Object(currObj));
            query->CheckAndAddToResult(queueData[i].key, data_rearranged_[tnum]);
        }
        visitedlistpool->releaseVisitedList(vl);
    }

    template class Hnsw<float>;
    template class Hnsw<int>;
}
