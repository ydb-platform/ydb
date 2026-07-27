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

#include <cmath>
#include <iostream>
#include <memory>

#include "portable_prefetch.h"
#include "portable_simd.h"
#include "knnquery.h"
#include "method/hnsw.h"
#include "method/hnsw_distfunc_opt_impl_inline.h"
#include "ported_boost_progress.h"
#include "rangequery.h"
#include "space.h"
#include "space/space_lp.h"
#include "space/space_scalar.h"
#include "thread_pool.h"
#include "utils.h"

#include <map>
#include <set>
#include <sstream>
#include <typeinfo>
#include <vector>

#include "sort_arr_bi.h"
#define MERGE_BUFFER_ALGO_SWITCH_THRESHOLD 100

#define USE_BITSET_FOR_INDEXING 1
#define EXTEND_USE_EXTENDED_NEIGHB_AT_CONSTR (0) // 0 is faster build, 1 is faster search on clustered data

// For debug purposes we also implemented saving an index to a text file
#define USE_TEXT_REGULAR_INDEX (false)

#define TOTAL_QTY       "TOTAL_QTY"
#define MAX_LEVEL       "MAX_LEVEL"
#define ENTER_POINT_ID  "ENTER_POINT_ID"
#define FIELD_M         "M"
#define FIELD_MAX_M     "MAX_M"
#define FIELD_MAX_M0    "MAX_M0"
#define CURR_LEVEL      "CURR_LEVEL"

#define EXTRA_MEM_PAD_SIZE 64

namespace similarity {

    float
    NegativeDotProduct(const float *pVect1, const float *pVect2, size_t &qty, float * __restrict TmpRes) {
        return -ScalarProduct(pVect1, pVect2, qty, TmpRes);
    }

    /*
     * Important note: This function is applicable only when both vectors are normalized!
     */
    float
    NormCosine(const float *pVect1, const float *pVect2, size_t &qty, float *__restrict TmpRes) {
        return std::max(0.0f, 1 - std::max(float(-1), std::min(float(1), ScalarProduct(pVect1, pVect2, qty, TmpRes))));
    }

    float L1NormWrapper(const float *pVect1, const float *pVect2, size_t &qty, float *) {
        return L1NormSIMD(pVect1, pVect2, qty);
    }

    float LInfNormWrapper(const float *pVect1, const float *pVect2, size_t &qty, float *) {
        return LInfNormSIMD(pVect1, pVect2, qty);
    }

    EfficientDistFunc getDistFunc(DistFuncType funcType) {
        switch (funcType) {
            case kL2Sqr16Ext : return L2Sqr16Ext;
            case kL2SqrExt   : return L2SqrExt;
            case kNormCosine : return NormCosine;
            case kNegativeDotProduct : return NegativeDotProduct;
            case kL1Norm : return L1NormWrapper;
            case kLInfNorm : return LInfNormWrapper;
        }

        return nullptr;
    }



// This is the counter to keep the size of neighborhood information (for one node)
    // TODO Can this one overflow? I really doubt
    typedef uint32_t SIZEMASS_TYPE;

    using namespace std;

    template <typename dist_t>
    Hnsw<dist_t>::Hnsw(bool PrintProgress, const Space<dist_t> &space, const ObjectVector &data)
        : Index<dist_t>(data)
        , space_(space)
        , PrintProgress_(PrintProgress)
        , visitedlistpool(nullptr)
        , enterpoint_(nullptr)
        , data_level0_memory_(nullptr)
        , linkLists_(nullptr)
        , fstdistfunc_(nullptr)
    {
    }

    void
    checkList1(vector<HnswNode *> list)
    {
        int ok = 1;
        for (size_t i = 0; i < list.size(); i++) {
            for (size_t j = 0; j < list[i]->allFriends_[0].size(); j++) {
                for (size_t k = j + 1; k < list[i]->allFriends_[0].size(); k++) {
                    if (list[i]->allFriends_[0][j] == list[i]->allFriends_[0][k]) {
                        cout << "\nDuplicate links\n\n\n\n\n!!!!!";
                        ok = 0;
                    }
                }
                if (list[i]->allFriends_[0][j] == list[i]) {
                    cout << "\nLink to the same element\n\n\n\n\n!!!!!";
                    ok = 0;
                }
            }
        }
        if (ok)
            cout << "\nOK\n";
        else
            cout << "\nNOT OK!!!\n";
        return;
    }

    void
    getDegreeDistr(string filename, vector<HnswNode *> list)
    {
        ofstream out(filename);
        size_t maxdegree = 0;
        for (HnswNode *node : list) {
            if (node->allFriends_[0].size() > maxdegree)
                maxdegree = node->allFriends_[0].size();
        }

        vector<int> distrin = vector<int>(1000);
        vector<int> distrout = vector<int>(1000);
        vector<int> inconnections = vector<int>(list.size());
        vector<int> outconnections = vector<int>(list.size());
        for (size_t i = 0; i < list.size(); i++) {
            for (HnswNode *node : list[i]->allFriends_[0]) {
                outconnections[list[i]->getId()]++;
                inconnections[node->getId()]++;
            }
        }

        for (size_t i = 0; i < list.size(); i++) {
            distrin[inconnections[i]]++;
            distrout[outconnections[i]]++;
        }

        for (size_t i = 0; i < distrin.size(); i++) {
            out << i << "\t" << distrin[i] << "\t" << distrout[i] << "\n";
        }
        out.close();
        return;
    }

    template <typename dist_t>
    void
    Hnsw<dist_t>::CreateIndex(const AnyParams &IndexParams)
    {
        AnyParamManager pmgr(IndexParams);

        pmgr.GetParamOptional("M", M_, 16);

        // Let's use a generic algorithm by default!
        pmgr.GetParamOptional(
            "searchMethod", searchMethod_, 0); // this is just to prevent terminating the program when searchMethod is specified
        searchMethod_ = 0;

        indexThreadQty_ = std::thread::hardware_concurrency();

        pmgr.GetParamOptional("indexThreadQty", indexThreadQty_, indexThreadQty_);
        // indexThreadQty_ = 1;
        pmgr.GetParamOptional("efConstruction", efConstruction_, 200);
        pmgr.GetParamOptional("maxM", maxM_, M_);
        pmgr.GetParamOptional("maxM0", maxM0_, M_ * 2);
        pmgr.GetParamOptional("mult", mult_, 1 / log(1.0 * M_));
        pmgr.GetParamOptional("delaunay_type", delaunay_type_, 2);
        int post_;
        pmgr.GetParamOptional("post", post_, 0);
        int skip_optimized_index = 0;
        pmgr.GetParamOptional("skip_optimized_index", skip_optimized_index, 0);

        LOG(LIB_INFO) << "M                   = " << M_;
        LOG(LIB_INFO) << "indexThreadQty      = " << indexThreadQty_;
        LOG(LIB_INFO) << "efConstruction      = " << efConstruction_;
        LOG(LIB_INFO) << "maxM			          = " << maxM_;
        LOG(LIB_INFO) << "maxM0			          = " << maxM0_;

        LOG(LIB_INFO) << "mult                = " << mult_;
        LOG(LIB_INFO) << "skip_optimized_index= " << skip_optimized_index;
        LOG(LIB_INFO) << "delaunay_type       = " << delaunay_type_;

        SetQueryTimeParams(getEmptyParams());

        if (this->data_.empty()) {
            pmgr.CheckUnused();
            return;
        }
        ElList_.resize(this->data_.size());
        // One entry should be added before all the threads are started, or else add() will not work properly
        HnswNode *first = new HnswNode(this->data_[0], 0 /* id == 0 */);
        first->init(getRandomLevel(mult_), maxM_, maxM0_);
        maxlevel_ = first->level;
        enterpoint_ = first;
        ElList_[0] = first;

        visitedlistpool = new VisitedListPool(indexThreadQty_, this->data_.size());

        unique_ptr<ProgressDisplay> progress_bar(PrintProgress_ ? new ProgressDisplay(this->data_.size(), cerr) : NULL);

        ParallelFor(1, this->data_.size(), indexThreadQty_, [&](int id, int threadId) {
            HnswNode *node = new HnswNode(this->data_[id], id);
            add(&space_, node);
            {
                unique_lock<mutex> lock(ElListGuard_);
                ElList_[id] = node;
                if (progress_bar)
                  ++(*progress_bar);
            }
        });
        if (progress_bar)
          progress_bar->finish();

        if (post_ == 1 || post_ == 2) {
            vector<HnswNode *> temp;
            temp.swap(ElList_);
            ElList_.resize(this->data_.size());
            first = new HnswNode(this->data_[0], 0 /* id == 0 */);
            first->init(getRandomLevel(mult_), maxM_, maxM0_);
            maxlevel_ = first->level;
            enterpoint_ = first;
            ElList_[0] = first;
            /// Making the same index in reverse order
            unique_ptr<ProgressDisplay> progress_bar1(PrintProgress_ ? new ProgressDisplay(this->data_.size(), cerr) : NULL);

            ParallelFor(1, this->data_.size(), indexThreadQty_, [&](int pos_id, int threadId) {
                // reverse ordering (so we iterate decreasing). given
                // parallelfor, this might not make a difference
                int id = this->data_.size() - pos_id;
                HnswNode *node = new HnswNode(this->data_[id], id);
                add(&space_, node);
                {
                    unique_lock<mutex> lock(ElListGuard_);
                    ElList_[id] = node;
                    if (progress_bar1)
                        ++(*progress_bar1);
                }
                if (progress_bar1)
                  progress_bar1->finish();
            });
            int maxF = 0;

// int degrees[100] = {0};
            ParallelFor(1, this->data_.size(), indexThreadQty_, [&](int id, int threadId) {
                HnswNode *node1 = ElList_[id];
                HnswNode *node2 = temp[id];
                vector<HnswNode *> f1 = node1->getAllFriends(0);
                vector<HnswNode *> f2 = node2->getAllFriends(0);
                unordered_set<size_t> intersect = unordered_set<size_t>();
                for (HnswNode *cur : f1) {
                    intersect.insert(cur->getId());
                }
                for (HnswNode *cur : f2) {
                    intersect.insert(cur->getId());
                }
                if (intersect.size() > maxF)
                    maxF = intersect.size();
                vector<HnswNode *> rez = vector<HnswNode *>();

                if (post_ == 2) {
                    priority_queue<HnswNodeDistCloser<dist_t>> resultSet;
                    for (int cur : intersect) {
                        resultSet.emplace(space_.IndexTimeDistance(ElList_[cur]->getData(), ElList_[id]->getData()),
                                          ElList_[cur]);
                    }

                    switch (delaunay_type_) {
                    case 0:
                        while (resultSet.size() > maxM0_)
                            resultSet.pop();
                        break;
                    case 2:
                    case 1:
                        ElList_[id]->getNeighborsByHeuristic1(resultSet, maxM0_, &space_);
                        break;
                    case 3:
                        ElList_[id]->getNeighborsByHeuristic3(resultSet, maxM0_, &space_, 0);
                        break;
                    }
                    while (!resultSet.empty()) {
                        rez.push_back(resultSet.top().getMSWNodeHier());
                        resultSet.pop();
                    }
                } else if (post_ == 1) {
                    maxM0_ = maxF;

                    for (int cur : intersect) {
                        rez.push_back(ElList_[cur]);
                    }
                }

                {
                    unique_lock<mutex> lock(ElList_[id]->accessGuard_);
                    ElList_[id]->allFriends_[0].swap(rez);
                }
                // degrees[ElList_[id]->allFriends_[0].size()]++;
            });
            for (int i = 0; i < temp.size(); i++)
                delete temp[i];
            temp.clear();
        }
        // Uncomment for debug mode
        // checkList1(ElList_);

        data_level0_memory_ = NULL;
        linkLists_ = NULL;

        enterpointId_ = enterpoint_->getId();

        if (skip_optimized_index) {
            LOG(LIB_INFO) << "searchMethod			  = " << searchMethod_;
            pmgr.CheckUnused();
            return;
        }

        int friendsSectionSize = (maxM0_ + 1) * sizeof(int);

        // Checking for maximum size of the datasection:
        int dataSectionSize = 1;
        for (int i = 0; i < ElList_.size(); i++) {
            if (ElList_[i]->getData()->bufferlength() > dataSectionSize)
                dataSectionSize = ElList_[i]->getData()->bufferlength();
        }

        // Selecting custom made functions
        dist_func_type_ = kDistTypeUnknown;

        // Although we removed double, let's keep this check here
        CHECK(sizeof(dist_t) == 4);


        const SpaceLp<dist_t>* pLpSpace = dynamic_cast<const SpaceLp<dist_t>*>(&space_);

        fstdistfunc_ = nullptr;
        iscosine_ = false;
        searchMethod_ = 3; // The same for all "optimized" indices
        if (pLpSpace != nullptr) {
            if (pLpSpace->getP() == 2) {
                LOG(LIB_INFO) << "\nThe space is Euclidean";
                vectorlength_ = ((dataSectionSize - 16) >> 2);
                LOG(LIB_INFO) << "Vector length=" << vectorlength_;
                if (vectorlength_ % 16 == 0) {
                    LOG(LIB_INFO) << "Thus using an optimised function for base 16";
                    dist_func_type_ = kL2Sqr16Ext;
                } else {
                    LOG(LIB_INFO) << "Thus using function with any base";
                    dist_func_type_ = kL2SqrExt;
                }
            } else if (pLpSpace->getP() == 1) {
                dist_func_type_ = kL1Norm;
            } else if (pLpSpace->getP() == -1) {
                dist_func_type_ = kLInfNorm;
            }
        } else if (dynamic_cast<const SpaceCosineSimilarity<dist_t>*>(&space_) != nullptr) {
            LOG(LIB_INFO) << "\nThe vector space is " << space_.StrDesc();
            vectorlength_ = ((dataSectionSize - 16) >> 2);
            LOG(LIB_INFO) << "Vector length=" << vectorlength_;
            dist_func_type_ = kNormCosine;
        } else if (dynamic_cast<const SpaceNegativeScalarProduct<dist_t>*>(&space_) != nullptr) {
            LOG(LIB_INFO) << "\nThe space is " << SPACE_NEGATIVE_SCALAR;
            vectorlength_ = ((dataSectionSize - 16) >> 2);
            LOG(LIB_INFO) << "Vector length=" << vectorlength_;
            dist_func_type_ = kNegativeDotProduct;
        }

        fstdistfunc_ = getDistFunc(dist_func_type_);
        iscosine_ = (dist_func_type_ == kNormCosine);

        if (fstdistfunc_ == nullptr) {
            LOG(LIB_INFO) << "No appropriate custom distance function for " << space_.StrDesc();
            searchMethod_ = 0;
            LOG(LIB_INFO) << "searchMethod			  = " << searchMethod_;
            pmgr.CheckUnused();
            return; // No optimized index
        }
        CHECK(dist_func_type_ != kDistTypeUnknown);

        pmgr.CheckUnused();
        LOG(LIB_INFO) << "searchMethod			  = " << searchMethod_;
        memoryPerObject_ = dataSectionSize + friendsSectionSize;

        size_t total_memory_allocated = (memoryPerObject_ * ElList_.size());
        // we allocate a few extra bytes to prevent prefetch from accessing out of range memory
        data_level0_memory_ = (char *)malloc((memoryPerObject_ * ElList_.size()) + EXTRA_MEM_PAD_SIZE);
        CHECK(data_level0_memory_);

        offsetLevel0_ = dataSectionSize;
        offsetData_ = 0;

        memset(data_level0_memory_, 1, memoryPerObject_ * ElList_.size());
        LOG(LIB_INFO) << "Making optimized index";
        data_rearranged_.resize(ElList_.size());
        for (long i = 0; i < ElList_.size(); i++) {
            ElList_[i]->copyDataAndLevel0LinksToOptIndex(
                data_level0_memory_ + (size_t)i * memoryPerObject_, offsetLevel0_, offsetData_);
            data_rearranged_[i] = new Object(data_level0_memory_ + (i)*memoryPerObject_ + offsetData_);
        };
        ////////////////////////////////////////////////////////////////////////
        //
        // The next step is needed only fos cosine similarity space
        // All vectors are normalized, so we don't have to normalize them later
        //
        ////////////////////////////////////////////////////////////////////////
        if (iscosine_) {
            for (long i = 0; i < ElList_.size(); i++) {
                float *v = (float *)(data_level0_memory_ + (size_t)i * memoryPerObject_ + offsetData_ + 16);
                NormalizeVect(v, vectorlength_);
            };
        }

        /////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////
        // we allocate a few extra bytes to prevent prefetch from accessing out of range memory
        linkLists_ = (char **)malloc((sizeof(void *) * ElList_.size()) + EXTRA_MEM_PAD_SIZE);
        CHECK(linkLists_);
        for (long i = 0; i < ElList_.size(); i++) {
            if (ElList_[i]->level < 1) {
                linkLists_[i] = nullptr;
                continue;
            }
            // TODO Can this one overflow? I really doubt
            SIZEMASS_TYPE sizemass = ((ElList_[i]->level) * (maxM_ + 1)) * sizeof(int);
            total_memory_allocated += sizemass;
            char *linkList = (char *)malloc(sizemass);
            CHECK(linkList);
            linkLists_[i] = linkList;
            ElList_[i]->copyHigherLevelLinksToOptIndex(linkList, 0);
        };

        LOG(LIB_INFO) << "Finished making optimized index";
        LOG(LIB_INFO) << "Maximum level = " << enterpoint_->level;
        LOG(LIB_INFO) << "Total memory allocated for optimized index+data: " << (total_memory_allocated >> 20) << " Mb";
    }

    template <typename dist_t>
    void
    Hnsw<dist_t>::SetQueryTimeParams(const AnyParams &QueryTimeParams)
    {
        AnyParamManager pmgr(QueryTimeParams);

        if (pmgr.hasParam("ef") && pmgr.hasParam("efSearch")) {
            throw runtime_error("The user shouldn't specify parameters ef and efSearch at the same time (they are synonyms)");
        }

        // ef and efSearch are going to be parameter-synonyms with the default value 20
        pmgr.GetParamOptional("ef", ef_, 20);
        pmgr.GetParamOptional("efSearch", ef_, ef_);

        int tmp;
        pmgr.GetParamOptional(
            "searchMethod", tmp, 0); // this is just to prevent terminating the program when searchMethod is specified

        string tmps;
        pmgr.GetParamOptional("algoType", tmps, "hybrid");
        ToLower(tmps);
        if (tmps == "v1merge")
            searchAlgoType_ = kV1Merge;
        else if (tmps == "old")
            searchAlgoType_ = kOld;
        else if (tmps == "hybrid")
            searchAlgoType_ = kHybrid;
        else {
            throw runtime_error("algoType should be one of the following: old, v1merge");
        }

        pmgr.CheckUnused();
        LOG(LIB_INFO) << "Set HNSW query-time parameters:";
        LOG(LIB_INFO) << "ef(Search)         =" << ef_;
        LOG(LIB_INFO) << "algoType           =" << searchAlgoType_;
    }

    template <typename dist_t>
    const std::string
    Hnsw<dist_t>::StrDesc() const
    {
        return METH_HNSW;
    }

    template <typename dist_t> Hnsw<dist_t>::~Hnsw()
    {
        delete visitedlistpool;
        if (data_level0_memory_)
            free(data_level0_memory_);
        if (linkLists_) {
            for (int i = 0; i < data_rearranged_.size(); i++) {
                if (linkLists_[i])
                    free(linkLists_[i]);
            }
            free(linkLists_);
        }
        for (int i = 0; i < ElList_.size(); i++)
            delete ElList_[i];
        for (const Object *p : data_rearranged_)
            delete p;
    }

    template <typename dist_t>
    void
    Hnsw<dist_t>::add(const Space<dist_t> *space, HnswNode *NewElement)
    {
        int curlevel = getRandomLevel(mult_);
        unique_lock<mutex> *lock = nullptr;
        if (curlevel > maxlevel_)
            lock = new unique_lock<mutex>(MaxLevelGuard_);

        NewElement->init(curlevel, maxM_, maxM0_);

        int maxlevelcopy = maxlevel_;
        HnswNode *ep = enterpoint_;
        if (curlevel < maxlevelcopy) {
            const Object *currObj = ep->getData();

            dist_t d = space->IndexTimeDistance(NewElement->getData(), currObj);
            dist_t curdist = d;
            HnswNode *curNode = ep;
            for (int level = maxlevelcopy; level > curlevel; level--) {
                bool changed = true;
                while (changed) {
                    changed = false;
                    unique_lock<mutex> lock(curNode->accessGuard_);
                    const vector<HnswNode *> &neighbor = curNode->getAllFriends(level);
                    int size = neighbor.size();
                    for (int i = 0; i < size; i++) {
                        HnswNode *node = neighbor[i];
                        PREFETCH((char *)(node)->getData(), _MM_HINT_T0);
                    }
                    for (int i = 0; i < size; i++) {
                        currObj = (neighbor[i])->getData();
                        d = space->IndexTimeDistance(NewElement->getData(), currObj);
                        if (d < curdist) {
                            curdist = d;
                            curNode = neighbor[i];
                            changed = true;
                        }
                    }
                }
            }
            ep = curNode;
        }

        for (int level = min(curlevel, maxlevelcopy); level >= 0; level--) {
            priority_queue<HnswNodeDistCloser<dist_t>> resultSet;
            kSearchElementsWithAttemptsLevel(space, NewElement->getData(), efConstruction_, resultSet, ep, level);

            switch (delaunay_type_) {
            case 0:
                while (resultSet.size() > M_)
                    resultSet.pop();
                break;
            case 1:
                NewElement->getNeighborsByHeuristic1(resultSet, M_, space);
                break;
            case 2:
                NewElement->getNeighborsByHeuristic2(resultSet, M_, space, level);
                break;
            case 3:
                NewElement->getNeighborsByHeuristic3(resultSet, M_, space, level);
                break;
            }
            while (!resultSet.empty()) {
                ep = resultSet.top().getMSWNodeHier(); // memorizing the closest
                link(resultSet.top().getMSWNodeHier(), NewElement, level, space, delaunay_type_);
                resultSet.pop();
            }
        }
        if (curlevel > enterpoint_->level) {
            enterpoint_ = NewElement;
            maxlevel_ = curlevel;
        }
        if (lock != nullptr)
            delete lock;
    }

    template <typename dist_t>
    void
    Hnsw<dist_t>::kSearchElementsWithAttemptsLevel(const Space<dist_t> *space, const Object *queryObj, size_t efConstruction,
                                                   priority_queue<HnswNodeDistCloser<dist_t>> &resultSet, HnswNode *ep,
                                                   int level) const
    {
#if EXTEND_USE_EXTENDED_NEIGHB_AT_CONSTR != 0
        priority_queue<HnswNodeDistCloser<dist_t>> fullResultSet;
#endif

#if USE_BITSET_FOR_INDEXING
        VisitedList *vl = visitedlistpool->getFreeVisitedList();
        vl_type *mass = vl->mass;
        vl_type curV = vl->curV;
#else
        unordered_set<HnswNode *> visited;
#endif
        HnswNode *provider = ep;
        priority_queue<HnswNodeDistFarther<dist_t>> candidateSet;
        dist_t d = space->IndexTimeDistance(queryObj, provider->getData());
        HnswNodeDistFarther<dist_t> ev(d, provider);

        candidateSet.push(ev);
        resultSet.emplace(d, provider);

#if EXTEND_USE_EXTENDED_NEIGHB_AT_CONSTR != 0
        fullResultSet.emplace(d, provider);
#endif

#if USE_BITSET_FOR_INDEXING
        size_t nodeId = provider->getId();
        mass[nodeId] = curV;
#else
        visited.insert(provider);
#endif

        while (!candidateSet.empty()) {
            const HnswNodeDistFarther<dist_t> &currEv = candidateSet.top();
            dist_t lowerBound = resultSet.top().getDistance();

            /*
            * Check if we reached a local minimum.
            */
            if (currEv.getDistance() > lowerBound) {
                break;
            }
            HnswNode *currNode = currEv.getMSWNodeHier();

            /*
            * This lock protects currNode from being modified
            * while we are accessing elements of currNode.
            */
            unique_lock<mutex> lock(currNode->accessGuard_);
            const vector<HnswNode *> &neighbor = currNode->getAllFriends(level);

            // Can't access curEv anymore! The reference would become invalid
            candidateSet.pop();

            // calculate distance to each neighbor
            for (auto iter = neighbor.begin(); iter != neighbor.end(); ++iter) {
                PREFETCH((char *)(*iter)->getData(), _MM_HINT_T0);
            }

            for (auto iter = neighbor.begin(); iter != neighbor.end(); ++iter) {
#if USE_BITSET_FOR_INDEXING
                size_t nodeId = (*iter)->getId();
                if (mass[nodeId] != curV) {
                    mass[nodeId] = curV;
#else
                if (visited.find((*iter)) == visited.end()) {
                    visited.insert(*iter);
#endif
                    d = space->IndexTimeDistance(queryObj, (*iter)->getData());
                    HnswNodeDistFarther<dist_t> evE1(d, *iter);

#if EXTEND_USE_EXTENDED_NEIGHB_AT_CONSTR != 0
                    fullResultSet.emplace(d, *iter);
#endif

                    if (resultSet.size() < efConstruction || resultSet.top().getDistance() > d) {
                        resultSet.emplace(d, *iter);
                        candidateSet.push(evE1);
                        if (resultSet.size() > efConstruction) {
                            resultSet.pop();
                        }
                    }
                }
            }
        }

#if EXTEND_USE_EXTENDED_NEIGHB_AT_CONSTR != 0
        resultSet.swap(fullResultSet);
#endif

#if USE_BITSET_FOR_INDEXING
        visitedlistpool->releaseVisitedList(vl);
#endif
    }

    template <typename dist_t>
    void
    Hnsw<dist_t>::Search(RangeQuery<dist_t> *query, IdType) const
    {
        throw runtime_error("Range search is not supported!");
    }

    template <typename dist_t>
    void
    Hnsw<dist_t>::Search(KNNQuery<dist_t> *query, IdType) const
    {
        if (this->data_.empty() && this->data_rearranged_.empty()) {
          return;
        }
        bool useOld = searchAlgoType_ == kOld || (searchAlgoType_ == kHybrid && ef_ >= 1000);
        // cout << "Ef = " << ef_ << " use old = " << useOld << endl;
        switch (searchMethod_) {
        case 0:
            /// Basic search using Nmslib data structure:
            if (useOld)
                const_cast<Hnsw *>(this)->baseSearchAlgorithmOld(query);
            else
                const_cast<Hnsw *>(this)->baseSearchAlgorithmV1Merge(query);
            break;
        case 3:
        case 4:
            /// Basic search using optimized index for l2, cosine, negative dot product
            if (useOld)
                const_cast<Hnsw *>(this)->SearchOld(query, iscosine_);
            else
                const_cast<Hnsw *>(this)->SearchV1Merge(query, iscosine_);
            break;
        default:
                throw runtime_error("Invalid searchMethod: " + ConvertToString(searchMethod_));
            break;
        };
    }

    template <typename dist_t>
    void
    Hnsw<dist_t>::SaveIndex(const string &location) {
        std::ofstream output(location,
                             std::ios::binary /* text files can be opened in binary mode as well */);
        CHECK_MSG(output, "Cannot open file '" + location + "' for writing");
        output.exceptions(ios::badbit | ios::failbit);

        unsigned int optimIndexFlag = data_level0_memory_ != nullptr;

        writeBinaryPOD(output, optimIndexFlag);

        if (!optimIndexFlag) {
#if USE_TEXT_REGULAR_INDEX
            SaveRegularIndexText(output);
#else

            SaveRegularIndexBin(output);
#endif
        } else {
            SaveOptimizedIndex(output);
        }

        output.close();
    }

    template <typename dist_t>
    void
    Hnsw<dist_t>::SaveOptimizedIndex(std::ostream& output) {
        totalElementsStored_ = ElList_.size();

        writeBinaryPOD(output, totalElementsStored_);
        writeBinaryPOD(output, memoryPerObject_);
        writeBinaryPOD(output, offsetLevel0_);
        writeBinaryPOD(output, offsetData_);
        writeBinaryPOD(output, maxlevel_);
        writeBinaryPOD(output, enterpointId_);
        writeBinaryPOD(output, maxM_);
        writeBinaryPOD(output, maxM0_);
        writeBinaryPOD(output, dist_func_type_);
        writeBinaryPOD(output, searchMethod_);

        size_t data_plus_links0_size = memoryPerObject_ * totalElementsStored_;
        LOG(LIB_INFO) << "writing " << data_plus_links0_size << " bytes";
        output.write(data_level0_memory_, data_plus_links0_size);

        // output.write(data_level0_memory_, memoryPerObject_*totalElementsStored_);

        // size_t total_memory_allocated = 0;

        for (size_t i = 0; i < totalElementsStored_; i++) {
            // TODO Can this one overflow? I really doubt
            SIZEMASS_TYPE sizemass = ((ElList_[i]->level) * (maxM_ + 1)) * sizeof(int);
            writeBinaryPOD(output, sizemass);
            if ((sizemass))
                output.write(linkLists_[i], sizemass);
        };

    }

    template <typename dist_t>
    void
    Hnsw<dist_t>::SaveRegularIndexBin(std::ostream& output) {
        totalElementsStored_ = ElList_.size();

        writeBinaryPOD(output, totalElementsStored_);
        writeBinaryPOD(output, maxlevel_);
        writeBinaryPOD(output, enterpointId_);
        writeBinaryPOD(output, M_);
        writeBinaryPOD(output, maxM_);
        writeBinaryPOD(output, maxM0_);

        for (unsigned i = 0; i < totalElementsStored_; ++i) {
            const HnswNode& node = *ElList_[i];
            unsigned currlevel = node.level;
            CHECK(currlevel + 1 == node.allFriends_.size());
            /*
             * This check strangely fails ...
            CHECK_MSG(maxlevel_ >= currlevel, ""
                    "maxlevel_ (" + ConvertToString(maxlevel_) + ") < node.allFriends_.size() (" + ConvertToString(currlevel));
                    */
            writeBinaryPOD(output, currlevel);
            for (unsigned level = 0; level <= currlevel; ++level) {
                const auto& friends = node.allFriends_[level];
                unsigned friendQty = friends.size();
                writeBinaryPOD(output, friendQty);
                for (unsigned k = 0; k < friendQty; ++k) {
                    IdType friendId = friends[k]->id_;
                    writeBinaryPOD(output, friendId);
                }
            }
        }
    }

    template <typename dist_t>
    void
    Hnsw<dist_t>::SaveRegularIndexText(std::ostream& output) {

        size_t lineNum = 0;

        totalElementsStored_ = ElList_.size();

        WriteField(output, TOTAL_QTY, totalElementsStored_); lineNum++;
        WriteField(output, MAX_LEVEL, maxlevel_); lineNum++;
        WriteField(output, ENTER_POINT_ID, enterpointId_); lineNum++;
        WriteField(output, FIELD_M, M_); lineNum++;
        WriteField(output, FIELD_MAX_M, maxM_); lineNum++;
        WriteField(output, FIELD_MAX_M0, maxM0_); lineNum++;

        vector<IdType> friendIds;

        for (unsigned i = 0; i < totalElementsStored_; ++i) {
            const HnswNode& node = *ElList_[i];
            unsigned currlevel = node.level;
            CHECK(currlevel + 1 == node.allFriends_.size());
            /*
             * This check strangely fails ...
            CHECK_MSG(maxlevel_ >= currlevel, ""
                    "maxlevel_ (" + ConvertToString(maxlevel_) + ") < node.allFriends_.size() (" + ConvertToString(currlevel));
                    */
            WriteField(output, CURR_LEVEL, currlevel); lineNum++;
            for (unsigned level = 0; level <= currlevel; ++level) {
                const auto& friends = node.allFriends_[level];
                unsigned friendQty = friends.size();

                friendIds.resize(friendQty);
                for (unsigned k = 0; k < friendQty; ++k) {
                    friendIds[k] = friends[k]->id_;
                }
                output << MergeIntoStr(friendIds, ' ') << endl; lineNum++;
            }
        }
        WriteField(output, LINE_QTY, lineNum);
    }


    template <typename dist_t>
    void
    Hnsw<dist_t>::LoadRegularIndexText(std::istream& input) {
        LOG(LIB_INFO) << "Loading regular index.";
        size_t lineNum = 0;
        ReadField(input, TOTAL_QTY, totalElementsStored_); lineNum++;
        ReadField(input, MAX_LEVEL, maxlevel_); lineNum++;
        ReadField(input, ENTER_POINT_ID, enterpointId_); lineNum++;
        ReadField(input, FIELD_M, M_); lineNum++;
        ReadField(input, FIELD_MAX_M, maxM_); lineNum++;
        ReadField(input, FIELD_MAX_M0, maxM0_); lineNum++;

        fstdistfunc_ = nullptr;
        dist_func_type_ = kDistTypeUnknown;
        searchMethod_ = 0;

        ElList_.resize(totalElementsStored_);
        for (unsigned id = 0; id < totalElementsStored_; ++id) {
            ElList_[id] = new HnswNode(this->data_[id], id);
        }

        enterpoint_ = ElList_[enterpointId_];

        string line;
        vector<IdType> friendIds;
        for (unsigned id = 0; id < totalElementsStored_; ++id) {
            HnswNode& node = *ElList_[id];
            unsigned currlevel;
            ReadField(input, CURR_LEVEL, currlevel); lineNum++;
            node.level = currlevel;
            node.allFriends_.resize(currlevel + 1);
            for (unsigned level = 0; level <= currlevel; ++level) {
                CHECK_MSG(getline(input, line),
                          "Failed to read line #" + ConvertToString(lineNum)); lineNum++;
                CHECK_MSG(SplitStr(line, friendIds, ' '),
                          "Failed to extract neighbor IDs from line #" + ConvertToString(lineNum));

                unsigned friendQty = friendIds.size();

                auto& friends = node.allFriends_[level];
                friends.resize(friendQty);
                for (unsigned k = 0; k < friendQty; ++k) {
                    IdType friendId = friendIds[k];
                    CHECK_MSG(friendId >= 0 && friendId < totalElementsStored_,
                              "Invalid friendId = " + ConvertToString(friendId) + " for node id: " + ConvertToString(id));
                    friends[k] = ElList_[friendId];
                }
            }
        }
        size_t ExpLineNum;
        ReadField(input, LINE_QTY, ExpLineNum);
        CHECK_MSG(lineNum == ExpLineNum,
                  DATA_MUTATION_ERROR_MSG + " (expected number of lines " + ConvertToString(ExpLineNum) +
                  " read so far doesn't match the number of read lines: " + ConvertToString(lineNum));
    }


    template <typename dist_t>
    void
    Hnsw<dist_t>::LoadRegularIndexBin(std::istream& input) {
        LOG(LIB_INFO) << "Loading regular index.";
        readBinaryPOD(input, totalElementsStored_);
        readBinaryPOD(input, maxlevel_);
        readBinaryPOD(input, enterpointId_);
        readBinaryPOD(input, M_);
        readBinaryPOD(input, maxM_);
        readBinaryPOD(input, maxM0_);

        fstdistfunc_ = nullptr;
        dist_func_type_ = kDistTypeUnknown;
        searchMethod_ = 0;

        CHECK_MSG(totalElementsStored_ == this->data_.size(),
             "The number of stored elements " + ConvertToString(totalElementsStored_) +
             " doesn't match the number of data points " + ConvertToString(this->data_.size()) +
             "! Did you forget to re-load data?")

        ElList_.resize(totalElementsStored_);
        for (unsigned id = 0; id < totalElementsStored_; ++id) {
            ElList_[id] = new HnswNode(this->data_[id], id);
        }

        if (!ElList_.empty()) {
            enterpoint_ = ElList_[enterpointId_];
        }

        for (unsigned id = 0; id < totalElementsStored_; ++id) {
            HnswNode& node = *ElList_[id];
            unsigned currlevel;
            readBinaryPOD(input, currlevel);
            node.level = currlevel;
            node.allFriends_.resize(currlevel + 1);
            for (unsigned level = 0; level <= currlevel; ++level) {
                auto& friends = node.allFriends_[level];
                unsigned friendQty;
                readBinaryPOD(input, friendQty);

                friends.resize(friendQty);
                for (unsigned k = 0; k < friendQty; ++k) {
                    IdType friendId;
                    readBinaryPOD(input, friendId);
                    CHECK_MSG(friendId >= 0 && friendId < totalElementsStored_,
                             "Invalid friendId = " + ConvertToString(friendId) + " for node id: " + ConvertToString(id));
                    friends[k] = ElList_[friendId];
                }
            }
        }
    }

    template <typename dist_t>
    void
    Hnsw<dist_t>::LoadIndex(const string &location) {
        LOG(LIB_INFO) << "Loading index from " << location;
        std::ifstream input(location, 
                            std::ios::binary); /* text files can be opened in binary mode as well */
        CHECK_MSG(input, "Cannot open file '" + location + "' for reading");

        input.exceptions(ios::badbit | ios::failbit);

#if USE_TEXT_REGULAR_INDEX
        LoadRegularIndexText(input);
#else
        unsigned int optimIndexFlag= 0;

        readBinaryPOD(input, optimIndexFlag);

        if (!optimIndexFlag) {
            LoadRegularIndexBin(input);
        } else {
            LoadOptimizedIndex(input);
        }
#endif
        input.close();

        LOG(LIB_INFO) << "Finished loading index";
        visitedlistpool = new VisitedListPool(1, totalElementsStored_);


    }


    template <typename dist_t>
    void
    Hnsw<dist_t>::LoadOptimizedIndex(std::istream& input) {
        LOG(LIB_INFO) << "Loading optimized index.";

        readBinaryPOD(input, totalElementsStored_);
        readBinaryPOD(input, memoryPerObject_);
        readBinaryPOD(input, offsetLevel0_);
        readBinaryPOD(input, offsetData_);
        readBinaryPOD(input, maxlevel_);
        readBinaryPOD(input, enterpointId_);
        readBinaryPOD(input, maxM_);
        readBinaryPOD(input, maxM0_);
        readBinaryPOD(input, dist_func_type_);
        readBinaryPOD(input, searchMethod_);

        LOG(LIB_INFO) << "searchMethod: " << searchMethod_;

        fstdistfunc_ = getDistFunc(dist_func_type_);
        iscosine_ = (dist_func_type_ == kNormCosine);
        CHECK_MSG(fstdistfunc_ != nullptr, "Unknown distance function code: " + ConvertToString(dist_func_type_));

        //        LOG(LIB_INFO) << input.tellg();
        LOG(LIB_INFO) << "Total: " << totalElementsStored_ << ", Memory per object: " << memoryPerObject_;
        size_t data_plus_links0_size = memoryPerObject_ * totalElementsStored_;
        // we allocate a few extra bytes to prevent prefetch from accessing out of range memory
        data_level0_memory_ = (char *)malloc(data_plus_links0_size + EXTRA_MEM_PAD_SIZE);
        CHECK(data_level0_memory_);
        input.read(data_level0_memory_, data_plus_links0_size);
        // we allocate a few extra bytes to prevent prefetch from accessing out of range memory
        linkLists_ = (char **)malloc( (sizeof(void *) * totalElementsStored_) + EXTRA_MEM_PAD_SIZE);
        CHECK(linkLists_);

        data_rearranged_.resize(totalElementsStored_);

        for (size_t i = 0; i < totalElementsStored_; i++) {
            SIZEMASS_TYPE linkListSize;
            readBinaryPOD(input, linkListSize);

            if (linkListSize == 0) {
                linkLists_[i] = nullptr;
            } else {
                linkLists_[i] = (char *)malloc(linkListSize);
                CHECK(linkLists_[i]);
                input.read(linkLists_[i], linkListSize);
            }
            data_rearranged_[i] = new Object(data_level0_memory_ + (i)*memoryPerObject_ + offsetData_);
        }

    }

    template <typename dist_t>
    void
    Hnsw<dist_t>::baseSearchAlgorithmOld(KNNQuery<dist_t> *query)
    {
        VisitedList *vl = visitedlistpool->getFreeVisitedList();
        vl_type *massVisited = vl->mass;
        vl_type currentV = vl->curV;

        HnswNode *provider;
        int maxlevel1 = enterpoint_->level;
        provider = enterpoint_;

        const Object *currObj = provider->getData();

        dist_t d = query->DistanceObjLeft(currObj);
        dist_t curdist = d;
        HnswNode *curNode = provider;
        for (int i = maxlevel1; i > 0; i--) {
            bool changed = true;
            while (changed) {
                changed = false;

                const vector<HnswNode *> &neighbor = curNode->getAllFriends(i);
                for (auto iter = neighbor.begin(); iter != neighbor.end(); ++iter) {
                    PREFETCH((char *)(*iter)->getData(), _MM_HINT_T0);
                }
                for (auto iter = neighbor.begin(); iter != neighbor.end(); ++iter) {
                    currObj = (*iter)->getData();
                    d = query->DistanceObjLeft(currObj);
                    if (d < curdist) {
                        curdist = d;
                        curNode = *iter;
                        changed = true;
                    }
                }
            }
        }

        priority_queue<HnswNodeDistFarther<dist_t>> candidateQueue;   // the set of elements which we can use to evaluate
        priority_queue<HnswNodeDistCloser<dist_t>> closestDistQueue1; // The set of closest found elements

        HnswNodeDistFarther<dist_t> ev(curdist, curNode);
        candidateQueue.emplace(curdist, curNode);
        closestDistQueue1.emplace(curdist, curNode);

        query->CheckAndAddToResult(curdist, curNode->getData());
        massVisited[curNode->getId()] = currentV;
        // visitedQueue.insert(curNode->getId());

        ////////////////////////////////////////////////////////////////////////////////
        // PHASE TWO OF THE SEARCH
        // Extraction of the neighborhood to find k nearest neighbors.
        ////////////////////////////////////////////////////////////////////////////////

        while (!candidateQueue.empty()) {
            auto iter = candidateQueue.top(); // This one was already compared to the query
            const HnswNodeDistFarther<dist_t> &currEv = iter;
            // Check condition to end the search
            dist_t lowerBound = closestDistQueue1.top().getDistance();
            if (currEv.getDistance() > lowerBound) {
                break;
            }

            HnswNode *initNode = currEv.getMSWNodeHier();
            candidateQueue.pop();

            const vector<HnswNode *> &neighbor = (initNode)->getAllFriends(0);

            size_t curId;

            for (auto iter = neighbor.begin(); iter != neighbor.end(); ++iter) {
                PREFETCH((char *)(*iter)->getData(), _MM_HINT_T0);
                PREFETCH((char *)(massVisited + (*iter)->getId()), _MM_HINT_T0);
            }
            // calculate distance to each neighbor
            for (auto iter = neighbor.begin(); iter != neighbor.end(); ++iter) {
                curId = (*iter)->getId();

                if (!(massVisited[curId] == currentV)) {
                    massVisited[curId] = currentV;
                    currObj = (*iter)->getData();
                    d = query->DistanceObjLeft(currObj);
                    if (closestDistQueue1.top().getDistance() > d || closestDistQueue1.size() < ef_) {
                        {
                            query->CheckAndAddToResult(d, currObj);
                            candidateQueue.emplace(d, *iter);
                            closestDistQueue1.emplace(d, *iter);
                            if (closestDistQueue1.size() > ef_) {
                                closestDistQueue1.pop();
                            }
                        }
                    }
                }
            }
        }
        visitedlistpool->releaseVisitedList(vl);
    }

    template <typename dist_t>
    void
    Hnsw<dist_t>::baseSearchAlgorithmV1Merge(KNNQuery<dist_t> *query)
    {
        VisitedList *vl = visitedlistpool->getFreeVisitedList();
        vl_type *massVisited = vl->mass;
        vl_type currentV = vl->curV;

        HnswNode *provider;
        int maxlevel1 = enterpoint_->level;
        provider = enterpoint_;

        const Object *currObj = provider->getData();

        dist_t d = query->DistanceObjLeft(currObj);
        dist_t curdist = d;
        HnswNode *curNode = provider;
        for (int i = maxlevel1; i > 0; i--) {
            bool changed = true;
            while (changed) {
                changed = false;

                const vector<HnswNode *> &neighbor = curNode->getAllFriends(i);
                for (auto iter = neighbor.begin(); iter != neighbor.end(); ++iter) {
                    PREFETCH((char *)(*iter)->getData(), _MM_HINT_T0);
                }
                for (auto iter = neighbor.begin(); iter != neighbor.end(); ++iter) {
                    currObj = (*iter)->getData();
                    d = query->DistanceObjLeft(currObj);
                    if (d < curdist) {
                        curdist = d;
                        curNode = *iter;
                        changed = true;
                    }
                }
            }
        }

        SortArrBI<dist_t, HnswNode *> sortedArr(max<size_t>(ef_, query->GetK()));
        sortedArr.push_unsorted_grow(curdist, curNode);

        int_fast32_t currElem = 0;

        typedef typename SortArrBI<dist_t, HnswNode *>::Item QueueItem;
        vector<QueueItem> &queueData = sortedArr.get_data();
        vector<QueueItem> itemBuff(1 + max(maxM_, maxM0_));

        massVisited[curNode->getId()] = currentV;
        // visitedQueue.insert(curNode->getId());

        ////////////////////////////////////////////////////////////////////////////////
        // PHASE TWO OF THE SEARCH
        // Extraction of the neighborhood to find k nearest neighbors.
        ////////////////////////////////////////////////////////////////////////////////

        while (currElem < min(sortedArr.size(), ef_)) {
            auto &e = queueData[currElem];
            CHECK(!e.used);
            e.used = true;
            HnswNode *initNode = e.data;
            ++currElem;

            size_t itemQty = 0;
            dist_t topKey = sortedArr.top_key();

            const vector<HnswNode *> &neighbor = (initNode)->getAllFriends(0);

            size_t curId;

            for (auto iter = neighbor.begin(); iter != neighbor.end(); ++iter) {
                PREFETCH((char *)(*iter)->getData(), _MM_HINT_T0);
                IdType curId = (*iter)->getId();
                CHECK(curId >= 0 && curId < this->data_.size());
                PREFETCH((char *)(massVisited + curId), _MM_HINT_T0);
            }
            // calculate distance to each neighbor
            for (auto iter = neighbor.begin(); iter != neighbor.end(); ++iter) {
                curId = (*iter)->getId();

                if (!(massVisited[curId] == currentV)) {
                    massVisited[curId] = currentV;
                    currObj = (*iter)->getData();
                    d = query->DistanceObjLeft(currObj);

                    if (d < topKey || sortedArr.size() < ef_) {
                        CHECK_MSG(itemBuff.size() > itemQty,
                                  "Perhaps a bug: buffer size is not enough " + 
                                  ConvertToString(itemQty) + " >= " + ConvertToString(itemBuff.size()));
                        itemBuff[itemQty++] = QueueItem(d, *iter);
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
                        // LOG(LIB_INFO) << "@@@ " << currElem << " -> " << insIndex;
                        currElem = insIndex;
                    }
                } else {
                    for (size_t ii = 0; ii < itemQty; ++ii) {
                        size_t insIndex = sortedArr.push_or_replace_non_empty_exp(itemBuff[ii].key, itemBuff[ii].data);

                        if (insIndex < currElem) {
                            // LOG(LIB_INFO) << "@@@ " << currElem << " -> " << insIndex;
                            currElem = insIndex;
                        }
                    }
                }
            }
            // To ensure that we either reach the end of the unexplored queue or currElem points to the first unused element
            while (currElem < sortedArr.size() && queueData[currElem].used == true)
                ++currElem;
        }

        for (uint_fast32_t i = 0; i < query->GetK() && i < sortedArr.size(); ++i) {
            query->CheckAndAddToResult(queueData[i].key, queueData[i].data->getData());
        }

        visitedlistpool->releaseVisitedList(vl);
    }


    template class Hnsw<float>;
    template class Hnsw<int>;
}
