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
#include <cmath>
#include <memory>
#include <iostream>
#include <cstddef>

#include "portable_prefetch.h"
#if defined(_WIN32) || defined(WIN32)
#include <intrin.h>
#endif

#include "portable_simd.h"
#include "space.h"
#include "knnquery.h"
#include "knnqueue.h"
#include "rangequery.h"
#include "ported_boost_progress.h"
#include "method/small_world_rand.h"
#include "sort_arr_bi.h"
#include "thread_pool.h"

#include <vector>
#include <set>
#include <map>
#include <sstream>
#include <typeinfo>

#define MERGE_BUFFER_ALGO_SWITCH_THRESHOLD 100
#define MAX_ID_TO_SIZE_RATIO               1.5

namespace similarity {

using namespace std;

template <typename dist_t>
struct IndexThreadParamsSW {
  const Space<dist_t>&                        space_;
  SmallWorldRand<dist_t>&                     index_;
  IdType                                      startNodeId_;
  const ObjectVector&                         batchData_;
  size_t                                      index_every_;
  size_t                                      out_of_;
  ProgressDisplay*                            progress_bar_;
  mutex&                                      display_mutex_;
  size_t                                      progress_update_qty_;
  
  IndexThreadParamsSW(
                     const Space<dist_t>&             space,
                     SmallWorldRand<dist_t>&          index, 
                     IdType                           startNodeId,
                     const ObjectVector&              batchData,
                     size_t                           index_every,
                     size_t                           out_of,
                     ProgressDisplay*                 progress_bar,
                     mutex&                           display_mutex,
                     size_t                           progress_update_qty
                      ) : 
                     space_(space),
                     index_(index), 
                     startNodeId_(startNodeId),
                     batchData_(batchData),
                     index_every_(index_every),
                     out_of_(out_of),
                     progress_bar_(progress_bar),
                     display_mutex_(display_mutex),
                     progress_update_qty_(progress_update_qty)
                     { }
};

template <typename dist_t>
struct IndexThreadSW {
  void operator()(IndexThreadParamsSW<dist_t>& prm) {
    ProgressDisplay*  progress_bar = prm.progress_bar_;
    mutex&            display_mutex(prm.display_mutex_); 
    /* 
     * Skip the first element, it was added already in the AddBatch
     */
    size_t futureNextNodeId = prm.startNodeId_ + prm.batchData_.size();

    size_t nextQty = prm.progress_update_qty_;
    for (size_t id = 1; id < prm.batchData_.size(); ++id) {
      if (prm.index_every_ == id % prm.out_of_) {
        MSWNode* node = new MSWNode(prm.batchData_[id], id + prm.startNodeId_);
        prm.index_.add(node, futureNextNodeId);
      
        if ((id + 1 >= min(prm.batchData_.size(), nextQty)) && progress_bar) {
          unique_lock<mutex> lock(display_mutex);
          (*progress_bar) += (nextQty - progress_bar->count());
          nextQty += prm.progress_update_qty_;
        }
      }
    }
    if (progress_bar) {
      unique_lock<mutex> lock(display_mutex);
      (*progress_bar) += (progress_bar->expected_count() - progress_bar->count());
    }
  }
};

template <typename dist_t>
SmallWorldRand<dist_t>::SmallWorldRand(bool PrintProgress,
                                       const Space<dist_t>& space,
                                       const ObjectVector& data) : 
                                       Index<dist_t>(data), space_(space), PrintProgress_(PrintProgress), use_proxy_dist_(false) {}

template <typename dist_t>
void SmallWorldRand<dist_t>::UpdateNextNodeId(size_t newNextNodeId)
{
  NextNodeId_ = newNextNodeId;
}

template <typename dist_t>
void SmallWorldRand<dist_t>::CompactIdsIfNeeded()
{
  if (ElList_.size() * MAX_ID_TO_SIZE_RATIO < NextNodeId_) {
    LOG(LIB_INFO) << "ID compactification started";
    NextNodeId_ = 0;
    for (auto it : ElList_) {
      MSWNode* node = it.second;
      node->setId(NextNodeId_);
      ++NextNodeId_;
    }
    LOG(LIB_INFO) << "ID compactification ended";
  }
}

template <typename dist_t>
void SmallWorldRand<dist_t>::AddBatch(const ObjectVector& batchData, 
                                      bool bPrintProgress,
                                      bool bCheckIDs /* this is a debug flag only, turning it on may affect performance */)
{
  if (batchData.empty()) return;
  changedAfterCreateIndex_ = true;

  size_t futureNextNodeId = NextNodeId_ + batchData.size();

  LOG(LIB_INFO) << "Current nextNodeId: " << NextNodeId_ 
                << " futureNextNodeId + 1 after batch addition: " << futureNextNodeId;

  // 2) One entry should be added before all the threads are started, or else add() will not work properly  


  bool isEmpty = false;

  {
    unique_lock<mutex> lock(ElListGuard_);
    isEmpty = ElList_.empty();
  }
  int start_add=0;

  if (isEmpty){
    addCriticalSection(new MSWNode(batchData[0], NextNodeId_));
    start_add = 1;
  }


  unique_ptr<ProgressDisplay> progress_bar(bPrintProgress ?
                                new ProgressDisplay(batchData.size(), cerr)
                                :NULL);

  if (indexThreadQty_ <= 1) {
    // Skip the first element, one element is already added
    if (progress_bar) ++(*progress_bar);
    for (size_t id = start_add; id < batchData.size(); ++id) {
      MSWNode* node = new MSWNode(batchData[id], id + NextNodeId_);
      add(node, futureNextNodeId);
      if (progress_bar) ++(*progress_bar);
    }
  } else {
    vector<thread>                                    threads(indexThreadQty_);
    vector<shared_ptr<IndexThreadParamsSW<dist_t>>>   threadParams; 
    mutex                                             progressBarMutex;

    for (size_t i = 0; i < indexThreadQty_; ++i) {
      threadParams.push_back(shared_ptr<IndexThreadParamsSW<dist_t>>(
                              new IndexThreadParamsSW<dist_t>(space_, *this, NextNodeId_, 
                                                              batchData, 
                                                              i, indexThreadQty_,
                                                              progress_bar.get(), progressBarMutex, 200)));
    }
    for (size_t i = 0; i < indexThreadQty_; ++i) {
      threads[i] = thread(IndexThreadSW<dist_t>(), ref(*threadParams[i]));
    }
    for (size_t i = 0; i < indexThreadQty_; ++i) {
      threads[i].join();
    }
    LOG(LIB_INFO) << indexThreadQty_ << " indexing threads have finished";
  }
  UpdateNextNodeId(futureNextNodeId);
  CompactIdsIfNeeded();
  if (bCheckIDs) CheckIDs();
  LOG(LIB_INFO) << "The number of data points: " << ElList_.size() << " NextNodeId_ = " << NextNodeId_;
}

template <typename dist_t>
void SmallWorldRand<dist_t>::DeleteBatch(const ObjectVector& batchData, int delStrategy, bool checkIDs) {
  vector<IdType> batchIds;
  for (auto o : batchData) batchIds.push_back(o->id()); 
  DeleteBatch(batchIds, delStrategy, checkIDs);
}

template <typename dist_t>
void SmallWorldRand<dist_t>::DeleteBatch(const vector<IdType>& batchData, int delStrategyCode, bool checkIDs) {
  if (batchData.empty() ||  // 1. batch is empty
      0 == NextNodeId_      // 2. no data is indexed
      ) return;
  changedAfterCreateIndex_ = true;
  /* 
   * Done in several stages.
   * 1) Identifying entries to be deleted & deleting nodes from ElList_.
   * 2) Removing neighbors with subsequent patching
   * 3) Actually removing nodes from ElList_ and freeing memory.
   */
  // Stage 1. Identifying entries to be deleted.
  vector<MSWNode*>        vToPatchNodes;
  vector<MSWNode*>        vToDelNodes;
  vector<bool>            delNodesBitset(NextNodeId_);

  for (IdType objId : batchData) {
    const auto it = ElList_.find(objId);
    CHECK_MSG(it != ElList_.end(), "An attempt to delete a non-existing object with id=" + ConvertToString(objId));
    MSWNode* delNode=it->second;
    IdType   delNodeId = delNode->getId();
    CHECK(delNodeId < (ptrdiff_t)delNodesBitset.size());
    delNodesBitset[delNodeId]=true;
    vToDelNodes.push_back(delNode);
    ElList_.erase(it);
  }
  for (MSWNode* node: vToDelNodes) {
    for (MSWNode* pNeighbor : node->getAllFriends()) {
      IdType neighbNodeId = pNeighbor->getId();
      CHECK(neighbNodeId < (ptrdiff_t)delNodesBitset.size());
      if (!delNodesBitset.at(neighbNodeId))
        vToPatchNodes.push_back(pNeighbor);
    }
  }

  // vToPatchNodes can have duplicates, but we must process one node exactly ones
  sort(vToPatchNodes.begin(), vToPatchNodes.end());
  auto it = unique(vToPatchNodes.begin(), vToPatchNodes.end()); 
  vToPatchNodes.resize(distance(vToPatchNodes.begin(), it));

  LOG(LIB_INFO) << "The number of nodes that need patching: " << vToPatchNodes.size();
  // Stage 2. Removing neighbors & possibly patching
  PatchingStrategy patchStrat = static_cast<PatchingStrategy>(delStrategyCode);
  CHECK_MSG(patchStrat == kNone || patchStrat == kNeighborsOnly,
            "Unsupported patching strategy code: " + ConvertToString(delStrategyCode));

  queue<MSWNode*> toPatchQueue;
  for (MSWNode* node : vToPatchNodes) toPatchQueue.push(node);
  mutex mtx;
  vector<thread> threads;

  if (indexThreadQty_ <= 1) {
    LOG(LIB_INFO) << "Single threaded batch delete: " << vToPatchNodes.size();
    MSWNode* node = nullptr;
    vector<MSWNode*> cacheDelNode;
    while(GetNextQueueObj(mtx, toPatchQueue, node)) {
      if (kNone == patchStrat) node->removeGivenFriends(delNodesBitset);
      else node->removeGivenFriendsPatchWithClosestNeighbor<dist_t>(space_, use_proxy_dist_,
                                                                    delNodesBitset, cacheDelNode);
    }

  } else {
    for (size_t i = 0; i < indexThreadQty_; ++i) {
      threads.push_back(thread(
        [&]() {
          MSWNode* node = nullptr;
          vector<MSWNode*> cacheDelNode;
          while(GetNextQueueObj(mtx, toPatchQueue, node)) {
            if (kNone == patchStrat) node->removeGivenFriends(delNodesBitset);
            else node->removeGivenFriendsPatchWithClosestNeighbor<dist_t>(space_, use_proxy_dist_,
                                                                          delNodesBitset, cacheDelNode);
          }
        }
      ));
    }
    for (auto& thread : threads) thread.join();
  }

  if (checkIDs) {
    for (auto it : ElList_) {
      MSWNode* node = it.second;
      IdType   nodeId = node->getId();
      CHECK(nodeId < (ptrdiff_t)delNodesBitset.size());
      CHECK(!delNodesBitset.at(nodeId));
      for (MSWNode* neighb : node->getAllFriends()) {
        IdType   neighNodeId = neighb->getId();
        CHECK(neighNodeId < (ptrdiff_t)delNodesBitset.size());
        if (delNodesBitset.at(neighNodeId)) {
        /* 
         * Two things to check here:
         *  1) Was the node with to-be-deleted neighbor in the list of nodes that need patching?
         *  2) Was the deleted node in the list of to-be-deleted nodes?
         *  3) Do we have a reciprocal neighbor situation here?
         */
          LOG(LIB_INFO) << "Bug: a to-be-deleted node is still found among neighbors!";
          LOG(LIB_INFO) << "Is this neighbor in the list of to-be-deleted nodes (as expected)? " << (find(vToDelNodes.begin(),vToDelNodes.end(),neighb)!=vToDelNodes.end());
          LOG(LIB_INFO) << "Is the connected node in the list of to-be-patched nodes (as expected)? " << (find(vToPatchNodes.begin(), vToPatchNodes.end(), node) != vToPatchNodes.end());
          bool isRecipNeighb = false;
          for (MSWNode* nn : neighb->getAllFriends()) {
            if (nn == neighb) {
              isRecipNeighb = true;
              break;
            }
          }
          LOG(LIB_INFO) << "Do we have a reciprocal neighbor situation here (as expected)? " << isRecipNeighb;
        }
        CHECK(!delNodesBitset.at(neighNodeId));
      }
    }
  }

  // Stage 4. Clean-up and ID update
  for (MSWNode* node : vToDelNodes) {
    delete node;
  }

  pEntryPoint_ = ElList_.empty() ? nullptr : ElList_.begin()->second; 
  CHECK(pEntryPoint_ != nullptr || ElList_.empty());
  
  CompactIdsIfNeeded();
  if (checkIDs) CheckIDs();
}

template <typename dist_t>
void SmallWorldRand<dist_t>::CheckIDs() const
{
  // ElList_.size() can be smaller though
  CHECK_MSG(NextNodeId_ >= (ptrdiff_t)ElList_.size(), 
            "Bug NextNodeId_ = " + ConvertToString(NextNodeId_) + 
            " is < ElList_.size() = " + ConvertToString(ElList_.size()));
  vector<bool>  visitedBitset(NextNodeId_);

  LOG(LIB_INFO) << "Checking validity of node IDs asslignment";
  
  //We check that each ID is unique and is within the range [0, NextNodeId_)
  for(ElementMap::const_iterator it = ElList_.begin(); it != ElList_.end(); ++it) {
    MSWNode* pNode = it->second;
    IdType nodeID = pNode->getId();
    CHECK_MSG(nodeID >= 0 && nodeID < NextNodeId_,
            "Bug: unexpected node ID " + ConvertToString(nodeID) +
            " for object ID " + ConvertToString(pNode->getData()->id()) +
            "NextNodeId_ = " + ConvertToString(NextNodeId_));
    CHECK_MSG(visitedBitset[nodeID]==false,
            "Bug: duplicating node ID " + ConvertToString(nodeID) +
            " encountered which check object ID " + ConvertToString(pNode->getData()->id()));
    visitedBitset[nodeID]=true;
  }
}

template <typename dist_t>
void SmallWorldRand<dist_t>::InitParamsManually(const AnyParams& IndexParams)
{
  AnyParamManager pmgr(IndexParams);

  pmgr.GetParamOptional("NN",                 NN_,                  10);
  pmgr.GetParamOptional("efConstruction",     efConstruction_,      NN_);
  efSearch_ = NN_;
  pmgr.GetParamOptional("indexThreadQty",     indexThreadQty_,      thread::hardware_concurrency());
  pmgr.GetParamOptional("useProxyDist",       use_proxy_dist_,      false);

  LOG(LIB_INFO) << "NN                  = " << NN_;
  LOG(LIB_INFO) << "efConstruction_     = " << efConstruction_;
  LOG(LIB_INFO) << "indexThreadQty      = " << indexThreadQty_;
  LOG(LIB_INFO) << "useProxyDist        = " << use_proxy_dist_;

  pmgr.CheckUnused();
}


template <typename dist_t>
void SmallWorldRand<dist_t>::CreateIndex(const AnyParams& IndexParams) 
{
  AnyParamManager pmgr(IndexParams);

  pmgr.GetParamOptional("NN",                 NN_,                  10);
  pmgr.GetParamOptional("efConstruction",     efConstruction_,      NN_);
  efSearch_ = NN_;
  pmgr.GetParamOptional("indexThreadQty",     indexThreadQty_,      thread::hardware_concurrency());
  pmgr.GetParamOptional("useProxyDist",       use_proxy_dist_,      false);

  LOG(LIB_INFO) << "NN                  = " << NN_;
  LOG(LIB_INFO) << "efConstruction_     = " << efConstruction_;
  LOG(LIB_INFO) << "indexThreadQty      = " << indexThreadQty_;
  LOG(LIB_INFO) << "useProxyDist        = " << use_proxy_dist_;

  pmgr.CheckUnused();

  SetQueryTimeParams(getEmptyParams());

  AddBatch(this->data_, PrintProgress_);

  changedAfterCreateIndex_ = false;
}

template <typename dist_t>
void 
SmallWorldRand<dist_t>::SetQueryTimeParams(const AnyParams& QueryTimeParams) {
  AnyParamManager pmgr(QueryTimeParams);
  pmgr.GetParamOptional("efSearch", efSearch_, NN_);
  string tmp;
  //pmgr.GetParamOptional("algoType", tmp, "v1merge");
  pmgr.GetParamOptional("algoType", tmp, "old");
  ToLower(tmp);
  if (tmp == "v1merge") searchAlgoType_ = kV1Merge;
  else if (tmp == "old") searchAlgoType_ = kOld;
  else {
    throw runtime_error("algoType should be one of the following: old, v1merge");
  }
  pmgr.CheckUnused();
  LOG(LIB_INFO) << "Set SmallWorldRand query-time parameters:";
  LOG(LIB_INFO) << "efSearch           =" << efSearch_;
  LOG(LIB_INFO) << "algoType           =" << searchAlgoType_;
}

template <typename dist_t>
const std::string SmallWorldRand<dist_t>::StrDesc() const {
  return METH_SMALL_WORLD_RAND;
}

template <typename dist_t>
SmallWorldRand<dist_t>::~SmallWorldRand() {
  for (auto e : ElList_) {
    MSWNode* pNode = e.second;
    delete pNode;
  }
}

template <typename dist_t>
void 
SmallWorldRand<dist_t>::searchForIndexing(const Object *queryObj,
                                          priority_queue<EvaluatedMSWNodeDirect<dist_t>> &resultSet,
                                          IdType nextNodeIdUpperBound) const
{
/*
 * The trick of using large dense bitsets instead of unordered_set was 
 * borrowed from Wei Dong's kgraph: https://github.com/aaalgo/kgraph
 *
 * This trick works really well even in a multi-threaded mode. Indeed, the amount
 * of allocated memory is small. For example, if one has 8M entries, the size of
 * the bitmap is merely 1 MB. Furthermore, setting 1MB of entries to zero via memset would take only
 * a fraction of millisecond.
 */
  vector<bool>                        visitedBitset(nextNodeIdUpperBound); // seems to be working efficiently even in a multi-threaded mode.

  vector<MSWNode*> neighborCopy;

  /**
   * Search for the k most closest elements to the query.
   */
  MSWNode* provider = pEntryPoint_;
  CHECK_MSG(provider != nullptr, "Bug: there is not entry point set!")

  priority_queue <dist_t>                     closestDistQueue;                      
  priority_queue <EvaluatedMSWNodeReverse<dist_t>>   candidateSet; 

  dist_t d = use_proxy_dist_ ?  space_.ProxyDistance(provider->getData(), queryObj) : 
                                space_.IndexTimeDistance(provider->getData(), queryObj);
  EvaluatedMSWNodeReverse<dist_t> ev(d, provider);

  candidateSet.push(ev);
  closestDistQueue.push(d);

  if (closestDistQueue.size() > efConstruction_) {
    closestDistQueue.pop();
  }

  IdType nodeId = provider->getId();
  CHECK_MSG(nodeId < nextNodeIdUpperBound, 
            "Bug: nodeId (" + ConvertToString(nodeId) + ") > nextNodeIdUpperBound (" + ConvertToString(nextNodeIdUpperBound));
  
  visitedBitset[nodeId] = true;
  resultSet.emplace(d, provider);
      
  if (resultSet.size() > NN_) { // TODO check somewhere that NN > 0
    resultSet.pop();
  }        

  while (!candidateSet.empty()) {
    const EvaluatedMSWNodeReverse<dist_t>& currEv = candidateSet.top();
    dist_t lowerBound = closestDistQueue.top();

    /*
     * Check if we reached a local minimum.
     */
    if (currEv.getDistance() > lowerBound) {
      break;
    }
    MSWNode* currNode = currEv.getMSWNode();

    /*
     * This lock protects currNode from being modified
     * while we are accessing elements of currNode.
     */
    size_t neighborQty = 0;
    {
      unique_lock<mutex> lock(currNode->accessGuard_);

      //const vector<MSWNode*>& neighbor = currNode->getAllFriends();
      const vector<MSWNode*>& neighbor = currNode->getAllFriends();
      neighborQty = neighbor.size();
      if (neighborQty > neighborCopy.size()) neighborCopy.resize(neighborQty);
      for (size_t k = 0; k < neighborQty; ++k)
        neighborCopy[k]=neighbor[k];
    }

    // Can't access curEv anymore! The reference would become invalid
    candidateSet.pop();

    // calculate distance to each neighbor
    for (size_t neighborId = 0; neighborId < neighborQty; ++neighborId) {
      MSWNode* pNeighbor = neighborCopy[neighborId];

      IdType nodeId = pNeighbor->getId();
      CHECK_MSG(nodeId < nextNodeIdUpperBound, 
                "Bug: nodeId (" + ConvertToString(nodeId) + ") > nextNodeIdUpperBound (" + ConvertToString(nextNodeIdUpperBound));
      if (!visitedBitset[nodeId]) {
        visitedBitset[nodeId] = true;
        d = use_proxy_dist_ ? space_.ProxyDistance(pNeighbor->getData(), queryObj) : 
                              space_.IndexTimeDistance(pNeighbor->getData(), queryObj);

        if (closestDistQueue.size() < efConstruction_ || d < closestDistQueue.top()) {
          closestDistQueue.push(d);
          if (closestDistQueue.size() > efConstruction_) {
            closestDistQueue.pop();
          }
          candidateSet.emplace(d, pNeighbor);
        }

        if (resultSet.size() < NN_ || resultSet.top().getDistance() > d) {
          resultSet.emplace(d, pNeighbor);
          if (resultSet.size() > NN_) { // TODO check somewhere that NN > 0
            resultSet.pop();
          }
        }
      }
    }
  }
}


template <typename dist_t>
void SmallWorldRand<dist_t>::add(MSWNode *newElement, IdType nextNodeIdUpperBound){
  newElement->removeAllFriends(); 

  bool isEmpty = false;

  {
    unique_lock<mutex> lock(ElListGuard_);
    isEmpty = ElList_.empty();
  }

  if(isEmpty){
  // Before add() is called, the first node should be created!
    LOG(LIB_INFO) << "Bug: the list of nodes shouldn't be empty!";
    throw runtime_error("Bug: the list of nodes shouldn't be empty!");
  }

  {
    priority_queue<EvaluatedMSWNodeDirect<dist_t>> resultSet;

    searchForIndexing(newElement->getData(), resultSet, nextNodeIdUpperBound);

    // TODO actually we might need to add elements in the reverse order in the future.
    // For the current implementation, however, the order doesn't seem to matter
    while (!resultSet.empty()) {
      MSWNode::link(resultSet.top().getMSWNode(), newElement);
      resultSet.pop();
    }
  }

  addCriticalSection(newElement);
}

template <typename dist_t>
void SmallWorldRand<dist_t>::addCriticalSection(MSWNode *newElement){
  unique_lock<mutex> lock(ElListGuard_);

  if (nullptr == pEntryPoint_) {
    // When adding the very first element, assign the value of the entry point!
    pEntryPoint_ = newElement;
    CHECK(ElList_.empty()); 
  }
  ElList_.insert(make_pair(newElement->getData()->id(), newElement));
}

template <typename dist_t>
void SmallWorldRand<dist_t>::Search(RangeQuery<dist_t>* query, IdType) const {
  throw runtime_error("Range search is not supported!");
}

template <typename dist_t>
void SmallWorldRand<dist_t>::Search(KNNQuery<dist_t>* query, IdType) const {
  if (searchAlgoType_ == kV1Merge) SearchV1Merge(query);
  else SearchOld(query);
}

template <typename dist_t>
void SmallWorldRand<dist_t>::SearchV1Merge(KNNQuery<dist_t>* query) const {
  if (ElList_.empty()) return;
  CHECK_MSG(efSearch_ > 0, "efSearch should be > 0");
/*
 * The trick of using large dense bitsets instead of unordered_set was
 * borrowed from Wei Dong's kgraph: https://github.com/aaalgo/kgraph
 *
 * This trick works really well even in a multi-threaded mode. Indeed, the amount
 * of allocated memory is small. For example, if one has 8M entries, the size of
 * the bitmap is merely 1 MB. Furthermore, setting 1MB of entries to zero via memset would take only
 * a fraction of millisecond.
 */
  vector<bool>                        visitedBitset(NextNodeId_);

  /**
   * Search of most k-closest elements to the query.
   */
  MSWNode* currNode = pEntryPoint_;
  CHECK_MSG(currNode != nullptr, "Bug: there is not entry point set!")

  SortArrBI<dist_t,MSWNode*> sortedArr(max<size_t>(efSearch_, query->GetK()));

  const Object* currObj = currNode->getData();
  dist_t d = query->DistanceObjLeft(currObj);
  sortedArr.push_unsorted_grow(d, currNode); // It won't grow

  IdType nodeId = currNode->getId();
  CHECK_MSG(nodeId < NextNodeId_, "Bug: nodeId (" + ConvertToString(nodeId) +  ") > NextNodeId_ (" +ConvertToString(NextNodeId_) +")");

  visitedBitset[nodeId] = true;

  uint_fast32_t  currElem = 0;

  typedef typename SortArrBI<dist_t,MSWNode*>::Item  QueueItem;

  vector<QueueItem>& queueData = sortedArr.get_data();
  vector<QueueItem>  itemBuff(8*NN_);

  // efSearch_ is always <= # of elements in the queueData.size() (the size of the BUFFER), but it can be
  // larger than sortedArr.size(), which returns the number of actual elements in the buffer
  while(currElem < min(sortedArr.size(),efSearch_)){
    auto& e = queueData[currElem];
    CHECK(!e.used);
    e.used = true;
    currNode = e.data;
    ++currElem;

    for (MSWNode* neighbor : currNode->getAllFriends()) {
      PREFETCH(reinterpret_cast<const char*>(const_cast<const Object*>(neighbor->getData())), _MM_HINT_T0);
    }
    for (MSWNode* neighbor : currNode->getAllFriends()) {
      PREFETCH(const_cast<const char*>(neighbor->getData()->data()), _MM_HINT_T0);
    }

    if (currNode->getAllFriends().size() > itemBuff.size())
      itemBuff.resize(currNode->getAllFriends().size());

    size_t itemQty = 0;

    dist_t topKey = sortedArr.top_key();
    //calculate distance to each neighbor
    for (MSWNode* neighbor : currNode->getAllFriends()) {
      nodeId = neighbor->getId();
      CHECK_MSG(nodeId < NextNodeId_, "Bug: nodeId (" + ConvertToString(nodeId) +  ") > NextNodeId_ (" +ConvertToString(NextNodeId_));

      if (!visitedBitset[nodeId]) {
        currObj = neighbor->getData();
        d = query->DistanceObjLeft(currObj);
        visitedBitset[nodeId] = true;
        if (sortedArr.size() < efSearch_ || d < topKey) {
          itemBuff[itemQty++]=QueueItem(d, neighbor);
        }
      }
    }

    if (itemQty) {
      PREFETCH(const_cast<const char*>(reinterpret_cast<char*>(&itemBuff[0])), _MM_HINT_T0);
      std::sort(itemBuff.begin(), itemBuff.begin() + itemQty);

      size_t insIndex=0;
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
    }

    // To ensure that we either reach the end of the unexplored queue or currElem points to the first unused element
    while (currElem < sortedArr.size() && queueData[currElem].used == true)
      ++currElem;
  }

  for (uint_fast32_t i = 0; i < query->GetK() && i < sortedArr.size(); ++i) {
    query->CheckAndAddToResult(queueData[i].key, queueData[i].data->getData());
  }
}


template <typename dist_t>
void SmallWorldRand<dist_t>::SearchOld(KNNQuery<dist_t>* query) const {

  if (ElList_.empty()) return;
  CHECK_MSG(efSearch_ > 0, "efSearch should be > 0");
/*
 * The trick of using large dense bitsets instead of unordered_set was
 * borrowed from Wei Dong's kgraph: https://github.com/aaalgo/kgraph
 *
 * This trick works really well even in a multi-threaded mode. Indeed, the amount
 * of allocated memory is small. For example, if one has 8M entries, the size of
 * the bitmap is merely 1 MB. Furthermore, setting 1MB of entries to zero via memset would take only
 * a fraction of millisecond.
 */
  vector<bool>                        visitedBitset(NextNodeId_);

  MSWNode* provider = pEntryPoint_;
  CHECK_MSG(provider != nullptr, "Bug: there is not entry point set!")

  priority_queue <dist_t>                          closestDistQueue; //The set of all elements which distance was calculated
  priority_queue <EvaluatedMSWNodeReverse<dist_t>> candidateQueue; //the set of elements which we can use to evaluate

  const Object* currObj = provider->getData();
  dist_t d = query->DistanceObjLeft(currObj);
  query->CheckAndAddToResult(d, currObj); // This should be done before the object goes to the queue: otherwise it will not be compared to the query at all!

  EvaluatedMSWNodeReverse<dist_t> ev(d, provider);
  candidateQueue.push(ev);
  closestDistQueue.emplace(d);

  IdType nodeId = provider->getId();
  CHECK_MSG(nodeId < NextNodeId_, "Bug: nodeId (" + ConvertToString(nodeId) +  ") > NextNodeId_ (" +ConvertToString(NextNodeId_) + ")");
  visitedBitset[nodeId] = true;

  while(!candidateQueue.empty()){

    auto iter = candidateQueue.top(); // This one was already compared to the query
    const EvaluatedMSWNodeReverse<dist_t>& currEv = iter;

    // Did we reach a local minimum?
    if (currEv.getDistance() > closestDistQueue.top()) {
      break;
    }

    for (MSWNode* neighbor : (currEv.getMSWNode())->getAllFriends()) {
      PREFETCH(reinterpret_cast<const char*>(const_cast<const Object*>(neighbor->getData())), _MM_HINT_T0);
    }
    for (MSWNode* neighbor : (currEv.getMSWNode())->getAllFriends()) {
      PREFETCH(const_cast<const char*>(neighbor->getData()->data()), _MM_HINT_T0);
    }

    const vector<MSWNode*>& neighbor = (currEv.getMSWNode())->getAllFriends();

    // Can't access curEv anymore! The reference would become invalid
    candidateQueue.pop();

    //calculate distance to each neighbor
    for (auto iter = neighbor.begin(); iter != neighbor.end(); ++iter){
      nodeId = (*iter)->getId();
      CHECK_MSG(nodeId < NextNodeId_, "Bug: nodeId (" + ConvertToString(nodeId) +  ") > NextNodeId_ (" +ConvertToString(NextNodeId_));
      if (!visitedBitset[nodeId]) {
        currObj = (*iter)->getData();
        d = query->DistanceObjLeft(currObj);
        visitedBitset[nodeId] = true;

        if (closestDistQueue.size() < efSearch_ || d < closestDistQueue.top()) {
          closestDistQueue.emplace(d);
          if (closestDistQueue.size() > efSearch_) {
            closestDistQueue.pop();
          }

          candidateQueue.emplace(d, *iter);
        }

        query->CheckAndAddToResult(d, currObj);
      }
    }
  }
}

template <typename dist_t>
void SmallWorldRand<dist_t>::SaveIndex(const string &location) {
  CHECK_MSG(!changedAfterCreateIndex_,
            "It seems that data was added/deleted after calling CreateIndex, in this case saving indices isn't possible");
  
  ofstream outFile(location);
  CHECK_MSG(outFile, "Cannot open file '" + location + "' for writing");
  outFile.exceptions(std::ios::badbit);
  size_t lineNum = 0;

  WriteField(outFile, METHOD_DESC, StrDesc()); lineNum++;
  WriteField(outFile, "NN", NN_); lineNum++;

  for(ElementMap::iterator it = ElList_.begin(); it != ElList_.end(); ++it) {
    MSWNode* pNode = it->second;
    IdType nodeID = pNode->getId();
    CHECK_MSG(nodeID >= 0 && nodeID < (ptrdiff_t)this->data_.size(),
              "Bug: unexpected node ID " + ConvertToString(nodeID) +
              " for object ID " + ConvertToString(pNode->getData()->id()) +
              "data_.size() = " + ConvertToString(this->data_.size()));
    outFile << nodeID << ":" << pNode->getData()->id() << ":";
    for (const MSWNode* pNodeFriend: pNode->getAllFriends()) {
      IdType nodeFriendID = pNodeFriend->getId();
      CHECK_MSG(nodeFriendID >= 0 && nodeFriendID < (ptrdiff_t)this->data_.size(),
                "Bug: unexpected node ID " + ConvertToString(nodeFriendID) +
                " for object ID " + ConvertToString(pNodeFriend->getData()->id()) +
                "data_.size() = " + ConvertToString(this->data_.size()));
      outFile << ' ' << nodeFriendID;
    }
    outFile << endl; lineNum++;
  }
  outFile << endl; lineNum++; // The empty line indicates the end of data entries
  WriteField(outFile, LINE_QTY, lineNum + 1 /* including this line */);
  outFile.close();
}

template <typename dist_t>
void SmallWorldRand<dist_t>::LoadIndex(const string &location) {
  vector<MSWNode *> ptrMapper(this->data_.size());

  for (unsigned pass = 0; pass < 2; ++ pass) {
    ifstream inFile(location);
    CHECK_MSG(inFile, "Cannot open file '" + location + "' for reading");
    inFile.exceptions(std::ios::badbit);

    size_t lineNum = 1;
    string methDesc;
    ReadField(inFile, METHOD_DESC, methDesc);
    lineNum++;
    CHECK_MSG(methDesc == StrDesc(),
              "Looks like you try to use an index created by a different method: " + methDesc);
    ReadField(inFile, "NN", NN_);
    lineNum++;

    string line;
    while (getline(inFile, line)) {
      if (line.empty()) {
        lineNum++; break;
      }
      stringstream str(line);
      str.exceptions(std::ios::badbit);
      char c1, c2;
      IdType nodeID, objID;
      CHECK_MSG((str >> nodeID) && (str >> c1) && (str >> objID) && (str >> c2),
                "Bug or inconsitent data, wrong format, line: " + ConvertToString(lineNum)
      );
      CHECK_MSG(c1 == ':' && c2 == ':',
                string("Bug or inconsitent data, wrong format, c1=") + c1 + ",c2=" + c2 +
                " line: " + ConvertToString(lineNum)
      );
      CHECK_MSG(nodeID >= 0 && nodeID < (ptrdiff_t)this->data_.size(),
                DATA_MUTATION_ERROR_MSG + " (unexpected node ID " + ConvertToString(nodeID) +
                " for object ID " + ConvertToString(objID) +
                " data_.size() = " + ConvertToString(this->data_.size()) + ")");
      CHECK_MSG(this->data_[nodeID]->id() == objID,
                DATA_MUTATION_ERROR_MSG + " (unexpected object ID " + ConvertToString(this->data_[nodeID]->id()) +
                " for data element with ID " + ConvertToString(nodeID) +
                " expected object ID: " + ConvertToString(objID) + ")"
      );
      if (pass == 0) {
        unique_ptr<MSWNode> node(new MSWNode(this->data_[nodeID], nodeID));
        ptrMapper[nodeID] = node.get();
        IdType dataId = node->getData()->id();
        ElList_.insert(make_pair(dataId, node.release()));
      } else {
        MSWNode *pNode = ptrMapper[nodeID];
        CHECK_MSG(pNode != NULL,
                  "Bug, got NULL pointer in the second pass for nodeID " + ConvertToString(nodeID));
        IdType nodeFriendID;
        while (str >> nodeFriendID) {
          CHECK_MSG(nodeFriendID >= 0 && nodeFriendID < (ptrdiff_t)this->data_.size(),
                    "Bug: unexpected node ID " + ConvertToString(nodeFriendID) +
                    "data_.size() = " + ConvertToString(this->data_.size()));
          MSWNode *pFriendNode = ptrMapper[nodeFriendID];
          CHECK_MSG(pFriendNode != NULL,
                    "Bug, got NULL pointer in the second pass for nodeID " + ConvertToString(nodeFriendID));
          pNode->addFriend(pFriendNode, false /* don't check for duplicates */);
        }
        CHECK_MSG(str.eof(),
                  "It looks like there is some extract erroneous stuff in the end of the line " +
                  ConvertToString(lineNum));
      }
      ++lineNum;
    }

    size_t ExpLineNum;
    ReadField(inFile, LINE_QTY, ExpLineNum);
    CHECK_MSG(lineNum == ExpLineNum,
              DATA_MUTATION_ERROR_MSG + " (expected number of lines " + ConvertToString(ExpLineNum) +
              " read so far doesn't match the number of read lines: " + ConvertToString(lineNum) + ")");
    inFile.close();
  }

  pEntryPoint_ = ElList_.empty() ? nullptr : ElList_.begin()->second; 
  CHECK(pEntryPoint_ != nullptr || ElList_.empty());
  NextNodeId_ = ElList_.size();

  LOG(LIB_INFO) << "Next node id: " << NextNodeId_ << " ElList_.size(): " << ElList_.size(); 
}

template class SmallWorldRand<float>;
template class SmallWorldRand<int>;

}
