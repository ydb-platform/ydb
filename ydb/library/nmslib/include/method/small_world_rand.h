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
#ifndef _SMALL_WORLD_RAND_H_
#define _SMALL_WORLD_RAND_H_

#include "index.h"
#include "params.h"
#include <set>
#include <limits>
#include <iostream>
#include <map>
#include <unordered_set>
#include <unordered_map>
#include <thread>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <queue>


#define METH_SMALL_WORLD_RAND                 "sw-graph"
#define METH_SMALL_WORLD_RAND_SYN             "small_world_rand"

namespace similarity {

using std::string;
using std::vector;
using std::unordered_map;
using std::thread;
using std::mutex;
using std::unique_lock;
using std::condition_variable;
using std::ref;

template <typename dist_t>
class Space;

/*
 *
 * A small world approach. It builds the knn-graph incrementally and relies on 
 * a straightforward randomized algorithm to insert an element. 
 * 
 * The main publication is as follows, but the basic algorithm was also presented as SISAP'12: 
 *    Malkov, Y., Ponomarenko, A., Logvinov, A., & Krylov, V., 2014. 
 *    Approximate nearest neighbor algorithm based on navigable small world graphs. Information Systems, 45, 61-68. 
 *
 */

//----------------------------------
class MSWNode{
public:
  MSWNode(const Object *Obj, size_t id) {
    nodeObj_ = Obj;
    id_ = id;
  }
  ~MSWNode(){};
  void removeAllFriends(){
    friends_.clear();
  }

  static void link(MSWNode* first, MSWNode* second){
    // addFriend checks for duplicates if the second argument is true
    first->addFriend(second, true);
    second->addFriend(first, true);
  }

  // Removes only friends from a given set
  void removeGivenFriends(const vector<bool>& delNodes) {
    size_t newQty = 0;
    /*
     * This in-place one-iteration deletion of elements in delNodes
     * Invariant in the beginning of each loop iteration:
     * i >= newQty
     * Furthermore:
     * i - newQty == the number of entries deleted in previous iterations
     */
    for (size_t i = 0; i < friends_.size(); ++i) {
      IdType id = friends_[i]->getId();
      if (!delNodes.at(id)) {
        friends_[newQty] = friends_[i];
        ++newQty;
      }
    }
    friends_.resize(newQty);
  }

  // Removes friends from a given set and attempts to replace them with friends' closest neighbors
  // cacheDelNode should be thread-specific (or else calling this function isn't thread-safe)
  template <class dist_t>
  void removeGivenFriendsPatchWithClosestNeighbor(const Space<dist_t>& space, bool use_proxy_dist,
                                                  const vector<bool>& delNodes, vector<MSWNode*>& cacheDelNode) {
    /*
     * This in-place one-iteration deletion of elements in delNodes
     * Loop invariants:
     * 1) i >= newQty
     * 2) i - newQty = delQty 
     * Hence, when the loop terminates delQty + newQty == friends_.size()
     */
    size_t newQty = 0, delQty = 0;

    for (size_t i = 0; i < friends_.size(); ++i) {
      MSWNode* oneFriend = friends_[i];
      IdType id = oneFriend->getId();
      if (!delNodes.at(id)) {
        friends_[newQty] = friends_[i];
        ++newQty;
      } else {
        if (cacheDelNode.size() <= delQty) cacheDelNode.resize(2*delQty + 1);
        cacheDelNode[delQty] = oneFriend;
        ++delQty;
      }
    }
    CHECK_MSG((delQty + newQty) == friends_.size(), 
              "Seems like a bug, delQty:" + ConvertToString(delQty) + 
              " newQty: " + ConvertToString(newQty) + 
              " friends_.size()=" + ConvertToString(friends_.size()));
    friends_.resize(newQty);
    // When patching use the function link()
    for (size_t i = 0; i < delQty; ++i) {
      MSWNode *toDelFriend = cacheDelNode[i];
      MSWNode *friendReplacement = nullptr;
      dist_t  dmin = numeric_limits<dist_t>::max();
      const Object* queryObj = this->getData();
      for (MSWNode* neighb : toDelFriend->getAllFriends()) {
        IdType neighbId = neighb->getId();
        if (!delNodes.at(neighbId)) {
          const MSWNode* provider = neighb;
          dist_t d = use_proxy_dist ?  space.ProxyDistance(provider->getData(), queryObj) : 
                                        space.IndexTimeDistance(provider->getData(), queryObj); 
          if (d < dmin) {
            dmin = d;
            friendReplacement = neighb;
          }
        }
      }
      if (friendReplacement != nullptr) {
        link(this, friendReplacement);
      }
    }
  }
  /* 
   * 1. The list of friend pointers is sorted.
   * 2. If bCheckForDup == true addFriend checks for
   *    duplicates using binary searching (via pointer comparison).
   */
  void addFriend(MSWNode* element, bool bCheckForDup) {
    unique_lock<mutex> lock(accessGuard_);

    if (bCheckForDup) {
      auto it = lower_bound(friends_.begin(), friends_.end(), element);
      if (it == friends_.end() || (*it) != element) {
        friends_.insert(it, element);
      }
    } else {
      friends_.push_back(element);
    }
  }
  const Object* getData() const {
    return nodeObj_;
  }
  size_t getId() const { return id_; }
  void setId(IdType id) { id_ = id; }
  /* 
   * THIS NOTE APPLIES ONLY TO THE INDEXING PHASE:
   *
   * Before getting access to the friends,
   * one needs to lock the mutex accessGuard_
   * The mutex can be released ONLY when
   * we exit the scope that has access to
   * the reference returned by getAllFriends()
   */
  const vector<MSWNode*>& getAllFriends() const {
    return friends_;
  }

  mutex accessGuard_;

private:
  const Object*       nodeObj_;
  size_t              id_;
  vector<MSWNode*>    friends_;
};
//----------------------------------
template <typename dist_t>
class EvaluatedMSWNodeReverse{
public:
  EvaluatedMSWNodeReverse() {
      distance = 0;
      element = NULL;
  }
  EvaluatedMSWNodeReverse(dist_t di, MSWNode* node) {
    distance = di;
    element = node;
  }
  ~EvaluatedMSWNodeReverse(){}
  dist_t getDistance() const {return distance;}
  MSWNode* getMSWNode() const {return element;}
  bool operator< (const EvaluatedMSWNodeReverse &obj1) const {
    return (distance > obj1.getDistance());
  }

private:
  dist_t distance;
  MSWNode* element;
};

template <typename dist_t>
class EvaluatedMSWNodeDirect{
public:
  EvaluatedMSWNodeDirect() {
      distance = 0;
      element = NULL;
  }
  EvaluatedMSWNodeDirect(dist_t di, MSWNode* node) {
    distance = di;
    element = node;
  }
  ~EvaluatedMSWNodeDirect(){}
  dist_t getDistance() const {return distance;}
  MSWNode* getMSWNode() const {return element;}
  bool operator< (const EvaluatedMSWNodeDirect &obj1) const {
    return (distance < obj1.getDistance());
  }

private:
  dist_t distance;
  MSWNode* element;
};

//----------------------------------
template <typename dist_t>
class SmallWorldRand : public Index<dist_t> {
public:
  virtual void SaveIndex(const string &location) override;

  virtual void LoadIndex(const string &location) override;

  SmallWorldRand(bool PrintProgress,
                 const Space<dist_t>& space,
                 const ObjectVector& data);
  void CreateIndex(const AnyParams& IndexParams) override;
  virtual void AddBatch(const ObjectVector& batchData, bool bPrintProgress, bool bCheckIDs = false) override;
  virtual void DeleteBatch(const ObjectVector& batchData, int delStrategy,
                           bool checkIDs = false/* this is a debug flag only, turning it on may affect performance */) override;

  virtual void DeleteBatch(const vector<IdType>& batchData, int delStrategy,
                           bool checkIDs = false/* this is a debug flag only, turning it on may affect performance */) override;
  ~SmallWorldRand();

  typedef unordered_map<IdType,MSWNode*> ElementMap;

  const std::string StrDesc() const override;
  void Search(RangeQuery<dist_t>* query, IdType) const override;
  void Search(KNNQuery<dist_t>* query, IdType) const override;
   
  void searchForIndexing(const Object *queryObj,
                         std::priority_queue<EvaluatedMSWNodeDirect<dist_t>> &resultSet,
                         IdType maxInternalId) const;
  void add(MSWNode *newElement, IdType maxInternalId);
  void addCriticalSection(MSWNode *newElement);

  void SetQueryTimeParams(const AnyParams& ) override;

  enum PatchingStrategy { kNone = 0, kNeighborsOnly = 1 };

  //This method should be called before LoadIndex to initialize parameters,
  //that are usually initialized in Create Index
  void InitParamsManually(const AnyParams& IndexParams);

private:

  size_t                NN_;
  size_t                efConstruction_;
  size_t                efSearch_;
  size_t                indexThreadQty_;
  string                pivotFile_;
  ObjectVector          pivots_;

  const Space<dist_t>&  space_;
  bool                  PrintProgress_;
  bool                  use_proxy_dist_;

  mutable mutex   ElListGuard_;
  ElementMap      ElList_;
  IdType          NextNodeId_ = 0; // This is internal node id
  bool            changedAfterCreateIndex_ = false;
  MSWNode*        pEntryPoint_ = nullptr;


  void SearchOld(KNNQuery<dist_t>* query) const;
  void SearchV1Merge(KNNQuery<dist_t>* query) const;

  void UpdateNextNodeId(size_t newNextNodeId);
  void CompactIdsIfNeeded();

  void CheckIDs() const;
  
  enum AlgoType { kOld, kV1Merge };

  AlgoType               searchAlgoType_;

protected:

  DISABLE_COPY_AND_ASSIGN(SmallWorldRand);
};




}

#endif
