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
#ifndef _PIVOT_NEIGHBORHOOD_INVINDEX_H
#define _PIVOT_NEIGHBORHOOD_INVINDEX_H

#include <vector>
#include <mutex>

#include "index.h"
#include "permutation_utils.h"
#include "ported_boost_progress.h"

#define METH_PIVOT_NEIGHB_INVINDEX      "pivot_neighb_invindx"
#define METH_PIVOT_NEIGHB_INVINDEX_SYN  "napp"

#include <method/pivot_neighb_common.h>

namespace similarity {

using std::vector;
using std::mutex;

/*
 * Neighborhood-APProximation Index (NAPP).
 *
 * The main idea of the method (indexing K most closest pivots using an inverted file)
 * was taken from the paper:
 *
 * Eric Sadit Tellez, Edgar Chavez and Gonzalo Navarro,
 * Succinct Nearest Neighbor Search, SISAP 2011
 *
 * In this implementation, we introduce several modifications:
 * 1) The inverted file is split into small parts. In doing so, we aim to
 *    achieve better caching properties of the counter array used in ScanCount.
 * 2) The index is not compressed (though it could be)
 * 3) We support different number of pivots K during indexing and searching (unlike the original paper),
 *    which is controlled by the parameter numPrefixSearch.
 * 4) Instead of the adaptive union algorithm, we use a well-known ScanCount algorithm (by default). 
 *    The overall time spent on processing of the inverted file is 20-30% of the overall
 *    search time. Thus, the retrieval time cannot be substantially improved by
 *    replacing ScanCount with even better approach (should one exist).
 * 5) We also implemented several other simple algorithms for posting processing, to compare
 *    against ScanCount. For instance, the merge-sort union algorithms is about 2-3 times as slow.
 *
 *  For an example of using ScanCount see, e.g.:
 *
 *        Li, Chen, Jiaheng Lu, and Yiming Lu. 
 *       "Efficient merging and filtering algorithms for approximate string searches." 
 *        In Data Engineering, 2008. ICDE 2008. 
 *        IEEE 24th International Conference on, pp. 257-266. IEEE, 2008.
 */

template <typename dist_t>
class PivotNeighbInvertedIndex : public Index<dist_t> {
 public:
  PivotNeighbInvertedIndex(bool PrintProgress,
                           const Space<dist_t>& space,
                           const ObjectVector& data);

  virtual void CreateIndex(const AnyParams& IndexParams) override;
  virtual void SaveIndex(const string &location) override;
  virtual void LoadIndex(const string &location) override;

  ~PivotNeighbInvertedIndex();

  const std::string StrDesc() const override;
  void Search(RangeQuery<dist_t>* query, IdType) const override;
  void Search(KNNQuery<dist_t>* query, IdType) const override;
  
  void IndexChunk(size_t chunkId, ProgressDisplay*, mutex&);
  void SetQueryTimeParams(const AnyParams& QueryTimeParams) override;
 private:

  const   Space<dist_t>&  space_;
  bool    PrintProgress_;
  bool    recreate_points_;

  size_t  chunk_index_size_;
  size_t  K_;
  size_t  knn_amp_;
  float   db_scan_frac_;
  size_t  num_prefix_;       // K in the original paper
  size_t  num_prefix_search_;// K used during search (our modification can use a different K)
  size_t  min_times_;        // t in the original paper
  bool    use_sort_;
  bool    skip_checking_;
  size_t  index_thread_qty_;
  size_t  num_pivot_;
  string  pivot_file_;
  bool    disable_pivot_index_;
  size_t hash_trick_dim_;

  unique_ptr<PivotIndex<dist_t>> pivot_index_;

  enum eAlgProctype {
    kScan,
    kMap,
    kMerge,
    kPriorQueue,
    kWAND
  } inv_proc_alg_;

  string toString(eAlgProctype type) const {
    if (type == kScan)   return PERM_PROC_FAST_SCAN;
    if (type == kMap)    return PERM_PROC_MAP;
    if (type == kMerge)  return PERM_PROC_MERGE;
    if (type == kPriorQueue) return PERM_PROC_PRIOR_QUEUE;
    if (type == kWAND) return PERM_PROC_WAND;
    return "unknown";
  }

  ObjectVector    pivot_;
  vector<IdType>  pivot_pos_;

  ObjectVector    genPivot_; // generated pivots

  size_t computeDbScan(size_t K, size_t chunkQty) const {
    size_t totalDbScan = static_cast<size_t>(db_scan_frac_ * this->data_.size());
    if (knn_amp_) { 
      totalDbScan = K * knn_amp_;
    }
    totalDbScan = min(totalDbScan, this->data_.size());
    CHECK_MSG(chunkQty, "Bug or inconsistent parameters: the number of index chunks cannot be zero!");
    return (totalDbScan + chunkQty - 1) / chunkQty;
  }

  void initPivotIndex() {
    if (disable_pivot_index_) {
      pivot_index_.reset(new DummyPivotIndex<dist_t>(space_, pivot_));
      LOG(LIB_INFO) << "Created a dummy pivot index";
    } else {
      pivot_index_.reset(space_.CreatePivotIndex(pivot_, hash_trick_dim_));
      LOG(LIB_INFO) << "Attempted to create an efficient pivot index (however only few spaces support such index)";
    }
  }
  
  vector<shared_ptr<vector<PostingListInt>>> posting_lists_;

  struct PostListQueryState {
    const PostingListInt*  post_;
    size_t                 post_pos_;
    PostListQueryState(const PostingListInt& pl)
        : post_(&pl), post_pos_(0) {}
  };

  template <typename QueryType> void GenSearch(QueryType* query, size_t K) const;

  void GetPermutationPPIndexEfficiently(const Object* object, Permutation& p) const;
  void GetPermutationPPIndexEfficiently(const Query<dist_t>* query, Permutation& p) const;
  void GetPermutationPPIndexEfficiently(Permutation &p, const vector <dist_t> &vDst) const;

  // disable copy and assign
  DISABLE_COPY_AND_ASSIGN(PivotNeighbInvertedIndex);

  void GetPermutationPPIndexEfficiently(Permutation &p, const vector<bool> &vDst) const;
};

}  // namespace similarity

#endif     // _PERMUTATION_SUCCINCT_INDEX_H_

