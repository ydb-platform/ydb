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
#include <unordered_map>
#include <queue>

#include "space.h"
#include "knnquery.h"
#include "method/simple_inverted_index.h"
#include "falconn_heap_mod.h"
#include "ported_boost_progress.h"

#define SANITY_CHECKS

namespace similarity {

const uint32_t VERSION_NUMBER = 1;

using namespace std;

template <typename dist_t>
void SimplInvIndex<dist_t>::Search(KNNQuery<dist_t>* query, IdType) const {
  // the query vector, its size is the number of query terms (non-zero dimensions of the query vector)
  vector<SparseVectElem<dist_t>>    query_vect;
  const Object* o = query->QueryObject();
  UnpackSparseElements(o->data(), o->datalength(), query_vect);

  size_t K = query->GetK();

  // sorted list (priority queue) of pairs (doc_id, its_position_in_the_posting_list)
  //   the doc_ids are negative to keep the queue ordered the way we need
  FalconnHeapMod1<IdType, int32_t>            postListQueue;
  // state information for each query-term posting list
  vector<unique_ptr<PostListQueryState>>      queryStates(query_vect.size());

  // number of valid query terms (that are in the dictionary)
  size_t wordQty = 0;
  // query term index
  unsigned qsi = 0;
  // initialize queryStates and postListQueue variables
  for (auto eQuery : query_vect) {
    auto it = index_.find(eQuery.id_);
    if (it != index_.end()) { // There may be out-of-vocabulary words
#ifdef SANITY_CHECKS
      CHECK(it->second.get() != nullptr);
#endif
      const PostList& pl = *it->second;
      CHECK(pl.qty_ > 0);
      ++wordQty;
      // initialize the queryStates[query_term_index]  to the first position in the posting list
      queryStates[qsi].reset(new PostListQueryState(pl, eQuery.val_, eQuery.val_ * pl.entries_[0].val_));
      // initialize the postListQueue to the first position - insert pair (-doc_id, query_term_index)
      postListQueue.push(-pl.entries_[0].doc_id_, qsi);
    }
    ++qsi;
  }

  // While some people expect the result set to always contain at least k entries,
  // it's not clear what we return here
  if (0 == wordQty) return;

  // temporary queue with the top-K results  (ordered by the accumulated values so that top value is the smallest=worst document)
  FalconnHeapMod1<dist_t, IdType>             tmpResQueue;
  // accumulation of PostListQueryState.qval_x_docval_ for each document (DAAT)
  dist_t   accum = 0;

  while (!postListQueue.empty()) {
    // index of the posting list with the current SMALLEST doc_id
    IdType minDocIdNeg = postListQueue.top_key();

    // this while accumulates values for one document (DAAT), specifically for the one with   doc_id = -minDocIdNeg
    while (!postListQueue.empty() && postListQueue.top_key() == minDocIdNeg) {
      unsigned qsi = postListQueue.top_data();
      PostListQueryState& queryState = *queryStates[qsi];
      const PostList& pl = *queryState.post_;
      accum += queryState.qval_x_docval_;
      //accum += queryState.qval_ * pl.entries_[queryState.post_pos_].val_;

      // move to next position in the posting list
      queryState.post_pos_++;
      /*
       * If we didn't reach the end of the posting list, we retrieve the next document id.
       * Then, we push this update element down the priority queue.
       *
       * On reaching the end of the posting list, we evict the entry from the priority queue.
       */
      if (queryState.post_pos_ < pl.qty_) {
        // the next entry in the posting list
        const PostEntry& eDoc = pl.entries_[queryState.post_pos_];
        /*
         * Leo thinks it may be beneficial to access the posting list entry only once.
         * This access is used for two things
         * 1) obtain the next doc id
         * 2) compute the contribution of the current document to the overall dot product (val_ * qval_)
         */
        postListQueue.replace_top_key(-eDoc.doc_id_);
        queryState.qval_x_docval_ = eDoc.val_ * queryState.qval_;
      } else postListQueue.pop();
    }

    // tmpResQueue is a MAX-QUEUE (which is what we need, b/c we maximize the dot product
    dist_t negAccum = -accum;
#if 1
    // This one seems to be a bit faster
    // DAVID: THIS CONSTRUCTION IS WRONG. DUE TO THE FIRST CONDITION, THERE MIGHT BE MORE THAN K DOCUMENTS IN THE
    // RESULT, RIGHT? BUT THE SECOND FORK ONLY REPLACES ONE OF THE "WORST" DOCUMENTS
    if (tmpResQueue.size() < K || tmpResQueue.top_key() == negAccum)
      tmpResQueue.push(negAccum, -minDocIdNeg);
    else if (tmpResQueue.top_key() > negAccum)
      tmpResQueue.replace_top(negAccum, -minDocIdNeg);
#else
    query->CheckAndAddToResult(negAccum, data_[-minDocIdNeg]);
#endif

    accum = 0;
  }

  while (!tmpResQueue.empty()) {
#ifdef SANITY_CHECKS
    CHECK(tmpResQueue.top_data() >= 0);
#endif

#if 0
    query->CheckAndAddToResult(-tmpResQueue.top_key(), data_[tmpResQueue.top_data()]);
#else
    // This branch recomputes the distance, but it normally has a negligibly small effect on the run-time
    query->CheckAndAddToResult(this->data_[tmpResQueue.top_data()]);
#endif
    tmpResQueue.pop();
  }
}

template <typename dist_t>
void SimplInvIndex<dist_t>::SaveIndex(const string& location) {
  std::ofstream output(location, std::ios::binary);
  CHECK_MSG(output, "Cannot open file '" + location + "' for writing");
  output.exceptions(ios::badbit | ios::failbit);

  // Save version number
  const uint32_t version = VERSION_NUMBER;
  writeBinaryPOD(output, version);

  size_t entryQty = index_.size(); 
  writeBinaryPOD(output, entryQty);

  for (const auto & e: index_) {
    uint32_t elemId = e.first;
    writeBinaryPOD(output, elemId);
    const PostList& pl = *e.second;
    writeBinaryPOD(output, pl.qty_);
    for (size_t i = 0; i < pl.qty_; i++) {
      const PostEntry& e = pl.entries_[i];
      writeBinaryPOD(output, e.doc_id_);
      writeBinaryPOD(output, e.val_);
    }
  }

  output.close();
}

template <typename dist_t>
void SimplInvIndex<dist_t>::LoadIndex(const string& location) {
  std::ifstream input(location, std::ios::binary);
  CHECK_MSG(input, "Cannot open file '" + location + "' for reading");
  input.exceptions(ios::badbit | ios::failbit);

  uint32_t version;
  readBinaryPOD(input, version);
  if (version != VERSION_NUMBER) {
    PREPARE_RUNTIME_ERR(err) << "File version number (" << version << ") differs from "
                             << "expected version (" << VERSION_NUMBER << ")";
    THROW_RUNTIME_ERR(err);
  }

  index_.clear();
  size_t entryQty = 0;
  readBinaryPOD(input, entryQty);

  index_.clear();

  for (size_t qi = 0; qi < entryQty; qi++) {
    uint32_t wordId = 0;
    readBinaryPOD(input, wordId);
    size_t postQty = 0;
    readBinaryPOD(input, postQty);
    auto pl = unique_ptr<PostList>(new PostList(postQty));
    for (size_t pi = 0; pi < postQty; pi++) {
      PostEntry& e = pl->entries_[pi];
      readBinaryPOD(input, e.doc_id_);
      readBinaryPOD(input, e.val_);
    }
    index_.insert(make_pair(wordId, unique_ptr<PostList>(pl.release())));
  }
}

template <typename dist_t>
void SimplInvIndex<dist_t>::CreateIndex(const AnyParams& IndexParams) {
  AnyParamManager pmgr(IndexParams);
  CreateIndex(pmgr);
}

template <typename dist_t>
void SimplInvIndex<dist_t>::CreateIndex(AnyParamManager& ParamManager) {
  ParamManager.CheckUnused();
  // Always call ResetQueryTimeParams() to set query-time parameters to their default values
  this->ResetQueryTimeParams();

  // Let's first calculate memory requirements
  unordered_map<unsigned, size_t>   dict_qty;
  vector<SparseVectElem<dist_t>>    tmp_vect;
  LOG(LIB_INFO) << "Collecting dictionary stat";

  {
    unique_ptr<ProgressDisplay> pbar(printProgress_ ?
                                     new ProgressDisplay(this->data_.size(), cerr) : nullptr);

    for (const Object* o : this->data_) {
      tmp_vect.clear();
      UnpackSparseElements(o->data(), o->datalength(), tmp_vect);
      for (const auto& e : tmp_vect) dict_qty[e.id_] ++;
      if (pbar) ++(*pbar);
    }
    if (pbar) pbar->finish();
  }

  LOG(LIB_INFO) << "Actually creating the index";
  // post_pos is a map of positions in the postings lists? (initialized as zeros)
  unordered_map<unsigned, size_t> post_pos;
  // Create posting-list place holders
  for (const auto dictEntry : dict_qty) {
    unsigned wordId = dictEntry.first;
    size_t   qty = dictEntry.second;
    post_pos.insert(make_pair(wordId, 0));
    // insert into index_ PostLists with pre-allocated number of PostEntries
    index_.insert(make_pair(wordId, unique_ptr<PostList>(new PostList(qty))));
  }

  {
    unique_ptr<ProgressDisplay> pbar(printProgress_ ?
                                     new ProgressDisplay(this->data_.size(), cerr) : nullptr);

    // Fill posting lists
    for (size_t did = 0; did < this->data_.size(); ++did) {
      tmp_vect.clear();
      UnpackSparseElements(this->data_[did]->data(), this->data_[did]->datalength(), tmp_vect);
      // iterate over all terms in the document (non-zero values in the sparse vector)
      for (const auto& e : tmp_vect) {
        const auto wordId = e.id_;
        // posting list for given term (the value is probably a pair [position/key, value] ?)
        auto itPost     = index_.find(wordId);
        // actual position in the posting list
        auto itPostPos  = post_pos.find(wordId);
  #ifdef SANITY_CHECKS
        CHECK(itPost != index_.end());
        CHECK(itPostPos != post_pos.end());
        CHECK(itPost->second.get() != nullptr);
  #endif
        // the actual posting list
        PostList& pl = *itPost->second;
        // get actual position in the list (and shift it by +1)
        size_t curr_pos = itPostPos->second++;;
        CHECK(curr_pos < pl.qty_);
        pl.entries_[curr_pos] = PostEntry(did, e.val_);
      }
      if (pbar) ++(*pbar);
    }

    if (pbar) pbar->finish();
  }
#ifdef SANITY_CHECKS
  // Sanity check
  LOG(LIB_INFO) << "Sanity check";
  for (const auto dictEntry : dict_qty) {
    unsigned wordId = dictEntry.first;
    size_t qty = dictEntry.second;
    CHECK(qty == post_pos[wordId]);
  }
#endif
}

template <typename dist_t>
SimplInvIndex<dist_t>::~SimplInvIndex() {
// nothing here yet
}

template <typename dist_t>
void 
SimplInvIndex<dist_t>::SetQueryTimeParams(const AnyParams& QueryTimeParams) {
  // Check if a user specified extra parameters, which can be also misspelled variants of existing ones
  AnyParamManager pmgr(QueryTimeParams);
  int dummy;
  // Note that GetParamOptional() should always have a default value
  pmgr.GetParamOptional("dummyParam", dummy, -1);
  LOG(LIB_INFO) << "Set dummy = " << dummy;
  pmgr.CheckUnused();
}


template class SimplInvIndex<float>;

}
