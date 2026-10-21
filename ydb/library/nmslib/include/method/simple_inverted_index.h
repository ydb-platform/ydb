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
#ifndef _SIMPLE_INV_INDEX_H_
#define _SIMPLE_INV_INDEX_H_

#include <string>
#include <sstream>
#include <unordered_map>
#include <memory>

#include "index.h"
#include "space/space_sparse_scalar_fast.h"

#define METH_SIMPLE_INV_INDEX             "simple_invindx"

namespace similarity {

using std::string;

template <typename dist_t>
class SimplInvIndex : public Index<dist_t> {
 public:
  /*
   * The constructor here space and data-objects' references,
   * which are guaranteed to be be valid during testing.
   * So, we can memorize them safely.
   */
  SimplInvIndex(bool printProgress, 
              Space<dist_t>& space, 
              const ObjectVector& data) : Index<dist_t>(data),
                                          printProgress_(printProgress),
                                          pSpace_(dynamic_cast<SpaceSparseNegativeScalarProductFast*>(&space)) {
    if (pSpace_ == nullptr) {
      PREPARE_RUNTIME_ERR(err) <<
          "The method " << StrDesc() << " works only with the space " << SPACE_SPARSE_NEGATIVE_SCALAR_FAST;
      THROW_RUNTIME_ERR(err);
    }
  }

  void CreateIndex(const AnyParams& IndexParams) override; 

  virtual void SaveIndex(const string& location) override;
  virtual void LoadIndex(const string& location) override;

  void SetQueryTimeParams(const AnyParams& QueryTimeParams) override;

  ~SimplInvIndex() override;

  const std::string StrDesc() const override { 
    return METH_SIMPLE_INV_INDEX;
  }

  void Search(RangeQuery<dist_t>* query, IdType) const override { 
    throw runtime_error("Range search is not supported!");
  }
  void Search(KNNQuery<dist_t>* query, IdType) const override;

  virtual bool DuplicateData() const override { return false; }

 protected:

  // a protected creator that has already a created parameter manager
  void CreateIndex(AnyParamManager& ParamManager);

  struct PostEntry {
    IdType   doc_id_; // IdType is signed
    dist_t   val_;
    PostEntry(int32_t doc_id = 0, dist_t val = 0) : doc_id_(doc_id), val_(val) {}
  };
  struct PostList {
    // Variables are const, so that they can be initialized only in constructor
    // However, the memory that entries_ point to isn't const and can be modified
    const size_t     qty_;
    PostEntry* const entries_;
    PostList(size_t qty) :
        qty_(qty),
        entries_(new PostEntry[qty]) {
    }
    ~PostList() {
      delete [] entries_;
    }
  };

  /**
   * A structure that keeps information about current state of search within one posting list.
   */
  struct PostListQueryState {
    // pointer to the posting list (fixed from the beginning)
    const PostList*  post_;
    // actual position in the list
    size_t           post_pos_;
    // value of the respective term in the query (fixed from the beginning)
    const dist_t           qval_;
    // product of the values in query in the document (for given term)
    dist_t           qval_x_docval_;

    PostListQueryState(const PostList& pl, dist_t qval, dist_t qval_x_docval)
        : post_(&pl), post_pos_(0), qval_(qval), qval_x_docval_(qval_x_docval) {}
  };

  bool                                                     printProgress_;
  SpaceSparseNegativeScalarProductFast*                    pSpace_;
  std::unordered_map<uint32_t, std::unique_ptr<PostList>>  index_;
  // disable copy and assign
  DISABLE_COPY_AND_ASSIGN(SimplInvIndex);
};

}   // namespace similarity

#endif     // _SIMPLE_INV_INDEX_H_
