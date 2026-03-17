//  MeCab -- Yet Another Part-of-Speech and Morphological Analyzer
//
//
//  Copyright(C) 2001-2006 Taku Kudo <taku@chasen.org>
//  Copyright(C) 2004-2006 Nippon Telegraph and Telephone Corporation
#ifndef MECAB_TAGGER_H
#define MECAB_TAGGER_H

#include <vector>
#include "mecab.h"
#include "freelist.h"
#include "feature_index.h"
#include "tokenizer.h"
#include "scoped_ptr.h"

namespace MeCab {

class FeatureIndex;

class LearnerTagger {
 public:
  bool empty() const { return (len_ == 0); }
  void close() {}
  void clear() {}

  explicit LearnerTagger(): tokenizer_(0), path_allocator_(0),
                            feature_index_(0), begin_(0), end_(0), len_(0) {}
  virtual ~LearnerTagger() {}

 protected:
  Tokenizer<LearnerNode, LearnerPath> *tokenizer_;
  Allocator<LearnerNode, LearnerPath> *allocator_;
  FreeList<LearnerPath>               *path_allocator_;
  FeatureIndex                        *feature_index_;
  scoped_string                        begin_data_;
  const char                          *begin_;
  const char                          *end_;
  size_t                               len_;
  std::vector<LearnerNode *>           begin_node_list_;
  std::vector<LearnerNode *>           end_node_list_;

  LearnerNode *lookup(size_t);
  bool connect(size_t, LearnerNode *);
  bool viterbi();
  bool buildLattice();
  bool initList();
};

class EncoderLearnerTagger: public LearnerTagger {
 public:
  bool open(Tokenizer<LearnerNode, LearnerPath> *tokenzier,
            Allocator<LearnerNode, LearnerPath> *allocator,
            FeatureIndex *feature_index,
            size_t eval_size, size_t unk_eval_size);
  bool read(std::istream *, std::vector<double> *);
  int eval(size_t *, size_t *, size_t *) const;
  double gradient(double *expected);
  explicit EncoderLearnerTagger(): eval_size_(1024), unk_eval_size_(1024) {}
  virtual ~EncoderLearnerTagger() { close(); }

 private:
  size_t eval_size_;
  size_t unk_eval_size_;
  std::vector<LearnerPath *> ans_path_list_;
};

class DecoderLearnerTagger: public LearnerTagger {
 public:
  bool open(const Param &);
  bool parse(std::istream *, std::ostream *);
  virtual ~DecoderLearnerTagger() { close(); }

 private:
  scoped_ptr<Tokenizer<LearnerNode, LearnerPath> > tokenizer_data_;
  scoped_ptr<Allocator<LearnerNode, LearnerPath> > allocator_data_;
  scoped_ptr<FeatureIndex> feature_index_data_;
};
}

#endif
