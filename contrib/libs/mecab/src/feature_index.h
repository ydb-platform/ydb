//  MeCab -- Yet Another Part-of-Speech and Morphological Analyzer
//
//
//  Copyright(C) 2001-2006 Taku Kudo <taku@chasen.org>
//  Copyright(C) 2004-2006 Nippon Telegraph and Telephone Corporation
#ifndef MECAB_FEATUREINDEX_H_
#define MECAB_FEATUREINDEX_H_

#include <map>
#include <vector>
#include "mecab.h"
#include "mmap.h"
#include "darts.h"
#include "freelist.h"
#include "common.h"
#include "learner_node.h"
#include "string_buffer.h"
#include "dictionary_rewriter.h"

namespace MeCab {

class Param;

class FeatureIndex {
 public:
  virtual bool open(const Param &param) = 0;
  virtual void clear() = 0;
  virtual void close() = 0;
  virtual bool buildFeature(LearnerPath *path) = 0;

  void set_alpha(const double *alpha);

  size_t size() const { return maxid_; }

  bool buildUnigramFeature(LearnerPath *, const char *);
  bool buildBigramFeature(LearnerPath *, const char *, const char*);

  void calcCost(LearnerPath *path);
  void calcCost(LearnerNode *node);

  const char *strdup(const char *str);

  static bool convert(const Param &param,
                      const char *text_filename, std::string *output);
  static bool compile(const Param &param,
                      const char *text_filename, const char *binary_filename);

  explicit FeatureIndex(): feature_freelist_(8192 * 32),
                           char_freelist_(8192 * 32),
                           maxid_(0), alpha_(0) {}
  virtual ~FeatureIndex() {}

 protected:
  std::vector<int>     feature_;
  ChunkFreeList<int>   feature_freelist_;
  ChunkFreeList<char>  char_freelist_;
  std::vector<const char*>   unigram_templs_;
  std::vector<const char*>   bigram_templs_;
  DictionaryRewriter   rewrite_;
  StringBuffer         os_;
  size_t               maxid_;
  const double         *alpha_;

  virtual int id(const char *key) = 0;
  const char* getIndex(char **, char **, size_t);
  bool openTemplate(const Param &param);
};

class EncoderFeatureIndex: public FeatureIndex {
 public:
  bool open(const Param &param);
  void close();
  void clear();

  bool reopen(const char *filename,
              const char *charset,
              std::vector<double> *alpha,
              Param *param);

  bool save(const char *filename, const char *header) const;
  void shrink(size_t freq,
              std::vector<double> *observed);
  bool buildFeature(LearnerPath *path);
  void clearcache();

 private:
  std::map<std::string, int> dic_;
  std::map<std::string, std::pair<const int*, size_t> > feature_cache_;
  int id(const char *key);
};

class DecoderFeatureIndex: public FeatureIndex {
 public:
  bool open(const Param &param);
  void clear();
  void close();
  bool buildFeature(LearnerPath *path);

  const char *charset() const {
    return charset_;
  }

 private:
  bool openFromArray(const char *begin, const char *end);
  bool openBinaryModel(const Param &param);
  bool openTextModel(const Param &param);
  int id(const char *key);

  Mmap<char>  mmap_;
  std::string model_buffer_;
  const uint64_t *key_;
  const char *charset_;
};
}
#endif
