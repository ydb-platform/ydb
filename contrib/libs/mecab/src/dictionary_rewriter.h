// MeCab -- Yet Another Part-of-Speech and Morphological Analyzer
//
//
//  Copyright(C) 2001-2006 Taku Kudo <taku@chasen.org>
//  Copyright(C) 2004-2006 Nippon Telegraph and Telephone Corporation
#ifndef MECAB_DICTIONARY_REWRITER_H
#define MECAB_DICTIONARY_REWRITER_H

#include <vector>
#include <string>
#include <map>
#include "common.h"
#include "mecab.h"
#include "freelist.h"

namespace MeCab {

class Iconv;

class RewritePattern {
 private:
  std::vector<std::string> spat_;
  std::vector<std::string> dpat_;
 public:
  bool set_pattern(const char *src, const char *dst);
  bool rewrite(size_t size,
               const char **input,
               std::string *output) const;
};

class RewriteRules: public std::vector<RewritePattern> {
 public:
  bool rewrite(size_t size, const char **input,
               std::string *output) const;
};

struct FeatureSet {
  std::string ufeature;
  std::string lfeature;
  std::string rfeature;
};

class DictionaryRewriter {
 private:
  RewriteRules unigram_rewrite_;
  RewriteRules left_rewrite_;
  RewriteRules right_rewrite_;
  std::map<std::string, FeatureSet> cache_;

 public:
  bool open(const char *filename,
            Iconv *iconv = 0);
  void clear();
  bool rewrite(const std::string &feature,
               std::string *ufeature,
               std::string *lfeature,
               std::string *rfeature) const;

  bool rewrite2(const std::string &feature,
                std::string *ufeature,
                std::string *lfeature,
                std::string *rfeature);
};

class POSIDGenerator {
 private:
  RewriteRules rewrite_;
 public:
  bool open(const char *filename,
            Iconv *iconv = 0);
  void clear() { rewrite_.clear(); }
  int id(const char *key) const;
};
}
#endif
