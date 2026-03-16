//  MeCab -- Yet Another Part-of-Speech and Morphological Analyzer
//
//
//  Copyright(C) 2001-2006 Taku Kudo <taku@chasen.org>
//  Copyright(C) 2004-2006 Nippon Telegraph and Telephone Corporation
#ifndef MECAB_VITERBI_H_
#define MECAB_VITERBI_H_

#include <vector>
#include "mecab.h"
#include "thread.h"

namespace MeCab {

class Lattice;
class Param;
class Connector;
template <typename N, typename P> class Tokenizer;

class Viterbi {
 public:
  bool open(const Param &param);

  bool analyze(Lattice *lattice) const;

  const Tokenizer<Node, Path> *tokenizer() const;

  const Connector *connector() const;

  const char *what() { return what_.str(); }

  static bool buildResultForNBest(Lattice *lattice);

  Viterbi();
  virtual ~Viterbi();

 private:
  template <bool IsAllPath, bool IsPartial> bool viterbi(Lattice *lattice) const;

  static bool forwardbackward(Lattice *lattice);
  static bool initPartial(Lattice *lattice);
  static bool initNBest(Lattice *lattice);
  static bool buildBestLattice(Lattice *lattice);
  static bool buildAllLattice(Lattice *lattice);
  static bool buildAlternative(Lattice *lattice);

  scoped_ptr<Tokenizer<Node, Path> > tokenizer_;
  scoped_ptr<Connector> connector_;
  int                   cost_factor_;
  whatlog               what_;
};
}
#endif  // MECAB_VITERBI_H_
