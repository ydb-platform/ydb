//  MeCab -- Yet Another Part-of-Speech and Morphological Analyzer
//
//
//  Copyright(C) 2001-2006 Taku Kudo <taku@chasen.org>
//  Copyright(C) 2004-2006 Nippon Telegraph and Telephone Corporation
#ifndef MECAB_WRITER_H_
#define MECAB_WRITER_H_

#include <string>
#include "common.h"
#include "mecab.h"
#include "utils.h"
#include "scoped_ptr.h"
#include "string_buffer.h"

namespace MeCab {

class Param;

class Writer {
 public:
  Writer();
  virtual ~Writer();
  bool open(const Param &param);
  void close();

  bool writeNode(Lattice *lattice,
                 const char *format,
                 const Node *node, StringBuffer *s) const;
  bool writeNode(Lattice *lattice,
                 const Node *node,
                 StringBuffer *s) const;

  bool write(Lattice *lattice, StringBuffer *node) const;

  const char *what() { return what_.str(); }

 private:
  scoped_string node_format_;
  scoped_string bos_format_;
  scoped_string eos_format_;
  scoped_string unk_format_;
  scoped_string eon_format_;
  whatlog what_;

  bool writeLattice(Lattice *lattice, StringBuffer *s) const;
  bool writeWakati(Lattice *lattice, StringBuffer *s) const;
  bool writeNone(Lattice *lattice, StringBuffer *s) const;
  bool writeUser(Lattice *lattice, StringBuffer *s) const;
  bool writeDump(Lattice *lattice, StringBuffer *s) const;
  bool writeEM(Lattice *lattice, StringBuffer *s) const;

  bool (Writer::*write_)(Lattice *lattice, StringBuffer *s) const;
};
}

#endif  // WRITER_H_
