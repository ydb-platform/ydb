//  MeCab -- Yet Another Part-of-Speech and Morphological Analyzer
//
//
//  Copyright(C) 2001-2006 Taku Kudo <taku@chasen.org>
//  Copyright(C) 2004-2006 Nippon Telegraph and Telephone Corporation
#ifndef MECAB_LEARNER_NODE_H_
#define MECAB_LEARNER_NODE_H_

#include <cstring>
#include "mecab.h"
#include "common.h"
#include "utils.h"

struct mecab_learner_path_t {
  struct mecab_learner_node_t*  rnode;
  struct mecab_learner_path_t*  rnext;
  struct mecab_learner_node_t*  lnode;
  struct mecab_learner_path_t*  lnext;
  double                        cost;
  const int                     *fvector;
};

struct mecab_learner_node_t {
  struct mecab_learner_node_t *prev;
  struct mecab_learner_node_t *next;
  struct mecab_learner_node_t *enext;
  struct mecab_learner_node_t *bnext;
  struct mecab_learner_path_t *rpath;
  struct mecab_learner_path_t *lpath;
  struct mecab_learner_node_t *anext;
  const char                  *surface;
  const char                  *feature;
  unsigned int                 id;
  unsigned short               length;
  unsigned short               rlength;
  unsigned short               rcAttr;
  unsigned short               lcAttr;
  unsigned short               posid;
  unsigned char                char_type;
  unsigned char                stat;
  unsigned char                isbest;
  double                       alpha;
  double                       beta;
  short                        wcost2;
  double                       wcost;
  double                       cost;
  const int                    *fvector;
  struct mecab_token_t         *token;
};

namespace MeCab {

typedef struct mecab_learner_path_t LearnerPath;
typedef struct mecab_learner_node_t LearnerNode;

template <class T1, class T2> T1 repeat_find_if(T1 b, T1 e,
                                                const T2& v, size_t n) {
  T1 r = b;
  for (size_t i = 0; i < n; ++i) {
    r = std::find(b, e, v);
    if (r == e) return e;
    b = r + 1;
  }
  return r;
}

// NOTE: first argment:   answer,
//       second argment:  system output
inline bool node_cmp_eq(const LearnerNode &node1,
                        const LearnerNode &node2,
                        size_t size, size_t unk_size) {
  if (node1.length == node2.length &&
      strncmp(node1.surface, node2.surface, node1.length) == 0) {
    const char *p1 = node1.feature;
    const char *p2 = node2.feature;
    // There is NO case when node1 becomes MECAB_UNK_NODE
    if (node2.stat == MECAB_UNK_NODE)
      size = unk_size;  // system cannot output other extra information
    const char *r1 = repeat_find_if(p1, p1 + std::strlen(p1), ',', size);
    const char *r2 = repeat_find_if(p2, p2 + std::strlen(p2), ',', size);
    if (static_cast<size_t>(r1 - p1) == static_cast<size_t>(r2 - p2) &&
        std::strncmp(p1, p2, static_cast<size_t>(r1 - p1)) == 0) {
      return true;
    }
  }

  return false;
}

inline bool is_empty(LearnerPath *path) {
  return ((!path->rnode->rpath && path->rnode->stat != MECAB_EOS_NODE) ||
          (!path->lnode->lpath && path->lnode->stat != MECAB_BOS_NODE) );
}

inline void calc_expectation(LearnerPath *path, double *expected, double Z) {
  if (is_empty(path)) {
    return;
  }

  const double c = std::exp(path->lnode->alpha +
                            path->cost +
                            path->rnode->beta - Z);

  for (const int *f = path->fvector; *f != -1; ++f) {
    expected[*f] += c;
  }

  if (path->rnode->stat != MECAB_EOS_NODE) {
    for (const int *f = path->rnode->fvector; *f != -1; ++f) {
      expected[*f] += c;
    }
  }
}

inline void calc_alpha(LearnerNode *n) {
  n->alpha = 0.0;
  for (LearnerPath *path = n->lpath; path; path = path->lnext) {
    n->alpha = logsumexp(n->alpha,
                         path->cost + path->lnode->alpha,
                         path == n->lpath);
  }
}

inline void calc_beta(LearnerNode *n) {
  n->beta = 0.0;
  for (LearnerPath *path = n->rpath; path; path = path->rnext) {
    n->beta = logsumexp(n->beta,
                        path->cost + path->rnode->beta,
                        path == n->rpath);
  }
}
}

#endif  // MECAB_LEARNER_NODE_H_
