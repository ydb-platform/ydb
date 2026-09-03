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
#ifndef NONMETRICSPACELIB_PIVOT_NEIGHB_COMMON_H_H
#define NONMETRICSPACELIB_PIVOT_NEIGHB_COMMON_H_H

#include <idtype.h>
#include <vector>

namespace similarity {

using std::vector;

#define PERM_PROC_FAST_SCAN       "scan"
#define PERM_PROC_MAP             "map"
#define PERM_PROC_MERGE           "merge"
#define PERM_PROC_PRIOR_QUEUE     "pqueue"
#define PERM_PROC_WAND            "wand"

struct IdCount {
  size_t id;
  size_t qty;

  IdCount(size_t id = 0, int qty = 0) : id(id), qty(qty) { }
};

typedef vector<IdCount> VectIdCount;

typedef vector<IdTypeUnsign> PostingListInt;

inline void postListUnion(const VectIdCount &lst1, const PostingListInt lst2, VectIdCount &res) {
  res.clear();
  res.reserve((lst1.size() + lst2.size()) / 2);
  auto i1 = lst1.begin();
  auto i2 = lst2.begin();

  while (i1 != lst1.end() && i2 != lst2.end()) {
    size_t id2 = static_cast<size_t>(*i2);
    if (i1->id < id2) {
      res.push_back(*i1);
      ++i1;
    } else if (i1->id > id2) {
      res.push_back(IdCount(id2, 1));
      ++i2;
    } else {
      res.push_back(IdCount(i1->id, i1->qty + 1));
      ++i1;
      ++i2;
    }
  }
  while (i1 != lst1.end()) {
    res.push_back(*i1);
    ++i1;
  }
  while (i2 != lst2.end()) {
    res.push_back(IdCount(*i2, 1));
    ++i2;
  }
}

}

#endif //NONMETRICSPACELIB_PIVOT_NEIGHB_COMMON_H_H
