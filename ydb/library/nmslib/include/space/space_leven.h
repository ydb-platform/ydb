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
#ifndef _SPACE_LEVENSHTEIN_H_
#define _SPACE_LEVENSHTEIN_H_

#include <string>
#include <map>
#include <stdexcept>
#include <algorithm>

#include <string.h>
#include "distcomp.h"
#include "space/space_string.h"

namespace similarity {

#define SPACE_LEVENSHTEIN       "leven"
#define SPACE_LEVENSHTEIN_NORM  "normleven"

class SpaceLevenshtein : public StringSpace<int> {
 public:
  explicit SpaceLevenshtein() {}
  virtual ~SpaceLevenshtein() {}
  virtual std::string StrDesc() const { return "Levenshtein distance"; }
 protected:
  virtual int HiddenDistance(const Object* obj1, const Object* obj2) const {
    CHECK(obj1->datalength() > 0);
    CHECK(obj2->datalength() > 0);
    const char* x = reinterpret_cast<const char*>(obj1->data());
    const char* y = reinterpret_cast<const char*>(obj2->data());
    const size_t len1 = obj1->datalength() / sizeof(char);
    const size_t len2 = obj2->datalength() / sizeof(char);

    return levenshtein(x, len1, y, len2);
  }
  DISABLE_COPY_AND_ASSIGN(SpaceLevenshtein);
};

class SpaceLevenshteinNormalized : public StringSpace<float> {
 public:
  explicit SpaceLevenshteinNormalized() {}
  virtual ~SpaceLevenshteinNormalized() {}
  virtual std::string StrDesc() const { return "Normalized Levenshtein distance"; }
 protected:
  virtual float HiddenDistance(const Object* obj1, const Object* obj2) const {
    CHECK(obj1->datalength() > 0);
    CHECK(obj2->datalength() > 0);
    const char* x = reinterpret_cast<const char*>(obj1->data());
    const char* y = reinterpret_cast<const char*>(obj2->data());
    const size_t len1 = obj1->datalength() / sizeof(char);
    const size_t len2 = obj2->datalength() / sizeof(char);

    if (0 == len1 && 0 == len2) return 0;
    CHECK(len1 || len2);
    return float(levenshtein(x, len1, y, len2))/std::max(len1, len2);
  }
  DISABLE_COPY_AND_ASSIGN(SpaceLevenshteinNormalized);
};

}  // namespace similarity

#endif
