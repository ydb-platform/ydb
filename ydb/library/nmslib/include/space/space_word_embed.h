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
#ifndef _SPACE_WORD_EMBED_H_
#define _SPACE_WORD_EMBED_H_

#include <string>
#include <map>
#include <stdexcept>
#include <sstream>
#include <memory>

#include <string.h>
#include "global.h"
#include "object.h"
#include "utils.h"
#include "space.h"
#include "space_vector.h"

#define SPACE_WORD_EMBED              "word_embed"
#define SPACE_WORD_EMBED_DIST_L2      "l2"
#define SPACE_WORD_EMBED_DIST_COSINE  "cosine"

namespace similarity {

using std::string;
using std::unique_ptr;

enum EmbedDistSpace { kEmbedDistL2 = 0, kEmbedDistCosine = 1};

template <typename dist_t>
class WordEmbedSpace : public VectorSpaceSimpleStorage<dist_t> {
 public:
  explicit WordEmbedSpace(EmbedDistSpace distType) : distType_(distType) {}
  virtual ~WordEmbedSpace() {}

  /** Standard functions to read/write/create objects */ 
    // Create a string representation of an object.
    virtual string CreateStrFromObj(const Object* pObj, const string& externId) const;
    /*
     * Read a string representation of the next object in a file as well
     * as its label. Return false, on EOF.
     */
    virtual bool ReadNextObjStr(DataFileInputState &, string& strObj, LabelType& label, string& externId) const;
  /** End of standard functions to read/write/create objects */ 
  virtual string StrDesc() const;
 protected:
  virtual dist_t HiddenDistance(const Object* obj1, const Object* obj2) const;
  EmbedDistSpace distType_;

  DISABLE_COPY_AND_ASSIGN(WordEmbedSpace);
};

}  // namespace similarity

#endif
