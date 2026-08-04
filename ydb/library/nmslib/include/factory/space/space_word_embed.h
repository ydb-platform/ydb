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
#ifndef FACTORY_SPACE_WORD_EMBED_H
#define FACTORY_SPACE_WORD_EMBED_H

#include <space/space_word_embed.h>

namespace similarity {

/*
 * Creating functions.
 */

template <typename dist_t>
Space<dist_t>* CreateWordEmbed(const AnyParams& AllParams) {
  AnyParamManager pmgr(AllParams);

  EmbedDistSpace distType;

  string s;

  pmgr.GetParamRequired("dist",  s);

  ToLower(s); 

  if (SPACE_WORD_EMBED_DIST_L2 == s) 
    distType = kEmbedDistL2;
  else if (SPACE_WORD_EMBED_DIST_COSINE == s)
    distType = kEmbedDistCosine;
  else throw runtime_error("Unsupported/unknown distance type for embeddings: '" + s + "'");

  pmgr.CheckUnused();

  return new WordEmbedSpace<dist_t>(distType);
}

/*
 * End of creating functions.
 */

}

#endif
