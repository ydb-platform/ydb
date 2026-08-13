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
#ifndef DISTCOMP_EDIST_HPP
#define DISTCOMP_EDIST_HPP

namespace similarity {

/* 
 * The maximum number of elements that will be kept on the stack
 * by the function levenshtein. 
 *
 * TODO:@leo If there are too many threads, we might run out stack memory.
 *           but it is probably extremely unlikely with the buffer of this size.
 *
 */

#define MAX_LEVEN_BUFFER_QTY  512

template<class T> int levenshtein(const T* p1, size_t len1, const T* p2, size_t len2) ;
template<class T> int levenshtein(const T &s1, const T & s2) {
  return levenshtein(s1.c_str(), s1.size(), s2.c_str(), s2.size());
}


}


#endif
