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
#ifndef _MEMORY_USAGE_H_
#define _MEMORY_USAGE_H_

namespace similarity {

class MemUsage {
 public:
   MemUsage();

  /** returns the current virtual memory size of
      the process with pid in MB **/
  double get_vmsize();

 private:
#ifdef __linux
  char status_file_[50];
#endif
};

}   // namespace similarity

#endif      // _MEMORY_USAGE_H_

