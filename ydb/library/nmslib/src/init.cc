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
#include "init.h"

/*
 * Important note: there is some issue when compling using Intel.
 * It involves header inclusion order. If we include spaces first
 * it then somehow handicaps ability of 
 * boost/math/special_functions/detail/lanczos_sse2.hpp
 * to properly use SSE.
 */
#include "factory/init_methods.h"
#include "factory/init_spaces.h"

#include "logging.h"

#include <iostream>
#include <memory>

namespace similarity {

int                                                 defaultRandomSeed = 0;
thread_local std::unique_ptr<RandomGeneratorType>   randomGen;

void initLibrary(int seed, LogChoice choice, const char* pLogFile) {
  defaultRandomSeed = seed;

  /*
   * Note: we deliberately do NOT call std::ios_base::sync_with_stdio(false)
   * here. It is a process-wide side effect and this library is embedded into a
   * larger application, which must be free to choose its own I/O
   * synchronization policy.
   */
  InitializeLogger(choice, pLogFile);
  initSpaces();
  initMethods();
}
}
