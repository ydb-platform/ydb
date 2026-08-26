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
#ifndef _INIT_H_
#define _INIT_H_

/*
 * This function should be called only *ONCE*,
 * but before actually using library functionality.
 */

#include "logging.h"

namespace similarity {

  void initLibrary(int seed = 0, LogChoice choice = LIB_LOGNONE, const char*pLogFile = NULL);
}

#endif
