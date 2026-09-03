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
#ifndef _CHRONO_TIMER_H_
#define _CHRONO_TIMER_H_

#include <chrono>
#include <cstdint>
#include <cmath>

using namespace std::chrono;
using std::round;

class WallClockTimer {
public:
  high_resolution_clock::time_point t1, t2;

  WallClockTimer() {
    reset();
  }

  void reset() {
    t1 = high_resolution_clock::now();
    t2 = t1;
  }

  uint64_t elapsed() {
    duration<double> elapsed_seconds = t2 - t1;
    return static_cast<uint64_t>(round(1e6 * elapsed_seconds.count()));
  }

  uint64_t split() {
    t2 = high_resolution_clock::now();
    return elapsed();
  }
};

#endif

