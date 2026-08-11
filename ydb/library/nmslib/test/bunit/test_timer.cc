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
#if !defined(_MSC_VER) && !defined(__APPLE__)

#include <sys/time.h>

#include "logging.h"
#include "ztimer.h"
#include "bunit.h"

namespace similarity {

// We expect both timers to differ in at most 1 ms
const unsigned TIMER_ERR_TOL = 1000;

/*
 * These test is intended to run only on Unix-like systems.
 */

/**
 *  author: Preston Bannister
 */
class WallClockTimerBannister {
public:
    struct timeval t1, t2;
    WallClockTimerBannister() :
        t1(), t2() {
        gettimeofday(&t1, 0);
        t2 = t1;
    }
    void reset() {
        gettimeofday(&t1, 0);
        t2 = t1;
    }
    uint64_t elapsed() {
        return ((t2.tv_sec - t1.tv_sec) * 1000ULL * 1000ULL) + ((t2.tv_usec - t1. tv_usec));
    }
    uint64_t split() {
        gettimeofday(&t2, 0);
        return elapsed();
    }
};

void BurnCPU(size_t qty = 10000000000) {
  size_t sum = 0;
  for (size_t i = 0; i < qty; ++i) {
    sum += i;
    sum *= qty;
  }
  LOG(LIB_INFO) << "Ignore: " << sum;
}

#ifdef DISABLE_LONG_TESTS
TEST(DISABLE_TestTimer) {
#else
TEST(TestTimer) {
#endif
  WallClockTimerBannister oldz;
  WallClockTimer          z;

  BurnCPU();
  oldz.split();
  z.split();

  LOG(LIB_INFO) << "Timer: " << z.elapsed() << " : " << oldz.elapsed();
  EXPECT_EQ(std::abs(static_cast<int64_t>(z.elapsed()) - static_cast<int64_t>(oldz.elapsed())) < TIMER_ERR_TOL, true);

  BurnCPU();
  oldz.split();
  z.split();

  LOG(LIB_INFO) << "Timer: " << z.elapsed() << " : " << oldz.elapsed();
  EXPECT_EQ(std::abs(static_cast<int64_t>(z.elapsed()) - static_cast<int64_t>(oldz.elapsed())) < TIMER_ERR_TOL, true);

  z.reset();
  oldz.reset();

  BurnCPU();
  oldz.split();
  z.split();

  LOG(LIB_INFO) << "Timer: " << z.elapsed() << " : " << oldz.elapsed();
  EXPECT_EQ(std::abs(static_cast<int64_t>(z.elapsed()) - static_cast<int64_t>(oldz.elapsed())) < TIMER_ERR_TOL, true);
}

}  // namespace similarity
#endif

