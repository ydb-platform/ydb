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
#include "logging.h"
#include "bunit.h"
#include "thread_pool.h"

namespace similarity {
TEST(TestParallelFor) {
  std::vector<double> squares(1000);
  ParallelFor(0, squares.size(), 0, [&](int id, int threadId) {
    squares[id] = id * id;
  });
  for (size_t i = 0; i < squares.size(); ++i) {
    EXPECT_EQ(squares[i], static_cast<double>(i * i));
  }
}

TEST(TestParallelForException) {
  // make sure that exceptions in threads are re-thrown in the main thread
  std::vector<double> squares(1000);
  bool has_thrown = false;
  std::string message = "not gonna do it";
  try {
    ParallelFor(0, squares.size(), 0, [&](int id, int threadId) {
      if (id == 50) throw std::invalid_argument(message);
      squares[id] = id * id;
    });
  } catch (const std::invalid_argument & e) {
    EXPECT_EQ(message == e.what(), true);
    has_thrown = true;
  }
  EXPECT_EQ(has_thrown, true);
}
}  // namespace similarity
