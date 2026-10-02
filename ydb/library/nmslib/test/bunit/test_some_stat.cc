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
#include <vector>

#include "logging.h"
#include "bunit.h"
#include "utils.h"
#include "my_isnan_isinf.h"

namespace similarity {

TEST(TestMean) {
  vector<vector<float>> vTestData {
    {},
    {1.0,2.0,3.0},
    {1.0,1.0,1.0},
    {55,56,57,58}
  };
  
  for (const auto& v : vTestData) {
    float sum = 0;
    for (float e : v) sum += e;

    float m = Mean(v.size() ? &v[0] : 0, v.size())*v.size();

    EXPECT_FALSE(my_isnan(m)); 
    EXPECT_EQ_EPS(sum, m, 1e-5f);
  }
}

}  // namespace similarity

