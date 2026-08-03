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
#include <memory>
#include <string>
#include <cmath>
#include <vector>
#include <limits>

#include "bunit.h"
#include "pow.h"
#include "logging.h"
#include "utils.h"
#include "my_isnan_isinf.h"

namespace similarity {

using namespace std;

const float MAX_REL_DIFF = 1e-6f;

vector<float>   addExps = { 0, 0.125, 0.25, 0.5, 1, 1.5, 2, 2.5 };
vector<float>   vals    = { 0.1, 0.5, 1, 1.5, 2, 4};
vector<float>   signs   = { 1, -1};

template <typename T> bool runTest() {
  for (unsigned i = 0; i <= 128; ++i) {
    for (float a : addExps) {
      T oneExp = a + i; 

      PowerProxyObject<T> obj(oneExp);

      for (float vpos : vals) 
      for (float s : signs) {
        float v = vpos * s;
        if (s < 0 && (oneExp - static_cast<unsigned>(oneExp) > 1e-10)) {
          continue; // negative to the fractional degree will be nan
        }
        T expRes = pow(T(v), oneExp); 
        if (my_isinf(expRes)) {
          continue;
        }
        //cout << "Not skiping: " << v << " ^ " << oneExp << endl;
        T res = obj.pow(v);

        T absDiff = fabs(res - expRes);
        T relDiff = getRelDiff(res, expRes);
        if ( relDiff > MAX_REL_DIFF ) {
          LOG(LIB_ERROR) << "Mismatch for base=" << v << " exponent=" << oneExp << " expected: " << expRes << " obtained: " << res << " abs diff: " << absDiff << " rel diff: " << relDiff;
          return false;
        }
      }
    }
  }

  return true;
}

TEST(pow_float) {
  EXPECT_TRUE(runTest<float>());
}

TEST(pow_double) {
  EXPECT_TRUE(runTest<double>());
}

}  // namespace similarity
