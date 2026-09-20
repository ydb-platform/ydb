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
#include "utils.h"
#include "bunit.h"

#include <limits>
#include <iostream>

#include <eval_metrics.h>

using namespace std;

namespace similarity {

template <class dist_t, class EvalObj>
void testMetric(
          size_t ExactResultSize,
          vector<ResultEntry<dist_t>> exactEntries,
          vector<ResultEntry<dist_t>> approxEntries,
          const double expVal) {
  unordered_set<IdType> exactIds;
  unordered_set<IdType> approxIds;

  // Let's sort the entries
  std::sort(exactEntries.begin(), exactEntries.end());
  std::sort(approxEntries.begin(), approxEntries.end());

  {
    size_t i = 0;
    for (const auto& e:exactEntries) {
      ++i;
      if (i > ExactResultSize) break;
      exactIds.insert(e.mId);
    }
  }
  for (const auto& e:approxEntries) approxIds.insert(e.mId);

  double val = EvalObj()(ExactResultSize, exactEntries, exactIds, approxEntries, approxIds);

  EXPECT_EQ_EPS(expVal, val, 1e-4);
};

typedef ResultEntry<float>  RESF;
typedef ResultEntry<double>  RESD;
typedef ResultEntry<int>    RESI;

const float  EPSF = numeric_limits<float>::min();
const double EPSD = numeric_limits<double>::min();

const size_t KNN = 10;

TEST(TestRecallFloatRealCase1) {
  vector<vector<RESF>> exactEntries = {
   {
    RESF(571409,0,-0.33415),
    RESF(3625626,0,-0.189035),
    RESF(3912183,0,-0.145867),
    RESF(1097649,0,-0.129897),
    RESF(805074,0,-0.076682),
    RESF(1016013,0,-0.0281219),
    RESF(1768728,0,-0.0281219),
    RESF(16198,0,-0.0230081),
    RESF(117286,0,0.0152135),
    RESF(3091007,0,0.0166574)
   }
  };
  vector<vector<RESF>> approxEntries = {
   {
    RESF(571409,0,-0.33415),
    RESF(3912183,0,-0.145867),
    RESF(1097649,0,-0.129897),
    RESF(805074,0,-0.076682),
    RESF(1016013,0,-0.0281219),
    RESF(1768728,0,-0.0281219),
    RESF(16198,0,-0.0230081),
    RESF(117286,0,0.0152135),
    RESF(3091007,0,0.0166574),
    RESF(2827082,0,0.0561426)
   }
  };
  vector<float> expLogRelPosError {
    static_cast<float>(0.15505974124111666)
  };
  
  EXPECT_EQ(exactEntries.size(), approxEntries.size());
  EXPECT_EQ(exactEntries.size(), expLogRelPosError.size());

  for (size_t i = 0; i < exactEntries.size(); ++i) {
    testMetric<float,EvalLogRelPosError<float>>(KNN, exactEntries[i], approxEntries[i], expLogRelPosError[i]); 
  }
}

TEST(TestRecallDouble) {
  vector<vector<RESD>> exactEntries = {
    {},
    {},
    {RESD(0, 0, 100)},
    { RESD(0, 0, 1), RESD(1, 0, 2), RESD(3, 0, 3), RESD(4, 0, 4), RESD(5, 0, 5), RESD(6, 0, 6), RESD(7, 0, 7), RESD(8, 0, 8), RESD(9, 0, 9), RESD(10, 0, 10), },
    { RESD(0, 0, 1), RESD(1, 0, 2), RESD(3, 0, 3), RESD(4, 0, 4), RESD(5, 0, 5), RESD(6, 0, 6), RESD(7, 0, 7), RESD(8, 0, 8), RESD(9, 0, 9), RESD(10, 0, 10), },
  };
  vector<vector<RESD>> approxEntries = {
    {},
    {RESD(0, 0, 100)},
    {},
    { RESD(0, 0, 1), RESD(3, 0, 3), RESD(5, 0, 5), RESD(7, 0, 7), RESD(9, 0, 9), },
    { RESD(0, 0, 1), RESD(1, 0, 2), RESD(3, 0, 3), RESD(4, 0, 4), RESD(5, 0, 5), RESD(6, 0, 6), RESD(7, 0, 7), RESD(8, 0, 8), RESD(9, 0, 9), RESD(10, 0, 10), },
  };
  vector<double> expRecall {
    1,
    1,
    0.0,
    0.5,
    1.0,
  };
  
  EXPECT_EQ(exactEntries.size(), approxEntries.size());
  EXPECT_EQ(exactEntries.size(), expRecall.size());

  for (size_t i = 0; i < exactEntries.size(); ++i) {
    testMetric<double,EvalRecall<double>>(KNN, exactEntries[i], approxEntries[i], expRecall[i]); 
    // In a special case when there are no results recall should be equal to 1
    testMetric<double,EvalRecall<double>>(0, exactEntries[i], approxEntries[i], 1.0);
  }
}


TEST(TestRecallFloat) {
  vector<vector<RESF>> exactEntries = {
    {},
    {},
    {RESF(0, 0, 100)},
    { RESF(0, 0, 1), RESF(1, 0, 2), RESF(3, 0, 3), RESF(4, 0, 4), RESF(5, 0, 5), RESF(6, 0, 6), RESF(7, 0, 7), RESF(8, 0, 8), RESF(9, 0, 9), RESF(10, 0, 10), },
    { RESF(0, 0, 1), RESF(1, 0, 2), RESF(3, 0, 3), RESF(4, 0, 4), RESF(5, 0, 5), RESF(6, 0, 6), RESF(7, 0, 7), RESF(8, 0, 8), RESF(9, 0, 9), RESF(10, 0, 10), },
  };
  vector<vector<RESF>> approxEntries = {
    {},
    {RESF(0, 0, 100)},
    {},
    { RESF(0, 0, 1), RESF(3, 0, 3), RESF(5, 0, 5), RESF(7, 0, 7), RESF(9, 0, 9), },
    { RESF(0, 0, 1), RESF(1, 0, 2), RESF(3, 0, 3), RESF(4, 0, 4), RESF(5, 0, 5), RESF(6, 0, 6), RESF(7, 0, 7), RESF(8, 0, 8), RESF(9, 0, 9), RESF(10, 0, 10), },
  };
  vector<double> expRecall {
    1,
    1,
    0.0,
    0.5,
    1.0,
  };
  
  EXPECT_EQ(exactEntries.size(), approxEntries.size());
  EXPECT_EQ(exactEntries.size(), expRecall.size());

  for (size_t i = 0; i < exactEntries.size(); ++i) {
    testMetric<float,EvalRecall<float>>(KNN, exactEntries[i], approxEntries[i], expRecall[i]); 
    // In a special case when there are no results recall should be equal to 1
    testMetric<float,EvalRecall<float>>(0, exactEntries[i], approxEntries[i], 1.0);
  }
}

TEST(TestRecallInt) {
  vector<vector<RESI>> exactEntries = {
    {},
    {},
    {RESI(0, 0, 100)},
    { RESI(0, 0, 1), RESI(1, 0, 2), RESI(3, 0, 3), RESI(4, 0, 4), RESI(5, 0, 5), RESI(6, 0, 6), RESI(7, 0, 7), RESI(8, 0, 8), RESI(9, 0, 9), RESI(10, 0, 10), },
    { RESI(0, 0, 1), RESI(1, 0, 2), RESI(3, 0, 3), RESI(4, 0, 4), RESI(5, 0, 5), RESI(6, 0, 6), RESI(7, 0, 7), RESI(8, 0, 8), RESI(9, 0, 9), RESI(10, 0, 10), },
  };
  vector<vector<RESI>> approxEntries = {
    {},
    {RESI(0, 0, 100)},
    {},
    { RESI(0, 0, 1), RESI(3, 0, 3), RESI(5, 0, 5), RESI(7, 0, 7), RESI(9, 0, 9), },
    { RESI(0, 0, 1), RESI(1, 0, 2), RESI(3, 0, 3), RESI(4, 0, 4), RESI(5, 0, 5), RESI(6, 0, 6), RESI(7, 0, 7), RESI(8, 0, 8), RESI(9, 0, 9), RESI(10, 0, 10), },
  };
  vector<double> expRecall {
    1,
    1,
    0.0,
    0.5,
    1.0,
  };
  
  EXPECT_EQ(exactEntries.size(), approxEntries.size());
  EXPECT_EQ(exactEntries.size(), expRecall.size());

  for (size_t i = 0; i < exactEntries.size(); ++i) {
    testMetric<int,EvalRecall<int>>(KNN, exactEntries[i], approxEntries[i], expRecall[i]); 
    // In a special case when there are no results recall should be equal to 1
    testMetric<int,EvalRecall<int>>(0, exactEntries[i], approxEntries[i], 1.0);
  }
}

TEST(TestNumCloserDouble) {
  vector<vector<RESD>> exactEntries = {
    {},
    { RESD(0, 0, 0), RESD(1, 0, 1), RESD(2, 0, 2) },
    { RESD(0, 0, 0), RESD(1, 0, 1), RESD(2, 0, 2) },
    { RESD(0, 0, 0), RESD(1, 0, 1), RESD(2, 0, 2) },
    { RESD(0, 0, 0), RESD(1, 0, 1), RESD(2, 0, 2) },
  };
  vector<vector<RESD>> approxEntries = {
    {},
    {},
    { RESD(33, 0, 0), RESD(1, 0, 1), RESD(2, 0, 2) },
    { RESD(33, 0, EPSD), RESD(1, 0, 1 + EPSD), RESD(2, 0, 2 + EPSD) },
    { RESD(11, 0, 2), RESD(12, 0, 2.0001), RESD(13, 0, 2.0001) },
  };
  vector<double> expNumCloser {
    0,
    static_cast<double>(exactEntries[1].size()),
    0,
    0,
    2
  };
  
  EXPECT_EQ(exactEntries.size(), approxEntries.size());
  EXPECT_EQ(exactEntries.size(), expNumCloser.size());

  for (size_t i = 0; i < exactEntries.size(); ++i) {
    testMetric<double,EvalNumberCloser<double>>(KNN, exactEntries[i], approxEntries[i], expNumCloser[i]); 
    // In a special case when there are no results the number of points that are closer is 0 
    testMetric<double,EvalNumberCloser<double>>(0, exactEntries[i], approxEntries[i], 0.0); 
  }
}

TEST(TestNumCloserFloat) {
  vector<vector<RESF>> exactEntries = {
    {},
    { RESF(0, 0, 0), RESF(1, 0, 1), RESF(2, 0, 2) },
    { RESF(0, 0, 0), RESF(1, 0, 1), RESF(2, 0, 2) },
    { RESF(0, 0, 0), RESF(1, 0, 1), RESF(2, 0, 2) },
    { RESF(0, 0, 0), RESF(1, 0, 1), RESF(2, 0, 2) },
  };
  vector<vector<RESF>> approxEntries = {
    {},
    {},
    { RESF(33, 0, 0), RESF(1, 0, 1), RESF(2, 0, 2) },
    { RESF(33, 0, EPSF), RESF(1, 0, 1 + EPSF), RESF(2, 0, 2 + EPSF) },
    { RESF(11, 0, 2), RESF(12, 0, 2.0001), RESF(13, 0, 2.0001) },
  };
  vector<double> expNumCloser {
    0,
    static_cast<double>(exactEntries[1].size()),
    0,
    0,
    2
  };
  
  EXPECT_EQ(exactEntries.size(), approxEntries.size());
  EXPECT_EQ(exactEntries.size(), expNumCloser.size());

  for (size_t i = 0; i < exactEntries.size(); ++i) {
    testMetric<float,EvalNumberCloser<float>>(KNN, exactEntries[i], approxEntries[i], expNumCloser[i]); 
    // In a special case when there are no results the number of points that are closer is 0 
    testMetric<float,EvalNumberCloser<float>>(0, exactEntries[i], approxEntries[i], 0.0); 
  }
}

TEST(TestNumCloserInt) {
  vector<vector<RESI>> exactEntries = {
    {},
    { RESI(0, 0, 1), RESI(1, 0, 1), RESI(2, 0, 1) },
    { RESI(0, 0, 1), RESI(1, 0, 1), RESI(2, 0, 1) },
    { RESI(0, 0, 1), RESI(1, 0, 3), RESI(2, 0, 3) },
    { RESI(0, 0, 1), RESI(1, 0, 1), RESI(2, 0, 1) },
  };
  vector<vector<RESI>> approxEntries = {
    {},
    {},
    { RESI(0, 0, 1), RESI(1, 0, 1), RESI(2, 0, 1) },
    { RESI(33, 0, 2), RESI(1, 0, 3), RESI(2, 0, 3) },
    { RESI(10, 0, 1), RESI(11, 0, 1), RESI(12, 0, 1) },
  };
  vector<double> expNumCloser {
    0,
    static_cast<double>(exactEntries[1].size()),
    0,
    1,
    0
  };
  
  EXPECT_EQ(exactEntries.size(), approxEntries.size());
  EXPECT_EQ(exactEntries.size(), expNumCloser.size());

  for (size_t i = 0; i < exactEntries.size(); ++i) {
    testMetric<int,EvalNumberCloser<int>>(KNN, exactEntries[i], approxEntries[i], expNumCloser[i]); 
    // In a special case when there are no results the number of points that are closer is 0 
    testMetric<int,EvalNumberCloser<int>>(0, exactEntries[i], approxEntries[i], 0.0); 
  }
}

TEST(TestRelPosErrorDouble) {
  vector<vector<RESD>> exactEntries = {
    {},
    { RESD(0, 0, 0), RESD(1, 0, 1), RESD(2, 0, 2) },
    { RESD(0, 0, 0), RESD(1, 0, 1), RESD(2, 0, 2), RESD(3, 0, 3) },
    { RESD(0, 0, 33), RESD(1, 0, 33), RESD(2, 0, 33) },
    { RESD(0, 0, 33), RESD(1, 0, 33), RESD(2, 0, 33) },
  };
  vector<vector<RESD>> approxEntries = {
    {},
    {},
    { RESD(10, 0, 1), RESD(11, 0, 3) },
    { RESD(10, 0, 33), RESD(11, 0, 33), RESD(12, 0, 33) },
    { RESD(10, 0, 33 + + numeric_limits<double>::epsilon()), RESD(11, 0, 33 + + numeric_limits<double>::epsilon()), RESD(12, 0, 33 + numeric_limits<double>::epsilon()) },
  };
  vector<double> expLogRelPosError {
    0,
    log(static_cast<double>(exactEntries[1].size())),
    log(2),
    0,
    0,
  };
  
  EXPECT_EQ(exactEntries.size(), approxEntries.size());
  EXPECT_EQ(exactEntries.size(), expLogRelPosError.size());

  for (size_t i = 0; i < exactEntries.size(); ++i) {
    testMetric<double,EvalLogRelPosError<double>>(KNN, exactEntries[i], approxEntries[i], expLogRelPosError[i]); 
    /* 
     * In a special case when there are no results the relative position error should be 1.0 and
     * its logarithm should be zero
     */
    testMetric<double,EvalLogRelPosError<double>>(0, exactEntries[i], approxEntries[i], 0.0); 
  }
}

TEST(TestRelPosErrorFloat) {
  vector<vector<RESF>> exactEntries = {
    {},
    { RESF(0, 0, 0), RESF(1, 0, 1), RESF(2, 0, 2) },
    { RESF(0, 0, 0), RESF(1, 0, 1), RESF(2, 0, 2), RESF(3, 0, 3) },
    { RESF(0, 0, 33), RESF(1, 0, 33), RESF(2, 0, 33) },
    { RESF(0, 0, 33), RESF(1, 0, 33), RESF(2, 0, 33) },
  };
  vector<vector<RESF>> approxEntries = {
    {},
    {},
    { RESF(10, 0, 1), RESF(11, 0, 3) },
    { RESF(10, 0, 33), RESF(11, 0, 33), RESF(12, 0, 33) },
    { RESF(10, 0, 33 + + numeric_limits<float>::epsilon()), RESF(11, 0, 33 + + numeric_limits<float>::epsilon()), RESF(12, 0, 33 + numeric_limits<float>::epsilon()) },
  };
  vector<double> expLogRelPosError {
    0,
    log(static_cast<double>(exactEntries[1].size())),
    log(2),
    0,
    0,
  };
  
  EXPECT_EQ(exactEntries.size(), approxEntries.size());
  EXPECT_EQ(exactEntries.size(), expLogRelPosError.size());

  for (size_t i = 0; i < exactEntries.size(); ++i) {
    testMetric<float,EvalLogRelPosError<float>>(KNN, exactEntries[i], approxEntries[i], expLogRelPosError[i]); 
    /* 
     * In a special case when there are no results the relative position error should be 1.0 and
     * its logarithm should be zero
     */
    testMetric<float,EvalLogRelPosError<float>>(0, exactEntries[i], approxEntries[i], 0.0); 
  }
}

TEST(TestRelPosErrorInt) {
  vector<vector<RESI>> exactEntries = {
    {},
    { RESI(0, 0, 0), RESI(1, 0, 1), RESI(2, 0, 2) },
    { RESI(0, 0, 0), RESI(1, 0, 1), RESI(2, 0, 2), RESI(3, 0, 3) },
    { RESI(0, 0, 33), RESI(1, 0, 33), RESI(2, 0, 33) },
  };
  vector<vector<RESI>> approxEntries = {
    {},
    {},
    { RESI(10, 0, 1), RESI(11, 0, 3) },
    { RESI(10, 0, 33), RESI(11, 0, 33), RESI(12, 0, 33) },
  };
  vector<double> expLogRelPosError {
    0,
    log(static_cast<double>(exactEntries[1].size())),
    log(2),
    0,
  };
  
  EXPECT_EQ(exactEntries.size(), approxEntries.size());
  EXPECT_EQ(exactEntries.size(), expLogRelPosError.size());

  for (size_t i = 0; i < exactEntries.size(); ++i) {
    testMetric<int,EvalLogRelPosError<int>>(KNN, exactEntries[i], approxEntries[i], expLogRelPosError[i]); 
    /* 
     * In a special case when there are no results the relative position error should be 1.0 and
     * its logarithm should be zero
     */
    testMetric<int,EvalLogRelPosError<int>>(0, exactEntries[i], approxEntries[i], 0.0); 
  }
}

TEST(TestPrecisionOfApproxDouble) {
  vector<vector<RESD>> exactEntries = {
    {},
    { RESD(0, 0, 0), RESD(1, 0, 1), RESD(2, 0, 2) },
    { RESD(0, 0, 0), RESD(1, 0, 1), RESD(2, 0, 2), RESD(3, 0, 3) },
    { RESD(0, 0, 33), RESD(1, 0, 33), RESD(2, 0, 33) },
    { RESD(0, 0, 33), RESD(1, 0, 33), RESD(2, 0, 33) },
  };
  vector<vector<RESD>> approxEntries = {
    {},
    {},
    { RESD(10, 0, 1), RESD(11, 0, 3) },
    { RESD(10, 0, 33), RESD(11, 0, 33), RESD(12, 0, 33) },
    { RESD(10, 0, 33 + + numeric_limits<double>::epsilon()), RESD(11, 0, 33 + + numeric_limits<double>::epsilon()), RESD(12, 0, 33 + numeric_limits<double>::epsilon()) },
  };
  vector<double> expPrecApprox {
    1.0,
    0.0,
    0.5,
    1.0,
    1.0,
  };
  
  EXPECT_EQ(exactEntries.size(), approxEntries.size());
  EXPECT_EQ(exactEntries.size(), expPrecApprox.size());

  for (size_t i = 0; i < exactEntries.size(); ++i) {
    testMetric<double,EvalPrecisionOfApprox<double>>(KNN, exactEntries[i], approxEntries[i], expPrecApprox[i]); 
    // In a special case when there are no results, the precision of approximation should be equal to 1.0
    testMetric<double,EvalPrecisionOfApprox<double>>(0, exactEntries[i], approxEntries[i], 1.0); 
  }
}

TEST(TestPrecisionOfApproxFloat) {
  vector<vector<RESF>> exactEntries = {
    {},
    { RESF(0, 0, 0), RESF(1, 0, 1), RESF(2, 0, 2) },
    { RESF(0, 0, 0), RESF(1, 0, 1), RESF(2, 0, 2), RESF(3, 0, 3) },
    { RESF(0, 0, 33), RESF(1, 0, 33), RESF(2, 0, 33) },
    { RESF(0, 0, 33), RESF(1, 0, 33), RESF(2, 0, 33) },
  };
  vector<vector<RESF>> approxEntries = {
    {},
    {},
    { RESF(10, 0, 1), RESF(11, 0, 3) },
    { RESF(10, 0, 33), RESF(11, 0, 33), RESF(12, 0, 33) },
    { RESF(10, 0, 33 + + numeric_limits<float>::epsilon()), RESF(11, 0, 33 + + numeric_limits<float>::epsilon()), RESF(12, 0, 33 + numeric_limits<float>::epsilon()) },
  };
  vector<double> expPrecApprox {
    1.0,
    0.0,
    0.5,
    1.0,
    1.0,
  };
  
  EXPECT_EQ(exactEntries.size(), approxEntries.size());
  EXPECT_EQ(exactEntries.size(), expPrecApprox.size());

  for (size_t i = 0; i < exactEntries.size(); ++i) {
    testMetric<float,EvalPrecisionOfApprox<float>>(KNN, exactEntries[i], approxEntries[i], expPrecApprox[i]); 
    // In a special case when there are no results, the precision of approximation should be equal to 1.0
    testMetric<float,EvalPrecisionOfApprox<float>>(0, exactEntries[i], approxEntries[i], 1.0); 
  }
}

TEST(TestPrecisionOfApproxInt) {
  vector<vector<RESI>> exactEntries = {
    {},
    { RESI(0, 0, 0), RESI(1, 0, 1), RESI(2, 0, 2) },
    { RESI(0, 0, 0), RESI(1, 0, 1), RESI(2, 0, 2), RESI(3, 0, 3) },
    { RESI(0, 0, 33), RESI(1, 0, 33), RESI(2, 0, 33) },
  };
  vector<vector<RESI>> approxEntries = {
    {},
    {},
    { RESI(10, 0, 1), RESI(11, 0, 3) },
    { RESI(10, 0, 33), RESI(11, 0, 33), RESI(12, 0, 33) },
  };
  vector<double> expPrecApprox {
    1.0,
    0.0,
    0.5,
    1.0,
  };
  
  EXPECT_EQ(exactEntries.size(), approxEntries.size());
  EXPECT_EQ(exactEntries.size(), expPrecApprox.size());

  for (size_t i = 0; i < exactEntries.size(); ++i) {
    testMetric<int,EvalPrecisionOfApprox<int>>(KNN, exactEntries[i], approxEntries[i], expPrecApprox[i]); 
    // In a special case when there are no results, the precision of approximation should be equal to 1.0
    testMetric<int,EvalPrecisionOfApprox<int>>(0, exactEntries[i], approxEntries[i], 1.0); 
  }
}

template <class dist_t>
void testResultEntryIO(string fileName, const vector<ResultEntry<dist_t>>& testData) {
  unique_ptr<fstream>      cacheGSBinary;

  cacheGSBinary.reset(new fstream(fileName.c_str(),
                      std::ios::trunc | std::ios::out | ios::binary));
  for (const ResultEntry<dist_t>& e: testData) {
    e.writeBinary(*cacheGSBinary);
  }

  cacheGSBinary->close();
  
  cacheGSBinary.reset(new fstream(fileName.c_str(),
                                  std::ios::in | ios::binary));

  ResultEntry<dist_t> tmp;
  for (size_t i = 0; i < testData.size(); ++i) {
    EXPECT_FALSE(cacheGSBinary->eof());
    tmp.readBinary(*cacheGSBinary);
    EXPECT_EQ(testData[i], tmp);
  }

  cacheGSBinary->close();
}

TEST(TestResultEntryIntIO) {
  vector<RESI> testData;

  for (int i = 0; i < 300000; ++i) {
    testData.push_back(RESI(i, i % 10, i * 2));
  }

  testResultEntryIO("tmpfile.bin", testData);
}


TEST(TestResultEntryFloatIO) {
  vector<RESF> testData;

  for (int i = 0; i < 300000; ++i) {
    testData.push_back(RESF(i, i % 10, i * 0.01f));
  }

  testResultEntryIO("tmpfile.bin", testData);
}

TEST(TestResultEntryDoubleIO) {
  vector<RESD> testData;

  for (int i = 0; i < 300000; ++i) {
    testData.push_back(RESD(i, i % 10, i * 0.01));
  }

  testResultEntryIO("tmpfile.bin", testData);
}

}
