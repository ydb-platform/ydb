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

#include "space/space_leven.h"
#include "distcomp_edist.h"
#include "bunit.h"
#include "testdataset.h"

namespace similarity {

const size_t NUM_TEST_SHORT_STR = 16;

using namespace std;

const char* pTestArr[NUM_TEST_SHORT_STR] = {
  "xyz", "beagcfa", "cea", "cb",
  "d", "c", "bdaf", "ddcd",
  "egbfa", "a", "fba", "bcccfe",
  "ab", "bfgbfdc", "bcbbgf", "bfbb"
};

class ShortStringDataSet : public TestDataset {
 public:
  ShortStringDataSet(SpaceLevenshtein& space) {

    for (unsigned i = 0; i < NUM_TEST_SHORT_STR; ++i) {
      dataobjects_.push_back(space.CreateObjFromStr(i, -1, pTestArr[i], NULL).release());
    }
  }
};


TEST(EditDistanceShort) {
  unique_ptr<SpaceLevenshtein> space(new SpaceLevenshtein());

  ShortStringDataSet dataset(*space);
  const ObjectVector& dataobjects = dataset.GetDataObjects();

  const int expected[16][16] = {
    {0, 7, 3, 3, 3, 3, 4, 4, 5, 3, 3, 6, 3, 7, 6, 4},
    {7, 0, 5, 6, 7, 6, 4, 6, 3, 6, 6, 4, 6, 5, 5, 6},
    {3, 5, 0, 2, 3, 2, 3, 4, 4, 2, 2, 5, 3, 7, 5, 4},
    {3, 6, 2, 0, 2, 1, 4, 3, 4, 2, 2, 5, 1, 6, 4, 3},
    {3, 7, 3, 2, 0, 1, 3, 3, 5, 1, 3, 6, 2, 6, 6, 4},
    {3, 6, 2, 1, 1, 0, 4, 3, 5, 1, 3, 5, 2, 6, 5, 4},
    {4, 4, 3, 4, 3, 4, 0, 3, 4, 3, 3, 4, 3, 5, 4, 3},
    {4, 6, 4, 3, 3, 3, 3, 0, 5, 4, 4, 5, 4, 6, 6, 4},
    {5, 3, 4, 4, 5, 5, 4, 5, 0, 4, 3, 5, 4, 4, 5, 4},
    {3, 6, 2, 2, 1, 1, 3, 4, 4, 0, 2, 6, 1, 7, 6, 4},
    {3, 6, 2, 2, 3, 3, 3, 4, 3, 2, 0, 6, 2, 5, 5, 2},
    {6, 4, 5, 5, 6, 5, 4, 5, 5, 6, 6, 0, 6, 5, 4, 5},
    {3, 6, 3, 1, 2, 2, 3, 4, 4, 1, 2, 6, 0, 6, 5, 3},
    {7, 5, 7, 6, 6, 6, 5, 6, 4, 7, 5, 5, 6, 0, 5, 4},
    {6, 5, 5, 4, 6, 5, 4, 6, 5, 6, 5, 4, 5, 5, 0, 3},
    {4, 6, 4, 3, 4, 4, 3, 4, 4, 4, 2, 5, 3, 4, 3, 0} };

  for (size_t i = 0; i < NUM_TEST_SHORT_STR; ++i) {
    for (size_t j = 0; j < NUM_TEST_SHORT_STR; ++j) {
      const int d = space->IndexTimeDistance(dataobjects[i], dataobjects[j]);
      if (expected[i][j] != d) {
        LOG(LIB_ERROR) << "Bug, expected: " << expected[i][j] << " got " << d
                       << "Strings: '" << pTestArr[i] << "' vs '" << pTestArr[j] << "'";
      }
      EXPECT_EQ(expected[i][j], d);
    }
  }
}

/*
 * This test is needed to merely verify the correctness
 * of the hybrid memory allocation strategy:
 * relatively short strings will use stack memory,
 * for longer ones we use malloc-based methods.
 */

string str1, str2;

class LongStringDataSet : public TestDataset {
 public:
  LongStringDataSet(SpaceLevenshtein& space) {

    str1 = string(MAX_LEVEN_BUFFER_QTY + 1, 'a');
    str2 = string(MAX_LEVEN_BUFFER_QTY, 'c') + str1 + string(MAX_LEVEN_BUFFER_QTY, 'b');

    dataobjects_.push_back(space.CreateObjFromStr(0, -1, str1, NULL).release());
    dataobjects_.push_back(space.CreateObjFromStr(0, -1, str2, NULL).release());
  }
};


TEST(EditDistanceLong) {
  unique_ptr<SpaceLevenshtein> space(new SpaceLevenshtein());

  LongStringDataSet dataset(*space);
  const ObjectVector& dataobjects = dataset.GetDataObjects();

  int d = space->IndexTimeDistance(dataobjects[0], dataobjects[1]);
  EXPECT_EQ(2 * MAX_LEVEN_BUFFER_QTY, d);
  d = space->IndexTimeDistance(dataobjects[1], dataobjects[0]);
  EXPECT_EQ(2 * MAX_LEVEN_BUFFER_QTY, d);

}

}  // namespace similarity
