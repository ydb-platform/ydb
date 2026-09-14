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

#include "space/space_lp.h"
#include "bunit.h"
#include "testdataset.h"

namespace similarity {

#define FLOAT_TYPE float

class VectorDataset1 : public TestDataset {
 public:
  VectorDataset1() {
    FLOAT_TYPE arr[8][5] = {
      {0.459, 0.04, 0.086, 0.599, 0.555},
      {0.842, 0.572, 0.801, 0.136, 0.87},
      {0.42, 0.773, 0.554, 0.198, 0.461},
      {0.958, 0.057, 0.376, 0.663, 0.419},
      {0.261, 0.312, 0.7, 0.108, 0.588},
      {0.079, 0.663, 0.921, 0.901, 0.564},
      {0.463, 0.806, 0.672, 0.388, 0.225},
      {0.174, 0.884, 0.801, 0.563, 0.092}
    };

    for (int i = 0; i < 8; ++i) {
      dataobjects_.push_back(new Object(i + 1, -1, 5 * sizeof(FLOAT_TYPE), &arr[i]));
    }
  }
};


TEST(EuclideanDistance) {
  VectorDataset1 dataset;
  const ObjectVector& dataobjects = dataset.GetDataObjects();

  unique_ptr<Space<FLOAT_TYPE>> space(new SpaceLp<FLOAT_TYPE>(2));
  const FLOAT_TYPE expected[8][8] = {
    {0.0, 1.120059, 0.96305295, 0.59664227, 0.85577684, 1.1493646, 1.04095581, 1.23306569},
    {1.120059, 0.0, 0.67128161, 0.96976079, 0.70403835, 1.1330097, 0.83340686, 1.15376817},
    {0.96305295, 0.67128161, 0.0, 1.02555984, 0.53230348, 0.87629218, 0.32963312, 0.63500551},
    {0.59664227, 0.96976079, 1.02555984, 0.0, 0.99619074, 1.23067908, 1.00344556, 1.26338394},
    {0.85577684, 0.70403835, 0.53230348, 0.99619074, 0.0, 0.91355952, 0.70412569, 0.89330565},
    {1.1493646, 1.1330097, 0.87629218, 1.23067908, 0.91355952, 0.0, 0.77974098, 0.63976089},
    {1.04095581, 0.83340686, 0.32963312, 1.00344556, 0.70412569, 0.77974098, 0.0, 0.39314119},
    {1.23306569, 1.15376817, 0.63500551, 1.26338394, 0.89330565, 0.63976089, 0.39314119, 0.0}
  };

  for (size_t i = 0; i < 8; ++i) {
    for (size_t j = 0; j < 8; ++j) {
      const FLOAT_TYPE d = space->IndexTimeDistance(dataobjects[i], dataobjects[j]);
      EXPECT_EQ_EPS(expected[i][j], d, 1e-5f);
    }
  }
}

TEST(ManhattanDistance) {
  VectorDataset1 dataset;
  const ObjectVector& dataobjects = dataset.GetDataObjects();

  unique_ptr<Space<FLOAT_TYPE>> space(new SpaceLp<FLOAT_TYPE>(1));
  const FLOAT_TYPE expected[8][8] = {
    {0.0, 2.408, 1.735, 1.006, 1.608, 2.149, 1.897, 2.343},
    {2.408, 0.0, 1.341, 2.034, 1.252, 2.045, 1.639, 2.185},
    {1.735, 1.341, 0.0, 1.939, 0.98299999, 1.624, 0.62, 1.338},
    {1.006, 2.034, 1.939, 0.0, 2.0, 2.41299999, 2.009, 2.463},
    {1.608, 1.252, 0.98299999, 2.0, 0.0, 1.571, 1.367, 1.711},
    {2.149, 2.045, 1.624, 2.41299999, 1.571, 0.0, 1.628, 1.246},
    {1.897, 1.639, 0.62, 2.009, 1.367, 1.628, 0.0, 0.804},
    {2.343, 2.185, 1.338, 2.463, 1.711, 1.246, 0.804, 0.0}
  };

  for (size_t i = 0; i < 8; ++i) {
    for (size_t j = 0; j < 8; ++j) {
      const FLOAT_TYPE d = space->IndexTimeDistance(dataobjects[i], dataobjects[j]);
      EXPECT_EQ_EPS(expected[i][j], d, 1e-5f);
    }
  }
}

TEST(ChebyshevDistance) {
  VectorDataset1 dataset;
  const ObjectVector& dataobjects = dataset.GetDataObjects();

  unique_ptr<Space<FLOAT_TYPE>> space(new SpaceLp<FLOAT_TYPE>(-1));
  const FLOAT_TYPE expected[8][8] = {
    {0.0, 0.715, 0.733, 0.499, 0.614, 0.835, 0.766, 0.844},
    {0.715, 0.0, 0.422, 0.527, 0.581, 0.765, 0.645, 0.778},
    {0.733, 0.422, 0.0, 0.716, 0.461, 0.703, 0.236, 0.369},
    {0.499, 0.527, 0.716, 0.0, 0.697, 0.879, 0.749, 0.827},
    {0.614, 0.581, 0.461, 0.697, 0.0, 0.793, 0.494, 0.572},
    {0.835, 0.765, 0.703, 0.879, 0.793, 0.0, 0.513, 0.472},
    {0.766, 0.645, 0.236, 0.749, 0.494, 0.513, 0.0, 0.289},
    {0.844, 0.778, 0.369, 0.827, 0.572, 0.472, 0.289, 0.0}
  };

  for (size_t i = 0; i < 8; ++i) {
    for (size_t j = 0; j < 8; ++j) {
      const FLOAT_TYPE d = space->IndexTimeDistance(dataobjects[i], dataobjects[j]);
      EXPECT_EQ_EPS(expected[i][j], d, 1e-5f);
    }
  }
}


}  // namespace similarity

