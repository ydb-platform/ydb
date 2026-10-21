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

#include "space/space_scalar.h"
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


TEST(SpaceNegativeScalarProduct) {
  VectorDataset1 dataset;
  const ObjectVector& dataobjects = dataset.GetDataObjects();

  unique_ptr<Space<FLOAT_TYPE>> space(new SpaceNegativeScalarProduct<FLOAT_TYPE>());
  const FLOAT_TYPE expected[8][8] = {
    {-0.886503, -1.042558, -0.645801, -1.10402, -0.583511, -0.994706, -0.659836, -0.572409},
    {-1.042558, -2.453145, -1.667548, -1.595114, -1.485174, -1.796691, -1.637668, -1.450365},
    {-0.645801, -1.667548, -1.33257, -0.979158, -1.031048, -1.494315, -1.370335, -1.354052},
    {-1.10402, -1.595114, -0.979158, -1.677519, -0.848998, -1.293448, -1.093687, -0.930073},
    {-0.583511, -1.485174, -1.031048, -0.848998, -1.012873, -1.301115, -1.016919, -0.996822},
    {-0.994706, -1.796691, -1.494315, -1.293448, -1.301115, -2.423948, -1.666355, -1.89671 },
    {-0.659836, -1.637668, -1.370335, -1.093687, -1.016919, -1.666355, -1.516758, -1.570482},
    {-0.572409, -1.450365, -1.354052, -0.930073, -0.996822, -1.89671, -1.570482, -1.778766}
  };

  for (size_t i = 0; i < 8; ++i) {
    for (size_t j = 0; j < 8; ++j) {
      const FLOAT_TYPE d = space->IndexTimeDistance(dataobjects[i], dataobjects[j]);
      EXPECT_EQ_EPS(expected[i][j], d, 1e-5f);
    }
  }
}


}  // namespace similarity

