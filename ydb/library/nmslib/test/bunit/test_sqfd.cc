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
#if defined(WITH_EXTRAS)

#include <string.h>
#include "space.h"
#include "bunit.h"
#include "testdataset.h"
#include "space_sqfd.h"

namespace similarity {

Object* CreateSqfdObject(
    const std::vector<std::vector<float>>& cluster,
    const std::vector<float>& weight) {
  CHECK(cluster.size() > 0);
  CHECK(weight.size() == cluster.size());
  std::vector<char> buf(
      2*sizeof(int) + (cluster[0].size() + 1) * cluster.size() * sizeof(float));
  int* h = reinterpret_cast<int*>(&buf[0]);
  h[0] = cluster.size();
  h[1] = cluster[0].size();
  float* obj = reinterpret_cast<float*>(&buf[2*sizeof(int)]);
  int pos = 0;
  for (size_t i = 0; i < cluster.size(); ++i) {
    for (size_t j = 0; j < cluster[i].size(); ++j) {
      obj[pos++] = cluster[i][j];
    }
    obj[pos++] = weight[i];
  }
  return new Object(-1, -1, buf.size(), &buf[0]);
}

TEST(Sqfd_From_Article) {
  std::vector<std::vector<float>> cq = {{3,3}, {8,7}};
  std::vector<float> wq = {0.5, 0.5};
  Object* q = CreateSqfdObject(cq, wq);

  std::vector<std::vector<float>> co = {{4,7}, {9,5}, {8,1}};
  std::vector<float> wo = {0.5, 0.25, 0.25};
  Object* o = CreateSqfdObject(co, wo);

  SqfdFunction<float>* f = new SqfdHeuristicFunction<float>(1.0);
  Space<float>* space = new SpaceSqfd<float>(f);

  /*
  >>> import numpy as np
  >>> import math
  >>> w = np.array([0.5,0.5,-0.5,-0.25,-0.25])
  >>> a = np.array([[1.0, 0.135, 0.195, 0.137, 0.157],
                    [0.135, 1.0, 0.2, 0.309, 0.143],
                    [0.195, 0.2, 1.0, 0.157, 0.122],
                    [0.137, 0.309, 0.157, 1.0, 0.195],
                    [0.157, 0.143, 0.122, 0.195, 1.0]])
  >>> math.sqrt(w.dot(a).dot(w.transpose()))
  0.807
  */

  float d = space->IndexTimeDistance(q, o);
  EXPECT_EQ_EPS(d, 0.808f, 0.01f);

  delete space;
  delete q;
  delete o;
}

TEST(Sqfd) {
  std::vector<std::vector<float>> cq = {
    {0.382806,0.397073,0.661498,0.683582,0.203314,0.0871583,1},
    {0.482246,0.368699,0.701657,0.731006,0.175442,0.132232,0.20056},
    {0.740454,0.434634,0.661071,0.850084,0.681469,0.0610024,0.218037},
    {0.178604,0.416208,0.62079,0.437091,0.757451,0.0982573,0.256335},
    {0.518211,0.432369,0.639439,0.23629,0.690716,0.191468,0.193767},
    {0.250961,0.416317,0.621276,0.344846,0.763613,0.0738424,1},
    {0.609122,0.331734,0.760648,0.710042,0.769226,0.0996582,1},
    {0.744822,0.425876,0.551634,0.223641,0.23818,0.0885243,1},
    {0.843675,0.543647,0.541379,0.798141,0.496724,0.0357157,1},
    {0.612551,0.408074,0.600394,0.266899,0.234377,0.143155,0.252654}};
  std::vector<float> wq = {
    0.0822,0.1005,0.1314,0.0878,0.1087,0.1413,0.0397,0.0886,0.0832,0.1366};
  Object* q = CreateSqfdObject(cq, wq);

  std::vector<std::vector<float>> co = {
    {0.720299,0.460648,0.609983,0.733792,0.279245,0.0940223,0.940909},
    {0.732504,0.470709,0.584041,0.849335,0.51135,0.105338,0.100655},
    {0.790358,0.446342,0.585623,0.633481,0.834874,0.0901916,0.855607},
    {0.265014,0.441256,0.551832,0.435405,0.199537,0.111734,0.240841},
    {0.714692,0.469428,0.333677,0.1261,0.297041,0.0146298,0.987917},
    {0.194637,0.449039,0.533339,0.482084,0.214012,0.0459264,1},
    {0.288555,0.430071,0.558277,0.17054,0.765986,0.0694933,1},
    {0.268943,0.460447,0.544101,0.583028,0.829013,0.0607609,1},
    {0.23752,0.443694,0.554333,0.345023,0.773767,0.089284,0.310363},
    {0.55076,0.411417,0.602403,0.311277,0.628119,0.171292,0.163618}};
  std::vector<float> wo = {
    0.066,0.2385,0.0651,0.1085,0.12,0.0968,0.0684,0.0541,0.0965,0.0861};
  Object* o = CreateSqfdObject(co, wo);

  SqfdFunction<float>* f = new SqfdHeuristicFunction<float>(1.0);
  Space<float>* space = new SpaceSqfd<float>(f);

  float d = space->IndexTimeDistance(q, o);
  EXPECT_EQ_EPS(d, 0.214f, 0.01f);

  delete space;
  delete q;
  delete o;
}

}  // namespace similarity

#endif

