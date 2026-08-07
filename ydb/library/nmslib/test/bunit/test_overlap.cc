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

#include <logging.h>
#include <idtype.h>
#include <distcomp.h>


#include <vector>
#include <cmath>

#include <space/space_sparse_vector_inter.h>
#include "bunit.h"
#include "my_isnan_isinf.h"

using namespace std;

namespace similarity {

vector<vector<IdType>> vvIds1 = {
 {1, 2, 3, 4},

 {1, 2, 3, 4},
 {1, 2, 3, 4},
 {1, 2, 3, 4},
 {1, 2, 3, 4},

 {1, 2, 3, 4},
 {1, 2, 3, 4},
 {1, 2, 3, 4},
 {1, 2, 3, 4},
 {1, 2, 3, 4},
 {1, 2, 3, 4},

 {1, 2, 3, 4},
 {1, 2, 3, 4},
 {1, 2, 3, 4},
 {1, 2, 3, 4},

 {1, 2, 3, 4},
 {},
 {1, 2, 3, 4},
 {1, 2, 3, 4},
 {1, 2, 3, 4},
};

vector<vector<IdType>> vvIds2 = {
 {1, 2, 3, 4},

 {2, 3, 4},
 {1, 3, 4},
 {1, 2, 4},
 {1, 2, 3},

 {1, 2},
 {1, 3},
 {1, 4},
 {2, 3},
 {2, 4},
 {3, 4},

 {1},
 {2},
 {3},
 {4},

 {},
 {},
 {5,6,7,8,9},
 {-2,-1,0},
 {-2,-1,0,5,6,7,8,9},
};

vector<size_t> vInterQty = {
 4,

 3,3,3,3,

 2,2,2,2,2,2,

 1,1,1,1,


 0, 0, 0, 0, 0
};

TEST(TestIntersect2Way) {
  EXPECT_EQ(vvIds1.size(), vvIds2.size());
  EXPECT_EQ(vvIds1.size(), vInterQty.size());

  for (size_t i = 0; i < vvIds1.size(); ++i) {
    size_t qty1 = IntersectSizeScalarFast(vvIds1[i].size() ? &vvIds1[i][0] : 0, vvIds1[i].size(),
                                          vvIds2[i].size() ? &vvIds2[i][0] : 0, vvIds2[i].size());
    size_t qty2 = IntersectSizeScalarStand(vvIds1[i].size() ? &vvIds1[i][0] : 0, vvIds1[i].size(),
                                           vvIds2[i].size() ? &vvIds2[i][0] : 0, vvIds2[i].size());
    EXPECT_EQ(qty1, vInterQty[i]);
    EXPECT_EQ(qty2, vInterQty[i]);

    if (qty1 != vInterQty[i] || 
        qty2 != vInterQty[i])
         LOG(LIB_INFO) << "Failed test (basic intersect funcs), index: " << i;

    const float oneElemVal1 = 0.1;
    const float oneElemVal2 = 0.2;
    vector<SparseVectElem<float>> elems1, elems2;
  
    for (IdType id1 : vvIds1[i]) {
      elems1.push_back(SparseVectElem<float>(id1, oneElemVal1));
    } 
    for (IdType id2 : vvIds2[i]) {
      elems2.push_back(SparseVectElem<float>(id2, oneElemVal2));
    } 
    OverlapInfo oinfo = SpaceSparseVectorInter<float>::ComputeOverlapInfo(elems1, elems2);

    if (oinfo.overlap_qty_ != vInterQty[i])
      LOG(LIB_INFO) << "Failed test (ComputeOverlapInfo), index: " << i;

    EXPECT_EQ((size_t)oinfo.overlap_qty_, vInterQty[i]);
    
    float vectQty1 = elems1.size();
    float norm1 = sqrt(oneElemVal1*oneElemVal1*vectQty1);
    float vectQty2 = elems2.size();
    float norm2 = sqrt(oneElemVal2*oneElemVal2*vectQty2);

    float overlap_qty = oinfo.overlap_qty_;
    float overlap_dotprod_norm = std::min(norm1, norm2) >0 ? overlap_qty * oneElemVal1 * oneElemVal2 / (norm1 * norm2) : 0; 
    CHECK(!my_isnan(overlap_dotprod_norm));
  
    const float eps = 0.0001f; // somewhat adhoc

    EXPECT_EQ_EPS(overlap_dotprod_norm, oinfo.overlap_dotprod_norm_, eps);
    //cerr << overlap_dotprod_norm << " # " << my_isnan(overlap_dotprod_norm) << " -> " << oinfo.overlap_dotprod_norm_ << endl;

    float diff_sum_left    = oneElemVal1*(vectQty1 - overlap_qty);
    CHECK(!my_isnan(diff_sum_left));
    EXPECT_EQ_EPS(diff_sum_left, oinfo.diff_sum_left_, eps);
    EXPECT_EQ_EPS(vectQty1 > overlap_qty ? oneElemVal1 : 0, oinfo.diff_mean_left_, eps);

    float overlap_sum_left = oneElemVal1*overlap_qty;
    CHECK(!my_isnan(overlap_sum_left));
    EXPECT_EQ_EPS(overlap_sum_left, oinfo.overlap_sum_left_, eps);
    EXPECT_EQ_EPS(overlap_qty > 0 ? oneElemVal1 : 0, oinfo.overlap_mean_left_, eps);

    float diff_sum_right = oneElemVal2*(vectQty2 - overlap_qty);
    CHECK(!my_isnan(diff_sum_right));
    EXPECT_EQ_EPS(diff_sum_right, oinfo.diff_sum_right_, eps);
    EXPECT_EQ_EPS(vectQty2 > overlap_qty ? oneElemVal2 : 0, oinfo.diff_mean_right_, eps);

    float overlap_sum_right = oneElemVal2*overlap_qty;
    CHECK(!my_isnan(overlap_sum_left));
    EXPECT_EQ_EPS(overlap_sum_right, oinfo.overlap_sum_right_, eps);
    EXPECT_EQ_EPS(overlap_qty > 0 ? oneElemVal2 : 0, oinfo.overlap_mean_right_, eps);
  }
}

TEST(TestOverlapInfoDetailed) {
  /* 
   * In this function, we carry out a more detailed testing with a focus on on correctness of computation of STD and mean.
   * We try to consider all typical combinations of the overlap and remaining part sizes.
   */
  const size_t COMBIN_QTY = 5;
  for (size_t qDiffLeft = 0; qDiffLeft < COMBIN_QTY; ++qDiffLeft)
  for (size_t qOverlap = 0; qOverlap < COMBIN_QTY; ++qOverlap)
  for (size_t qDiffRight = 0; qDiffRight < COMBIN_QTY; ++qDiffRight) {
    vector<float> vDiffLeft, vOverlapLeft;
    vector<float> vDiffRight, vOverlapRight;

    vector<IdType>                vIds1, vIds2;
    vector<SparseVectElem<float>> elems1, elems2;

    const float oneElemMul1 = 0.1;
    const float oneElemMul2 = 0.2;

    float norm1 = 0, norm2 = 0;

    for (size_t id = 0; id < qDiffLeft; ++id) {
      vIds1.push_back(id);
      float val1 = id * oneElemMul1;
      elems1.emplace_back(id, val1);
      vDiffLeft.push_back(val1);
      norm1 += val1 * val1;
    }
    float overlap_dotprod_norm = 0;
    for (size_t id = COMBIN_QTY; id < COMBIN_QTY + qOverlap; ++id) {
      vIds1.push_back(id);
      float val1 = id * oneElemMul1;
      elems1.emplace_back(id, val1);
      vOverlapLeft.push_back(val1);
      norm1 += val1 * val1;

      float val2 = id * oneElemMul2;
      vIds2.push_back(id);
      elems2.emplace_back(id, val2);
      vOverlapRight.push_back(val2);
      norm2 += val2 * val2;

      overlap_dotprod_norm += val1 * val2;
    }
    for (size_t id = 2*COMBIN_QTY; id < 2*COMBIN_QTY + qDiffRight; ++id) {
      vIds2.push_back(id);
      float val2 = id * oneElemMul2;
      elems2.emplace_back(id, val2);
      vDiffRight.push_back(val2);
      norm2 += val2 * val2;
    }
    norm1 = sqrt(norm1);
    norm2 = sqrt(norm2);
    if (norm1 > 0) overlap_dotprod_norm /= norm1;
    if (norm2 > 0) overlap_dotprod_norm /= norm2;

    size_t qty1 = IntersectSizeScalarFast(vIds1.size() ? &vIds1[0] : 0, vIds1.size(),
            vIds2.size() ? &vIds2[0] : 0, vIds2.size());
    size_t qty2 = IntersectSizeScalarStand(vIds1.size() ? &vIds1[0] : 0, vIds1.size(),
            vIds2.size() ? &vIds2[0] : 0, vIds2.size());
    EXPECT_EQ(qty1, qOverlap);
    EXPECT_EQ(qty2, qOverlap);

    OverlapInfo oinfo = SpaceSparseVectorInter<float>::ComputeOverlapInfo(elems1, elems2);

    EXPECT_EQ(qOverlap, (size_t)oinfo.overlap_qty_);
    
    const float eps = 0.0001f; // somewhat adhoc

    EXPECT_EQ_EPS(overlap_dotprod_norm, oinfo.overlap_dotprod_norm_, eps);

    EXPECT_EQ_EPS(Sum(vDiffLeft.size() ? &vDiffLeft[0] : 0, vDiffLeft.size()), oinfo.diff_sum_left_, eps);
    EXPECT_EQ_EPS(Mean(vDiffLeft.size() ? &vDiffLeft[0] : 0, vDiffLeft.size()), oinfo.diff_mean_left_, eps);
    EXPECT_EQ_EPS(StdDev(vDiffLeft.size() ? &vDiffLeft[0] : 0, vDiffLeft.size()), oinfo.diff_std_left_, eps);

    EXPECT_EQ_EPS(Sum(vOverlapLeft.size() ? &vOverlapLeft[0] : 0, vOverlapLeft.size()), oinfo.overlap_sum_left_, eps);
    EXPECT_EQ_EPS(Mean(vOverlapLeft.size() ? &vOverlapLeft[0] : 0, vOverlapLeft.size()), oinfo.overlap_mean_left_, eps);
    EXPECT_EQ_EPS(StdDev(vOverlapLeft.size() ? &vOverlapLeft[0] : 0, vOverlapLeft.size()), oinfo.overlap_std_left_, eps);

    EXPECT_EQ_EPS(Sum(vDiffRight.size() ? &vDiffRight[0] : 0, vDiffRight.size()), oinfo.diff_sum_right_, eps);
    EXPECT_EQ_EPS(Mean(vDiffRight.size() ? &vDiffRight[0] : 0, vDiffRight.size()), oinfo.diff_mean_right_, eps);
    EXPECT_EQ_EPS(StdDev(vDiffRight.size() ? &vDiffRight[0] : 0, vDiffRight.size()), oinfo.diff_std_right_, eps);

    EXPECT_EQ_EPS(Sum(vOverlapRight.size() ? &vOverlapRight[0] : 0, vOverlapRight.size()), oinfo.overlap_sum_right_, eps);
    EXPECT_EQ_EPS(Mean(vOverlapRight.size() ? &vOverlapRight[0] : 0, vOverlapRight.size()), oinfo.overlap_mean_right_, eps);
    EXPECT_EQ_EPS(StdDev(vOverlapRight.size() ? &vOverlapRight[0] : 0, vOverlapRight.size()), oinfo.overlap_std_right_, eps);

  } 
}

vector<vector<IdType>> vvAddIds1 = {
  {1,2,3},  // 0
  {1,2,3},  // 1

  {1,2,3},  // 2
  {1,2,3},  // 3
  {1,2,3},  // 4

  {1,2,3},  // 5
  {1,2,3},  // 6
  {1,2,3},  // 7

  {1,2,3},  // 8

  {1,2},    // 9
  {1,2},    // 10
  {1,3},    // 11

  {1},      // 12
  {2},      // 13
  {3},      // 14

  {1,2,3},  // 15
};

vector<vector<IdType>> vvAddIds2 = {
  {1,2,3}, // 0
  {1,2,3}, // 1

  {1,2},   // 2
  {1,3},   // 3
  {2,3},   // 4

  {1,2},   // 5
  {1,3},   // 6
  {2,3},   // 7

  {2,3},   // 8

  {1},     // 9
  {2},     // 10
  {3},     // 11

  {1},     // 12
  {2},     // 13
  {3},     // 14
 
  {1,2,3}, // 15
};

vector<vector<IdType>> vvAddIds3 = {
  {1,2,3}, // 0
  {1,2},   // 1

  {1,2},   // 2
  {1,3},   // 3
  {2,3},   // 4

  {1},     // 5
  {3},     // 6
  {2},     // 7

  {1},     // 8

  {1},     // 9
  {2},     // 10
  {3},     // 11

  {3},     // 12
  {1},     // 13
  {2},     // 14

  {},      // 15
};

vector<size_t> vAddInterQty = {
  3, // 0
  2, // 1

  2, // 2
  2, // 3
  2, // 4

  1,
  1,
  1, // 7

  0, // 8

  1,
  1,
  1, // 11

  0, // 12
  0, // 13
  0,
  
  0
};

TEST(TestIntersect3Way) {
  EXPECT_EQ(vvAddIds1.size(), vvAddIds2.size());
  EXPECT_EQ(vvAddIds1.size(), vvAddIds3.size());
  EXPECT_EQ(vvAddIds1.size(), vAddInterQty.size());

  
  for (size_t i = 0; i < vvAddIds1.size(); ++i) {
    size_t qty1 = IntersectSizeScalar3way(vvAddIds1[i].size() ? &vvAddIds1[i][0] : 0, vvAddIds1[i].size(),
                                          vvAddIds2[i].size() ? &vvAddIds2[i][0] : 0, vvAddIds2[i].size(),
                                          vvAddIds3[i].size() ? &vvAddIds3[i][0] : 0, vvAddIds3[i].size());
    size_t qty2 = IntersectSizeScalar3way(vvAddIds3[i].size() ? &vvAddIds3[i][0] : 0, vvAddIds3[i].size(),
                                          vvAddIds1[i].size() ? &vvAddIds1[i][0] : 0, vvAddIds1[i].size(),
                                          vvAddIds2[i].size() ? &vvAddIds2[i][0] : 0, vvAddIds2[i].size());
    size_t qty3 = IntersectSizeScalar3way(vvAddIds3[i].size() ? &vvAddIds3[i][0] : 0, vvAddIds3[i].size(),
                                          vvAddIds2[i].size() ? &vvAddIds2[i][0] : 0, vvAddIds2[i].size(),
                                          vvAddIds1[i].size() ? &vvAddIds1[i][0] : 0, vvAddIds1[i].size());
    if (qty1 != vAddInterQty[i] || 
        qty2 != vAddInterQty[i] || 
        qty3 != vAddInterQty[i]) LOG(LIB_INFO) << "Failed test (3-way intersect funcs), index: " << i;

    EXPECT_EQ(qty1, vAddInterQty[i]);
    EXPECT_EQ(qty2, vAddInterQty[i]);
    EXPECT_EQ(qty3, vAddInterQty[i]);
  }
}


}  // namespace similarity

