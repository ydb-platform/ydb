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
#include "falconn_heap_mod.h"
#include "bunit.h"

//====================================================================

using namespace std;

namespace similarity {

TEST(FalconnHeapMod1Test0) {
  FalconnHeapMod1<float, int> h;
  h.push(-2.0, 2);
  h.push(-1.0, 1);
  h.push(-5.0, 5);
  h.push(-3.0, 3);

  EXPECT_EQ(-1.0f, h.top_key());
  h.replace_top_key(0);
  EXPECT_EQ(0.0f, h.top_key());
}

TEST(FalconnHeapMod1Test1) {
  FalconnHeapMod1<float, int> h;
  h.resize(10);
  h.push_unsorted(-2.0, 2);
  h.push_unsorted(-1.0, 1);
  h.push_unsorted(-5.0, 5);
  h.push_unsorted(-3.0, 3);
  h.heapify();

  float k;
  int d;
  h.extract_top(k, d);
  EXPECT_EQ(-1.0f, k);
  EXPECT_EQ(1, d);
  h.extract_top(k, d);
  EXPECT_EQ(-2.0f, k);
  EXPECT_EQ(2, d);

  h.push(-4.0f, 4);
  h.extract_top(k, d);
  EXPECT_EQ(-3.0f, k);
  EXPECT_EQ(3, d);
  h.extract_top(k, d);
  EXPECT_EQ(-4.0f, k);
  EXPECT_EQ(4, d);
  h.extract_top(k, d);
  EXPECT_EQ(-5.0f, k);
  EXPECT_EQ(5, d);

  h.reset(); 
  h.push_unsorted(-2.0, 2);
  h.push_unsorted(-10.0, 10);
  h.push_unsorted(-8.0, 8);
  h.heapify();
  h.extract_top(k, d);
  EXPECT_EQ(-2.0f, k);
  EXPECT_EQ(2, d);
  h.extract_top(k, d);
  EXPECT_EQ(-8.0f, k);
  EXPECT_EQ(8, d);

  h.push(-9.5, 9);
  h.extract_top(k, d);
  EXPECT_EQ(-9.5f, k);
  EXPECT_EQ(9, d);

  h.extract_top(k, d);
  EXPECT_EQ(-10.0f, k);
  EXPECT_EQ(10, d);
}

TEST(FalconnHeapMod1Test2) {
  // Same as above, but without initial resize
  FalconnHeapMod1<float, int> h;
  h.push_unsorted(-2.0, 2);
  h.push_unsorted(-1.0, 1);
  h.push_unsorted(-5.0, 5);
  h.push_unsorted(-3.0, 3);
  h.heapify();

  float k;
  int d;
  h.extract_top(k, d);
  EXPECT_EQ(-1.0f, k);
  EXPECT_EQ(1, d);
  h.extract_top(k, d);
  EXPECT_EQ(-2.0f, k);
  EXPECT_EQ(2, d);

  h.push(-4.0, 4);
  EXPECT_EQ(-3.0f, h.top_item().key);
  EXPECT_EQ(3, h.top_item().data);
  h.extract_top(k, d);
  EXPECT_EQ(-3.0f, k);
  EXPECT_EQ(3, d);
  h.extract_top(k, d);
  EXPECT_EQ(-4.0f, k);
  EXPECT_EQ(4, d);
  h.extract_top(k, d);
  EXPECT_EQ(-5.0f, k);
  EXPECT_EQ(5, d);

  h.reset(); 
  h.push_unsorted(-2.0, 2);
  h.push_unsorted(-10.0, 10);
  h.push_unsorted(-8.0, 8);
  h.heapify();
  h.extract_top(k, d);
  EXPECT_EQ(-2.0f, k);
  EXPECT_EQ(2, d);
  h.extract_top(k, d);
  EXPECT_EQ(-8.0f, k);
  EXPECT_EQ(8, d);

  h.push(-9.5, 9);
  h.extract_top(k, d);
  EXPECT_EQ(-9.5f, k);
  EXPECT_EQ(9, d);

  h.extract_top(k, d);
  EXPECT_EQ(-10.0f, k);
  EXPECT_EQ(10, d);
}

TEST(FalconnHeapMod1Test3) {
  FalconnHeapMod1<float, int> h;
  h.push_unsorted(-2.0, 2);
  h.push_unsorted(-1.0, 1);
  h.push_unsorted(-5.0, 5);
  h.push_unsorted(-3.0, 3);
  h.heapify();

  EXPECT_EQ(-1.0f, h.top_key());
  EXPECT_EQ(-1.0f, h.top_item().key);
  EXPECT_EQ(1, h.top_item().data);

  h.replace_top(-0.5, 0);
  float k;
  int d;
  h.extract_top(k, d);
  EXPECT_EQ(-0.5f, k);
  EXPECT_EQ(0, d);

  h.extract_top(k, d);
  EXPECT_EQ(-2.0f, k);
  EXPECT_EQ(2, d);
}

//==========================================================

TEST(FalconnHeapMod2Test1) {
  FalconnHeapMod2<float> h;
  h.resize(10);
  h.push_unsorted(-2.0);
  h.push_unsorted(-1.0);
  h.push_unsorted(-5.0);
  h.push_unsorted(-3.0);
  h.heapify();

  float k;

  h.extract_top(k);
  EXPECT_EQ(-1.0f, k);
  h.extract_top(k);
  EXPECT_EQ(-2.0f, k);

  h.push(-4.0f);
  h.extract_top(k);
  EXPECT_EQ(-3.0f, k);
  h.extract_top(k);
  EXPECT_EQ(-4.0f, k);
  h.extract_top(k);
  EXPECT_EQ(-5.0f, k);

  h.reset(); 
  h.push_unsorted(-2.0);
  h.push_unsorted(-10.0);
  h.push_unsorted(-8.0);
  h.heapify();
  h.extract_top(k);
  EXPECT_EQ(-2.0f, k);
  h.extract_top(k);
  EXPECT_EQ(-8.0f, k);

  h.push(-9.5);
  h.extract_top(k);
  EXPECT_EQ(-9.5f, k);

  h.extract_top(k);
  EXPECT_EQ(-10.0f, k);
}

TEST(FalconnHeapMod2Test2) {
  // Same as above, but without initial resize
  FalconnHeapMod2<float> h;
  h.push_unsorted(-2.0);
  h.push_unsorted(-1.0);
  h.push_unsorted(-5.0);
  h.push_unsorted(-3.0);
  h.heapify();

  float k;
  h.extract_top(k);
  EXPECT_EQ(-1.0f, k);
  h.extract_top(k);
  EXPECT_EQ(-2.0f, k);

  h.push(-4.0);
  EXPECT_EQ(-3.0f, h.top());
  h.extract_top(k);
  EXPECT_EQ(-3.0f, k);
  h.extract_top(k);
  EXPECT_EQ(-4.0f, k);
  h.extract_top(k);
  EXPECT_EQ(-5.0f, k);

  h.reset(); 
  h.push_unsorted(-2.0);
  h.push_unsorted(-10.0);
  h.push_unsorted(-8.0);
  h.heapify();
  h.extract_top(k);
  EXPECT_EQ(-2.0f, k);
  h.extract_top(k);
  EXPECT_EQ(-8.0f, k);

  h.push(-9.5);
  h.extract_top(k);
  EXPECT_EQ(-9.5f, k);

  h.extract_top(k);
  EXPECT_EQ(-10.0f, k);
}

TEST(FalconnHeapMod2Test3) {
  FalconnHeapMod2<float> h;
  h.push_unsorted(-2.0);
  h.push_unsorted(-1.0);
  h.push_unsorted(-5.0);
  h.push_unsorted(-3.0);
  h.heapify();

  EXPECT_EQ(-1.0f, h.top());

  h.replace_top(-0.5);
  float k;
  h.extract_top(k);
  EXPECT_EQ(-0.5f, k);

  h.extract_top(k);
  EXPECT_EQ(-2.0f, k);
}

}
