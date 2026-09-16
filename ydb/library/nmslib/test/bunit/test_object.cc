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
#include <string.h>
#include "object.h"
#include "bunit.h"

#include <vector>
#include <string>

using namespace std;

namespace similarity {

TEST(Object) {
  vector<string> strs = {
      "xyz", "beagcfa", "cea", "cb",
      "d", "c", "bdaf", "ddcd",
      "egbfa", "a", "fba", "bcccfe",
      "ab", "bfgbfdc", "bcbbgf", "bfbb"
  };

  for (int i = 0; i < 16; ++i) {
    LabelType label = i * 1000 + i;
    IdType    id = i + 1;
    Object* obj = new Object(id, label, strs[i].size(), strs[i].c_str());
    EXPECT_EQ(id, obj->id()); 
    EXPECT_EQ(label, obj->label()); 
    EXPECT_EQ(strs[i].size(), obj->datalength());

    string tmp(obj->data(), obj->datalength());
    EXPECT_TRUE(tmp == strs[i]);
    delete obj;
  }
}

TEST(ExtractLabel) {
  string str;
  int lab;

  str = "label:3456 12 34 56";
  lab = Object::extractLabel(str); 

  EXPECT_EQ(lab, 3456);
  EXPECT_TRUE(str == "12 34 56");

  str = "label:9 1";

  lab = Object::extractLabel(str); 

  EXPECT_EQ(lab, 9);
  EXPECT_TRUE(str == "1");

  str = "33";

  lab = Object::extractLabel(str); 

  EXPECT_EQ(lab, EMPTY_LABEL);
  EXPECT_TRUE(str == "33");
}

TEST(AddLabel) {
  string str = "1 23 4 5 6";
  Object::addLabel(str, 2345);
  
  EXPECT_TRUE(str == "label:2345 1 23 4 5 6");
}


}  // namespace similarity
