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
#include <cstring>
#include <map>
#include <limits>
#include <iostream>
#include <stdexcept>

#include "utils.h"
#include "params.h"
#include "logging.h"
#include "space.h"

#include <cmath>

using namespace std;

namespace similarity {

using std::multimap;
using std::string;

void ParseSpaceArg(const string& descStr, string& SpaceType, vector<string>& SpaceDesc) {
  vector<string> tmp;
  if (!SplitStr(descStr, tmp, ':') || tmp.size() > 2  || !tmp.size()) {
    throw runtime_error("Wrong format of the space argument: '" + descStr + "'");
  }

  SpaceType = tmp[0];
  SpaceDesc.clear();

  if (tmp.size() == 2) {
    if (!SplitStr(tmp[1], SpaceDesc, ',')) {
      throw runtime_error("Cannot split space arguments in: '" + tmp[1] + "'");
    }
  }
}

void ParseArg(const string& descStr, vector<string>& vDesc) {
  vDesc.clear();

  if (descStr.empty()) return;

  if (!SplitStr(descStr, vDesc, ',')) {
    throw runtime_error("Cannot split arguments in: '" + descStr + "'");
  }
}


};


