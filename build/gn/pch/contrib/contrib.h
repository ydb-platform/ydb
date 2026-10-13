#pragma once

// The precompiled header of contrib (pch_mode = "global", see
// //build/gn/pch/BUILD.gn, compiled from contrib.cc): for every C++ file of
// contrib_pch_targets but contrib_pch_exclude (targets.gni). The C++ standard
// library only -- contrib knows nothing of util and ydb -- the headers most of
// its files parse. Only headers whose content depends on no -D that differs
// between those files: build/gn/scripts/pch_check.py fails the build otherwise.

#include <algorithm>
#include <atomic>
#include <bitset>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <iostream>
#include <iterator>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <queue>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>
