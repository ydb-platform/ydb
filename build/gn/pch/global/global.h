#pragma once

// The global precompiled header (pch_mode = "global", see
// //build/gn/pch/BUILD.gn, compiled from global.cc): for every C++ file of a
// library(), a program or a test but contrib_pch_targets and
// global_pch_exclude (targets.gni). Only headers whose content depends on no
// -D that differs between those files: build/gn/scripts/pch_check.py fails
// the build otherwise.

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "util/datetime/base.h"
#include "util/datetime/cputimer.h"
#include "util/generic/deque.h"
#include "util/generic/hash.h"
#include "util/generic/hash_set.h"
#include "util/generic/map.h"
#include "util/generic/maybe.h"
#include "util/generic/ptr.h"
#include "util/generic/queue.h"
#include "util/generic/set.h"
#include "util/generic/strbuf.h"
#include "util/generic/string.h"
#include "util/generic/vector.h"
#include "util/generic/yexception.h"
#include "util/network/ip.h"
#include "util/stream/mem.h"
#include "util/stream/output.h"
#include "util/stream/str.h"
#include "util/string/builder.h"
#include "util/string/cast.h"
#include "util/string/join.h"
#include "util/system/mutex.h"
#include "util/system/spinlock.h"

#include "google/protobuf/arenastring.h"
#include "google/protobuf/extension_set.h"
#include "google/protobuf/generated_message_bases.h"
#include "google/protobuf/message.h"
#include "google/protobuf/repeated_field.h"

#include "library/cpp/json/json_reader.h"
#include "library/cpp/json/json_value.h"
#include "library/cpp/lwtrace/shuttle.h"
#include "library/cpp/monlib/dynamic_counters/counters.h"
#include "library/cpp/monlib/service/pages/templates.h"
#include "library/cpp/threading/future/future.h"

#include "ydb/library/actors/core/actor.h"
#include "ydb/library/actors/core/actor_bootstrapped.h"
#include "ydb/library/actors/core/event_local.h"
#include "ydb/library/actors/core/events.h"
#include "ydb/library/actors/core/hfunc.h"
#include "ydb/library/actors/core/log.h"
#include "ydb/library/aclib/aclib.h"

#include "yql/essentials/public/issue/yql_issue.h"

#include "ydb/core/base/appdata.h"
#include "ydb/core/base/defs.h"
#include "ydb/core/base/events.h"
#include "ydb/core/base/path.h"
#include "ydb/core/base/tablet_pipe.h"
#include "ydb/core/protos/kqp.pb.h"
#include "ydb/core/tablet/tablet_counters.h"
#include "ydb/core/tx/scheme_cache/scheme_cache.h"
#include "ydb/core/tx/tx.h"
