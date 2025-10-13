#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NKafka {

static constexpr bool DEBUG_ENABLED = false;

TString Hex(const char* begin, const char* end);

inline TString LogPrefix() { return {}; }

}
