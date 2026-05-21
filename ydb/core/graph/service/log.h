#pragma once

#include <util/generic/string.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr {
namespace NGraph {

TString GetLogPrefix();

}
}

#define Y_ENSURE_LOG(cond, stream) if (!(cond)) { ALOG_ERROR(NKikimrServices::GRAPH, GetLogPrefix() << "Failed condition \"" << #cond << "\" " << stream); }
