#pragma once

#include <ydb/services/metadata/abstract/events.h>

#include <util/generic/ptr.h>
#include <util/system/types.h>

namespace NKikimr::NSchemeShard {

class TPath;
struct TStreamingQueryInfo;

namespace NStreamingQuery {

THolder<NMetadata::NProvider::TEvTrackOperationCompletion> MakeStreamingOperationTrackerRequest(
    const TPath& path, ui64 ssGeneration, const TStreamingQueryInfo& query);

} // namespace NStreamingQuery

} // namespace NKikimr::NSchemeShard
