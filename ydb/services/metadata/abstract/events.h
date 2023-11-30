#pragma once
#include <ydb/core/base/events.h>
#include <ydb/library/actors/core/events.h>

namespace NKikimr::NMetadata::NProvider {

enum EEvents {
    EvRefreshSubscriberData = EventSpaceBegin(TKikimrEvents::ES_METADATA_PROVIDER),
    EvRefresh,
    EvEnrichSnapshotResult,
    EvEnrichSnapshotProblem,
    EvAskLocal,
    EvSubscribeLocal,
    EvUnsubscribeLocal,
    EvAskExternal,
    EvSubscribeExternal,
    EvUnsubscribeExternal,
    EvYQLResponse,
    EvAlterObjects,
    EvPrepareManager,
    EvManagerPrepared,
    EvTimeout,
    EvTableDescriptionFailed,
    EvTableDescriptionSuccess,
    EvAccessorSimpleResult,
    EvAccessorSimpleError,
    EvAccessorSimpleTableAbsent,
    EvPathExistsCheckFailed,
    EvPathExistsCheckResult,
    EvStartMetadataService,
    EvStartRegistration,
    EvRecheckExistence,
    EvEnd
};

static_assert(EEvents::EvEnd < EventSpaceEnd(TKikimrEvents::ES_METADATA_PROVIDER), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_METADATA_PROVIDER)");

}
