#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>

#include <util/generic/hash.h>

namespace NKikimr::NMetadata::NProvider {

inline NActors::TActorId MakeServiceId(ui32 nodeId) {
    return NActors::TActorId(nodeId, "SrvcMetaData");
}

using TOperationProperties = THashMap<TString, TString>;

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
    EvResetManagerRegistration,
    EvTrackOperationCompletion,
    EvTrackOperationFinished,
    EvEnd
};

static_assert(EEvents::EvEnd < EventSpaceEnd(TKikimrEvents::ES_METADATA_PROVIDER), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_METADATA_PROVIDER)");

class TEvTrackOperationCompletion : public TEventLocal<TEvTrackOperationCompletion, EEvents::EvTrackOperationCompletion> {
public:
    YDB_ACCESSOR_DEF(TString, Database);
    YDB_ACCESSOR_DEF(TString, DatabaseId);
    YDB_ACCESSOR_DEF(TString, TypeId);
    YDB_ACCESSOR_DEF(TString, ObjectId);
    YDB_ACCESSOR_DEF(TPathId, PathId);
    YDB_ACCESSOR_DEF(ui64, RequestGeneration);
    YDB_ACCESSOR_DEF(ui64, ObjectGeneration);
    YDB_ACCESSOR_DEF(NActors::TActorId, OperationOwner);
    YDB_ACCESSOR_DEF(TOperationProperties, Properties);
    YDB_ACCESSOR_DEF(ui64, SchemeTxId);
};

class TEvTrackOperationFinished : public TEventLocal<TEvTrackOperationFinished, EEvents::EvTrackOperationFinished> {
public:
    YDB_ACCESSOR_DEF(TString, DatabaseId);
    YDB_ACCESSOR_DEF(TString, TypeId);
    YDB_ACCESSOR_DEF(TString, ObjectId);
    YDB_ACCESSOR_DEF(TPathId, PathId);
    YDB_ACCESSOR_DEF(ui64, RequestGeneration);
    YDB_ACCESSOR_DEF(ui64, ObjectGeneration);
};

} // namespace NKikimr::NMetadata::NProvider
