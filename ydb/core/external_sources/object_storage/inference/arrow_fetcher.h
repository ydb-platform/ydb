#pragma once

#include <ydb/core/external_sources/object_storage/inference/arrow_inferencinator.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NExternalSource::NObjectStorage::NInference {

NActors::IActor* CreateArrowFetchingActor(NActors::TActorId s3FetcherId, EFileFormat format, const THashMap<TString, TString>& params);
} // namespace NKikimr::NExternalSource::NObjectStorage::NInference
