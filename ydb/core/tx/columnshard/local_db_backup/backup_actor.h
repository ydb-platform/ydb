#pragma once

#include <ydb/library/actors/core/actor.h>

#include <memory>

namespace NKikimr::NColumnShard::NLocalDbBackup {

std::unique_ptr<NActors::IActor> CreateLocalDbBackupActor(const NActors::TActorId& tablet, ui64 tabletId);

}   // namespace NKikimr::NColumnShard::NLocalDbBackup
