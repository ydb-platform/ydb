#pragma once

#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/services/metadata/manager/abstract.h>

namespace NKikimr::NKqp {

// Makes sure every S3-backed channel of the volume is served by a BlobDepot virtual group in the storage pool the
// channel is going to be bound to, and completes only once all of those groups report the WORKING state.
//
// Allocation is idempotent by group name, so a retried CREATE VOLUME reuses the depot instead of leaking a new one.
// The group is deliberately not owned by the volume: DROP VOLUME leaves it (and the S3 objects) in place, because
// dropping a depot that still holds data has to stay an explicit administrative operation.
NThreading::TFuture<NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus> AllocateS3Channels(
    const NKqpProto::TKqpCreateKeyValueVolume& operation,
    const NMetadata::NModifications::IOperationsManager::TExternalModificationContext& context);

}   // namespace NKikimr::NKqp
