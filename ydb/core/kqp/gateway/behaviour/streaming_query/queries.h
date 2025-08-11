#pragma once

#include "object.h"

namespace NKikimr::NKqp {

TStreamingQueryConfig::TAsyncStatus DoCreateStreamingQuery(const NKikimrSchemeOp::TModifyScheme& schemeTx, const NMetadata::NModifications::IOperationsManager::TExternalModificationContext& context);

TStreamingQueryConfig::TAsyncStatus DoAlterStreamingQuery(const NKikimrSchemeOp::TModifyScheme& schemeTx, const NMetadata::NModifications::IOperationsManager::TExternalModificationContext& context);

TStreamingQueryConfig::TAsyncStatus DoDropStreamingQuery(const NKikimrSchemeOp::TModifyScheme& schemeTx, const NMetadata::NModifications::IOperationsManager::TExternalModificationContext& context);

}  // namespace NKikimr::NKqp
