#include "read_metadata.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/reader/plain_reader/iterator/iterator.h>
#include <ydb/core/tx/columnshard/engines/reader/plain_reader/iterator/plain_read_data.h>

namespace NKikimr::NOlap::NReader::NPlain {

std::unique_ptr<TScanIteratorBase> TReadMetadata::StartScan(const std::shared_ptr<TReadContext>& readContext) const {
    return std::make_unique<TColumnShardScanIterator>(readContext, readContext->GetReadMetadataPtrVerifiedAs<TReadMetadata>());
}

TConclusionStatus TReadMetadata::DoInitCustom(
    const NColumnShard::TColumnShard* owner, const TReadDescription& readDescription, const TDataStorageAccessor& dataAccessor) {
    CommittedBlobs =
        dataAccessor.GetCommitedBlobs(readDescription, ResultIndexSchema->GetIndexInfo().GetReplaceKey(), LockId, GetRequestSnapshot());

    if (LockId) {
        for (auto&& i : CommittedBlobs) {
            if (!i.IsCommitted()) {
                if (owner->HasLongTxWrites(i.GetInsertWriteId())) {
                } else {
                    auto op = owner->GetOperationsManager().GetOperationByInsertWriteIdVerified(i.GetInsertWriteId());
                    AddWriteIdToCheck(i.GetInsertWriteId(), op->GetLockId());
                }
            }
        }
    }

    return TConclusionStatus::Success();
}

std::shared_ptr<IDataReader> TReadMetadata::BuildReader(const std::shared_ptr<TReadContext>& context) const {
    return std::make_shared<TPlainReadData>(context);
}

}   // namespace NKikimr::NOlap::NReader::NPlain
