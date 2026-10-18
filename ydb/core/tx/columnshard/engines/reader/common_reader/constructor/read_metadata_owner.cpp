#include "read_metadata.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/data_locks/locks/list.h>
#include <ydb/core/tx/columnshard/engines/portions/written.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/source.h>
#include <ydb/core/tx/columnshard/engines/reader/plain_reader/iterator/constructors.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/collections/constructors.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/collections/constructors.h>
#include <ydb/core/tx/columnshard/transactions/locks/read_finished.h>
#include <ydb/core/tx/columnshard/transactions/locks/read_start.h>

namespace NKikimr::NOlap::NReader::NCommon {

TConclusionStatus TReadMetadata::Init(const NColumnShard::TColumnShard* owner, const TReadDescription& read, const EReaderClass readerClass) {
    SetPKRangesFilter(read.PKRangesFilter);
    InitShardingInfo(read.GetTableMetadataAccessor());
    TxId = read.TxId;
    LockId = read.LockId;
    auto lockNodeId = read.LockNodeId;
    LockMode = read.LockMode;
    if (LockId) {
        owner->GetOperationsManager().RegisterLock(*LockId, owner->Generation());
        if (lockNodeId.has_value()) {
            owner->SubscribeLockIfNotAlready(LockId.value(), lockNodeId.value());
        }
        LockSharingInfo = owner->GetOperationsManager().GetLockVerified(*LockId).GetSharingInfo();
    }
    if (!owner->GetIndexOptional()) {
        switch (readerClass) {
            case EReaderClass::Plain:
                SourcesConstructor = NReader::NPlain::TPortionSources::BuildEmpty();
                break;
            case EReaderClass::Simple:
                SourcesConstructor = NReader::NSimple::TPortionsSources::BuildEmpty();
                break;
            case EReaderClass::Trivial:
                SourcesConstructor = NReader::NTrivial::TPortionsSources::BuildEmpty();
                break;
        }
        SourcesConstructor->InitCursor(nullptr);
        return TConclusionStatus::Success();
    }

    ITableMetadataAccessor::TSelectMetadataContext context(
        owner->GetTablesManager(), owner->GetIndexVerified(), read.Orbit, owner->GetDataLocksManager());
    SourcesConstructor = read.GetTableMetadataAccessor()->SelectMetadata(context, read, readerClass);

    if (!SourcesConstructor) {
        return TConclusionStatus::Fail("cannot build sources constructor for " + read.GetTableMetadataAccessor()->GetTablePath());
    }

    SourcesConstructor->InitCursor(read.GetScanCursorVerified());

    {
        auto customConclusion = DoInitCustom(owner, read);
        if (customConclusion.IsFail()) {
            return customConclusion;
        }
    }

    StatsMode = read.StatsMode;
    GroupedMemoryLimiterOperator = read.GroupedMemoryLimiterOperator;

    if (read.readConflictingPortions) {
        auto& opManager = owner->GetOperationsManager();
        std::vector<TPortionInfo::TConstPtr> conflictingPortions = SourcesConstructor->GetConflictingPortions();
        if (!conflictingPortions.empty()) {
            for (const TPortionInfo::TConstPtr& p : conflictingPortions) {
                // add maybe conflicting writes
                if (!p->IsCommitted()) {
                    AFL_VERIFY(p->GetPortionType() == EPortionType::Written);
                    auto* written = static_cast<const TWrittenPortionInfo*>(p.get());
                    auto writeId = written->GetInsertWriteId();
                    auto op = opManager.GetOperationByInsertWriteIdVerified(writeId);
                    // we do not need to check our own uncommitted writes
                    if (op->GetLockId() != *LockId) {
                        AddMaybeConflictingWrite(writeId, op->GetLockId());
                    }
                }
            }

            // register the lock in the end, when Init() is successful for sure
            DataLockGuard = owner->GetDataLocksManager()->RegisterLock<NDataLocks::TListPortionsLock>(
                read.GetLockName(), conflictingPortions, NDataLocks::ELockCategory::Scan, true);
        }
    }
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NOlap::NReader::NCommon
