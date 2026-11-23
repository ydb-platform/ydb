#include <ydb/core/kqp/common/kqp_tx_manager.h>
#include <ydb/core/kqp/common/kqp_shard_meta.h>

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme_types/scheme_type_info.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

namespace {

TVector<TCell> SingleCell(ui32 value) {
    return { TCell::Make(value) };
}

// The key schema of the SingleCell borders below.
TVector<NScheme::TTypeInfo> Uint32KeyColumnTypes() {
    return { NScheme::TTypeInfo(NScheme::NTypeIds::Uint32) };
}

TPartitioning::TCPtr MakePartitioning(std::initializer_list<std::pair<ui64, ui32>> ends, ui64 version) {
    TVector<TKeyDesc::TPartitionInfo> partitions;
    for (const auto& [id, end] : ends) {
        TKeyDesc::TPartitionInfo partition(id);
        partition.Range.ConstructInPlace();
        if (end != Max<ui32>()) {
            partition.Range->EndKeyPrefix = TSerializedCellVec(SingleCell(end));
        }
        partition.Range->IsInclusive = false;
        partitions.push_back(std::move(partition));
    }
    return std::make_shared<TPartitioning>(std::move(partitions), version);
}

NKikimrDataEvents::TLock MakeWriteLock(ui64 owner, ui64 createdOn, ui64 wsn = 1) {
    NKikimrDataEvents::TLock lock;
    lock.SetLockId(1);
    lock.SetDataShard(owner);
    lock.SetCreatedOn(createdOn);
    lock.SetSchemeShard(1);
    lock.SetPathId(100);
    lock.SetGeneration(3);
    lock.SetCounter(4);
    lock.SetHasWrites(true);
    auto* writer = lock.AddWriteSeqNums();
    writer->SetWriterIndex(7);
    writer->SetWriteSeqNum(wsn);
    return lock;
}

} // namespace

Y_UNIT_TEST_SUITE(TxManagerLockParticipants) {
    Y_UNIT_TEST(IndependentLocksUseFullCreationRanges) {
        auto manager = CreateKqpTransactionManager();
        const TTableId table(1, 100);
        const auto creation = MakePartitioning({{10, Max<ui32>()}}, 1);
        const auto secondCreation = MakePartitioning({{20, Max<ui32>()}}, 2);
        const auto physical = MakePartitioning({{30, 10}, {31, Max<ui32>()}}, 3);

        manager->AddShard(10, false, "/Root/Table", table.PathId, MakeShardMeta(table.PathId, creation, 10, 1));
        manager->AddShard(20, false, "/Root/Table", table.PathId, MakeShardMeta(table.PathId, secondCreation, 20, 1));
        manager->AddShard(30, false, "/Root/Table", table.PathId, MakeShardMeta(table.PathId, physical, 30, 1));
        manager->AddAction(10, IKqpTransactionManager::EAction::WRITE, 123);
        manager->AddAction(20, IKqpTransactionManager::EAction::WRITE, 456);
        manager->AddAction(30, IKqpTransactionManager::EAction::WRITE, 123);
        UNIT_ASSERT(manager->AddLock(30, MakeWriteLock(5, 10)));
        UNIT_ASSERT(manager->AddLock(30, MakeWriteLock(5, 20)));

        // The physical copy covers only keys below 10. Both original creation
        // ranges cover the whole table and require copies on both new shards.
        const auto current = MakePartitioning({{40, 50}, {41, Max<ui32>()}}, 4);
        UNIT_ASSERT(manager->UpdateTableParticipants(table.PathId, TShardPartitionIndex(current), Uint32KeyColumnTypes()));
        UNIT_ASSERT_VALUES_EQUAL(manager->GetParticipatingShards().size(), 2);
        for (const ui64 shard : {40, 41}) {
            const auto locks = manager->GetLocks(shard);
            UNIT_ASSERT_VALUES_EQUAL(locks.size(), 2);
            THashSet<ui64> createdOn;
            for (const auto& lock : locks) {
                createdOn.insert(lock.GetCreatedOn());
            }
            UNIT_ASSERT(createdOn.contains(10));
            UNIT_ASSERT(createdOn.contains(20));
            const auto spanIds = manager->GetShardBreakerQuerySpanIds(shard);
            UNIT_ASSERT_VALUES_EQUAL(spanIds.size(), 2);
            UNIT_ASSERT(FindPtr(spanIds, 123));
            UNIT_ASSERT(FindPtr(spanIds, 456));
        }
    }

    Y_UNIT_TEST(LateEchoOnUnchangedPartitioningAddsEmptyCopy) {
        auto manager = CreateKqpTransactionManager();
        const TPathId table(1, 100);
        const auto initial = MakePartitioning({{10, Max<ui32>()}}, 1);
        const TShardPartitionIndex split(MakePartitioning({{20, 50}, {21, Max<ui32>()}}, 2));
        manager->AddShard(10, false, "/Root/Table", table, MakeShardMeta(table, initial, 10, 1));
        manager->AddAction(10, IKqpTransactionManager::EAction::WRITE, 42);

        // No ACK or lock yet. Both successors inherit WRITE even though the
        // writer may only have a batch for tablet 20.
        UNIT_ASSERT(manager->UpdateTableParticipants(table, split, Uint32KeyColumnTypes()));
        UNIT_ASSERT(!manager->HasShard(10));
        for (const ui64 shard : {20, 21}) {
            UNIT_ASSERT(manager->GetParticipatingShards().contains(shard));
            UNIT_ASSERT(manager->GetLocks(shard).empty());
        }

        UNIT_ASSERT(manager->AddLock(20, MakeWriteLock(10, 10, 8)));
        for (size_t round = 0; round < 2; ++round) {
            UNIT_ASSERT(manager->UpdateTableParticipants(table, split, Uint32KeyColumnTypes()));
            for (const ui64 shard : {20, 21}) {
                const auto locks = manager->GetLocks(shard);
                UNIT_ASSERT_VALUES_EQUAL(locks.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(locks.front().GetCreatedOn(), 10);
                UNIT_ASSERT_VALUES_EQUAL(locks.front().GetWriteSeqNums(0).GetWriteSeqNum(), 8);
                UNIT_ASSERT_VALUES_EQUAL(manager->GetShardBreakerQuerySpanIds(shard).size(), 1);
            }
        }
    }

    Y_UNIT_TEST(UnobservedMergeAndSplitKeepMaximumPerWriter) {
        auto manager = CreateKqpTransactionManager();
        const TTableId table(1, 100);
        const auto creation = MakePartitioning({{10, Max<ui32>()}}, 1);
        const auto copies = MakePartitioning({{20, 50}, {21, Max<ui32>()}}, 2);

        // Sending the original write records its range before any ACK. The
        // first lock echo arrives from a successor after the original is gone.
        manager->AddShard(10, false, "/Root/Table", table.PathId, MakeShardMeta(table.PathId, creation, 10, 1));
        for (const ui64 shard : {20, 21}) {
            manager->AddShard(shard, false, "/Root/Table", table.PathId, MakeShardMeta(table.PathId, copies, shard, 1));
            manager->AddAction(shard, IKqpTransactionManager::EAction::WRITE);
        }
        UNIT_ASSERT(manager->UpdateTableParticipants(table.PathId, TShardPartitionIndex(copies), Uint32KeyColumnTypes()));
        auto lock = MakeWriteLock(10, 10, 8);
        UNIT_ASSERT(manager->AddLock(20, lock));
        lock.MutableWriteSeqNums(0)->SetWriteSeqNum(3);
        auto* secondWriter = lock.AddWriteSeqNums();
        secondWriter->SetWriterIndex(9);
        secondWriter->SetWriteSeqNum(5);
        UNIT_ASSERT(manager->AddLock(21, lock));
        // KQP never observes the intermediate merged tablet at version 3.
        const auto current = MakePartitioning({{40, 25}, {41, Max<ui32>()}}, 4);
        UNIT_ASSERT(manager->UpdateTableParticipants(table.PathId, TShardPartitionIndex(current), Uint32KeyColumnTypes()));
        for (const ui64 shard : {40, 41}) {
            const auto locks = manager->GetLocks(shard);
            UNIT_ASSERT_VALUES_EQUAL(locks.size(), 1);
            THashMap<ui64, ui64> states;
            for (const auto& state : locks.at(0).GetWriteSeqNums()) {
                states[state.GetWriterIndex()] = state.GetWriteSeqNum();
            }
            UNIT_ASSERT_VALUES_EQUAL(states.at(7), 8);
            UNIT_ASSERT_VALUES_EQUAL(states.at(9), 5);
        }
    }

    Y_UNIT_TEST(WriteSeqNumRegressionDependsOnReplyOrigin) {
        for (const ui64 origin : {10, 20}) {
            auto manager = CreateKqpTransactionManager();
            manager->AddShard(origin, false, "/Root/Table", TPathId(1, 100));
            // The owner and CreatedOn are identical in both wire locks. Only
            // the replying tablet distinguishes a current echo from an ancestor.
            UNIT_ASSERT(manager->AddLock(origin, MakeWriteLock(10, 10, 7)));
            UNIT_ASSERT(manager->AddLock(origin, MakeWriteLock(10, 10, 8)));
            UNIT_ASSERT(manager->AddLock(origin, MakeWriteLock(10, 10, 8)));
            UNIT_ASSERT_VALUES_EQUAL(manager->AddLock(origin, MakeWriteLock(10, 10, 3)), origin != 10);
            UNIT_ASSERT_VALUES_EQUAL(manager->BrokenLocks(), origin == 10);
            UNIT_ASSERT_VALUES_EQUAL(manager->GetLocks(origin).at(0).GetWriteSeqNums(0).GetWriteSeqNum(), 8);
        }
    }

    Y_UNIT_TEST(MismatchedCopiesRejectCommit) {
        enum class EMismatch { None, Generation, Counter };
        for (const auto mismatch : {EMismatch::None, EMismatch::Generation, EMismatch::Counter}) {
            auto manager = CreateKqpTransactionManager();
            const TTableId table(1, 100);
            const auto creation = MakePartitioning({{10, Max<ui32>()}}, 1);
            const auto copies = MakePartitioning({{20, 50}, {21, Max<ui32>()}}, 2);

            manager->AddShard(10, false, "/Root/Table", table.PathId, MakeShardMeta(table.PathId, creation, 10, 1));
            for (const ui64 shard : {20, 21}) {
                manager->AddShard(shard, false, "/Root/Table", table.PathId, MakeShardMeta(table.PathId, copies, shard, 1));
            }
            auto lock = MakeWriteLock(10, 10);
            UNIT_ASSERT(manager->AddLock(20, lock));
            if (mismatch == EMismatch::Counter) {
                lock.SetCounter(5);
            } else if (mismatch == EMismatch::Generation) {
                lock.SetGeneration(4);
            }
            UNIT_ASSERT(manager->AddLock(21, lock));
            const bool matches = mismatch == EMismatch::None;
            UNIT_ASSERT_VALUES_EQUAL(manager->UpdateTableParticipants(table.PathId, TShardPartitionIndex(copies), Uint32KeyColumnTypes()), matches);
            UNIT_ASSERT_VALUES_EQUAL(manager->BrokenLocks(), !matches);
            if (!matches) {
                UNIT_ASSERT_GT(manager->GetBrokenLocksCount(), 0);
                UNIT_ASSERT(manager->HasShard(10));
                UNIT_ASSERT_VALUES_EQUAL(manager->GetLocks(20).size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(manager->GetLocks(21).size(), 1);
            }
        }
    }

    Y_UNIT_TEST(RetiredParticipantWithActionsRequiresBorders) {
        auto manager = CreateKqpTransactionManager();
        const TPathId table(1, 100);
        manager->AddShard(10, false, "/Root/Table", table);
        manager->AddAction(10, IKqpTransactionManager::EAction::WRITE);
        const TShardPartitionIndex split(MakePartitioning({{20, 50}, {21, Max<ui32>()}}, 2));

        UNIT_ASSERT_EXCEPTION(manager->UpdateTableParticipants(table, split, Uint32KeyColumnTypes()), yexception);
    }

    Y_UNIT_TEST(RetiredParticipantCannotHoldAnotherTablesLock) {
        auto manager = CreateKqpTransactionManager();
        const TPathId table(1, 100);
        const auto initial = MakePartitioning({{10, Max<ui32>()}}, 1);
        manager->AddShard(10, false, "/Root/Table", table, MakeShardMeta(table, initial, 10, 1));
        auto lock = MakeWriteLock(10, 10);
        lock.SetPathId(200);
        UNIT_ASSERT(manager->AddLock(10, lock));
        const TShardPartitionIndex split(MakePartitioning({{20, 50}, {21, Max<ui32>()}}, 2));

        UNIT_ASSERT_EXCEPTION(manager->UpdateTableParticipants(table, split, Uint32KeyColumnTypes()), yexception);
    }

    Y_UNIT_TEST(QuerySpansFollowSourceRanges) {
        for (const bool split : {false, true}) {
            auto manager = CreateKqpTransactionManager();
            const TTableId table(1, 100);
            const auto original = MakePartitioning({{10, 50}, {11, Max<ui32>()}}, 1);

            for (const ui64 shard : {10, 11}) {
                manager->AddShard(shard, false, "/Root/Table", table.PathId, MakeShardMeta(table.PathId, original, shard, 1));
                manager->AddAction(shard, IKqpTransactionManager::EAction::WRITE, shard * 100);
                UNIT_ASSERT(manager->AddLock(shard, MakeWriteLock(shard, shard)));
            }
            const auto current = split
                ? MakePartitioning({{20, 25}, {21, 50}, {11, Max<ui32>()}}, 2)
                : original;
            UNIT_ASSERT(manager->UpdateTableParticipants(table.PathId, TShardPartitionIndex(current), Uint32KeyColumnTypes()));
            for (const ui64 shard : manager->GetParticipatingShards()) {
                const auto spans = manager->GetShardBreakerQuerySpanIds(shard);
                UNIT_ASSERT_VALUES_EQUAL(spans.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(spans.at(0), shard == 11 ? 1100 : 1000);
            }
        }
    }

    Y_UNIT_TEST(SharedColumnShardKeepsBothTablesLocks) {
        auto manager = CreateKqpTransactionManager();
        for (const ui64 pathId : {100, 200}) {
            manager->AddShard(10, true, "/Root/ColumnTable", TPathId(1, pathId));
            manager->AddAction(10, IKqpTransactionManager::EAction::WRITE);
            auto lock = MakeWriteLock(10, 10);
            lock.SetPathId(pathId);
            UNIT_ASSERT(manager->AddLock(10, lock));
        }
        UNIT_ASSERT_VALUES_EQUAL(manager->GetLocks(10).size(), 2);
        manager->StartPrepare();
        UNIT_ASSERT(manager->ConsumePrepareTransactionResult({10, 100, 200, 777}));
        manager->StartExecute();
        UNIT_ASSERT_VALUES_EQUAL(manager->GetCommitInfo().ShardsInfo.size(), 1);
        UNIT_ASSERT(manager->ConsumeCommitResult(10));
    }

    Y_UNIT_TEST(OwnerCreationModeIsGlobalAcrossCopies) {
        auto manager = CreateKqpTransactionManager();
        manager->AddShard(20, false, "/Root/Table", TPathId(1, 100));
        manager->AddShard(21, false, "/Root/Table", TPathId(1, 100));
        UNIT_ASSERT(manager->AddLock(20, MakeWriteLock(10, 10)));
        UNIT_ASSERT(!manager->AddLock(21, MakeWriteLock(10, 15)));
        UNIT_ASSERT_VALUES_EQUAL(manager->GetLocks(20).size(), 1);
        UNIT_ASSERT(manager->BrokenLocks());
    }

    Y_UNIT_TEST(WriteSeqNumEchoRequiresCreatedOn) {
        auto manager = CreateKqpTransactionManager();
        manager->AddShard(10, false, "/Root/Table", TPathId(1, 100));
        auto lock = MakeWriteLock(10, 10);
        lock.ClearCreatedOn();
        UNIT_ASSERT_EXCEPTION(manager->AddLock(10, lock), yexception);
    }

    Y_UNIT_TEST(TableShardIndexSurvivesMoveAndRepeatedUpdates) {
        auto manager = CreateKqpTransactionManager();
        const TTableId first(1, 100);
        const TTableId second(1, 200);
        for (const auto& [table, shard] : {std::pair{first, 10}, std::pair{second, 11}}) {
            const auto initial = MakePartitioning({{shard, Max<ui32>()}}, 1);

            manager->AddShard(shard, false, "/Root/Table", table.PathId, MakeShardMeta(table.PathId, initial, shard, 1));
            manager->AddAction(shard, IKqpTransactionManager::EAction::WRITE);
            auto lock = MakeWriteLock(shard, shard);
            lock.SetPathId(table.PathId.LocalPathId);
            UNIT_ASSERT(manager->AddLock(shard, lock));
        }
        const auto firstCurrent = MakePartitioning({{20, 50}, {21, Max<ui32>()}}, 2);
        const auto secondCurrent = MakePartitioning({{30, Max<ui32>()}}, 2);
        for (size_t round = 0; round < 2; ++round) {
            if (round == 1) {
                // A late read can register the retired tablet without resending its metadata.
                manager->AddShard(10, false, "/Root/Table");
                manager->AddAction(10, IKqpTransactionManager::EAction::READ);
                UNIT_ASSERT(manager->AddLock(10, MakeWriteLock(10, 10)));
            }
            UNIT_ASSERT(manager->UpdateTableParticipants(first.PathId, TShardPartitionIndex(firstCurrent), Uint32KeyColumnTypes()));
            UNIT_ASSERT(manager->UpdateTableParticipants(second.PathId, TShardPartitionIndex(secondCurrent), Uint32KeyColumnTypes()));
            UNIT_ASSERT_VALUES_EQUAL(manager->GetParticipatingShards().size(), 3);
            for (const ui64 shard : {20, 21, 30}) {
                UNIT_ASSERT_VALUES_EQUAL(manager->GetLocks(shard).size(), 1);
            }
            UNIT_ASSERT(!manager->HasShard(10));
            UNIT_ASSERT(!manager->HasShard(11));
        }
    }

    Y_UNIT_TEST(RedistributionRequiresRegisteredCreationRange) {
        auto manager = CreateKqpTransactionManager();
        const TTableId table(1, 100);
        const auto partitioning = MakePartitioning({{20, Max<ui32>()}}, 2);

        manager->AddShard(20, false, "/Root/Table", table.PathId, MakeShardMeta(table.PathId, partitioning, 20, 1));
        UNIT_ASSERT(manager->AddLock(20, MakeWriteLock(10, 10)));
        UNIT_ASSERT_EXCEPTION(manager->UpdateTableParticipants(table.PathId, TShardPartitionIndex(partitioning), Uint32KeyColumnTypes()), yexception);
    }

    Y_UNIT_TEST(CreationRangeSurvivesUnknownPartitioningVersion) {
        auto manager = CreateKqpTransactionManager();
        const TPathId table(1, 100);
        const auto initial = MakePartitioning({{10, Max<ui32>()}}, 0);
        const auto current = MakePartitioning({{20, 50}, {21, Max<ui32>()}}, 1);
        manager->AddShard(10, false, "/Root/Table", table, MakeShardMeta(table, initial, 10, 1));
        UNIT_ASSERT(!manager->GetShardMeta(10).IsKnown());
        UNIT_ASSERT(manager->AddLock(10, MakeWriteLock(10, 10)));
        UNIT_ASSERT(manager->UpdateTableParticipants(table, TShardPartitionIndex(current), Uint32KeyColumnTypes()));
        UNIT_ASSERT_VALUES_EQUAL(manager->GetLocks(20).size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(manager->GetLocks(21).size(), 1);
    }

    Y_UNIT_TEST(UnusedPartitionDoesNotSupplyCreationRange) {
        auto manager = CreateKqpTransactionManager();
        const TTableId table(1, 100);
        const auto creation = MakePartitioning({{10, Max<ui32>()}}, 1);
        const auto current = MakePartitioning({{20, Max<ui32>()}}, 2);

        manager->AddShard(20, false, "/Root/Table", table.PathId, MakeShardMeta(table.PathId, current, 20, 1));
        manager->SetTableMeta(table, {creation, Uint32KeyColumnTypes()});
        // Merely resolving tablet 10 records no write or lock request to it.
        // Its range must come from a registered request before accepting an echo.
        UNIT_ASSERT(manager->AddLock(20, MakeWriteLock(10, 10)));
        UNIT_ASSERT_EXCEPTION(manager->UpdateTableParticipants(table.PathId, TShardPartitionIndex(current), Uint32KeyColumnTypes()), yexception);
    }

    Y_UNIT_TEST(EmptyCopyJoinsPrepareCoordinatorAndCommitWaits) {
        auto manager = CreateKqpTransactionManager();
        const TTableId table(1, 100);
        const auto creation = MakePartitioning({{10, Max<ui32>()}}, 1);

        manager->AddShard(10, false, "/Root/Table", table.PathId, MakeShardMeta(table.PathId, creation, 10, 1));
        manager->AddAction(10, IKqpTransactionManager::EAction::WRITE);
        UNIT_ASSERT(manager->AddLock(10, MakeWriteLock(10, 10)));
        UNIT_ASSERT(manager->CanUseImmediateCommit());

        const auto current = MakePartitioning({{20, 100}, {21, Max<ui32>()}}, 2);
        UNIT_ASSERT(manager->UpdateTableParticipants(table.PathId, TShardPartitionIndex(current), Uint32KeyColumnTypes()));
        manager->AddAction(20, IKqpTransactionManager::EAction::WRITE);
        UNIT_ASSERT(!manager->CanUseImmediateCommit());
        manager->StartPrepare();
        const auto prepare = manager->GetPrepareTransactionInfo();
        UNIT_ASSERT_VALUES_EQUAL(prepare.SendingShards.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(prepare.ReceivingShards.size(), 2);
        UNIT_ASSERT(!manager->ConsumePrepareTransactionResult({20, 100, 200, 777}));
        UNIT_ASSERT(manager->ConsumePrepareTransactionResult({21, 100, 200, 777}));
        manager->StartExecute();
        const auto commit = manager->GetCommitInfo();
        UNIT_ASSERT_VALUES_EQUAL(commit.ShardsInfo.size(), 2);
        for (const auto& shard : commit.ShardsInfo) {
            UNIT_ASSERT(shard.AffectedFlags & IKqpTransactionManager::EAction::WRITE);
        }
        UNIT_ASSERT(!manager->ConsumeCommitResult(20));
        UNIT_ASSERT(manager->ConsumeCommitResult(21));
    }

    Y_UNIT_TEST(LockRedistributionPreservesInheritedRolesOnEmptyShard) {
        auto manager = CreateKqpTransactionManager();
        const TTableId table(1, 100);
        const auto creation = MakePartitioning({{10, 50}, {11, Max<ui32>()}}, 1);

        manager->AddShard(10, false, "/Root/Table", table.PathId, MakeShardMeta(table.PathId, creation, 10, 1));
        manager->AddAction(10, IKqpTransactionManager::EAction::WRITE);
        UNIT_ASSERT(manager->AddLock(10, MakeWriteLock(10, 10)));

        const auto merged = MakePartitioning({{20, Max<ui32>()}}, 2);
        UNIT_ASSERT(manager->UpdateTableParticipants(table.PathId, TShardPartitionIndex(merged), Uint32KeyColumnTypes()));

        const auto current = MakePartitioning({{30, 50}, {31, Max<ui32>()}}, 3);
        UNIT_ASSERT(manager->UpdateTableParticipants(table.PathId, TShardPartitionIndex(current), Uint32KeyColumnTypes()));
        UNIT_ASSERT(manager->UpdateTableParticipants(table.PathId, TShardPartitionIndex(current), Uint32KeyColumnTypes()));
        UNIT_ASSERT_VALUES_EQUAL(manager->GetLocks(30).size(), 1);
        UNIT_ASSERT(manager->GetLocks(31).empty());
        UNIT_ASSERT_VALUES_EQUAL(manager->GetParticipatingShards().size(), 2);

        // The creation range ends at 50, but the other shard retains WRITE
        // inherited through the merged tablet even after its lock copy is gone.
        manager->StartPrepare();
        const auto prepare = manager->GetPrepareTransactionInfo();
        UNIT_ASSERT_VALUES_EQUAL(prepare.SendingShards.size(), 1);
        UNIT_ASSERT(prepare.SendingShards.contains(30));
        UNIT_ASSERT_VALUES_EQUAL(prepare.ReceivingShards.size(), 2);
        UNIT_ASSERT(prepare.ReceivingShards.contains(30));
        UNIT_ASSERT(prepare.ReceivingShards.contains(31));
        UNIT_ASSERT(!manager->ConsumePrepareTransactionResult({30, 100, 200, 777}));
        UNIT_ASSERT(manager->ConsumePrepareTransactionResult({31, 100, 200, 777}));
        manager->StartExecute();
        for (const auto& shard : manager->GetCommitInfo().ShardsInfo) {
            UNIT_ASSERT(shard.AffectedFlags & IKqpTransactionManager::EAction::WRITE);
        }
        UNIT_ASSERT(!manager->ConsumeCommitResult(30));
        UNIT_ASSERT(manager->ConsumeCommitResult(31));
    }

}

Y_UNIT_TEST_SUITE(TxManagerShardMeta) {

    Y_UNIT_TEST(LockIdentityIncludesCreatedOn) {
        auto txManager = CreateKqpTransactionManager();
        txManager->AddShard(100, false, "/Root/Table", TPathId(1, 100));
        NKikimrDataEvents::TLock lock;
        lock.SetLockId(1);
        lock.SetDataShard(10);
        lock.SetCreatedOn(20);
        lock.SetSchemeShard(1);
        lock.SetPathId(2);
        lock.SetGeneration(3);
        lock.SetCounter(4);
        UNIT_ASSERT(txManager->AddLock(100, lock));
        lock.SetCreatedOn(30);
        lock.SetCounter(5);
        UNIT_ASSERT(txManager->AddLock(100, lock));
        UNIT_ASSERT_VALUES_EQUAL(txManager->GetLocks(100).size(), 2);
        // A repeat replaces neither independent identity.
        UNIT_ASSERT(txManager->AddLock(100, lock));
        UNIT_ASSERT_VALUES_EQUAL(txManager->GetLocks(100).size(), 2);
    }


    Y_UNIT_TEST(AddShardStoresAndReturnsMeta) {
        auto txManager = CreateKqpTransactionManager();

        TShardMeta meta;
        meta.PathId = TPathId(1, 100);
        meta.Borders = TOwnedTableRange(SingleCell(10), true, SingleCell(20), false);
        meta.PartitioningVersion = 7;

        txManager->AddShard(1, false, "/Root/Table", TPathId(1, 100), meta);
        UNIT_ASSERT(txManager->GetShardMeta(1).IsKnown());
        UNIT_ASSERT(txManager->GetShardMeta(1).PathId == meta.PathId);
        UNIT_ASSERT(SameBorders(txManager->GetShardMeta(1).Borders, meta.Borders));
        UNIT_ASSERT_VALUES_EQUAL(txManager->GetShardMeta(1).PartitioningVersion, 7);
    }

    Y_UNIT_TEST(EmptyMetaDoesNotClobberKnownMeta) {
        auto txManager = CreateKqpTransactionManager();

        TShardMeta meta;
        meta.PathId = TPathId(1, 100);
        meta.Borders = TOwnedTableRange(SingleCell(10), true, SingleCell(20), false);
        meta.PartitioningVersion = 7;

        txManager->AddShard(1, false, "/Root/Table", TPathId(1, 100), meta);
        txManager->AddShard(1, false, "/Root/Table", TPathId(1, 100));

        UNIT_ASSERT(txManager->GetShardMeta(1).IsKnown());
        UNIT_ASSERT_VALUES_EQUAL(txManager->GetShardMeta(1).PartitioningVersion, 7);
        UNIT_ASSERT(SameBorders(txManager->GetShardMeta(1).Borders, meta.Borders));
    }

    Y_UNIT_TEST(VersionOnlyMetaKeepsBorders) {
        // A caller that knows only the version (no borders) must not clear borders.
        // A version-only meta describes a shard that is absent from the resolved
        // partitioning, so it must not move the version of a stored meta that has
        // borders either: the shard is not part of that partitioning (e.g. it was
        // removed by a split/merge) and its version must keep matching the stored
        // borders when checking the partitioning version of a retired participant.
        auto txManager = CreateKqpTransactionManager();

        TShardMeta withBorders;
        withBorders.PathId = TPathId(1, 100);
        withBorders.Borders = TOwnedTableRange(SingleCell(10), true, SingleCell(20), false);
        withBorders.PartitioningVersion = 7;

        TShardMeta versionOnly;
        versionOnly.PathId = TPathId(1, 100);
        versionOnly.PartitioningVersion = 9;

        txManager->AddShard(1, false, "/Root/Table", TPathId(1, 100), withBorders);
        txManager->AddShard(1, false, "/Root/Table", TPathId(1, 100), versionOnly);

        UNIT_ASSERT(txManager->GetShardMeta(1).IsKnown());
        UNIT_ASSERT_VALUES_EQUAL(txManager->GetShardMeta(1).PartitioningVersion, 7);
        UNIT_ASSERT(SameBorders(txManager->GetShardMeta(1).Borders, withBorders.Borders));
    }

    Y_UNIT_TEST(BordersAdoptIncomingVersion) {
        // When a stored version-only meta later receives borders, the borders must
        // bring their own version with them (never mixed with the stored one): the
        // stored version-only value described a different partitioning snapshot.
        auto txManager = CreateKqpTransactionManager();

        TShardMeta versionOnly;
        versionOnly.PathId = TPathId(1, 100);
        versionOnly.PartitioningVersion = 9;
        txManager->AddShard(1, false, "/Root/Table", TPathId(1, 100), versionOnly);

        TShardMeta withBorders;
        withBorders.PathId = TPathId(1, 100);
        withBorders.Borders = TOwnedTableRange(SingleCell(10), true, SingleCell(20), false);
        withBorders.PartitioningVersion = 7;
        txManager->AddShard(1, false, "/Root/Table", TPathId(1, 100), withBorders);

        UNIT_ASSERT(txManager->GetShardMeta(1).IsKnown());
        UNIT_ASSERT_VALUES_EQUAL(txManager->GetShardMeta(1).PartitioningVersion, 7);
        UNIT_ASSERT(SameBorders(txManager->GetShardMeta(1).Borders, withBorders.Borders));
    }

    Y_UNIT_TEST(VersionMax) {
        auto txManager = CreateKqpTransactionManager();

        TShardMeta meta;
        meta.PathId = TPathId(1, 100);
        meta.Borders = TOwnedTableRange(SingleCell(10), true, SingleCell(20), false);
        meta.PartitioningVersion = 5;
        txManager->AddShard(1, false, "/Root/Table", TPathId(1, 100), meta);

        TShardMeta older;
        older.PathId = meta.PathId;
        older.Borders = meta.Borders;
        older.PartitioningVersion = 3;
        txManager->AddShard(1, false, "/Root/Table", TPathId(1, 100), older);
        // An older snapshot of the same borders must not move the version backwards.
        UNIT_ASSERT_VALUES_EQUAL(txManager->GetShardMeta(1).PartitioningVersion, 5);

        TShardMeta newer;
        newer.PathId = meta.PathId;
        newer.Borders = meta.Borders;
        newer.PartitioningVersion = 12;
        txManager->AddShard(1, false, "/Root/Table", TPathId(1, 100), newer);
        UNIT_ASSERT_VALUES_EQUAL(txManager->GetShardMeta(1).PartitioningVersion, 12);
        UNIT_ASSERT(SameBorders(txManager->GetShardMeta(1).Borders, meta.Borders));
    }

    Y_UNIT_TEST(BorderMismatchAborts) {
        auto txManager = CreateKqpTransactionManager();

        TShardMeta meta;
        meta.PathId = TPathId(1, 100);
        meta.Borders = TOwnedTableRange(SingleCell(10), true, SingleCell(20), false);
        meta.PartitioningVersion = 5;
        txManager->AddShard(1, false, "/Root/Table", TPathId(1, 100), meta);

        TShardMeta conflicting;
        conflicting.PathId = meta.PathId;
        conflicting.Borders = TOwnedTableRange(SingleCell(10), true, SingleCell(30), false);
        conflicting.PartitioningVersion = 6;

        UNIT_ASSERT_EXCEPTION(txManager->AddShard(1, false, "/Root/Table", TPathId(1, 100), conflicting), yexception);
    }

    Y_UNIT_TEST(EmptyMetaIsUnknown) {
        // Table identity is available even when no partitioning was supplied.
        auto txManager = CreateKqpTransactionManager();
        const TTableId table(1, 100, 5);
        txManager->AddShard(42, false, "/Root/Table", table.PathId);
        UNIT_ASSERT(!txManager->GetShardMeta(42).IsKnown());
        UNIT_ASSERT(txManager->GetShardMeta(42).PathId == table.PathId);
        UNIT_ASSERT(txManager->GetShardMeta(42).Borders.From.empty());
        UNIT_ASSERT(txManager->AddLock(42, MakeWriteLock(42, 42)));
    }

    Y_UNIT_TEST(PlainReadThenWriteRegistersTableForSplit) {
        auto manager = CreateKqpTransactionManager();
        const TPathId table(1, 100);
        manager->AddShard(10, false, "/Root/Table");
        manager->AddAction(10, IKqpTransactionManager::EAction::READ);
        UNIT_ASSERT(!manager->GetShardMeta(10).PathId);

        const auto original = MakePartitioning({{10, Max<ui32>()}}, 1);
        manager->AddShard(10, false, "/Root/Table", table, MakeShardMeta(table, original, 10, 1));
        UNIT_ASSERT(manager->AddLock(10, MakeWriteLock(10, 10)));
        const TShardPartitionIndex split(MakePartitioning({{20, 100}, {21, Max<ui32>()}}, 2));
        UNIT_ASSERT(manager->UpdateTableParticipants(table, split, Uint32KeyColumnTypes()));
        UNIT_ASSERT(!manager->HasShard(10));
        UNIT_ASSERT_VALUES_EQUAL(manager->GetLocks(20).size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(manager->GetLocks(21).size(), 1);
    }

    Y_UNIT_TEST(IndexLayoutChangeKeepsSerializableReadLocks) {
        auto manager = CreateKqpTransactionManager();
        manager->SetIsolationLevel(NKqpProto::ISOLATION_LEVEL_SERIALIZABLE);
        const TTableId before(1, 100, 5);
        const TTableId after(1, 100, 6);
        manager->AddShard(10, false, "/Root/Index", before.PathId);
        manager->AddAction(10, IKqpTransactionManager::EAction::READ);
        auto lock = MakeWriteLock(10, 10);
        lock.ClearWriteSeqNums();
        lock.ClearCreatedOn();
        lock.SetHasWrites(false);
        UNIT_ASSERT(manager->AddLock(10, lock));
        manager->AddShard(10, false, "/Root/Index", after.PathId);
        UNIT_ASSERT_VALUES_EQUAL(manager->GetLocks(10).size(), 1);
    }

    Y_UNIT_TEST(SetTableMeta) {
        auto txManager = CreateKqpTransactionManager();

        const TTableId tableId(1, 100);
        UNIT_ASSERT(!txManager->GetTableMeta(tableId).InitialPartitioning);
        UNIT_ASSERT(txManager->GetTableMeta(tableId).KeyColumnTypes.empty());

        txManager->SetTableMeta(tableId, IKqpTransactionManager::TTableMeta{
            .KeyColumnTypes = Uint32KeyColumnTypes()});
        UNIT_ASSERT(!txManager->GetTableMeta(tableId).InitialPartitioning);
        UNIT_ASSERT_VALUES_EQUAL(txManager->GetTableMeta(tableId).KeyColumnTypes.size(), 1u);

        // Overwrites the previously stored metadata.
        txManager->SetTableMeta(tableId, {});
        UNIT_ASSERT(!txManager->GetTableMeta(tableId).InitialPartitioning);
        UNIT_ASSERT(txManager->GetTableMeta(tableId).KeyColumnTypes.empty());
    }

    Y_UNIT_TEST(SuccessorsUseResolvedBorders) {
        auto manager = CreateKqpTransactionManager();
        const TPathId table(1, 100);
        const auto original = MakePartitioning({{1, Max<ui32>()}}, 5);
        const TShardPartitionIndex split(MakePartitioning({{2, 20}, {3, Max<ui32>()}}, 6));
        manager->AddShard(1, false, "/Root/Table", table, MakeShardMeta(table, original, 1, 1));
        manager->AddAction(1, IKqpTransactionManager::EAction::WRITE);
        manager->AddShard(2, false, "/Root/Table", table, MakeShardMeta(table, split, 2, 1));

        UNIT_ASSERT(manager->UpdateTableParticipants(table, split, Uint32KeyColumnTypes()));
        UNIT_ASSERT(!manager->HasShard(1));
        for (const ui64 id : {2, 3}) {
            const auto expected = MakeShardMeta(table, split, id, 1);
            const auto& actual = manager->GetShardMeta(id);
            UNIT_ASSERT(actual.PathId == table);
            UNIT_ASSERT_VALUES_EQUAL(actual.PartitioningVersion, 6);
            UNIT_ASSERT(SameBorders(actual.Borders, expected.Borders));
        }
    }
}

Y_UNIT_TEST_SUITE(TxManagerPessimisticLock) {

    Y_UNIT_TEST(PessimisticLockMakesTxNotReadOnly) {
        auto txManager = CreateKqpTransactionManager();
        txManager->AddShard(1, false, "/Root/Table", TPathId(1, 100));
        txManager->AddAction(1, IKqpTransactionManager::EAction::PESSIMISTIC_LOCK);

        UNIT_ASSERT(!txManager->IsReadOnly());
        UNIT_ASSERT(txManager->NeedCommit());
    }

    Y_UNIT_TEST(ReadActionStaysReadOnly) {
        auto txManager = CreateKqpTransactionManager();
        txManager->AddShard(1, false, "/Root/Table", TPathId(1, 100));
        txManager->AddAction(1, IKqpTransactionManager::EAction::READ);

        UNIT_ASSERT(txManager->IsReadOnly());
    }

    Y_UNIT_TEST(PessimisticLockShardParticipatesInCommit) {
        auto txManager = CreateKqpTransactionManager();
        txManager->AddShard(1, false, "/Root/Table", TPathId(1, 100));
        txManager->AddAction(1, IKqpTransactionManager::EAction::PESSIMISTIC_LOCK);

        UNIT_ASSERT(!txManager->IsEmpty());
        UNIT_ASSERT(txManager->IsSingleShard());
    }

    Y_UNIT_TEST(PessimisticLockShardIsReceiverAndSendsWithLocks) {
        for (const bool hasLocks : {false, true}) {
            auto txManager = CreateKqpTransactionManager();
            txManager->AddShard(1, false, "/Root/Table", TPathId(1, 100));
            txManager->AddAction(1, IKqpTransactionManager::EAction::PESSIMISTIC_LOCK);
            if (hasLocks) {
                UNIT_ASSERT(txManager->AddLock(1, {}));
            }

            txManager->StartPrepare();
            const auto info = txManager->GetPrepareTransactionInfo();
            UNIT_ASSERT_VALUES_EQUAL(info.SendingShards.contains(1), hasLocks);
            UNIT_ASSERT(info.ReceivingShards.contains(1));
            UNIT_ASSERT(txManager->ConsumePrepareTransactionResult({1, 100, 200, 777}));
            txManager->StartExecute();
            UNIT_ASSERT_VALUES_EQUAL(txManager->GetCommitInfo().ShardsInfo.size(), 1);
            UNIT_ASSERT(txManager->ConsumeCommitResult(1));
        }
    }

    Y_UNIT_TEST(CommitInfoMasksPessimisticLock) {
        // The coordinator's affected-set flags only distinguish read (1) and
        // write (2): PESSIMISTIC_LOCK must not leak into the flags, a shard
        // pessimistically locked (with or without reads) is reported as write.
        auto txManager = CreateKqpTransactionManager();
        txManager->AddShard(1, false, "/Root/Table", TPathId(1, 100));
        txManager->AddAction(1, IKqpTransactionManager::EAction::PESSIMISTIC_LOCK);
        // In production the flag is only registered together with locks taken
        // on the shard (see the lock actors), so model that state here.
        txManager->AddLock(1, {});
        txManager->AddShard(2, false, "/Root/Table", TPathId(1, 100));
        txManager->AddAction(2, IKqpTransactionManager::EAction::WRITE | IKqpTransactionManager::EAction::PESSIMISTIC_LOCK);
        txManager->AddShard(3, false, "/Root/Table", TPathId(1, 100));
        txManager->AddAction(3, IKqpTransactionManager::EAction::WRITE);
        txManager->AddShard(4, false, "/Root/Table", TPathId(1, 100));
        txManager->AddAction(4, IKqpTransactionManager::EAction::READ | IKqpTransactionManager::EAction::PESSIMISTIC_LOCK);
        // In production the flag is only registered together with locks taken
        // on the shard (see the lock actors), so model that state here.
        txManager->AddLock(4, {});

        txManager->StartPrepare();
        UNIT_ASSERT(!txManager->ConsumePrepareTransactionResult({1, 100, 200, 777}));
        UNIT_ASSERT(!txManager->ConsumePrepareTransactionResult({2, 100, 200, 777}));
        UNIT_ASSERT(!txManager->ConsumePrepareTransactionResult({3, 100, 200, 777}));
        UNIT_ASSERT(txManager->ConsumePrepareTransactionResult({4, 100, 200, 777}));
        txManager->StartExecute();

        const auto commitInfo = txManager->GetCommitInfo();
        UNIT_ASSERT_VALUES_EQUAL(commitInfo.ShardsInfo.size(), 4u);

        THashMap<ui64, ui32> flags;
        for (const auto& shardInfo : commitInfo.ShardsInfo) {
            flags.emplace(shardInfo.ShardId, shardInfo.AffectedFlags);
        }
        UNIT_ASSERT_VALUES_EQUAL(flags.at(1), static_cast<ui32>(IKqpTransactionManager::EAction::WRITE));
        UNIT_ASSERT_VALUES_EQUAL(flags.at(2), static_cast<ui32>(IKqpTransactionManager::EAction::WRITE));
        UNIT_ASSERT_VALUES_EQUAL(flags.at(3), static_cast<ui32>(IKqpTransactionManager::EAction::WRITE));
        UNIT_ASSERT_VALUES_EQUAL(flags.at(4), static_cast<ui32>(
            IKqpTransactionManager::EAction::READ | IKqpTransactionManager::EAction::WRITE));
    }
}

Y_UNIT_TEST_SUITE(SameBorders) {

    // TTableRange holds non-owning TConstArrayRef borders, so the cell vectors
    // must outlive the ranges under comparison.
    Y_UNIT_TEST(IdenticalRanges) {
        const TVector<TCell> from = SingleCell(10);
        const TVector<TCell> to = SingleCell(20);
        const TTableRange a(from, true, to, false);
        const TTableRange b(from, true, to, false);
        UNIT_ASSERT(SameBorders(a, b));
    }

    Y_UNIT_TEST(DifferentCells) {
        const TVector<TCell> from = SingleCell(10);
        const TVector<TCell> to1 = SingleCell(20);
        const TVector<TCell> to2 = SingleCell(30);
        const TTableRange a(from, true, to1, false);
        const TTableRange b(from, true, to2, false);
        UNIT_ASSERT(!SameBorders(a, b));
    }

    Y_UNIT_TEST(DifferentInclusivity) {
        const TVector<TCell> from = SingleCell(10);
        const TVector<TCell> to = SingleCell(20);
        const TTableRange a(from, true, to, false);
        const TTableRange b(from, true, to, true);
        UNIT_ASSERT(!SameBorders(a, b));
    }

    Y_UNIT_TEST(DifferentBorderLengths) {
        const TVector<TCell> from = SingleCell(10);
        const TVector<TCell> to = SingleCell(20);
        const TTableRange a(from, true, TVector<TCell>{}, false);
        const TTableRange b(from, true, to, false);
        UNIT_ASSERT(!SameBorders(a, b));
    }

    Y_UNIT_TEST(MultiColumnBorders) {
        const TVector<TCell> fromA{TCell::Make(1), TCell::Make(2)};
        const TVector<TCell> toA{TCell::Make(3), TCell::Make(4)};
        const TVector<TCell> fromB{TCell::Make(1), TCell::Make(2)};
        const TVector<TCell> toB{TCell::Make(3), TCell::Make(4)};
        const TVector<TCell> fromC{TCell::Make(1), TCell::Make(5)};

        const TTableRange a(fromA, true, toA, false);
        const TTableRange b(fromB, true, toB, false);
        UNIT_ASSERT(SameBorders(a, b));

        const TTableRange c(fromC, true, toB, false);
        UNIT_ASSERT(!SameBorders(a, c));
    }

    Y_UNIT_TEST(NullCells) {
        const TVector<TCell> nullKey{TCell()};
        const TTableRange a(nullKey, true, TVector<TCell>{}, false);
        const TTableRange b(nullKey, true, TVector<TCell>{}, false);
        UNIT_ASSERT(SameBorders(a, b));
    }

    Y_UNIT_TEST(PointFlag) {
        const TVector<TCell> point = SingleCell(10);
        const TTableRange a(point);
        const TTableRange b(point);
        UNIT_ASSERT(SameBorders(a, b));

        const TTableRange c(point, true, point, true, false);
        UNIT_ASSERT(!SameBorders(a, c));
    }

    Y_UNIT_TEST(OwnedVsOwned) {
        const TOwnedTableRange a(SingleCell(10), true, SingleCell(20), false);
        const TOwnedTableRange b(SingleCell(10), true, SingleCell(20), false);
        UNIT_ASSERT(SameBorders(a, b));
    }
}

} // namespace NKikimr::NKqp
