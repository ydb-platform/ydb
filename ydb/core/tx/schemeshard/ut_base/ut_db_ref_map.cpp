#include <ydb/core/tx/schemeshard/schemeshard_db_ref_map.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/olap/store/store.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

TVector<TTableShardInfo> MakeShards(ui32 n, ui64 ownerId = 1) {
    TVector<TTableShardInfo> v;
    v.reserve(n);
    for (ui32 i = 0; i < n; ++i) {
        TString range = (i + 1 < n) ? TString(1, char(i + 1)) : TString{};
        v.emplace_back(TShardIdx(ownerId, i), range);
    }
    return v;
}

template <class TTest>
void WithSchemeShard(TTest test) {
    TSchemeShard* ss = nullptr;
    auto factory = [&ss](const TActorId& tablet, TTabletStorageInfo* info) {
        ss = new TSchemeShard(tablet, info);
        return ss;
    };
    TTestBasicRuntime runtime;
    TTestEnv env(runtime, TTestEnvOptions(), factory);
    runtime.RunCall([&]() {
        const TPathId pathId = TPath::Resolve("/MyRoot", ss).Base()->PathId;
        // Use a real registered map and path for reference reconciliation. The
        // temporary table entry is removed before returning to the event loop.
        UNIT_ASSERT(!ss->Tables.contains(pathId));
        test(*ss, pathId);
        UNIT_ASSERT(!ss->Tables.contains(pathId));
        ss->DebugCheckDbRefIntegrity();
        return true;
    });
}

} // namespace

Y_UNIT_TEST_SUITE(TDbRefMapTest) {

    // at() must hand out a read-only view of whatever smart pointer the map holds:
    // TIntrusivePtr -> TIntrusiveConstPtr, std::shared_ptr -> shared_ptr<const>.
    Y_UNIT_TEST(ConstViewTypeMapping) {
        static_assert(std::is_same_v<
            NDbRefDetail::TConstView<TIntrusivePtr<TTableInfo>>::type,
            TIntrusiveConstPtr<TTableInfo>>);
        static_assert(std::is_same_v<
            NDbRefDetail::TConstView<TIntrusiveConstPtr<TTableInfo>>::type,
            TIntrusiveConstPtr<TTableInfo>>);
        static_assert(std::is_same_v<
            NDbRefDetail::TConstView<std::shared_ptr<TOlapStoreInfo>>::type,
            std::shared_ptr<const TOlapStoreInfo>>);
    }

    Y_UNIT_TEST(MembershipOwnsExactlyOnePathReference) {
        WithSchemeShard([](TSchemeShard& ss, const TPathId& pathId) {
            const auto initialRefs = ss.PathsById.at(pathId)->DbRefCount;
            auto first = MakeIntrusive<TTableInfo>();
            auto second = MakeIntrusive<TTableInfo>();

            ss.Tables.Set(pathId, first);
            UNIT_ASSERT_VALUES_EQUAL(ss.PathsById.at(pathId)->DbRefCount, initialRefs + 1);
            ss.Tables.Set(pathId, second);
            UNIT_ASSERT_EQUAL(ss.Tables.at(pathId).Get(), second.Get());
            UNIT_ASSERT_VALUES_EQUAL(ss.PathsById.at(pathId)->DbRefCount, initialRefs + 1);
            UNIT_ASSERT_VALUES_EQUAL(ss.Tables.erase(pathId), 1);
            UNIT_ASSERT_VALUES_EQUAL(ss.PathsById.at(pathId)->DbRefCount, initialRefs);
            UNIT_ASSERT_VALUES_EQUAL(ss.Tables.erase(pathId), 0);
            UNIT_ASSERT_VALUES_EQUAL(ss.PathsById.at(pathId)->DbRefCount, initialRefs);
        });
    }

    Y_UNIT_TEST(InsertAndReplaceUndoAfterPathCounterRestoration) {
        WithSchemeShard([](TSchemeShard& ss, const TPathId& pathId) {
            const auto initialRefs = ss.PathsById.at(pathId)->DbRefCount;
            auto first = MakeIntrusive<TTableInfo>();
            auto second = MakeIntrusive<TTableInfo>();
            first->AlterVersion = 10;
            second->AlterVersion = 20;

            TMemoryChanges changes;

            changes.GrabPath(&ss, pathId);
            changes.GrabNewTable(&ss, pathId);
            ss.Tables.Set(pathId, first);
            changes.GrabTable(&ss, pathId);
            first->AlterVersion = 11;
            changes.GrabTable(&ss, pathId);
            ss.Tables.Set(pathId, second);
            changes.GrabTable(&ss, pathId);
            second->AlterVersion = 21;
            UNIT_ASSERT_VALUES_EQUAL(ss.PathsById.at(pathId)->DbRefCount, initialRefs + 1);

            changes.UnDo(&ss);

            // Paths restore the count first. Undoing the insertion must not
            // decrement it again; typed snapshots restore their own objects.
            UNIT_ASSERT(!ss.Tables.contains(pathId));
            UNIT_ASSERT_VALUES_EQUAL(ss.PathsById.at(pathId)->DbRefCount, initialRefs);
            UNIT_ASSERT_VALUES_EQUAL(first->AlterVersion, 10);
            UNIT_ASSERT_VALUES_EQUAL(second->AlterVersion, 20);
        });
    }

    Y_UNIT_TEST(ReplacementAndSnapshotsShareReverseOrder) {
        WithSchemeShard([](TSchemeShard& ss, const TPathId& pathId) {
            auto first = MakeIntrusive<TTableInfo>();
            auto second = MakeIntrusive<TTableInfo>();
            first->AlterVersion = 10;
            second->AlterVersion = 20;
            ss.Tables.Set(pathId, first);
            const auto initialRefs = ss.PathsById.at(pathId)->DbRefCount;

            TMemoryChanges changes;

            changes.GrabPath(&ss, pathId);
            changes.GrabTable(&ss, pathId);
            ss.Tables.Update(pathId)->AlterVersion = 11;
            changes.GrabTable(&ss, pathId);
            ss.Tables.Set(pathId, second);
            changes.GrabTable(&ss, pathId);
            ss.Tables.Update(pathId)->AlterVersion = 21;

            changes.UnDo(&ss);

            UNIT_ASSERT_EQUAL(ss.Tables.at(pathId).Get(), first.Get());
            UNIT_ASSERT_VALUES_EQUAL(first->AlterVersion, 10);
            UNIT_ASSERT_VALUES_EQUAL(second->AlterVersion, 20);
            UNIT_ASSERT_VALUES_EQUAL(ss.PathsById.at(pathId)->DbRefCount, initialRefs);
            ss.Tables.erase(pathId);
        });
    }

    Y_UNIT_TEST(AlterDataUndoPreservesTableIdentity) {
        WithSchemeShard([](TSchemeShard& ss, const TPathId& pathId) {
            auto table = MakeIntrusive<TTableInfo>();
            table->SetPartitioning(MakeShards(2));
            auto previous = MakeIntrusive<TTableInfo::TAlterTableInfo>();
            table->AlterData = previous;
            ss.Tables.Set(pathId, table);
            const auto* alias = table.Get();
            TMemoryChanges changes;

            auto writable = ss.Tables.Update(pathId);
            changes.GrabTable(&ss, pathId);
            auto candidate = MakeIntrusive<TTableInfo::TAlterTableInfo>();
            candidate->AlterVersion = table->AlterVersion + 1;
            writable->PrepareAlter(candidate);
            UNIT_ASSERT_EQUAL(table->AlterData.Get(), candidate.Get());

            changes.UnDo(&ss);

            UNIT_ASSERT_EQUAL(ss.Tables.at(pathId).Get(), alias);
            UNIT_ASSERT_EQUAL(alias->AlterData.Get(), previous.Get());
            UNIT_ASSERT_VALUES_EQUAL(alias->GetPartitions().size(), 2);
            alias->VerifyConsistency();
            ss.Tables.erase(pathId);
        });
    }

    Y_UNIT_TEST(GrabTableRestoresStateAndKeepsAliases) {
        WithSchemeShard([](TSchemeShard& ss, const TPathId& pathId) {
            auto table = MakeIntrusive<TTableInfo>();
            table->SetPartitioning(MakeShards(3));
            table->AlterVersion = 10;
            auto previousAlter = MakeIntrusive<TTableInfo::TAlterTableInfo>();
            table->AlterData = previousAlter;
            ss.Tables.Set(pathId, table);
            ss.TTLEnabledTables[pathId] = table;
            const auto initialRefs = ss.PathsById.at(pathId)->DbRefCount;
            const auto initialOwners = table.RefCount();

            TMemoryChanges changes;

            changes.GrabTable(&ss, pathId);
            table->AlterVersion = 20;
            table->AlterData = MakeIntrusive<TTableInfo::TAlterTableInfo>();
            // Destroy the original storage. A shallow snapshot would retain
            // pointers to those old partition nodes after rollback.
            table->SetPartitioning(MakeShards(2, 2));
            ss.Tables.Set(pathId, MakeIntrusive<TTableInfo>());
            changes.UnDo(&ss);


            UNIT_ASSERT_EQUAL(ss.Tables.at(pathId).Get(), table.Get());
            UNIT_ASSERT_EQUAL(ss.TTLEnabledTables.at(pathId).Get(), table.Get());
            UNIT_ASSERT_VALUES_EQUAL(table.RefCount(), initialOwners);
            UNIT_ASSERT_VALUES_EQUAL(table->AlterVersion, 10);
            UNIT_ASSERT_EQUAL(table->AlterData.Get(), previousAlter.Get());
            UNIT_ASSERT_VALUES_EQUAL(table->GetPartitions().size(), 3);
            for (ui32 i = 0; i < 3; ++i) {
                UNIT_ASSERT_EQUAL(table->GetPartitions()[i]->ShardIdx, TShardIdx(1, i));
            }
            // Both the snapshot and original storage are gone now.
            table->VerifyConsistency();
            UNIT_ASSERT_VALUES_EQUAL(ss.PathsById.at(pathId)->DbRefCount, initialRefs);
            ss.TTLEnabledTables.erase(pathId);
            ss.Tables.erase(pathId);
        });
    }

    Y_UNIT_TEST(RepeatedTypedSnapshotsRestoreInReverseOrder) {
        WithSchemeShard([](TSchemeShard& ss, const TPathId& pathId) {
            auto table = MakeIntrusive<TTableInfo>();
            table->SetPartitioning(MakeShards(2));
            table->AlterVersion = 10;
            ss.Tables.Set(pathId, table);

            TMemoryChanges changes;

            changes.GrabTable(&ss, pathId);
            table->AlterVersion = 20;
            changes.GrabTable(&ss, pathId);
            table->AlterVersion = 30;
            changes.UnDo(&ss);


            UNIT_ASSERT_EQUAL(ss.Tables.at(pathId).Get(), table.Get());
            UNIT_ASSERT_VALUES_EQUAL(table->AlterVersion, 10);
            table->VerifyConsistency();
            ss.Tables.erase(pathId);
        });
    }

    Y_UNIT_TEST(TopicSnapshotOwnsPartitionsAndRestoresGraph) {
        WithSchemeShard([](TSchemeShard& ss, const TPathId& pathId) {
            const TShardIdx shardId(1, 1);
            auto topic = MakeIntrusive<TTopicInfo>();
            for (ui32 id = 0; id < 2; ++id) {
                auto part = MakeHolder<TTopicTabletInfo::TTopicPartitionInfo>();
                part->PqId = id;
                part->CreateVersion = 1;
                part->AlterVersion = 1;
                if (id == 1) {
                    part->ParentPartitionIds.insert(0);
                }
                topic->AddPartition(shardId, part.Release());
            }
            topic->InitSplitMergeGraph();
            topic->Partitions.at(0)->KeyRange.ConstructInPlace();
            topic->Partitions.at(0)->KeyRange->FromBound = "before";
            ss.Topics.Set(pathId, topic);
            const auto initialRefs = ss.PathsById.at(pathId)->DbRefCount;

            TMemoryChanges changes;
            changes.GrabTopic(&ss, pathId);
            topic->Partitions.at(0)->KeyRange->FromBound = "after";
            topic->Partitions.at(0)->Status = NKikimrPQ::ETopicPartitionStatus::Inactive;
            topic->Partitions.at(0)->ChildPartitionIds.clear();
            topic->Partitions.at(1)->ParentPartitionIds.clear();
            topic->AlterData = MakeIntrusive<TTopicInfo>();
            auto added = MakeHolder<TTopicTabletInfo::TTopicPartitionInfo>();
            added->PqId = 2;
            added->CreateVersion = 2;
            topic->AddPartition(TShardIdx(1, 2), added.Release());
            // Release every original partition before restoring the snapshot.
            topic->Partitions.clear();
            topic->Shards.clear();
            changes.UnDo(&ss);

            auto restored = ss.Topics.at(pathId);
            UNIT_ASSERT_VALUES_EQUAL(restored->Shards.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(restored->Partitions.size(), 2);
            UNIT_ASSERT(!restored->AlterData);
            const auto* parent = restored->Partitions.at(0);
            UNIT_ASSERT_VALUES_EQUAL(*parent->KeyRange->FromBound, "before");
            UNIT_ASSERT_EQUAL(parent->Status, NKikimrPQ::ETopicPartitionStatus::Active);
            UNIT_ASSERT(parent->ChildPartitionIds.contains(1));
            UNIT_ASSERT(restored->Partitions.at(1)->ParentPartitionIds.contains(0));
            for (const auto& part : restored->Shards.at(shardId)->Partitions) {
                UNIT_ASSERT_EQUAL(restored->Partitions.at(part->PqId), part.Get());
            }
            UNIT_ASSERT_VALUES_EQUAL(ss.PathsById.at(pathId)->DbRefCount, initialRefs);
            ss.Topics.erase(pathId);
        });
    }

    Y_UNIT_TEST(VolumeSnapshotsRestoreTokensAndOwnedPartitions) {
        WithSchemeShard([](TSchemeShard& ss, const TPathId& pathId) {
            const TShardIdx shardId(1, 1);
            auto volume = MakeIntrusive<TBlockStoreVolumeInfo>();
            volume->MountToken = "before";
            volume->TokenVersion = 3;
            volume->Shards[shardId] = MakeIntrusive<TBlockStorePartitionInfo>();
            volume->Shards.at(shardId)->PartitionId = 7;
            auto solomon = MakeIntrusive<TSolomonVolumeInfo>(4);
            solomon->Partitions[shardId] = MakeIntrusive<TSolomonPartitionInfo>(8);
            ss.BlockStoreVolumes.Set(pathId, volume);
            ss.SolomonVolumes.Set(pathId, solomon);

            TMemoryChanges changes;
            changes.GrabBlockStoreVolume(&ss, pathId);
            changes.GrabSolomonVolume(&ss, pathId);
            volume->MountToken = "after";
            ++volume->TokenVersion;
            volume->Shards.at(shardId)->PartitionId = 99;
            volume->Shards[TShardIdx(1, 2)] = MakeIntrusive<TBlockStorePartitionInfo>();
            volume->AlterData = MakeIntrusive<TBlockStoreVolumeInfo>();
            solomon->Partitions.at(shardId)->PartitionId = 99;
            solomon->AlterData = solomon->CreateAlter();
            changes.UnDo(&ss);

            auto restored = ss.BlockStoreVolumes.at(pathId);
            UNIT_ASSERT_VALUES_EQUAL(restored->MountToken, "before");
            UNIT_ASSERT_VALUES_EQUAL(restored->TokenVersion, 3);
            UNIT_ASSERT_VALUES_EQUAL(restored->Shards.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(restored->Shards.at(shardId)->PartitionId, 7);
            UNIT_ASSERT(!restored->AlterData);
            UNIT_ASSERT_VALUES_EQUAL(ss.SolomonVolumes.at(pathId)->Partitions.at(shardId)->PartitionId, 8);
            UNIT_ASSERT(!ss.SolomonVolumes.at(pathId)->AlterData);
            ss.BlockStoreVolumes.erase(pathId);
            ss.SolomonVolumes.erase(pathId);
        });
    }

    Y_UNIT_TEST(ConfigSnapshotsCopyOwnedAlterConfigs) {
        WithSchemeShard([](TSchemeShard& ss, const TPathId& pathId) {
            auto fs = MakeIntrusive<TFileStoreInfo>();
            fs->Version = 4;
            fs->AlterVersion = 5;
            fs->AlterConfig = MakeHolder<NKikimrFileStore::TConfig>();
            fs->AlterConfig->SetBlockSize(4096);
            auto kesus = MakeIntrusive<TKesusInfo>();
            kesus->Version = 6;
            kesus->AlterVersion = 7;
            kesus->AlterConfig = MakeHolder<Ydb::Coordination::Config>();
            kesus->AlterConfig->set_path("before");
            auto replication = MakeIntrusive<TReplicationInfo>(8);
            ss.FileStoreInfos.Set(pathId, fs);
            ss.KesusInfos.Set(pathId, kesus);
            ss.Replications.Set(pathId, replication);

            TMemoryChanges changes;
            changes.GrabFileStoreInfo(&ss, pathId);
            changes.GrabKesusInfo(&ss, pathId);
            changes.GrabReplication(&ss, pathId);
            UNIT_ASSERT(fs->AlterConfig);
            UNIT_ASSERT(kesus->AlterConfig);
            fs->AlterConfig->SetBlockSize(8192);
            ++fs->AlterVersion;
            kesus->AlterConfig->set_path("after");
            ++kesus->AlterVersion;
            replication->CreateNextVersion();
            changes.UnDo(&ss);

            UNIT_ASSERT_VALUES_EQUAL(ss.FileStoreInfos.at(pathId)->AlterConfig->GetBlockSize(), 4096);
            UNIT_ASSERT_VALUES_EQUAL(ss.FileStoreInfos.at(pathId)->AlterVersion, 5);
            UNIT_ASSERT_VALUES_EQUAL(ss.KesusInfos.at(pathId)->AlterConfig->path(), "before");
            UNIT_ASSERT_VALUES_EQUAL(ss.KesusInfos.at(pathId)->AlterVersion, 7);
            UNIT_ASSERT(!ss.Replications.at(pathId)->AlterData);
            ss.FileStoreInfos.erase(pathId);
            ss.KesusInfos.erase(pathId);
            ss.Replications.erase(pathId);
        });
    }

    Y_UNIT_TEST(UpdateDoesNotSnapshotTwoHundredThousandShards) {
        WithSchemeShard([](TSchemeShard& ss, const TPathId& pathId) {
            constexpr ui32 shardCount = 200000;
            auto table = MakeIntrusive<TTableInfo>();
            table->SetPartitioning(MakeShards(shardCount));
            table->AlterData = MakeIntrusive<TTableInfo::TAlterTableInfo>();
            ss.Tables.Set(pathId, table);
            UNIT_ASSERT_VALUES_EQUAL(table->GetStats().PartitionStats.size(), shardCount);
            const auto* partition = table->GetPartitions().front();
            const auto* stats = &table->GetStats().PartitionStats.at(partition->ShardIdx);
            const auto alterOwners = table->AlterData.RefCount();

            TMemoryChanges changes;

            for (ui32 i = 0; i < 256; ++i) {
                const auto& writable = ss.Tables.Update(pathId);
                UNIT_ASSERT_EQUAL(writable.Get(), table.Get());
                // A retained whole-table snapshot would copy the AlterData
                // smart pointer too, increasing its owner count even though
                // the live table's partition/statistics addresses stay unchanged.
                UNIT_ASSERT_VALUES_EQUAL(writable->AlterData.RefCount(), alterOwners);
            }
            changes.UnDo(&ss);

            UNIT_ASSERT_EQUAL(ss.Tables.at(pathId).Get(), table.Get());
            UNIT_ASSERT_EQUAL(table->GetPartitions().front(), partition);
            UNIT_ASSERT_EQUAL(&table->GetStats().PartitionStats.at(partition->ShardIdx), stats);
            UNIT_ASSERT_VALUES_EQUAL(table->AlterData.RefCount(), alterOwners);
            table->VerifyConsistency();
            ss.Tables.erase(pathId);
        });
    }
}
