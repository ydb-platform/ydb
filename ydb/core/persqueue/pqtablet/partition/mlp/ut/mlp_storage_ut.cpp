#include "mlp_storage.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/protos/pqdata_mlp.pb.h>
#include <util/string/join.h>
#include <util/stream/labeled.h>

namespace NKikimr::NPQ::NMLP {

void AssertMessagesLocks(const TTabletPercentileCounter& /*messageLocks*/, std::unordered_map<ui64, ui64>&& /*expected*/) {

    // auto debugString = [&]() {
    //     TStringBuilder sb;
    //     sb << "[";
    //     for (size_t i = 0; i < messageLocks.GetRangeCount(); ++i) {
    //         sb << "{" << i << "," << messageLocks.GetRangeValue(i) << "}, ";
    //     }
    //     sb << "]";
    //     return sb;
    // };

    // auto expectedDebugString = [&]() {
    //     TStringBuilder sb;
    //     sb << "[";
    //     for (size_t i = 0; i < messageLocks.GetRangeCount(); ++i) {
    //         sb << "{" << i << "," << expected[i] << "}, ";
    //     }
    //     sb << "]";
    //     return sb;
    // };

    // for (size_t i = 0; i < messageLocks.GetRangeCount(); ++i) {
    //     auto value = messageLocks.GetRangeValue(i);
    //     auto expectedValue = expected[i];

    //     UNIT_ASSERT_VALUES_EQUAL_C(value, expectedValue, "Value for range " << i << " is not equal to expected: " << debugString() << " != " << expectedDebugString());
    // }
}

Y_UNIT_TEST_SUITE(TMLPStorageTests) {

struct MockTimeProvider : public ITimeProvider {

    MockTimeProvider() {
        Value = TInstant::Seconds(1761034384);
    }

    TInstant Now() override {
        return Value;
    }

    void Tick(TDuration duration) {
        Value += duration;
    }

    TInstant Value;
};

struct TUtils {
    TUtils()
        : TUtils(TStorage::TStorageSettings{.MinMessages = 1, .MaxMessages = 8, .KeepMessageOrder = true})
    {
    }

    explicit TUtils( TStorage::TStorageSettings storageSettings)
        : TimeProvider(TIntrusivePtr<MockTimeProvider>(new MockTimeProvider()))
        , Storage(TimeProvider, std::move(storageSettings))
        , BaseWriteTimestamp(TimeProvider->Now() - TDuration::Seconds(8))
    {
        Storage.SetMaxMessageProcessingCount(1);
        Storage.SetRetentionPeriod(TDuration::Seconds(10));
        Storage.SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
    }

    TIntrusivePtr<MockTimeProvider> TimeProvider;
    TStorage Storage;

    TInstant BaseWriteTimestamp;
    ui64 Offset = 0;

    NKikimrPQ::TMLPStorageSnapshot BeginSnapshot;
    NKikimrPQ::TMLPStorageSnapshot EndSnapshot;
    NKikimrPQ::TMLPStorageWAL WAL;

    void AddMessage(size_t count) {
        for (size_t i = 0; i < count; ++i) {
            Storage.AddMessage(Offset, true, Offset, BaseWriteTimestamp + TDuration::Seconds(Offset));
            ++Offset;
        }
    }

    void Begin() {
        BeginSnapshot = CreateSnapshot();
    }

    void End() {
        WAL = CreateWAL();
        EndSnapshot = CreateSnapshot();
    }

    void AssertLoad() {
        {
            Cerr << "LOAD BeginSnapshot+WAL\n";
            TUtils utils(TStorage::TStorageSettings{.KeepMessageOrder = Storage.GetKeepMessageOrder()});
            utils.LoadSnapshot(BeginSnapshot);
            utils.LoadWAL(WAL);
            utils.Storage.InitMetrics();

            utils.AssertEquals(*this);
        }
        {
            Cerr << "LOAD EndSnapshot+WAL\n";
            TUtils utils(TStorage::TStorageSettings{.KeepMessageOrder = Storage.GetKeepMessageOrder()});
            utils.LoadSnapshot(EndSnapshot);
            utils.Storage.InitMetrics();
            utils.AssertEquals(*this);
        }
    }

    NKikimrPQ::TMLPStorageSnapshot CreateSnapshot() {
        // Clear batch
        auto batch = Storage.ExtractBatch();
        Y_UNUSED(batch);

        NKikimrPQ::TMLPStorageSnapshot snapshot;
        Storage.SerializeTo(snapshot);
        Cerr << "CREATE" << Endl;
        Cerr << "> STORAGE DUMP: " << Storage.DebugString() << Endl;
        Cerr << "> SNAPSHOT: " << snapshot.ShortDebugString() << Endl;
        return snapshot;
    }

    NKikimrPQ::TMLPStorageWAL CreateWAL() {
        NKikimrPQ::TMLPStorageWAL wal;
        Storage.ExtractBatch().SerializeTo(wal);
        Cerr << "CREATE" << Endl;
        Cerr << "> STORAGE DUMP: " << Storage.DebugString() << Endl;
        Cerr << "> WAL: " << wal.ShortDebugString() << Endl;
        return wal;
    }

    void LoadSnapshot(const NKikimrPQ::TMLPStorageSnapshot& snapshot) {
        Cerr << "LOAD" << Endl;
        Cerr << "< SNAPSHOT: " << snapshot.ShortDebugString() << Endl;
        Storage.Initialize(snapshot);
        Cerr << "< STORAGE DUMP: " << Storage.DebugString() << Endl;
    }

    void LoadWAL(const NKikimrPQ::TMLPStorageWAL& wal) {
        Cerr << "LOAD" << Endl;
        Cerr << "< WAL: " << wal.ShortDebugString() << Endl;
        Storage.ApplyWAL(wal);
        Cerr << "< STORAGE DUMP: " << Storage.DebugString() << Endl;
    }

    void AssertSlowZone(std::vector<ui64> expectedOffsets) {
        auto i = expectedOffsets.begin();
        auto m = Storage.begin();

        while (i != expectedOffsets.end() && m != Storage.end()) {
            UNIT_ASSERT_VALUES_EQUAL(*i, (*m).Offset);
            UNIT_ASSERT((*m).SlowZone);
            ++i;
            ++m;
        }

        UNIT_ASSERT(i == expectedOffsets.end());
        if (m != Storage.end()) {
            UNIT_ASSERT(!(*m).SlowZone);
        }
    }

    std::optional<TStorage::TMessageWrapper> GetMessage(ui64 offset) {
        for (auto it = Storage.begin(); it != Storage.end(); ++it) {
            if ((*it).Offset == offset) {
                return *it;
            }
        }

        return std::nullopt;
    }

    ui64 Next(TDuration timeout = TDuration::Seconds(8), TStringBuf descr = {}) {
        TStorage::TPosition position;
        auto result = Storage.Next(TimeProvider->Now() + timeout, position);
        UNIT_ASSERT_C(result.has_value(), descr);
        return result.value().Offset;
    }

    void CheckNoNext(TDuration timeout = TDuration::Seconds(8), TStringBuf descr = {}) {
        TStorage::TPosition position;
        auto result = Storage.Next(TimeProvider->Now() + timeout, position);
        UNIT_ASSERT_C(!result.has_value(), descr);
    }

    bool Commit(ui64 offset) {
        return Storage.Commit(offset) == EOperationResult::Success;
    }

    bool Unlock(ui64 offset) {
        return Storage.Unlock(offset) == EOperationResult::Success;
    }

    void AddMessageWithGroup(ui64 offset, ui32 groupHash) {
        Storage.AddMessage(offset, true, groupHash, BaseWriteTimestamp + TDuration::Seconds(offset));
        Offset = Max(Offset, offset + 1);
    }

    std::deque<TReadMessage> ReadMessages(
        size_t maxCount,
        const TString& receiveAttemptId = {},
        TDuration visibilityTimeout = TDuration::Seconds(8)
    ) {
        TStorage::TPosition position;
        absl::flat_hash_set<ui32> skipMessageGroups;
        return Storage.Read(
            TimeProvider->Now(),
            TimeProvider->Now() + visibilityTimeout,
            position,
            skipMessageGroups,
            maxCount,
            receiveAttemptId
        );
    }

    void AssertEquals(TUtils& other) {
        auto i = other.Storage.begin();
        auto m = Storage.begin();

        while (i != other.Storage.end() && m != Storage.end()) {
            UNIT_ASSERT_VALUES_EQUAL((*i).Offset, (*m).Offset);
            UNIT_ASSERT_VALUES_EQUAL_C((*i).SlowZone, (*m).SlowZone, (*i).Offset);
            UNIT_ASSERT_VALUES_EQUAL_C((*i).Status, (*m).Status, (*i).Offset);
            UNIT_ASSERT_VALUES_EQUAL_C((*i).ProcessingCount, (*m).ProcessingCount, (*i).Offset);
            UNIT_ASSERT_VALUES_EQUAL_C((*i).ProcessingDeadline, (*m).ProcessingDeadline, (*i).Offset);
            UNIT_ASSERT_VALUES_EQUAL_C((*i).WriteTimestamp, (*m).WriteTimestamp, (*i).Offset);
            UNIT_ASSERT_VALUES_EQUAL_C((*i).LockingTimestamp, (*m).LockingTimestamp, (*i).Offset);

            ++i;
            ++m;
        }

        UNIT_ASSERT(i == other.Storage.end());
        UNIT_ASSERT(m == Storage.end());

        auto join = [](const auto& vs) {
            return JoinRange(",", vs.begin(), vs.end());
        };

        UNIT_ASSERT_VALUES_EQUAL(join(other.Storage.GetDLQMessages()), join(Storage.GetDLQMessages()));
        UNIT_ASSERT_VALUES_EQUAL(join(other.Storage.GetLockedMessageGroupsId()), join(Storage.GetLockedMessageGroupsId()));

        UNIT_ASSERT_VALUES_EQUAL(other.Storage.GetFirstOffset(), Storage.GetFirstOffset());
        UNIT_ASSERT_VALUES_EQUAL(other.Storage.GetLastOffset(), Storage.GetLastOffset());
        // FirstUncommitted/FirstUnlocked are advanced eagerly in runtime, but ApplyWAL only
        // clamps them to FirstOffset — do not require exact equality after BeginSnapshot+WAL.
        UNIT_ASSERT_VALUES_EQUAL(other.Storage.GetBaseDeadline(), Storage.GetBaseDeadline());
        UNIT_ASSERT_VALUES_EQUAL(other.Storage.GetBaseWriteTimestamp(), Storage.GetBaseWriteTimestamp());

        auto& ometrics = other.Storage.GetMetrics();
        auto& metrics = Storage.GetMetrics();

        UNIT_ASSERT_VALUES_EQUAL(ometrics.InflightMessageCount, metrics.InflightMessageCount);
        UNIT_ASSERT_VALUES_EQUAL(ometrics.UnprocessedMessageCount, metrics.UnprocessedMessageCount);
        UNIT_ASSERT_VALUES_EQUAL(ometrics.LockedMessageCount, metrics.LockedMessageCount);
        UNIT_ASSERT_VALUES_EQUAL(ometrics.LockedMessageGroupCount, metrics.LockedMessageGroupCount);
        UNIT_ASSERT_VALUES_EQUAL(ometrics.InflightMessageGroupCount, metrics.InflightMessageGroupCount);
        UNIT_ASSERT_VALUES_EQUAL(ometrics.DelayedMessageCount, metrics.DelayedMessageCount);
        UNIT_ASSERT_VALUES_EQUAL(ometrics.CommittedMessageCount, metrics.CommittedMessageCount);
        UNIT_ASSERT_VALUES_EQUAL(ometrics.DeadlineExpiredMessageCount, metrics.DeadlineExpiredMessageCount);
        UNIT_ASSERT_VALUES_EQUAL(ometrics.DLQMessageCount, metrics.DLQMessageCount);

        UNIT_ASSERT_VALUES_EQUAL(ometrics.TotalCommittedMessageCount, metrics.TotalCommittedMessageCount);
        UNIT_ASSERT_VALUES_EQUAL(ometrics.TotalMovedToDLQMessageCount, metrics.TotalMovedToDLQMessageCount);
        UNIT_ASSERT_VALUES_EQUAL(ometrics.TotalScheduledToDLQMessageCount, metrics.TotalScheduledToDLQMessageCount);
        UNIT_ASSERT_VALUES_EQUAL(ometrics.TotalPurgedMessageCount, metrics.TotalPurgedMessageCount);
        UNIT_ASSERT_VALUES_EQUAL(ometrics.TotalDeletedByDeadlinePolicyMessageCount, metrics.TotalDeletedByDeadlinePolicyMessageCount);
        UNIT_ASSERT_VALUES_EQUAL(ometrics.TotalDeletedByRetentionMessageCount, metrics.TotalDeletedByRetentionMessageCount);
    }
};

Y_UNIT_TEST(NextFromEmptyStorage) {
    TStorage storage(CreateDefaultTimeProvider(), {});

    TStorage::TPosition position;
    auto result = storage.Next(TInstant::Now(), position);
    UNIT_ASSERT(!result.has_value());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {});
}

Y_UNIT_TEST(CommitToEmptyStorage) {
    TStorage storage(CreateDefaultTimeProvider(), {});

    auto result = storage.Commit(123);
    UNIT_ASSERT(result == EOperationResult::NotFound);

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {});
}

Y_UNIT_TEST(UnlockToEmptyStorage) {
    TStorage storage(CreateDefaultTimeProvider(), {});

    auto result = storage.Unlock(123);
    UNIT_ASSERT(result == EOperationResult::NotFound);

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {});
}

Y_UNIT_TEST(ChangeDeadlineEmptyStorage) {
    TStorage storage(CreateDefaultTimeProvider(), {});

    auto result = storage.ChangeMessageDeadline(123, TInstant::Now());
    UNIT_ASSERT(result == EOperationResult::NotFound);

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {});
}

Y_UNIT_TEST(AddMessageToEmptyStorage) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto writeTimestamp = timeProvider->Now() - TDuration::Seconds(113);

    TStorage storage(timeProvider, {});

    storage.AddMessage(0, true, 5, writeTimestamp);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 0);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 1);

    timeProvider->Tick(TDuration::Seconds(7));

    auto it = storage.begin();
    UNIT_ASSERT(it != storage.end());
    auto message = *it;
    UNIT_ASSERT_VALUES_EQUAL(message.Offset, 0);
    UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    ++it;
    UNIT_ASSERT(it == storage.end());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageGroupCount, 1); // STD builds group structures too
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {{0, 1}});
}

Y_UNIT_TEST(AddBatchedMessageToEmptyStorage) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto writeTimestamp = timeProvider->Now() - TDuration::Seconds(113);

    TStorage storage(timeProvider, {.MinMessages = 1, .MaxMessages = 8});

    NKikimrPQ::TMLPStorageSnapshot emptySnapshot;
    storage.SerializeTo(emptySnapshot);

    storage.AddMessage(3, true, 5, writeTimestamp, TDuration::Zero(), 4);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 3);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 7);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUncommittedOffset(), 3);

    auto checkBatchState = [&](TStorage& storage) {
        auto& metrics = storage.GetMetrics();
        UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 4);
        UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 3);

        TStorage::TPosition position;
        auto read = storage.Next(timeProvider->Now() + TDuration::Seconds(30), position);
        UNIT_ASSERT(read);
        UNIT_ASSERT_VALUES_EQUAL(read->Offset, 3);

        auto empty = storage.Next(timeProvider->Now() + TDuration::Seconds(30), position);
        UNIT_ASSERT(!empty);

        UNIT_ASSERT(storage.Commit(3) == EOperationResult::Success);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUncommittedOffset(), 7);
    };

    {
        NKikimrPQ::TMLPStorageSnapshot snapshot;
        storage.SerializeTo(snapshot);

        TStorage loaded(timeProvider, {.MinMessages = 1, .MaxMessages = 8});
        loaded.Initialize(snapshot);
        loaded.InitMetrics();
        checkBatchState(loaded);
    }

    {
        NKikimrPQ::TMLPStorageWAL wal;
        storage.ExtractBatch().SerializeTo(wal);

        TStorage loaded(timeProvider, {.MinMessages = 1, .MaxMessages = 8});
        loaded.Initialize(emptySnapshot);
        loaded.ApplyWAL(wal);
        loaded.InitMetrics();
        checkBatchState(loaded);
    }
}

Y_UNIT_TEST(AddNotFirstMessageToEmptyStorage) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto writeTimestamp = timeProvider->Now() - TDuration::Seconds(113);

    TStorage storage(timeProvider, {});

    storage.AddMessage(3, true, 5, writeTimestamp);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 3);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 4);

    auto it = storage.begin();
    UNIT_ASSERT(it != storage.end());
    auto message = *it;
    UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
    UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    ++it;
    UNIT_ASSERT(it == storage.end());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageGroupCount, 1); // STD builds group structures too
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {{0, 1}});
}

Y_UNIT_TEST(AddMessageWithSkippedMessage) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto writeTimestamp3 = timeProvider->Now() - TDuration::Seconds(137);
    auto writeTimestamp7 = timeProvider->Now() - TDuration::Seconds(113);

    TStorage storage(timeProvider, {});

    storage.AddMessage(3, true, 5, writeTimestamp3);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 3);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 4);

    Cerr << "DUMP 1: " << storage.DebugString() << Endl;

    storage.AddMessage(7, true, 5, writeTimestamp7);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 7);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 8);

    Cerr << "DUMP 2: " << storage.DebugString() << Endl;

    auto it = storage.begin();
    {
        UNIT_ASSERT(it != storage.end());
        auto message = *it;
        UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
        UNIT_ASSERT(message.SlowZone);
        UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
        UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
        UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp3);
    }
    ++it;
    {
        UNIT_ASSERT(it != storage.end());
        auto message = *it;
        UNIT_ASSERT_VALUES_EQUAL(message.Offset, 7);
        UNIT_ASSERT(!message.SlowZone);
        UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
        UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
        UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp7);
    }
    ++it;
    UNIT_ASSERT(it == storage.end());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 2);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 2);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageGroupCount, 1); // STD builds group structures too
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {{0, 2}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);
}

static void AddMessageWithOffsetGapsReturnsAllMessagesImpl(bool keepMessageOrder) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto writeTimestamp = timeProvider->Now() - TDuration::Seconds(1);

    TStorage storage(timeProvider, TStorage::TStorageSettings{.MinMessages = 1, .MaxMessages = 8, .KeepMessageOrder = keepMessageOrder});
    TVector<ui64> addedOffsets;
    for (ui64 offset = 0; offset < 8; offset += 2) {
        storage.AddMessage(offset, true, static_cast<ui32>(offset), writeTimestamp);
        addedOffsets.push_back(offset);
    }

    TSet<ui64> returned;
    TStorage::TPosition position;
    while (auto result = storage.Next(timeProvider->Now() + TDuration::Seconds(30), position)) {
        returned.insert(result->Offset);
    }

    for (ui64 offset : addedOffsets) {
        UNIT_ASSERT_C(returned.contains(offset), "message with offset " << offset << " was lost, returned: [" << JoinSeq(",", returned) << "]");
    }
    UNIT_ASSERT_VALUES_EQUAL(returned.size(), addedOffsets.size());
}

Y_UNIT_TEST(AddMessageWithOffsetGapsReturnsAllMessages_WithoutKeepMessageOrder) {
    AddMessageWithOffsetGapsReturnsAllMessagesImpl(false);
}

Y_UNIT_TEST(AddMessageWithOffsetGapsReturnsAllMessages_WithKeepMessageOrder) {
    AddMessageWithOffsetGapsReturnsAllMessagesImpl(true);
}

static void AddMessageWithOffsetGapPreservesNonCommittedMessagesImpl(const ui64 commitOffset) {
    TUtils utils;
    auto ts = utils.TimeProvider->Now();

    utils.Begin();

    utils.Storage.AddMessage(0, true, 100, ts);
    utils.Storage.AddMessage(1, true, 101, ts);
    utils.Storage.AddMessage(2, true, 102, ts);

    UNIT_ASSERT(utils.Commit(commitOffset));
    const ui64 lockedOffset = utils.Next();
    UNIT_ASSERT_VALUES_UNEQUAL(lockedOffset, commitOffset);
    Cerr << "DUMP 1: " << utils.Storage.DebugString() << Endl;

    // Offset gap: move all to slow zone
    utils.Storage.AddMessage(5, true, 105, ts);

    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetFirstOffset(), 5);
    Cerr << "DUMP 2: " << utils.Storage.DebugString() << Endl;

    for (const auto& message : utils.Storage) {
        const TString descr = TStringBuilder() << LabeledOutput(message.Offset, lockedOffset);
        if (message.Offset == 5) {
            UNIT_ASSERT_C(!message.SlowZone, descr);
            UNIT_ASSERT_VALUES_EQUAL_C(message.Status, TStorage::EMessageStatus::Unprocessed, descr);
        } else if (message.Offset == lockedOffset) {
            UNIT_ASSERT_C(message.SlowZone, descr);
            UNIT_ASSERT_VALUES_EQUAL_C(message.Status, TStorage::EMessageStatus::Locked, descr);
        } else {
            UNIT_ASSERT_C(IsIn({0, 1, 2}, message.Offset), descr);
            UNIT_ASSERT_C(message.SlowZone, descr);
            UNIT_ASSERT_VALUES_EQUAL_C(message.Status, TStorage::EMessageStatus::Unprocessed, descr);
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMessageCount(), 3);

    auto& metrics = utils.Storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 3);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 2);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);  // offset `commitOffset` was removed from slow zone
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    utils.End();
    utils.AssertLoad();
}

Y_UNIT_TEST(AddMessageWithOffsetGapPreservesNonCommittedMessages0) {
    AddMessageWithOffsetGapPreservesNonCommittedMessagesImpl(0);
}

Y_UNIT_TEST(AddMessageWithOffsetGapPreservesNonCommittedMessages1) {
    AddMessageWithOffsetGapPreservesNonCommittedMessagesImpl(1);
}

Y_UNIT_TEST(AddMessageWithDelay) {
    TUtils utils;
    utils.Begin();

    utils.Storage.AddMessage(3, true, 0, utils.TimeProvider->Now(), TDuration::Seconds(137));

    auto it = utils.Storage.begin();
    UNIT_ASSERT(it != utils.Storage.end());
    auto message = *it;
    UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
    UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Delayed);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, utils.TimeProvider->Now() + TDuration::Seconds(137));

    auto& metrics = utils.Storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageGroupCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DelayedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {{0, 1}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    utils.End();
    utils.AssertLoad();
}

Y_UNIT_TEST(AddMessageWithBigDelay) {
    TUtils utils;
    utils.Begin();

    utils.Storage.AddMessage(3, false, 0, utils.TimeProvider->Now(), TDuration::Days(1));

    auto it = utils.Storage.begin();
    UNIT_ASSERT(it != utils.Storage.end());
    auto message = *it;
    UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
    UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Delayed);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, utils.TimeProvider->Now() + TDuration::Hours(12)); // max deadline

    auto& metrics = utils.Storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DelayedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {{0, 1}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    utils.End();
    utils.AssertLoad();
}

Y_UNIT_TEST(AddMessageWithZeroDelay) {
    TUtils utils;
    utils.Begin();

    utils.Storage.AddMessage(3, false, 0, utils.TimeProvider->Now(), TDuration::Seconds(0));

    auto it = utils.Storage.begin();
    UNIT_ASSERT(it != utils.Storage.end());
    auto message = *it;
    UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
    UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());

    auto& metrics = utils.Storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DelayedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {{0, 1}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    utils.End();
    utils.AssertLoad();
}

void AddMessageWithDelay_UnlockImpl(bool keepMessageOrder, bool differentGroups) {
    TUtils utils(TStorage::TStorageSettings{.MinMessages = 1, .MaxMessages = 8, .KeepMessageOrder = keepMessageOrder});

    ui32 groupHash = 100;
    utils.Storage.AddMessage(3, true, groupHash += differentGroups, utils.TimeProvider->Now(), TDuration::Seconds(1));
    utils.Storage.AddMessage(4, true, groupHash += differentGroups, utils.TimeProvider->Now());

    auto result = utils.Next(TDuration::Seconds(10));
    UNIT_ASSERT_VALUES_EQUAL(result, 4); // offset 3 delayed by 1 second, so it should be the next message

    utils.Begin();

    utils.TimeProvider->Tick(TDuration::Seconds(2)); // delay of message with offset 3 expired
    utils.Storage.ProccessDeadlines();

    auto it = utils.Storage.begin();
    UNIT_ASSERT(it != utils.Storage.end());
    auto message = *it;
    UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
    UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());

    auto& metrics = utils.Storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 2);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, keepMessageOrder ? 1 : 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageGroupCount, differentGroups ? 2 : 1); // STD builds group structures too
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {{0, 1}, {1, 1}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    utils.End();
    utils.AssertLoad();
}

Y_UNIT_TEST(AddMessageWithDelay_Unlock) {
    AddMessageWithDelay_UnlockImpl(false, false);
}

Y_UNIT_TEST(AddMessageWithDelayWithKeepOrderDifferentGroups_Unlock) {
    AddMessageWithDelay_UnlockImpl(true, true);
}

Y_UNIT_TEST(AddMessageWithDelayWithKeepOrder_Unlock) {
    TUtils utils(TStorage::TStorageSettings{.MinMessages = 1, .MaxMessages = 8, .KeepMessageOrder = true});

    utils.Storage.AddMessage(3, true, 0, utils.TimeProvider->Now(), TDuration::Seconds(1));
    utils.Storage.AddMessage(4, true, 0, utils.TimeProvider->Now());

    utils.CheckNoNext(TDuration::Seconds(10)); // offset 3 delayed by 1 second, but 4 should wait for it

    utils.Begin();

    utils.TimeProvider->Tick(TDuration::Seconds(2)); // delay of message with offset 3 expired
    utils.Storage.ProccessDeadlines();

    auto it = utils.Storage.begin();
    UNIT_ASSERT(it != utils.Storage.end());
    auto message = *it;
    UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
    UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());

    auto& metrics = utils.Storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 2);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 2);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {{0, 1}, {1, 1}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    utils.End();
    utils.AssertLoad();
}

Y_UNIT_TEST(NextWithoutKeepMessageOrderStorage) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto writeTimestamp = timeProvider->Now() - TDuration::Seconds(113);
    auto processingDeadline = timeProvider->Now() + TDuration::Seconds(13);

    TStorage storage(timeProvider, {});

    storage.AddMessage(3, true, 5, writeTimestamp);

    TStorage::TPosition position;
    auto result = storage.Next(processingDeadline + TDuration::MilliSeconds(31), position);
    UNIT_ASSERT(result.has_value());
    UNIT_ASSERT_VALUES_EQUAL(result->Offset, 3);

    auto it = storage.begin();
    UNIT_ASSERT(it != storage.end());
    auto message = *it;
    UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
    UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Locked);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, processingDeadline + TDuration::Seconds(1));
    UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    ++it;
    UNIT_ASSERT(it == storage.end());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {{1, 1}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);
}

Y_UNIT_TEST(NextWithKeepMessageOrderStorage) {
    TStorage storage(CreateDefaultTimeProvider(), TStorage::TStorageSettings{.KeepMessageOrder = true});
    storage.AddMessage(3, true, 5, TInstant::Now());

    TStorage::TPosition position;
    auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
    UNIT_ASSERT(result.has_value());
    UNIT_ASSERT_VALUES_EQUAL(result->Offset, 3);

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageGroupCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {{1, 1}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);
}

Y_UNIT_TEST(NextWithWriteRetentionPeriod) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());

    TStorage storage(timeProvider, {});
    storage.SetRetentionPeriod(TDuration::Seconds(5));

    storage.AddMessage(3, true, 5, timeProvider->Now());
    storage.AddMessage(4, true, 5, timeProvider->Now() + TDuration::Seconds(7));

    timeProvider->Tick(TDuration::Seconds(6));

    // skip message by retention
    TStorage::TPosition position;
    auto result = storage.Next(timeProvider->Now() + TDuration::Seconds(1), position);
    UNIT_ASSERT(result.has_value());
    UNIT_ASSERT_VALUES_EQUAL(result->Offset, 4);

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 2);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {{0, 1}, {1, 1}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);
}

Y_UNIT_TEST(NextWithInfinityRetentionPeriod) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());

    TStorage storage(timeProvider, {});
    storage.SetRetentionPeriod(std::nullopt);

    storage.AddMessage(3, true, 5, timeProvider->Now());

    // skip message by retention
    TStorage::TPosition position;
    auto result = storage.Next(timeProvider->Now() + TDuration::Seconds(1), position);
    UNIT_ASSERT(result.has_value());
    UNIT_ASSERT_VALUES_EQUAL(result->Offset, 3);

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {{1, 1}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);
}

Y_UNIT_TEST(SkipLockedMessage) {
    TStorage storage(CreateDefaultTimeProvider(), {});
    {
        TStorage::TPosition position;
        storage.AddMessage(3, true, 5, TInstant::Now());
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT(result.has_value());
    }

    TStorage::TPosition position;
    auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
    UNIT_ASSERT_C(!result.has_value(), "The message already locked");

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {{1, 1}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);
}

Y_UNIT_TEST(SkipLockedMessageGroups) {
    TStorage storage(CreateDefaultTimeProvider(), TStorage::TStorageSettings{.KeepMessageOrder = true});
    storage.AddMessage(3, true, 5, TInstant::Now());
    storage.AddMessage(4, true, 5, TInstant::Now());
    storage.AddMessage(5, true, 7, TInstant::Now());

    {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT(result.has_value());
        UNIT_ASSERT_VALUES_EQUAL(result->Offset, 3);
    }

    TStorage::TPosition position;
    auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
    UNIT_ASSERT(result.has_value());
    UNIT_ASSERT_VALUES_EQUAL(result->Offset, 5);

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 3);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 2);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 2);
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageGroupCount, 2);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {{0, 1}, {1, 2}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);
}

Y_UNIT_TEST(CommitLockedMessage_WithoutKeepMessageOrder) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto writeTimestamp = timeProvider->Now() - TDuration::Seconds(113);

    TStorage storage(timeProvider, {});
    storage.AddMessage(3, true, 5, writeTimestamp);

    {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT(result.has_value());
        UNIT_ASSERT_VALUES_EQUAL(result->Offset, 3);
    }
    {
        auto result = storage.Commit(3);
        UNIT_ASSERT(result == EOperationResult::Success);
    }

    auto it = storage.begin();
    UNIT_ASSERT(it != storage.end());
    auto message = *it;
    UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
    UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Committed);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    ++it;
    UNIT_ASSERT(it == storage.end());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);
}

Y_UNIT_TEST(CommitLockedMessage_WithKeepMessageOrder) {
    TStorage storage(CreateDefaultTimeProvider(), TStorage::TStorageSettings{.KeepMessageOrder = true});
    storage.AddMessage(3, true, 5, TInstant::Now());

    {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT(result.has_value());
    }

    auto result = storage.Commit(3);
    UNIT_ASSERT(result == EOperationResult::Success);

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);
}

Y_UNIT_TEST(CommitUnlockedMessage) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto writeTimestamp = timeProvider->Now() - TDuration::Seconds(113);

    TStorage storage(timeProvider, {});
    storage.AddMessage(3, true, 5, writeTimestamp);

    auto result = storage.Commit(3);
    UNIT_ASSERT(result == EOperationResult::Success);

    auto it = storage.begin();
    UNIT_ASSERT(it != storage.end());
    auto message = *it;
    UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
    UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Committed);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    ++it;
    UNIT_ASSERT(it == storage.end());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);
}

Y_UNIT_TEST(CommitCommittedMessage) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto writeTimestamp = timeProvider->Now() - TDuration::Seconds(113);

    TStorage storage(timeProvider, {});
    storage.AddMessage(3, true, 5, writeTimestamp);

    {
        auto result = storage.Commit(3);
        UNIT_ASSERT(result == EOperationResult::Success);
    }
    {
        auto result = storage.Commit(3);
        UNIT_ASSERT(result == EOperationResult::NotFound);
    }

    auto it = storage.begin();
    UNIT_ASSERT(it != storage.end());
    auto message = *it;
    UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
    UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Committed);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    ++it;
    UNIT_ASSERT(it == storage.end());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);
}

Y_UNIT_TEST(UnlockLockedMessage_WithoutKeepMessageOrder) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto writeTimestamp = timeProvider->Now() - TDuration::Seconds(113);

    TStorage storage(timeProvider, {});
    storage.AddMessage(3, true, 5, writeTimestamp);

    {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT(result.has_value());
    }

    auto result = storage.Unlock(3);
    UNIT_ASSERT(result == EOperationResult::Success);

    auto it = storage.begin();
    UNIT_ASSERT(it != storage.end());
    auto message = *it;
    UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
    UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    ++it;
    UNIT_ASSERT(it == storage.end());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {{1, 1}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);
}

Y_UNIT_TEST(UnlockLockedMessage_WithKeepMessageOrder) {
    TStorage storage(CreateDefaultTimeProvider(), TStorage::TStorageSettings{.KeepMessageOrder = true});
    {
        storage.AddMessage(3, true, 5, TInstant::Now());
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT(result.has_value());
    }

    auto result = storage.Unlock(3);
    UNIT_ASSERT(result == EOperationResult::Success);

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {{1, 1}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);
}

Y_UNIT_TEST(UnlockUnlockedMessage) {
    TStorage storage(CreateDefaultTimeProvider(), {});
    storage.AddMessage(3, true, 5, TInstant::Now());

    auto result = storage.Unlock(3);
    UNIT_ASSERT(result == EOperationResult::NotInFlight);

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {{0, 1}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);
}

Y_UNIT_TEST(UnlockCommittedMessage) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto writeTimestamp = timeProvider->Now() - TDuration::Seconds(113);

    TStorage storage(timeProvider, {});
    storage.AddMessage(3, true, 5, writeTimestamp);

    {
        auto result = storage.Commit(3);
        UNIT_ASSERT(result == EOperationResult::Success);
    }

    auto result = storage.Unlock(3);
    UNIT_ASSERT(result == EOperationResult::NotFound);

    auto it = storage.begin();
    UNIT_ASSERT(it != storage.end());
    auto message = *it;
    UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
    UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Committed);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    ++it;
    UNIT_ASSERT(it == storage.end());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);
}

Y_UNIT_TEST(ChangeDeadlineLockedMessage) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto writeTimestamp = timeProvider->Now() - TDuration::Seconds(113);

    TStorage storage(timeProvider, {});
    storage.AddMessage(3, true, 5, writeTimestamp);

    {
        TStorage::TPosition position;
        auto result = storage.Next(timeProvider->Now() + TDuration::Seconds(5), position);
        UNIT_ASSERT(result.has_value());
    }

    timeProvider->Tick(TDuration::Seconds(1));

    auto result = storage.ChangeMessageDeadline(3, timeProvider->Now() + TDuration::Seconds(7));
    UNIT_ASSERT(result == EOperationResult::Success);

    auto it = storage.begin();
    UNIT_ASSERT(it != storage.end());
    auto message = *it;
    UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
    UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Locked);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, timeProvider->Now() + TDuration::Seconds(7));
    UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    ++it;
    UNIT_ASSERT(it == storage.end());

    auto& metrics = storage.GetMetrics();

    AssertMessagesLocks(metrics.MessageLocks, {{1, 1}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);
}

Y_UNIT_TEST(ChangeDeadlineUnlockedMessage) {
    auto now = TInstant::Now();

    TStorage storage(CreateDefaultTimeProvider(), {});
    storage.AddMessage(3, true, 5, TInstant::Now());

    auto result = storage.ChangeMessageDeadline(3, now + TDuration::Seconds(5));
    UNIT_ASSERT(result == EOperationResult::NotInFlight);

    auto deadline = storage.GetMessageDeadline(3);
    UNIT_ASSERT_VALUES_EQUAL(deadline, TInstant::Zero());
    auto& metrics = storage.GetMetrics();

    AssertMessagesLocks(metrics.MessageLocks, {{0, 1}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);
}

Y_UNIT_TEST(EmptyStorageSerialization) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());

    NKikimrPQ::TMLPStorageSnapshot snapshot;

    {
        TStorage storage(timeProvider, {});

        storage.SerializeTo(snapshot);

        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetFormatVersion(), 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetFirstOffset(), 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetFirstUncommittedOffset(), 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetBaseDeadlineSeconds(), storage.GetBaseDeadline().Seconds());
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetBaseWriteTimestampSeconds(), 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMessages().size(), 0);
    }
    {
        TStorage storage(timeProvider, {});

        storage.Initialize(snapshot);

        UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 0);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 0);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUnlockedOffset(), 0);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUncommittedOffset(), 0);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetBaseDeadline().Seconds(), snapshot.GetMeta().GetBaseDeadlineSeconds());
        UNIT_ASSERT_VALUES_EQUAL(storage.GetBaseWriteTimestamp().Seconds(), 0);

        auto& metrics = storage.GetMetrics();
        UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
    }
}

Y_UNIT_TEST(StorageSerialization) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());

    NKikimrPQ::TMLPStorageSnapshot snapshot;

    auto writeTimestamp3 = timeProvider->Now() - TDuration::Seconds(10);
    auto writeTimestamp4 = timeProvider->Now() - TDuration::Seconds(9);
    auto writeTimestamp5 = timeProvider->Now() - TDuration::Seconds(9);
    auto writeTimestamp6 = timeProvider->Now() - TDuration::Seconds(8);

    auto deadline4 = timeProvider->Now() + TDuration::Seconds(113);

    {
        TStorage storage(timeProvider, TStorage::TStorageSettings{.KeepMessageOrder = true});

        storage.AddMessage(3, true, 5, writeTimestamp3);
        storage.AddMessage(4, true, 7, writeTimestamp4);
        storage.AddMessage(5, true, 11, writeTimestamp5);
        storage.AddMessage(6, true, 13, writeTimestamp6);

        storage.Commit(3);
        TStorage::TPosition position;
        storage.Next(deadline4, position);
        storage.Commit(5);

        storage.SerializeTo(snapshot);

        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetFormatVersion(), 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetFirstOffset(), 3);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetFirstUncommittedOffset(), 4);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetBaseDeadlineSeconds(), storage.GetBaseDeadline().Seconds());
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetMeta().GetBaseWriteTimestampSeconds(), timeProvider->Now().Seconds() - 10);
        UNIT_ASSERT(snapshot.GetMessages().size() > 0);

        Cerr << "DUMP 1: " << storage.DebugString() << Endl;
    }

    timeProvider->Tick(TDuration::Seconds(13));

    {
        TStorage storage(timeProvider, TStorage::TStorageSettings{.KeepMessageOrder = true});

        storage.Initialize(snapshot);
        storage.InitMetrics();
        Cerr << "DUMP 2: " << storage.DebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 3);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 7);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUnlockedOffset(), 6);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUncommittedOffset(), 4);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetBaseDeadline().Seconds(), snapshot.GetMeta().GetBaseDeadlineSeconds());

        auto it = storage.begin();
        {
            UNIT_ASSERT(it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Committed);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 0);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
            UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp3);
        }
        ++it;
        {
            UNIT_ASSERT(it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 4);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Locked);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 1);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, deadline4);
            UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp4);
        }
        ++it;
        {
            UNIT_ASSERT(it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 5);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Committed);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 0);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
            UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp5);
        }
        ++it;
        {
            UNIT_ASSERT(it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 6);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 0);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
            UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp6);
        }
        ++it;
        UNIT_ASSERT(it == storage.end());

        auto& metrics = storage.GetMetrics();
        UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 4);
        UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageGroupCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
    }
}

Y_UNIT_TEST(StorageSerialization_WAL_Unlocked) {
    TUtils utils;

    auto writeTimestamp = utils.TimeProvider->Now() - TDuration::Seconds(9);

    utils.Begin();
    utils.Storage.AddMessage(3, true, 5, writeTimestamp);
    utils.End();

    auto it = utils.Storage.begin();
    {
        UNIT_ASSERT(it != utils.Storage.end());
        auto message = *it;
        UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
        UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
        UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
        UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    }
    ++it;
    UNIT_ASSERT(it == utils.Storage.end());

    auto& metrics = utils.Storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {{0, 1}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    utils.TimeProvider->Tick(TDuration::Seconds(5));

    utils.AssertLoad();
}

Y_UNIT_TEST(StorageSerialization_WAL_Locked) {
    TUtils utils;

    auto writeTimestamp = utils.TimeProvider->Now() - TDuration::Seconds(3);

    utils.Begin();
    utils.Storage.AddMessage(3, true, 5, writeTimestamp);
    UNIT_ASSERT_VALUES_EQUAL(utils.Next(TDuration::Seconds(7)), 3);
    utils.End();

    auto it = utils.Storage.begin();
    {
        UNIT_ASSERT(it != utils.Storage.end());
        auto message = *it;
        UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
        UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Locked);
        UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, utils.TimeProvider->Now() + TDuration::Seconds(7));
        UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    }
    ++it;
    UNIT_ASSERT(it == utils.Storage.end());

    auto& metrics = utils.Storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageGroupCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {{1, 1}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    utils.TimeProvider->Tick(TDuration::Seconds(5));

    utils.AssertLoad();
}

Y_UNIT_TEST(StorageSerialization_WAL_Committed) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());

    auto writeTimestamp = timeProvider->Now() - TDuration::Seconds(13);

    NKikimrPQ::TMLPStorageSnapshot snapshot;
    NKikimrPQ::TMLPStorageWAL wal;

    {
        TStorage storage(timeProvider, TStorage::TStorageSettings{.KeepMessageOrder = true});
        storage.SerializeTo(snapshot);

        storage.AddMessage(3, true, 5, writeTimestamp);

        auto r = storage.Commit(3);
        UNIT_ASSERT(r == EOperationResult::Success);

        auto batch = storage.ExtractBatch();
        UNIT_ASSERT_VALUES_EQUAL(batch.AddedMessageCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(batch.ChangedMessageCount(), 1);
        batch.SerializeTo(wal);
    }

    timeProvider->Tick(TDuration::Seconds(5));

    {
        TStorage storage(timeProvider, TStorage::TStorageSettings{.KeepMessageOrder = true});

        storage.Initialize(snapshot);
        storage.ApplyWAL(wal);
        storage.InitMetrics();

        auto it = storage.begin();
        {
            UNIT_ASSERT(it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Committed);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 0);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
            UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
        }
        ++it;
        UNIT_ASSERT(it == storage.end());

        auto& metrics = storage.GetMetrics();
        UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
    }
}

Y_UNIT_TEST(StorageSerialization_WAL_DLQ) {
    TUtils utils;
    auto writeTimestamp = utils.BaseWriteTimestamp + TDuration::Seconds(7);

    utils.Begin();
    utils.Storage.AddMessage(3, true, 5, writeTimestamp);

    auto r = utils.Next();
    UNIT_ASSERT(r);
    utils.Storage.Unlock(3);

    utils.End();

    auto it = utils.Storage.begin();
    {
        UNIT_ASSERT(it != utils.Storage.end());
        auto message = *it;
        UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
        UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::DLQ);
        UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
        UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    }
    ++it;
    UNIT_ASSERT(it == utils.Storage.end());

    const auto& dlq = utils.Storage.GetDLQMessages();
    UNIT_ASSERT_VALUES_EQUAL(dlq.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(dlq.front().Offset, 3);

    utils.TimeProvider->Tick(TDuration::Seconds(5));

    utils.AssertLoad();
}

Y_UNIT_TEST(StorageSerialization_WAL_DeadLetterPolicy_Delete) {
    TUtils utils;
    utils.Storage.SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_DELETE);
    auto writeTimestamp = utils.BaseWriteTimestamp + TDuration::Seconds(7);

    utils.Begin();
    utils.Storage.AddMessage(3, true, 5, writeTimestamp);

    auto r = utils.Next();
    UNIT_ASSERT(r);
    utils.Storage.Unlock(3);

    utils.End();

    auto it = utils.Storage.begin();
    {
        UNIT_ASSERT(it != utils.Storage.end());
        auto message = *it;
        UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
        UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Committed);
        UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
        UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    }
    ++it;
    UNIT_ASSERT(it == utils.Storage.end());

    const auto& dlq = utils.Storage.GetDLQMessages();
    UNIT_ASSERT_VALUES_EQUAL(dlq.size(), 0);

    utils.TimeProvider->Tick(TDuration::Seconds(5));

    utils.AssertLoad();
}

Y_UNIT_TEST(StorageSerialization_WAL_WithHole) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());

    auto writeTimestamp3 = timeProvider->Now() - TDuration::Seconds(17);
    auto writeTimestamp7 = timeProvider->Now() - TDuration::Seconds(13);

    NKikimrPQ::TMLPStorageSnapshot snapshot;
    NKikimrPQ::TMLPStorageWAL wal;

    {
        TStorage storage(timeProvider, TStorage::TStorageSettings{.KeepMessageOrder = true});
        storage.SerializeTo(snapshot);

        storage.AddMessage(3, true, 5, writeTimestamp3);
        storage.AddMessage(7, true, 5, writeTimestamp7);

        auto batch = storage.ExtractBatch();
        UNIT_ASSERT_VALUES_EQUAL(batch.AddedMessageCount(), 2);
        batch.SerializeTo(wal);
    }

    timeProvider->Tick(TDuration::Seconds(5));

    {
        TStorage storage(timeProvider, TStorage::TStorageSettings{.KeepMessageOrder = true});

        storage.Initialize(snapshot);
        storage.ApplyWAL(wal);
        storage.InitMetrics();

        UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 7);

        auto it = storage.begin();
        {
            UNIT_ASSERT(it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
            UNIT_ASSERT(message.SlowZone);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 0);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
            UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp3);
        }
        ++it;
        {
            UNIT_ASSERT(it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 7);
            UNIT_ASSERT(!message.SlowZone);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 0);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, TInstant::Zero());
            UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp7);
        }
        ++it;
        UNIT_ASSERT(it == storage.end());

        auto& metrics = storage.GetMetrics();
        UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
    }
}

Y_UNIT_TEST(StorageSerialization_WAL_WithMoveBaseTime_Deadline) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());

    auto writeTimestamp3 = timeProvider->Now() - TDuration::Seconds(17);
    auto writeTimestamp4 = timeProvider->Now() - TDuration::Seconds(13);

    auto deadline3 = timeProvider->Now() + TDuration::Seconds(5);
    auto deadline4 = timeProvider->Now() + TDuration::Seconds(13);

    NKikimrPQ::TMLPStorageSnapshot snapshot;
    NKikimrPQ::TMLPStorageWAL wal;

    {
        TStorage storage(timeProvider, TStorage::TStorageSettings{.KeepMessageOrder = true});
        storage.SerializeTo(snapshot);

        storage.AddMessage(3, true, 5, writeTimestamp3);
        storage.AddMessage(4, true, 6, writeTimestamp4);
        {
            TStorage::TPosition position;
            auto r = storage.Next(deadline3, position);
            UNIT_ASSERT(r);
            UNIT_ASSERT_VALUES_EQUAL(r->Offset, 3);
        }

        {
            auto [message, _] = storage.GetMessage(3);
            UNIT_ASSERT(message);
            UNIT_ASSERT_VALUES_EQUAL(message->DeadlineDelta, 5);
            UNIT_ASSERT_VALUES_EQUAL(message->LockingTimestampMilliSecondsDelta, 0);
        }

        timeProvider->Tick(TDuration::Seconds(3));
        storage.MoveBaseDeadline();
        {
            TStorage::TPosition position;
            auto r = storage.Next(deadline4, position);
            UNIT_ASSERT(r);
            UNIT_ASSERT_VALUES_EQUAL(r->Offset, 4);
        }

        {
            auto [message, _] = storage.GetMessage(3);
            UNIT_ASSERT(message);
            UNIT_ASSERT_VALUES_EQUAL(message->DeadlineDelta, 2); // 5 - 3
            UNIT_ASSERT_VALUES_EQUAL(message->LockingTimestampMilliSecondsDelta, 3000);
            UNIT_ASSERT_VALUES_EQUAL(message->LockingTimestampSign, 1);
        }
        {
            auto [message, _] = storage.GetMessage(4);
            UNIT_ASSERT(message);
            UNIT_ASSERT_VALUES_EQUAL(message->DeadlineDelta, 10);
            UNIT_ASSERT_VALUES_EQUAL(message->LockingTimestampMilliSecondsDelta, 0);
            UNIT_ASSERT_VALUES_EQUAL(message->LockingTimestampSign, 0);
        }

        auto batch = storage.ExtractBatch();
        batch.SerializeTo(wal);
    }

    timeProvider->Tick(TDuration::Seconds(7));

    {
        TStorage storage(timeProvider, TStorage::TStorageSettings{.KeepMessageOrder = true});

        storage.Initialize(snapshot);
        storage.ApplyWAL(wal);
        storage.InitMetrics();

        UNIT_ASSERT_VALUES_EQUAL(storage.GetBaseDeadline(), timeProvider->Now() - TDuration::Seconds(7));

        auto it = storage.begin();
        {
            UNIT_ASSERT(it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Locked);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 1);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, deadline3);
            UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp3);
        }
        ++it;
        {
            UNIT_ASSERT(it != storage.end());
            auto message = *it;
            UNIT_ASSERT_VALUES_EQUAL(message.Offset, 4);
            UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Locked);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingCount, 1);
            UNIT_ASSERT_VALUES_EQUAL(message.ProcessingDeadline, deadline4);
            UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp4);
        }
        ++it;
        UNIT_ASSERT(it == storage.end());

        auto& metrics = storage.GetMetrics();
        UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);
    }
}

Y_UNIT_TEST(CompactStorage_ByCommittedOffset) {
    TStorage storage(CreateDefaultTimeProvider(), {});
    storage.AddMessage(3, true, 5, TInstant::Now());
    storage.AddMessage(4, true, 7, TInstant::Now());
    storage.AddMessage(5, true, 11, TInstant::Now());
    storage.AddMessage(6, true, 13, TInstant::Now());

    storage.Commit(3);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 3);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUncommittedOffset(), 4);
    storage.Commit(5);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 3);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUncommittedOffset(), 4);

    auto result = storage.Compact();
    UNIT_ASSERT_VALUES_EQUAL_C(result, 1, "must remove only message with offset 3 because it is committed");

    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 4);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 7);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUnlockedOffset(), 4);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUncommittedOffset(), 4);

    auto it = storage.begin();
    {
        UNIT_ASSERT(it != storage.end());
        auto message = *it;
        UNIT_ASSERT_VALUES_EQUAL(message.Offset, 4);
        UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
    }
    ++it;
    {
        UNIT_ASSERT(it != storage.end());
        auto message = *it;
        UNIT_ASSERT_VALUES_EQUAL(message.Offset, 5);
        UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Committed);
    }
    ++it;
    {
        UNIT_ASSERT(it != storage.end());
        auto message = *it;
        UNIT_ASSERT_VALUES_EQUAL(message.Offset,6);
        UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
    }
    ++it;
    UNIT_ASSERT(it == storage.end());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 3);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 2); // offsets 4 and 6
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 1); // offset 5
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {{0, 2}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 2);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);
}

Y_UNIT_TEST(CompactStorage_ByRetention) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto writeTimestamp = timeProvider->Now() + TDuration::Seconds(13);

    TStorage storage(timeProvider, {});
    storage.SetRetentionPeriod(TDuration::Seconds(1));

    storage.AddMessage(3, true, 5, timeProvider->Now());
    storage.AddMessage(4, true, 7, timeProvider->Now() + TDuration::Seconds(11));
    storage.AddMessage(5, true, 11, writeTimestamp);

    timeProvider->Tick(TDuration::Seconds(12));

    Cerr << "BEFORE: " << storage.DebugString() << Endl;
    auto result = storage.Compact();
    Cerr << "AFTER: " << storage.DebugString() << Endl;
    UNIT_ASSERT_VALUES_EQUAL_C(result, 2, "must remove message with offset 3 and 4");

    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 5);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 6);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUnlockedOffset(), 5);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstUncommittedOffset(), 5);

    auto it = storage.begin();
    {
        UNIT_ASSERT(it != storage.end());
        auto message = *it;
        UNIT_ASSERT_VALUES_EQUAL(message.Offset, 5);
        UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Unprocessed);
        UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    }
    ++it;
    UNIT_ASSERT(it == storage.end());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 1); // offset 5
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {{0, 1}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 2);
}

Y_UNIT_TEST(CompactStorage_ByDeadline) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    auto writeTimestamp = timeProvider->Now() + TDuration::Seconds(7);

    TStorage storage(timeProvider, {});
    storage.SetRetentionPeriod(TDuration::Seconds(1));

    storage.AddMessage(3, true, 5, writeTimestamp, TDuration::Seconds(1));

    timeProvider->Tick(TDuration::Seconds(5));

    Cerr << "BEFORE: " << storage.DebugString() << Endl;
    auto result = storage.Compact();
    Cerr << "AFTER: " << storage.DebugString() << Endl;
    UNIT_ASSERT_VALUES_EQUAL_C(result, 0, "they should not be deleted by deadline");

    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 3);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 4);

    auto it = storage.begin();
    {
        UNIT_ASSERT(it != storage.end());
        auto message = *it;
        UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
        UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Delayed);
        UNIT_ASSERT_VALUES_EQUAL(message.WriteTimestamp, writeTimestamp);
    }
    ++it;
    UNIT_ASSERT(it == storage.end());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DelayedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {{0, 1}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);
}

Y_UNIT_TEST(CompactStorage_WithDLQ) {
    TStorage storage(CreateDefaultTimeProvider(), {});
    storage.SetMaxMessageProcessingCount(1);
    storage.SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);

    storage.AddMessage(3, true, 5, TInstant::Now());
    storage.AddMessage(4, true, 7, TInstant::Now());

    TStorage::TPosition position;
    storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
    storage.Unlock(3);
    storage.Commit(4);

    {
        auto [message, _] = storage.GetMessage(3);
        UNIT_ASSERT_VALUES_EQUAL(message->GetStatus(), TStorage::EMessageStatus::DLQ);
    }
    {
        auto [message, _] = storage.GetMessage(4);
        UNIT_ASSERT_VALUES_EQUAL(message->GetStatus(), TStorage::EMessageStatus::Committed);
    }

    auto result = storage.Compact();
    UNIT_ASSERT_VALUES_EQUAL_C(result, 0, "Keep DLQ messages");

    auto it = storage.begin();
    {
        UNIT_ASSERT(it != storage.end());
        auto message = *it;
        UNIT_ASSERT_VALUES_EQUAL(message.Offset, 3);
        UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::DLQ);
    }
    ++it;
    {
        UNIT_ASSERT(it != storage.end());
        auto message = *it;
        UNIT_ASSERT_VALUES_EQUAL(message.Offset, 4);
        UNIT_ASSERT_VALUES_EQUAL(message.Status, TStorage::EMessageStatus::Committed);
    }
    ++it;
    UNIT_ASSERT(it == storage.end());

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 2);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 1); // offset 4
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 1); // offset 3

    AssertMessagesLocks(metrics.MessageLocks, {});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);
}

Y_UNIT_TEST(ProccessDeadlines) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());

    TStorage storage(timeProvider, TStorage::TStorageSettings{.KeepMessageOrder = true});
    storage.AddMessage(3, true, 5, timeProvider->Now());
    storage.AddMessage(4, true, 7, timeProvider->Now());
    storage.AddMessage(5, true, 11, timeProvider->Now());
    storage.AddMessage(6, true, 13, timeProvider->Now());

    TStorage::TPosition position;
    storage.Next(timeProvider->Now() + TDuration::Seconds(10), position);
    timeProvider->Tick(TDuration::Seconds(5));
    storage.Next(timeProvider->Now() + TDuration::Seconds(10), position);
    timeProvider->Tick(TDuration::Seconds(7));

    auto result = storage.ProccessDeadlines();
    UNIT_ASSERT_VALUES_EQUAL(result, 1);

    {
        auto [message, _] = storage.GetMessage(3);
        UNIT_ASSERT_VALUES_EQUAL(message->GetStatus(), TStorage::EMessageStatus::Unprocessed);
        UNIT_ASSERT_VALUES_EQUAL(message->ProcessingCount, 1);
    }
    {
        auto [message, _] = storage.GetMessage(4);
        UNIT_ASSERT_VALUES_EQUAL(message->GetStatus(), TStorage::EMessageStatus::Locked);
        UNIT_ASSERT_VALUES_EQUAL(message->ProcessingCount, 1);
    }

    auto& metrics = storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 4);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 3); // offsets 4 and 5 and 6
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 1); // offset 3
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {{0, 2}, {1, 2}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);
}

Y_UNIT_TEST(MoveBaseDeadline) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());

    TStorage storage(timeProvider, TStorage::TStorageSettings{.KeepMessageOrder = true});
    storage.AddMessage(3, true, 5, timeProvider->Now());
    storage.AddMessage(4, true, 7, timeProvider->Now());
    storage.AddMessage(5, true, 11, timeProvider->Now());

    TStorage::TPosition position;
    storage.Next(timeProvider->Now() + TDuration::Seconds(3), position);
    storage.Next(timeProvider->Now() + TDuration::Seconds(5), position);
    storage.Next(timeProvider->Now() + TDuration::Seconds(7), position);

    timeProvider->Tick(TDuration::Seconds(5));

    storage.MoveBaseDeadline();

    {
        auto [message, _] = storage.GetMessage(3);
        UNIT_ASSERT_VALUES_EQUAL(message->GetStatus(), TStorage::EMessageStatus::Locked);
        UNIT_ASSERT_VALUES_EQUAL(message->DeadlineDelta, 0);
    }
    {
        auto [message, _] = storage.GetMessage(4);
        UNIT_ASSERT_VALUES_EQUAL(message->GetStatus(), TStorage::EMessageStatus::Locked);
        UNIT_ASSERT_VALUES_EQUAL(message->DeadlineDelta, 0);
    }
    {
        auto [message, _] = storage.GetMessage(5);
        UNIT_ASSERT_VALUES_EQUAL(message->GetStatus(), TStorage::EMessageStatus::Locked);
        UNIT_ASSERT_VALUES_EQUAL(message->DeadlineDelta, 2);
    }

    auto& metrics = storage.GetMetrics();
    AssertMessagesLocks(metrics.MessageLocks, {{1, 3}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);
}

Y_UNIT_TEST(SlowZone_MoveUnprocessedToSlowZone) {
    TUtils utils;
    utils.AddMessage(6);
    utils.Begin();
    utils.AddMessage(1);
    utils.End();

    utils.AssertSlowZone({ 0 });
    auto message = utils.GetMessage(0);
    UNIT_ASSERT(message);
    UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Unprocessed);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingDeadline, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(message->WriteTimestamp, utils.BaseWriteTimestamp);

    auto& metrics = utils.Storage.GetMetrics();
    AssertMessagesLocks(metrics.MessageLocks, {{0, 7}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_MoveLockedToSlowZone) {
    TUtils utils;
    utils.AddMessage(6);
    UNIT_ASSERT_VALUES_EQUAL(utils.Next(TDuration::Seconds(13)), 0);
    utils.Begin();
    utils.AddMessage(1);
    utils.End();

    utils.AssertSlowZone({ 0 });
    auto message = utils.GetMessage(0);
    UNIT_ASSERT(message);
    UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Locked);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingDeadline, utils.TimeProvider->Now() + TDuration::Seconds(13));
    UNIT_ASSERT_VALUES_EQUAL(message->WriteTimestamp, utils.BaseWriteTimestamp);

    auto& metrics = utils.Storage.GetMetrics();
    AssertMessagesLocks(metrics.MessageLocks, {{0, 6}, {1, 1}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_MoveCommittedToSlowZone) {
    TUtils utils;
    utils.AddMessage(6);
    UNIT_ASSERT(utils.Commit(0));
    utils.Begin();
    utils.AddMessage(1);
    utils.End();

    // Committed message isn't moved to SlowZone
    utils.AssertSlowZone({ });

    auto& metrics = utils.Storage.GetMetrics();
    AssertMessagesLocks(metrics.MessageLocks, {{0, 6}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_MoveDLQToSlowZone) {
    TUtils utils;
    utils.AddMessage(6);
    UNIT_ASSERT_VALUES_EQUAL(utils.Next(TDuration::Seconds(13)), 0);
    UNIT_ASSERT(utils.Unlock(0));
    utils.Begin();
    utils.AddMessage(1);
    utils.End();

    utils.AssertSlowZone({ 0 });
    auto message = utils.GetMessage(0);
    UNIT_ASSERT(message);
    UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::DLQ);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingDeadline, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(message->WriteTimestamp, utils.BaseWriteTimestamp);

    auto& metrics = utils.Storage.GetMetrics();
    AssertMessagesLocks(metrics.MessageLocks, {{0, 6}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_MoveToSlowZoneAndLock) {
    TUtils utils;
    utils.AddMessage(6);
    utils.Begin();
    utils.AddMessage(1);
    UNIT_ASSERT_VALUES_EQUAL(utils.Next(TDuration::Seconds(13)), 0);
    utils.End();

    utils.AssertSlowZone({ 0 });
    auto message = utils.GetMessage(0);
    UNIT_ASSERT(message);
    UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Locked);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingDeadline, utils.TimeProvider->Now() + TDuration::Seconds(13));
    UNIT_ASSERT_VALUES_EQUAL(message->WriteTimestamp, utils.BaseWriteTimestamp);

    auto& metrics = utils.Storage.GetMetrics();
    AssertMessagesLocks(metrics.MessageLocks, {{0, 6}, {1, 1}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_MoveToSlowZoneAndCommit) {
    TUtils utils;
    utils.AddMessage(6);
    utils.Begin();
    utils.AddMessage(1);
    UNIT_ASSERT(utils.Commit(0));
    utils.End();

    utils.AssertSlowZone({ });

    auto& metrics = utils.Storage.GetMetrics();
    AssertMessagesLocks(metrics.MessageLocks, {{0, 6}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_MoveToSlowZoneAndDLQ) {
    TUtils utils;
    utils.AddMessage(6);
    AssertMessagesLocks(utils.Storage.GetMetrics().MessageLocks, {{0, 6}});
    utils.Begin();
    utils.AddMessage(1);
    AssertMessagesLocks(utils.Storage.GetMetrics().MessageLocks, {{0, 7}});
    UNIT_ASSERT_VALUES_EQUAL(utils.Next(TDuration::Seconds(13)), 0);
    AssertMessagesLocks(utils.Storage.GetMetrics().MessageLocks, {{0, 6}, {1, 1}});
    UNIT_ASSERT(utils.Unlock(0));
    AssertMessagesLocks(utils.Storage.GetMetrics().MessageLocks, {{0, 6}});
    utils.End();

    utils.AssertSlowZone({ 0 });
    auto message = utils.GetMessage(0);
    UNIT_ASSERT(message);
    UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::DLQ);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingDeadline, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(message->WriteTimestamp, utils.BaseWriteTimestamp);


    auto& metrics = utils.Storage.GetMetrics();
    AssertMessagesLocks(metrics.MessageLocks, {{0, 6}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_Lock) {
    TUtils utils;
    utils.AddMessage(8);
    utils.Begin();
    UNIT_ASSERT_VALUES_EQUAL(utils.Next(TDuration::Seconds(13)), 0);
    utils.End();

    utils.AssertSlowZone({ 0, 1 });
    auto message = utils.GetMessage(0);
    UNIT_ASSERT(message);
    UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Locked);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingDeadline, utils.TimeProvider->Now() + TDuration::Seconds(13));
    UNIT_ASSERT_VALUES_EQUAL(message->WriteTimestamp, utils.BaseWriteTimestamp);

    auto& metrics = utils.Storage.GetMetrics();
    AssertMessagesLocks(metrics.MessageLocks, {{0, 7}, {1, 1}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_Commit_First) {
    TUtils utils;
    utils.AddMessage(8);
    utils.Begin();
    UNIT_ASSERT(utils.Commit(0));
    utils.End();

    utils.AssertSlowZone({1});

    auto& metrics = utils.Storage.GetMetrics();
    AssertMessagesLocks(metrics.MessageLocks, {{0, 7}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_Commit) {
    TUtils utils;
    utils.AddMessage(8);
    utils.Begin();
    UNIT_ASSERT(utils.Commit(1));
    utils.End();

    utils.AssertSlowZone({0});

    auto& metrics = utils.Storage.GetMetrics();
    AssertMessagesLocks(metrics.MessageLocks, {{0, 7}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_DLQ) {
    TUtils utils;
    utils.AddMessage(8);
    utils.Begin();
    UNIT_ASSERT_VALUES_EQUAL(utils.Next(TDuration::Seconds(13)), 0);
    UNIT_ASSERT(utils.Unlock(0));
    utils.End();

    utils.AssertSlowZone({ 0, 1 });
    auto message = utils.GetMessage(0);
    UNIT_ASSERT(message);
    UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::DLQ);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingDeadline, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(message->WriteTimestamp, utils.BaseWriteTimestamp);

    auto& metrics = utils.Storage.GetMetrics();
    AssertMessagesLocks(metrics.MessageLocks, {{0, 7}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_CommitToFast) {
    TUtils utils;
    utils.AddMessage(8);
    utils.Begin();
    UNIT_ASSERT(utils.Commit(2));
    utils.Storage.Compact();
    utils.End();

    utils.AssertSlowZone({0, 1});
    // Compaction removed the message with offset 2
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().InflightMessageCount, 7);

    auto& metrics = utils.Storage.GetMetrics();
    AssertMessagesLocks(metrics.MessageLocks, {{0, 7}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_CommitAndAdd) {
    TUtils utils;
    utils.AddMessage(8);
    utils.Begin();
    UNIT_ASSERT(utils.Commit(1));
    utils.AddMessage(1);
    utils.End();

    utils.AssertSlowZone({0, 2});

    auto& metrics = utils.Storage.GetMetrics();
    AssertMessagesLocks(metrics.MessageLocks, {{0, 8}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_Retention_1message) {
    TUtils utils;
    utils.AddMessage(8);
    utils.Begin();

    utils.TimeProvider->Tick(TDuration::Seconds(2));
    utils.Storage.Compact();

    utils.End();

    utils.AssertSlowZone({1});
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetFirstOffset(), 2);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().InflightMessageCount, 7);

    auto& metrics = utils.Storage.GetMetrics();
    AssertMessagesLocks(metrics.MessageLocks, {{0, 7}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 1);

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_Retention_2message) {
    TUtils utils;
    utils.AddMessage(8);
    utils.Begin();

    utils.TimeProvider->Tick(TDuration::Seconds(3));
    utils.Storage.Compact();

    utils.End();

    utils.AssertSlowZone({});
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetFirstOffset(), 2);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().InflightMessageCount, 6);

    auto& metrics = utils.Storage.GetMetrics();
    AssertMessagesLocks(metrics.MessageLocks, {{0, 6}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 2);

    utils.AssertLoad();
}

Y_UNIT_TEST(SlowZone_Retention_3message) {
    TUtils utils;
    utils.AddMessage(8);
    utils.Begin();

    utils.TimeProvider->Tick(TDuration::Seconds(4));
    utils.Storage.Compact();

    utils.End();

    utils.AssertSlowZone({});
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetFirstOffset(), 3);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().InflightMessageCount, 5);

    auto& metrics = utils.Storage.GetMetrics();
    AssertMessagesLocks(metrics.MessageLocks, {{0, 5}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 3);

    utils.AssertLoad();
}

Y_UNIT_TEST(ChangeDeadLetterPolicy_Delete) {
    TUtils utils;
    utils.Storage.SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
    utils.Storage.SetMaxMessageProcessingCount(1);

    utils.AddMessage(1);
    utils.Next();
    utils.Storage.Unlock(0);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetDLQMessages().size(), 1);

    utils.Begin();
    utils.Storage.SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_DELETE);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetDLQMessages().size(), 0);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().InflightMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().CommittedMessageCount, 1);
    utils.End();

    auto& metrics = utils.Storage.GetMetrics();
    AssertMessagesLocks(metrics.MessageLocks, {});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    utils.AssertLoad();
}

Y_UNIT_TEST(ChangeDeadLetterPolicy_Unspecified) {
    TUtils utils;
    utils.Storage.SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
    utils.Storage.SetMaxMessageProcessingCount(1);

    utils.AddMessage(1);
    utils.Next();
    utils.Storage.Unlock(0);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetDLQMessages().size(), 1);

    utils.Begin();
    utils.Storage.SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_UNSPECIFIED);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetDLQMessages().size(), 0);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().InflightMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().UnprocessedMessageCount, 1);
    utils.End();

    auto& metrics = utils.Storage.GetMetrics();
    AssertMessagesLocks(metrics.MessageLocks, {{1, 1}});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 0);

    utils.AssertLoad();
}

Y_UNIT_TEST(SkipAddByRetentionPolicy) {
    TUtils utils;

    auto writeTimestamp = utils.TimeProvider->Now() - TDuration::Seconds(13);

    utils.Begin();
    UNIT_ASSERT(utils.Storage.AddMessage(3, true, 5, writeTimestamp));
    utils.End();

    auto it = utils.Storage.begin();
    UNIT_ASSERT(it == utils.Storage.end());

    // Retention skip must advance cursors so the consumer does not re-fetch forever.
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetFirstOffset(), 4);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetLastOffset(), 4);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetFirstUncommittedOffset(), 4);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetFirstUnlockedOffset(), 4);

    auto& metrics = utils.Storage.GetMetrics();
    UNIT_ASSERT_VALUES_EQUAL(metrics.InflightMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.LockedMessageGroupCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.CommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DeadlineExpiredMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.DLQMessageCount, 0);

    AssertMessagesLocks(metrics.MessageLocks, {});

    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalCommittedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalMovedToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalScheduledToDLQMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalPurgedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByDeadlinePolicyMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(metrics.TotalDeletedByRetentionMessageCount, 1);

    utils.TimeProvider->Tick(TDuration::Seconds(5));

    utils.AssertLoad();
}

Y_UNIT_TEST(MarkDLQMoved_unexpected) {
    TUtils utils;
    utils.Storage.SetRetentionPeriod(TDuration::Seconds(10));
    utils.AddMessage(3);

    utils.Next(TDuration::Seconds(1));
    utils.Next(TDuration::Seconds(1));
    utils.Next(TDuration::Seconds(1));

    Cerr << utils.Storage.DebugString() << Endl;

    // Move message to DLQ
    utils.TimeProvider->Tick(TDuration::Seconds(2));
    utils.Storage.ProccessDeadlines();

    Cerr << utils.Storage.DebugString() << Endl;

    {
        auto messages = utils.Storage.GetDLQMessages();
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].Offset, 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].SeqNo, 2);
        UNIT_ASSERT_VALUES_EQUAL(messages[1].Offset, 2);
        UNIT_ASSERT_VALUES_EQUAL(messages[1].SeqNo, 3);
    }

    utils.Storage.Compact();
    utils.Storage.Commit(2);

    {
        auto result = utils.Storage.MarkDLQMoved({
            .Offset = 1,
            .SeqNo = 1
        });
        UNIT_ASSERT_VALUES_EQUAL(result, true);
    }
    {
        auto result = utils.Storage.MarkDLQMoved({
            .Offset = 2,
            .SeqNo = 2
        });
        UNIT_ASSERT_VALUES_EQUAL(result, true);
    }
    {
        auto result = utils.Storage.MarkDLQMoved({
            .Offset = 1,
            .SeqNo = 1
        });
        UNIT_ASSERT_VALUES_EQUAL(result, true);
    }
    {
        auto result = utils.Storage.MarkDLQMoved({
            .Offset = 3,
            .SeqNo = 3
        });
        UNIT_ASSERT_VALUES_EQUAL(result, true);
    }
}


Y_UNIT_TEST(NextFromLockedStorage) {
    TStorage storage(CreateDefaultTimeProvider(), {.KeepMessageOrder = true, .ParentPartitionId = {4}});

    storage.AddMessage(3, true, 5, TInstant::Now());
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 3);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 4);

    {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now(), position);
        UNIT_ASSERT(!result.has_value());
    }
    {
        NKikimrPQ::TExternalLockedMessageGroupsId unlock;
        unlock.SetParentPartitionId(4);
        unlock.SetGeneration(1);
        unlock.SetConsumerGeneration(1);
        unlock.SetStep(10);
        unlock.SetMode(NKikimrPQ::EReadWithKeepOrder::READ_WITH_KEEP_ORDER_ALLOW_ALL);
        storage.UpdateExternalLockedMessageGroupsId(unlock);
    }
    {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT(result.has_value());
        UNIT_ASSERT_VALUES_EQUAL(result->Offset, 3);
    }
}

void NextFromLockedWithSequenceOfUnlocksImpl(bool initiatialBlockAll) {
    TStorage storage(CreateDefaultTimeProvider(), {.KeepMessageOrder = true, .ParentPartitionId = {0}});

    auto sendUpdate = [&, step = 10](NKikimrPQ::EReadWithKeepOrder mode, TConstArrayRef<ui32> blacklist) mutable {
        NKikimrPQ::TExternalLockedMessageGroupsId unlock;
        unlock.SetParentPartitionId(0);
        unlock.SetGeneration(1);
        unlock.SetConsumerGeneration(1);
        unlock.SetStep(step);
        unlock.SetMode(mode);
        for (ui32 h : blacklist) {
            unlock.MutableFullBlacklist()->AddParentLockedMessageGroupsIdHash(h);
        }
        storage.UpdateExternalLockedMessageGroupsId(unlock);
        ++step;
    };

    constexpr ui32 A = 0xAAAA;
    constexpr ui32 B = 0xBBBB;

    storage.AddMessage(0, true, A, TInstant::Now());
    storage.AddMessage(1, true, B, TInstant::Now());
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 0);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 2);
    {   // all locked
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now(), position);
        UNIT_ASSERT(!result.has_value());
    }

    if (initiatialBlockAll)  {
        sendUpdate(NKikimrPQ::EReadWithKeepOrder::READ_WITH_KEEP_ORDER_BLOCK_ALL, {});
    } else {
        sendUpdate(NKikimrPQ::EReadWithKeepOrder::READ_WITH_KEEP_ORDER_BLACKLIST, {A, B});
    }
    {   // still all locked
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now(), position);
        UNIT_ASSERT(!result.has_value());
    }

    sendUpdate(NKikimrPQ::EReadWithKeepOrder::READ_WITH_KEEP_ORDER_BLACKLIST, {A});
    sendUpdate(NKikimrPQ::EReadWithKeepOrder::READ_WITH_KEEP_ORDER_ALLOW_ALL, {});

    {   // all unlocked
        TSet<ui32> offsets;
        TStorage::TPosition position;

        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT(result.has_value());
        offsets.insert(result->Offset);

        result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT(result.has_value());
        offsets.insert(result->Offset);

        result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);

        UNIT_ASSERT_EQUAL_C(offsets, TSet<ui32>({0, 1}), '[' + JoinSeq(",", offsets) + ']');
    }
}

Y_UNIT_TEST(NextFromLockedWithSequenceOfUnlocksBlockStart) {
    NextFromLockedWithSequenceOfUnlocksImpl(true);
}

Y_UNIT_TEST(NextFromLockedWithSequenceOfUnlocksBlacklistStart) {
    NextFromLockedWithSequenceOfUnlocksImpl(false);
}

Y_UNIT_TEST(LockedStorageGeneration) {
    TStorage storage(CreateDefaultTimeProvider(), {.KeepMessageOrder = true, .ParentPartitionId = {4}});

    storage.AddMessage(3, true, 5, TInstant::Now());
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 3);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 4);

    {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now(), position);
        UNIT_ASSERT(!result.has_value());
    }
    {
        NKikimrPQ::TExternalLockedMessageGroupsId lock;
        lock.SetParentPartitionId(4);
        lock.SetGeneration(1);
        lock.SetConsumerGeneration(1);
        lock.SetStep(10);
        lock.SetMode(NKikimrPQ::EReadWithKeepOrder::READ_WITH_KEEP_ORDER_BLOCK_ALL);
        storage.UpdateExternalLockedMessageGroupsId(lock);
    }
    {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now(), position);
        UNIT_ASSERT(!result.has_value());
    }
    {
        NKikimrPQ::TExternalLockedMessageGroupsId unlock;
        unlock.SetParentPartitionId(4);
        unlock.SetGeneration(1);
        unlock.SetConsumerGeneration(1);
        unlock.SetStep(9); // prev step
        unlock.SetMode(NKikimrPQ::EReadWithKeepOrder::READ_WITH_KEEP_ORDER_ALLOW_ALL);
        storage.UpdateExternalLockedMessageGroupsId(unlock);
    }
    {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT(!result.has_value());
    }
    {
        NKikimrPQ::TExternalLockedMessageGroupsId unlock;
        unlock.SetParentPartitionId(4);
        unlock.SetGeneration(1);
        unlock.SetConsumerGeneration(1);
        unlock.SetStep(11); // next step
        unlock.SetMode(NKikimrPQ::EReadWithKeepOrder::READ_WITH_KEEP_ORDER_ALLOW_ALL);
        storage.UpdateExternalLockedMessageGroupsId(unlock);
    }
    {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT(result.has_value());
        UNIT_ASSERT_VALUES_EQUAL(result->Offset, 3);
    }
}

Y_UNIT_TEST(NextFromBlacklistedStorage) {
    constexpr TDuration deadline = TDuration::Seconds(50000);
    TStorage storage(CreateDefaultTimeProvider(), {.KeepMessageOrder = true, .ParentPartitionId = {4}});

    storage.AddMessage(3, true, 3, TInstant::Now());
    storage.AddMessage(4, true, 4, TInstant::Now());
    storage.AddMessage(5, true, 5, TInstant::Now());
    storage.AddMessage(6, true, 6, TInstant::Now());
    UNIT_ASSERT_VALUES_EQUAL(storage.GetFirstOffset(), 3);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetLastOffset(), 7);

    {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now(), position);
        UNIT_ASSERT(!result.has_value());
    }

    auto setLocked = [&, step = 10](std::vector<ui32> locked) mutable {
        NKikimrPQ::TExternalLockedMessageGroupsId unlock;
        unlock.SetParentPartitionId(4);
        unlock.SetGeneration(1);
        unlock.SetConsumerGeneration(1);
        unlock.SetStep(step++);
        unlock.SetMode(NKikimrPQ::EReadWithKeepOrder::READ_WITH_KEEP_ORDER_BLACKLIST);
        auto* blacklist = unlock.MutableFullBlacklist();
        for (auto hash : locked) {
            blacklist->AddParentLockedMessageGroupsIdHash(hash);
        }
        storage.UpdateExternalLockedMessageGroupsId(unlock);
    };
    setLocked({3, 5, 6});
    {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now() + deadline, position);
        UNIT_ASSERT(result.has_value());
        UNIT_ASSERT_VALUES_EQUAL(result->Offset, 4);

        result = storage.Next(TInstant::Now() + deadline, position);
        UNIT_ASSERT(!result.has_value());
    }
    setLocked({3, 6});
    {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now() + deadline, position);
        UNIT_ASSERT(result.has_value());
        UNIT_ASSERT_VALUES_EQUAL(result->Offset, 5);

        result = storage.Next(TInstant::Now() + deadline, position);
        UNIT_ASSERT(!result.has_value());
    }
    setLocked({});
    {
        TStorage::TPosition position;
        THashSet<ui64> expected = {{3, 6}};
        while (expected.size() > 0) {
            const TString caseDesc = TStringBuilder() << "Expected=[" << JoinSeq(",", expected) << "]";
            auto result = storage.Next(TInstant::Now() + deadline, position);
            UNIT_ASSERT_C(result.has_value(), caseDesc);
            UNIT_ASSERT_C(expected.contains(result->Offset), LabeledOutput(result->Offset) << " " << caseDesc);
            expected.erase(result->Offset);
        }
        auto result = storage.Next(TInstant::Now() + deadline, position);
        UNIT_ASSERT(!result.has_value());
    }
}

Y_UNIT_TEST(TOrderedMessageGroupIdHash) {
    TOrderedMessageGroupIdHash a(500);
    TOrderedMessageGroupIdHash b(500);
    TOrderedMessageGroupIdHash c(600);
    UNIT_ASSERT_EQUAL(a, b);
    UNIT_ASSERT_UNEQUAL(a, c);
    UNIT_ASSERT_UNEQUAL(b, c);
}

struct TFairnessModel {
    explicit TFairnessModel(bool keepMessageOrder = true)
        : KeepMessageOrder(keepMessageOrder)
        , Storage(CreateDefaultTimeProvider(), TStorage::TStorageSettings{.KeepMessageOrder = keepMessageOrder})
    {
    }

    bool KeepMessageOrder = true;
    TStorage Storage;
    ui64 Offset = 0;
    TSet<ui64> Infly;
    TSet<ui64> Committed;
    TMap<ui64, ui32> Hashes;
    TSet<ui64> Groupless;
    TMap<ui32, ui32> LastReadTime;
    ui32 ReadTime = 1;

    // Returns the set of offsets that Next is currently allowed to return.
    TSet<ui64> GetAvailalableOffsets() const {
        return KeepMessageOrder ? GetAvailalableOffsetsFifo() : GetAvailalableOffsetsStd();
    }

    // STD: many messages of one group may be in flight at once, so every not-yet-committed, not-in-flight,
    // non-expired message (grouped or groupless) is a valid candidate. Fairness only affects the order in
    // which they come out, not the drainable set, so the model does not constrain the within-group order.
    TSet<ui64> GetAvailalableOffsetsStd() const {
        TSet<ui64> res;
        for (auto [o, _] : Hashes) {
            if (!Committed.contains(o) && !Infly.contains(o)) {
                res.insert(o);
            }
        }
        return res;
    }

    // FIFO: at most one message per group is in flight; while a group is busy its other messages are blocked.
    TSet<ui64> GetAvailalableOffsetsFifo() const {
        TSet<ui64> res;
        for (ui64 o : Groupless) {
            if (!Committed.contains(o) && !Infly.contains(o)) {
                res.insert(o);
            }
        }
        TSet<ui32> visited;
        for (ui64 o : Infly) {
            if (!Groupless.contains(o)) {
                auto [_, ins] = visited.insert(Hashes.at(o));
                Y_ASSERT(ins);
            }
        }

        TSet<ui64> grouped;
        ui32 oldestReadTime = -1;
        for (auto [o, h] : Hashes) {
            if (Committed.contains(o) || Infly.contains(o) || Groupless.contains(o) || visited.contains(h)) {
                continue;
            }
            if (auto* lastReadTime = LastReadTime.FindPtr(h); lastReadTime == nullptr) {
                res.insert(o);
                visited.insert(h);
            } else if (*lastReadTime > oldestReadTime) {
                continue;
            } else if (*lastReadTime < oldestReadTime) {
                oldestReadTime = *lastReadTime;
                grouped.clear();
            }
            visited.insert(h);
            grouped.insert(o);
        }

        res.insert(grouped.begin(), grouped.end());
        return res;
    }

    void AddMessage(ui32 group, bool hasGroup = true) {
        Cerr << "Add message " << LabeledOutput(Offset, group, hasGroup) << "\n";
        Storage.AddMessage(Offset, hasGroup, group, TInstant::Now());
        Hashes[Offset] = group;
        if (!hasGroup) {
            Groupless.insert(Offset);
        }
        ++Offset;
    }

    struct TMessage {
        ui64 Offset;
        ui32 Group;
    };

    std::vector<TMessage> Next(size_t count) {
        std::vector<TMessage> res;
        TStorage::TPosition position;
        for (size_t i = 0; i < count; ++i) {
            auto result = Storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
            const auto available = GetAvailalableOffsets();
            Cerr << "Next message " << (i + 1) << '/' << count << ": ";
            UNIT_ASSERT_VALUES_EQUAL_C(result.has_value(), !available.empty(), i);
            if (result.has_value()) {
                ui64 offset = result->Offset;
                ui32 hash = Hashes.at(result->Offset);
                TString availableStr = "{" + JoinSeq(", ", available) + "}";
                Cerr << LabeledOutput(offset, hash, availableStr) << "\n";
                UNIT_ASSERT_C(available.contains(result->Offset), i);
                Infly.insert(offset);
                res.push_back(TMessage{.Offset = offset, .Group = hash});
                if (!Groupless.contains(offset)) {
                    LastReadTime[hash] = ReadTime;
                }
            } else {
                Cerr << "none\n";
            }
        }
        ++ReadTime;
        return res;
    }

    void Commit(ui64 offset) {
        Y_ASSERT(Infly.contains(offset));
        Y_ASSERT(!Committed.contains(offset));
        Infly.erase(offset);
        Committed.insert(offset);

        const auto available = GetAvailalableOffsets();
        TString availableStr = "{" + JoinSeq(", ", available) + "}";
        Cerr << "Commit message " << LabeledOutput(offset, Hashes.at(offset), availableStr) << "\n";

        Storage.Commit(offset);
    }

    void Unlock(ui64 offset) {
        Y_ASSERT(Infly.contains(offset));
        Y_ASSERT(!Committed.contains(offset));
        Infly.erase(offset);

        const auto available = GetAvailalableOffsets();
        TString availableStr = "{" + JoinSeq(", ", available) + "}";
        Cerr << "Unlock message " << LabeledOutput(offset, Hashes.at(offset), availableStr) << "\n";

        Storage.Unlock(offset);
    }
};


static void DrainWithCommitStd(TFairnessModel& model, size_t readSize = 3) {
    for (int guard = 0; guard < 500; ++guard) {
        auto batch = model.Next(readSize);
        if (batch.empty()) {
            break;
        }
        for (auto& m : batch) {
            model.Commit(m.Offset);
        }
    }
}

void NextWithFairnessBasicImpl(TConstArrayRef<ui32> groups, size_t readSize, bool keepMessageOrder = true) {
    TFairnessModel model(keepMessageOrder);
    const size_t n = groups.size();
    for (ui32 g : groups) {
        model.AddMessage(g);
    }

    for (size_t i = 0; i < n / readSize + 2; ++i) {
        for (auto& m : model.Next(readSize)) {
            model.Commit(m.Offset);
        }
    }
    // FIFO serves at most one message per group per round, so a large single group may not fully drain within
    // the fixed number of rounds. STD has no such restriction and must be fully drained here.
    if (!keepMessageOrder) {
        UNIT_ASSERT_VALUES_EQUAL(model.Committed.size(), model.Offset);
    }
}


Y_UNIT_TEST(NextWithFairness1) {
    NextWithFairnessBasicImpl({0, 1, 0, 3, 0, 5, 0, 7, 0, 9, 0, 11, 0, 13, 0, 15, 0, 17, 0, 19, 0, 21, 0, 23, 0}, 1);
}

Y_UNIT_TEST(NextWithFairness2) {
    NextWithFairnessBasicImpl({0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4}, 1);
}

Y_UNIT_TEST(NextWithFairness3) {
    NextWithFairnessBasicImpl({0, 1, 0, 3, 0, 5, 0, 7, 0, 9, 0, 11, 0, 13, 0, 15, 0, 17, 0, 19, 0, 21, 0, 23, 0}, 3);
}

Y_UNIT_TEST(NextWithFairness4) {
    NextWithFairnessBasicImpl({0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4}, 3);
}

Y_UNIT_TEST(NextWithFairnessStd1) {
    NextWithFairnessBasicImpl({0, 1, 0, 3, 0, 5, 0, 7, 0, 9, 0, 11, 0, 13, 0, 15, 0, 17, 0, 19, 0, 21, 0, 23, 0}, 1, /*keepMessageOrder*/ false);
}

Y_UNIT_TEST(NextWithFairnessStd2) {
    NextWithFairnessBasicImpl({0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4}, 1, /*keepMessageOrder*/ false);
}

Y_UNIT_TEST(NextWithFairnessStd3) {
    NextWithFairnessBasicImpl({0, 1, 0, 3, 0, 5, 0, 7, 0, 9, 0, 11, 0, 13, 0, 15, 0, 17, 0, 19, 0, 21, 0, 23, 0}, 3, /*keepMessageOrder*/ false);
}

Y_UNIT_TEST(NextWithFairnessStd4) {
    NextWithFairnessBasicImpl({0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4}, 3, /*keepMessageOrder*/ false);
}

Y_UNIT_TEST(StdManyInflightPerGroup) {
    // In STD mode there is no one-in-flight-per-group restriction: all messages sharing a group may be handed
    // out simultaneously in a single read burst, unlike FIFO where a busy group blocks its tail.
    TFairnessModel model(/*keepMessageOrder*/ false);
    for (int i = 0; i < 5; ++i) {
        model.AddMessage(7); // all in a single group
    }

    auto batch = model.Next(5);
    UNIT_ASSERT_VALUES_EQUAL_C(batch.size(), 5, "all messages of one group must be available at once in STD");
    TSet<ui64> offsets;
    for (auto& m : batch) {
        UNIT_ASSERT_VALUES_EQUAL(m.Group, 7u);
        offsets.insert(m.Offset);
    }
    UNIT_ASSERT_VALUES_EQUAL(offsets, (TSet<ui64>{0, 1, 2, 3, 4}));

    for (auto& m : batch) {
        model.Commit(m.Offset);
    }
    UNIT_ASSERT_VALUES_EQUAL(model.Committed.size(), model.Offset);
}

Y_UNIT_TEST(StdFairnessRoundRobinAcrossGroups) {
    // STD still spreads reads across groups: a burst smaller than the total should touch several groups rather
    // than draining one group first. Here three groups of two messages each; the fairness order rotates.
    TFairnessModel model(/*keepMessageOrder*/ false);
    for (ui32 g : {ui32(0), ui32(0), ui32(1), ui32(1), ui32(2), ui32(2)}) {
        model.AddMessage(g);
    }

    // First three reads must come from three distinct groups (round-robin), leaving one message per group.
    auto batch = model.Next(3);
    UNIT_ASSERT_VALUES_EQUAL(batch.size(), 3);
    TSet<ui32> firstRoundGroups;
    for (auto& m : batch) {
        firstRoundGroups.insert(m.Group);
    }
    UNIT_ASSERT_VALUES_EQUAL_C(firstRoundGroups.size(), 3, "STD fairness must rotate across all groups first");

    for (auto& m : batch) {
        model.Commit(m.Offset);
    }
    DrainWithCommitStd(model);
    UNIT_ASSERT_VALUES_EQUAL(model.Committed.size(), model.Offset);
}

Y_UNIT_TEST(StdInterleavedCommitUnlock) {
    // Mix Next / Commit / Unlock in STD mode: unlocked messages return to their group and must be redelivered,
    // committed ones must not, and every message is eventually drained exactly once.
    TFairnessModel model(/*keepMessageOrder*/ false);
    const int n = 24;
    for (int i = 0; i < n; ++i) {
        model.AddMessage(i % 4);
    }

    for (int round = 0; round < n; ++round) {
        auto v = model.Next(3);
        for (size_t j = 0; j < v.size(); ++j) {
            if (j % 2 == 0) {
                model.Commit(v[j].Offset);
            } else {
                model.Unlock(v[j].Offset);
            }
        }
    }
    DrainWithCommitStd(model);
    UNIT_ASSERT_VALUES_EQUAL(model.Committed.size(), model.Offset);
}

Y_UNIT_TEST(StdWithGrouplessMessages) {
    // Groupless messages coexist with grouped ones in STD mode and are all drainable.
    TFairnessModel model(/*keepMessageOrder*/ false);
    model.AddMessage(0, /*hasGroup*/ false); // 0
    model.AddMessage(1, /*hasGroup*/ true);  // 1
    model.AddMessage(1, /*hasGroup*/ true);  // 2
    model.AddMessage(0, /*hasGroup*/ false); // 3
    model.AddMessage(2, /*hasGroup*/ true);  // 4

    DrainWithCommitStd(model);
    UNIT_ASSERT_VALUES_EQUAL(model.Committed.size(), model.Offset);
}

Y_UNIT_TEST(StdGrouplessReturnsAscendingOffsets) {
    // No inflight message has a MessageGroupId, so Next keeps the legacy ascending-offset scan.
    TStorage storage(CreateDefaultTimeProvider(), TStorage::TStorageSettings{.KeepMessageOrder = false});
    storage.AddMessage(0, false, 0, TInstant::Now());
    storage.AddMessage(1, false, 0, TInstant::Now());
    storage.AddMessage(2, false, 0, TInstant::Now());
    storage.AddMessage(3, false, 0, TInstant::Now());

    TStorage::TPosition position;
    std::vector<ui64> seen;
    for (int i = 0; i < 4; ++i) {
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT_C(result.has_value(), i);
        seen.push_back(result->Offset);
    }
    UNIT_ASSERT_VALUES_EQUAL(seen, (std::vector<ui64>{0, 1, 2, 3}));

    auto empty = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
    UNIT_ASSERT(!empty.has_value());
}

Y_UNIT_TEST(StdGroupsRestoredFromSnapshotAndWAL) {
    // The per-group structures are not serialized; they must be rebuilt on restore for STD too. Read some
    // messages (leaving several in flight), snapshot + WAL, reload and verify the state matches exactly.
    TUtils utils(TStorage::TStorageSettings{.MinMessages = 1, .MaxMessages = 16, .KeepMessageOrder = false});
    for (ui32 i = 0; i < 8; ++i) {
        utils.AddMessageWithGroup(i, i % 3); // groups 0,1,2 with several messages each
    }

    utils.Begin();
    // Lock a few messages so groups have a mix of Locked and Unprocessed members.
    auto read = utils.ReadMessages(4);
    UNIT_ASSERT_VALUES_EQUAL(read.size(), 4);
    UNIT_ASSERT(utils.Commit(read.front().Offset));
    utils.End();

    // InflightMessageGroupCount must reflect the three groups that still have messages.
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().InflightMessageGroupCount, 3);

    utils.AssertLoad();
}

Y_UNIT_TEST(NextWithFairnessInterleavedCommit) {
    // Interleave Next and Commit: pull a batch, commit only part of it, keep the rest in flight so following
    // Next calls must respect that those groups are still busy.
    TFairnessModel model;
    const int n = 25;
    for (int i = 0; i < n; ++i) {
        model.AddMessage((i % 2 == 0) ? 0 : i);
    }

    std::vector<ui64> inflight;
    for (int i = 0; i < 2 * n; ++i) {
        for (auto& m : model.Next(3)) {
            inflight.push_back(m.Offset);
        }
        if (!inflight.empty()) {
            model.Commit(inflight.front());
            inflight.erase(inflight.begin());
        }
    }
    for (ui64 offset : inflight) {
        model.Commit(offset);
    }
}

Y_UNIT_TEST(NextWithFairnessInterleavedUnlock) {
    // Interleave Next, Unlock and Commit: unlocked messages must become eligible for their group again while
    // still obeying round-robin ordering.
    TFairnessModel model;
    const int n = 25;
    const int groupsSize = 6;
    for (int i = 0; i < n; ++i) {
        model.AddMessage(i / groupsSize);
    }

    for (int i = 0; i < n; ++i) {
        auto v = model.Next(2);
        for (size_t j = 0; j < v.size(); ++j) {
            if (j % 2 == 0) {
                model.Commit(v[j].Offset);
            } else {
                model.Unlock(v[j].Offset);
            }
        }
    }
    // Drain whatever remains.
    for (int i = 0; i < n; ++i) {
        for (auto& m : model.Next(1)) {
            model.Commit(m.Offset);
        }
    }
}

Y_UNIT_TEST(FairnessNormalInterleaved) {
    TFairnessModel model;
    const int n = 30;
    for (int i = 0; i < n; ++i) {
        model.AddMessage((i % 3 == 0) ? 0 : (i % 3 == 1) ? 1 : i);
    }

    for (int i = 0; i < n / 2; ++i) {
        for (auto& m : model.Next(1)) {
            model.Commit(m.Offset);
        }
    }
    for (int i = 0; i < n; ++i) {
        auto v = model.Next(4);
        for (auto& m : v) {
            model.Commit(m.Offset);
        }
    }
}

Y_UNIT_TEST(FairnessGrouplessAlwaysAvailable) {
    // FIFO still blocks the rest of a group while its head is in flight. Groupless messages stay readable
    // and every message is delivered. The mix order is not asserted.
    TFairnessModel model;
    model.AddMessage(1, /*hasGroup*/ true);  // 0
    model.AddMessage(1, /*hasGroup*/ true);  // 1
    model.AddMessage(0, /*hasGroup*/ false); // 2
    model.AddMessage(0, /*hasGroup*/ false); // 3

    TSet<ui64> seen;
    for (int i = 0; i < 8; ++i) {
        for (auto& message : model.Next(1)) {
            seen.insert(message.Offset);
            model.Commit(message.Offset);
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(seen, (TSet<ui64>{0, 1, 2, 3}));
}

// Collects the set of offsets that Next may return in the current storage state by repeatedly locking them and
// then unlocking them, so the returned messages can be treated as an unordered candidate set.
static TSet<ui64> ProbeCandidates(TStorage& storage, const absl::flat_hash_set<ui32>& skip = {}) {
    TSet<ui64> res;
    std::vector<ui64> locked;
    while (true) {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position, skip);
        if (!result.has_value()) {
            break;
        }
        res.insert(result->Offset);
        locked.push_back(result->Offset);
    }
    for (ui64 offset : locked) {
        storage.Unlock(offset);
    }
    return res;
}

Y_UNIT_TEST(StdFairnessCycleIgnoresInitialInsertionOrder) {
    // One pass visits every eligible group once, in whatever order the fairness list was built.
    // The next pass repeats that same cycle. Which offset of a group comes first is not fixed.
    // The test does not assert which group is first: a not-yet-read group may be inserted anywhere.
    TStorage storage(CreateDefaultTimeProvider(), TStorage::TStorageSettings{.KeepMessageOrder = false});
    const ui32 groupByOffset[] = {1, 2, 3, 1, 2, 3};
    for (ui64 offset = 0; offset < 6; ++offset) {
        storage.AddMessage(offset, true, groupByOffset[offset], TInstant::Now());
    }

    auto nextOffset = [&]() {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT(result.has_value());
        return result->Offset;
    };

    std::vector<ui32> firstRound;
    std::vector<ui64> firstOffsets;
    TSet<ui32> seen;
    for (int i = 0; i < 3; ++i) {
        ui64 offset = nextOffset();
        ui32 group = groupByOffset[offset];
        UNIT_ASSERT_C(!seen.contains(group), "a group must not be served twice before the other eligible groups");
        seen.insert(group);
        firstRound.push_back(group);
        firstOffsets.push_back(offset);
    }
    UNIT_ASSERT_VALUES_EQUAL(seen.size(), 3);

    for (size_t i = 0; i < firstRound.size(); ++i) {
        ui64 offset = nextOffset();
        UNIT_ASSERT_VALUES_EQUAL(groupByOffset[offset], firstRound[i]);
        UNIT_ASSERT_VALUES_UNEQUAL(offset, firstOffsets[i]);
    }
}

Y_UNIT_TEST(StdFairnessOneGroupManyMessages) {
    constexpr ui64 MessageCount = 99'000;
    TStorage storage(CreateDefaultTimeProvider(), TStorage::TStorageSettings{.KeepMessageOrder = false});
    for (ui64 offset = 0; offset < MessageCount; ++offset) {
        UNIT_ASSERT(storage.AddMessage(offset, true, 1, TInstant::Now()));
    }

    std::vector<char> seen(MessageCount);
    const TInstant deadline = TInstant::Now() + TDuration::Hours(1);
    for (ui64 i = 0; i < MessageCount; ++i) {
        TStorage::TPosition position;
        auto result = storage.Next(deadline, position);
        UNIT_ASSERT(result.has_value());
        UNIT_ASSERT(result->Offset < MessageCount);
        UNIT_ASSERT(!seen[result->Offset]);
        seen[result->Offset] = true;
    }
    TStorage::TPosition position;
    UNIT_ASSERT(!storage.Next(deadline, position).has_value());
}

Y_UNIT_TEST(StdGrouplessNotStarvedByOneGroup) {
    // One eligible group must not take every read while groupless messages are waiting, and the groupless
    // pool must not take every read while that group still has a message. The order itself is not fixed.
    TStorage storage(CreateDefaultTimeProvider(), TStorage::TStorageSettings{.KeepMessageOrder = false});
    constexpr ui64 Count = 8;
    TSet<ui64> groupless;
    TSet<ui64> grouped;
    for (ui64 offset = 0; offset < Count; ++offset) {
        storage.AddMessage(offset, false, 0, TInstant::Now());
        groupless.insert(offset);
    }
    for (ui64 offset = Count; offset < 2 * Count; ++offset) {
        storage.AddMessage(offset, true, 7, TInstant::Now());
        grouped.insert(offset);
    }

    size_t lockedGroupless = 0;
    size_t lockedGrouped = 0;
    bool grouplessWhileGroupRemains = false;
    bool groupedWhileGrouplessRemains = false;
    while (lockedGroupless < Count && lockedGrouped < Count) {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT(result.has_value());
        if (groupless.contains(result->Offset)) {
            ++lockedGroupless;
            grouplessWhileGroupRemains = true;
        } else {
            UNIT_ASSERT(grouped.contains(result->Offset));
            ++lockedGrouped;
            groupedWhileGrouplessRemains = true;
        }
    }
    UNIT_ASSERT(grouplessWhileGroupRemains);
    UNIT_ASSERT(groupedWhileGrouplessRemains);
}

Y_UNIT_TEST(StdFairnessRestoredFromSnapshotThenNext) {
    TUtils utils(TStorage::TStorageSettings{.MinMessages = 1, .MaxMessages = 16, .KeepMessageOrder = false});
    utils.Storage.AddMessage(0, true, 1, utils.TimeProvider->Now());
    utils.Storage.AddMessage(1, true, 2, utils.TimeProvider->Now());

    utils.Begin();
    auto read = utils.ReadMessages(1);
    UNIT_ASSERT_VALUES_EQUAL(read.size(), 1);
    utils.End();

    const ui64 lockedOffset = read.front().Offset;
    const ui64 expectedOffset = lockedOffset == 0 ? 1 : 0;

    TUtils loaded(TStorage::TStorageSettings{.MinMessages = 1, .MaxMessages = 16, .KeepMessageOrder = false});
    loaded.LoadSnapshot(utils.BeginSnapshot);
    loaded.LoadWAL(utils.WAL);
    loaded.Storage.InitMetrics();

    TStorage::TPosition position;
    auto result = loaded.Storage.Next(loaded.TimeProvider->Now() + TDuration::Seconds(1), position);
    UNIT_ASSERT(result.has_value());
    UNIT_ASSERT_VALUES_EQUAL(result->Offset, expectedOffset);

    auto empty = loaded.Storage.Next(loaded.TimeProvider->Now() + TDuration::Seconds(1), position);
    UNIT_ASSERT(!empty.has_value());
}

Y_UNIT_TEST(StdFairnessRetentionSkipsExpiredAndDoesNotHideOtherGroups) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    TStorage storage(timeProvider, TStorage::TStorageSettings{.KeepMessageOrder = false});
    storage.SetRetentionPeriod(TDuration::Seconds(5));

    // Group 1: expired head, fresh tail. Group 2: both expired. Group 3: fresh.
    storage.AddMessage(0, true, 1, timeProvider->Now());
    storage.AddMessage(1, true, 1, timeProvider->Now() + TDuration::Seconds(20));
    storage.AddMessage(2, true, 2, timeProvider->Now());
    storage.AddMessage(3, true, 2, timeProvider->Now());
    storage.AddMessage(4, true, 3, timeProvider->Now() + TDuration::Seconds(20));
    timeProvider->Tick(TDuration::Seconds(6));

    auto candidates = ProbeCandidates(storage);
    UNIT_ASSERT_C(!candidates.contains(0), "expired head is not returned");
    UNIT_ASSERT(candidates.contains(1));
    UNIT_ASSERT_C(!candidates.contains(2) && !candidates.contains(3), "a fully expired group is not returned");
    UNIT_ASSERT(candidates.contains(4));
    UNIT_ASSERT_VALUES_EQUAL(candidates.size(), 2);
}

Y_UNIT_TEST(StdFairnessSkipMessageGroups) {
    TStorage storage(CreateDefaultTimeProvider(), TStorage::TStorageSettings{.KeepMessageOrder = false});
    storage.AddMessage(0, true, 1, TInstant::Now());
    storage.AddMessage(1, true, 1, TInstant::Now());
    storage.AddMessage(2, true, 2, TInstant::Now());
    storage.AddMessage(3, true, 3, TInstant::Now());

    {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position, {1, 2, 3});
        UNIT_ASSERT(!result.has_value());
    }

    auto skipped = ProbeCandidates(storage, {1});
    UNIT_ASSERT_VALUES_EQUAL(skipped, (TSet<ui64>{2, 3}));

    auto all = ProbeCandidates(storage);
    UNIT_ASSERT_VALUES_EQUAL(all, (TSet<ui64>{0, 1, 2, 3}));
}

Y_UNIT_TEST(StdDelayedMessageBecomesReadableAfterUndelay) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    TStorage storage(timeProvider, TStorage::TStorageSettings{.KeepMessageOrder = false});
    storage.AddMessage(0, true, 1, timeProvider->Now(), TDuration::Seconds(1));
    storage.AddMessage(1, true, 2, timeProvider->Now());

    {
        TStorage::TPosition position;
        auto result = storage.Next(timeProvider->Now() + TDuration::Seconds(30), position);
        UNIT_ASSERT(result.has_value());
        UNIT_ASSERT_VALUES_EQUAL(result->Offset, 1);
    }

    timeProvider->Tick(TDuration::Seconds(2));
    storage.ProccessDeadlines();
    {
        auto [message, _] = storage.GetMessage(0);
        UNIT_ASSERT(message != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(message->GetStatus(), TStorage::EMessageStatus::Unprocessed);
    }

    TSet<ui64> seen;
    for (int i = 0; i < 4; ++i) {
        TStorage::TPosition position;
        auto result = storage.Next(timeProvider->Now() + TDuration::Seconds(30), position);
        if (!result.has_value()) {
            break;
        }
        seen.insert(result->Offset);
    }
    UNIT_ASSERT(seen.contains(0));
}

Y_UNIT_TEST(StdDlqDoesNotBlockTheRestOfTheGroup) {
    TStorage storage(CreateDefaultTimeProvider(), TStorage::TStorageSettings{.KeepMessageOrder = false});
    storage.SetMaxMessageProcessingCount(1);
    storage.SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
    storage.AddMessage(0, true, 1, TInstant::Now());
    storage.AddMessage(1, true, 1, TInstant::Now());
    storage.AddMessage(2, true, 2, TInstant::Now());

    TStorage::TPosition position;
    auto first = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
    UNIT_ASSERT(first.has_value());
    const ui64 dlqOffset = first->Offset;
    UNIT_ASSERT(storage.Unlock(dlqOffset) == EOperationResult::Success);
    {
        auto [message, _] = storage.GetMessage(dlqOffset);
        UNIT_ASSERT(message != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(message->GetStatus(), TStorage::EMessageStatus::DLQ);
    }

    TSet<ui64> readable;
    for (int i = 0; i < 4; ++i) {
        TStorage::TPosition nextPosition;
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), nextPosition);
        if (!result.has_value()) {
            break;
        }
        readable.insert(result->Offset);
    }
    UNIT_ASSERT_C(!readable.contains(dlqOffset), "a DLQ message is not returned");
    for (ui64 offset : {ui64(0), ui64(1), ui64(2)}) {
        if (offset != dlqOffset) {
            UNIT_ASSERT_C(readable.contains(offset), "STD does not block the rest of the group or other groups");
        }
    }

    UNIT_ASSERT(storage.WakeUpDLQ());
    TStorage::TPosition afterWake;
    auto woken = storage.Next(TInstant::Now() + TDuration::Seconds(1), afterWake);
    UNIT_ASSERT(woken.has_value());
    UNIT_ASSERT_VALUES_EQUAL(woken->Offset, dlqOffset);
}

Y_UNIT_TEST(StdSlowZoneMiddleUnlinkKeepsTheChain) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    TStorage storage(timeProvider, TStorage::TStorageSettings{.MinMessages = 1, .MaxMessages = 8, .KeepMessageOrder = false});
    auto now = timeProvider->Now();
    storage.AddMessage(0, true, 7, now);
    storage.AddMessage(1, true, 7, now);
    storage.AddMessage(2, true, 7, now);
    UNIT_ASSERT(storage.AddMessage(10, true, 8, now));

    for (const auto& message : storage) {
        if (message.Offset <= 2) {
            UNIT_ASSERT_C(message.SlowZone, message.Offset);
        }
    }

    UNIT_ASSERT(storage.Commit(1) == EOperationResult::Success);
    {
        auto [message, _] = storage.GetMessage(1);
        UNIT_ASSERT(message == nullptr);
    }

    auto candidates = ProbeCandidates(storage);
    UNIT_ASSERT_VALUES_EQUAL(candidates, (TSet<ui64>{0, 2, 10}));
}

Y_UNIT_TEST(StdGrouplessSkipsLockedHole) {
    TStorage storage(CreateDefaultTimeProvider(), TStorage::TStorageSettings{.KeepMessageOrder = false});
    storage.AddMessage(0, false, 0, TInstant::Now());
    storage.AddMessage(1, false, 0, TInstant::Now());
    storage.AddMessage(2, false, 0, TInstant::Now());

    {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT(result.has_value());
        UNIT_ASSERT_VALUES_EQUAL(result->Offset, 0);
    }

    TStorage::TPosition position;
    auto first = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
    UNIT_ASSERT(first.has_value());
    UNIT_ASSERT_VALUES_EQUAL(first->Offset, 1);
    auto second = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
    UNIT_ASSERT(second.has_value());
    UNIT_ASSERT_VALUES_EQUAL(second->Offset, 2);
}

Y_UNIT_TEST(StdGrouplessReadsSlowZoneBeforeFast) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    TStorage storage(timeProvider, TStorage::TStorageSettings{.MinMessages = 1, .MaxMessages = 8, .KeepMessageOrder = false});
    auto now = timeProvider->Now();
    storage.AddMessage(0, false, 0, now);
    storage.AddMessage(1, false, 0, now);
    UNIT_ASSERT(storage.AddMessage(8, false, 0, now));

    TStorage::TPosition position;
    std::vector<ui64> seen;
    for (int i = 0; i < 3; ++i) {
        auto result = storage.Next(timeProvider->Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT(result.has_value());
        seen.push_back(result->Offset);
    }
    UNIT_ASSERT_VALUES_EQUAL(seen, (std::vector<ui64>{0, 1, 8}));
}

Y_UNIT_TEST(StdFairnessSkipsExpiredGroupless) {
    // An expired groupless offset is not returned. The fresh groupless message and the grouped one both are.
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    TStorage storage(timeProvider, TStorage::TStorageSettings{.KeepMessageOrder = false});
    storage.SetRetentionPeriod(TDuration::Seconds(5));
    storage.AddMessage(0, false, 0, timeProvider->Now());
    storage.AddMessage(1, true, 7, timeProvider->Now() + TDuration::Seconds(20));
    storage.AddMessage(2, false, 0, timeProvider->Now() + TDuration::Seconds(20));
    timeProvider->Tick(TDuration::Seconds(6));

    TSet<ui64> seen;
    for (int i = 0; i < 2; ++i) {
        TStorage::TPosition position;
        auto result = storage.Next(timeProvider->Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT(result.has_value());
        seen.insert(result->Offset);
    }
    UNIT_ASSERT_VALUES_EQUAL(seen, (TSet<ui64>{1, 2}));
}

Y_UNIT_TEST(StdGrouplessSkipsRetentionExpired) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    TStorage storage(timeProvider, TStorage::TStorageSettings{.KeepMessageOrder = false});
    storage.SetRetentionPeriod(TDuration::Seconds(5));
    storage.AddMessage(0, false, 0, timeProvider->Now());
    storage.AddMessage(1, false, 0, timeProvider->Now() + TDuration::Seconds(20));
    timeProvider->Tick(TDuration::Seconds(6));

    TStorage::TPosition position;
    auto result = storage.Next(timeProvider->Now() + TDuration::Seconds(1), position);
    UNIT_ASSERT(result.has_value());
    UNIT_ASSERT_VALUES_EQUAL(result->Offset, 1);
}

Y_UNIT_TEST(StdSwitchesToFairnessWhenGroupedMessageAppears) {
    // With no group, Next is ascending offset. Once a grouped message arrives, fairness is used and
    // every still-unprocessed message, grouped or not, is delivered. The mix order is not asserted.
    TStorage storage(CreateDefaultTimeProvider(), TStorage::TStorageSettings{.KeepMessageOrder = false});
    storage.AddMessage(0, false, 0, TInstant::Now());
    storage.AddMessage(1, false, 0, TInstant::Now());
    storage.AddMessage(2, false, 0, TInstant::Now());

    {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT(result.has_value());
        UNIT_ASSERT_VALUES_EQUAL(result->Offset, 0);
    }

    storage.AddMessage(3, true, 7, TInstant::Now());

    TSet<ui64> seen;
    for (int i = 0; i < 3; ++i) {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT_C(result.has_value(), i);
        seen.insert(result->Offset);
    }
    UNIT_ASSERT_VALUES_EQUAL(seen, (TSet<ui64>{1, 2, 3}));
}

Y_UNIT_TEST(StdOffsetOrderResumesAfterLastGroupedMessageRemoved) {
    // A grouped message kept in the slow zone is deleted on commit. After that no inflight message has a
    // MessageGroupId, and the messages that remain are returned by ascending offset.
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    TStorage storage(timeProvider, TStorage::TStorageSettings{.MinMessages = 1, .MaxMessages = 8, .KeepMessageOrder = false});
    auto now = timeProvider->Now();
    storage.AddMessage(0, true, 7, now);
    storage.AddMessage(1, false, 0, now);
    UNIT_ASSERT(storage.AddMessage(10, false, 0, now));
    UNIT_ASSERT(storage.Commit(0) == EOperationResult::Success);

    TStorage::TPosition position;
    auto first = storage.Next(now + TDuration::Seconds(1), position);
    auto second = storage.Next(now + TDuration::Seconds(1), position);
    UNIT_ASSERT(first.has_value());
    UNIT_ASSERT(second.has_value());
    UNIT_ASSERT_VALUES_EQUAL(first->Offset, 1);
    UNIT_ASSERT_VALUES_EQUAL(second->Offset, 10);
}

Y_UNIT_TEST(StdUnlockMakesMessageReadableAgain) {
    // Unlock returns the message to Unprocessed. List position after Unlock is intentionally not asserted:
    // moving the group to the back is allowed for any remaining size, and so is leaving it in place.
    TStorage storage(CreateDefaultTimeProvider(), TStorage::TStorageSettings{.KeepMessageOrder = false});
    storage.AddMessage(0, true, 1, TInstant::Now());
    storage.AddMessage(1, true, 1, TInstant::Now());

    TStorage::TPosition position;
    auto first = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
    auto second = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
    UNIT_ASSERT(first.has_value());
    UNIT_ASSERT(second.has_value());
    UNIT_ASSERT_VALUES_EQUAL(first->Offset, 0);
    UNIT_ASSERT_VALUES_EQUAL(second->Offset, 1);

    UNIT_ASSERT(storage.Unlock(1) == EOperationResult::Success);

    TStorage::TPosition again;
    auto unlocked = storage.Next(TInstant::Now() + TDuration::Seconds(1), again);
    UNIT_ASSERT(unlocked.has_value());
    UNIT_ASSERT_VALUES_EQUAL(unlocked->Offset, 1);
}

Y_UNIT_TEST(FairnessWithRetention) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    TStorage storage(timeProvider, TStorage::TStorageSettings{.KeepMessageOrder = true});
    storage.SetRetentionPeriod(TDuration::Seconds(5));

    // Group A (hash 1): offset 0 old (will expire), offset 1 fresh. Group B (hash 2): offset 2 fresh.
    storage.AddMessage(0, true, 1, timeProvider->Now());
    storage.AddMessage(1, true, 1, timeProvider->Now() + TDuration::Seconds(20));
    storage.AddMessage(2, true, 2, timeProvider->Now() + TDuration::Seconds(20));

    timeProvider->Tick(TDuration::Seconds(6)); // offset 0 is now expired

    {
        auto candidates = ProbeCandidates(storage);
        UNIT_ASSERT_C(!candidates.contains(0), "expired offset 0 must be skipped");
        UNIT_ASSERT_VALUES_EQUAL(candidates.size(), 2);
        UNIT_ASSERT(candidates.contains(1));
        UNIT_ASSERT(candidates.contains(2));
    }

    storage.Compact();
    {
        auto candidates = ProbeCandidates(storage);
        UNIT_ASSERT_C(!candidates.contains(0), "expired offset 0 must be gone after compaction");
        UNIT_ASSERT_VALUES_EQUAL(candidates.size(), 2);
        UNIT_ASSERT(candidates.contains(1));
        UNIT_ASSERT(candidates.contains(2));
    }

    TSet<ui64> seen;
    for (int i = 0; i < 2; ++i) {
        TStorage::TPosition position;
        auto result = storage.Next(timeProvider->Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT_C(result.has_value(), LabeledOutput(i));
        UNIT_ASSERT_C(result->Offset != 0, "expired offset 0 must never be returned");
        seen.insert(result->Offset);
        UNIT_ASSERT(storage.Commit(result->Offset) == EOperationResult::Success);
    }
    UNIT_ASSERT_VALUES_EQUAL(seen.size(), 2);
    UNIT_ASSERT(seen.contains(1) && seen.contains(2));
}

Y_UNIT_TEST(FairnessWithDlq) {
    TStorage storage(CreateDefaultTimeProvider(), TStorage::TStorageSettings{.KeepMessageOrder = true});
    storage.SetMaxMessageProcessingCount(1);
    storage.SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);

    storage.AddMessage(0, true, 1, TInstant::Now());
    storage.AddMessage(1, true, 1, TInstant::Now());
    storage.AddMessage(2, true, 2, TInstant::Now());

    {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT(result.has_value());
        UNIT_ASSERT_VALUES_EQUAL(result->Offset, 0);
        storage.Unlock(0);
    }
    {
        auto [message, _] = storage.GetMessage(0);
        UNIT_ASSERT_VALUES_EQUAL(message->GetStatus(), TStorage::EMessageStatus::DLQ);
    }

    // While offset 0 is a DLQ head, group A is blocked: only group B's offset 2 is eligible, and offset 1 must
    // not be returned.
    {
        auto candidates = ProbeCandidates(storage);
        UNIT_ASSERT_C(!candidates.contains(1), "group A is blocked by its DLQ head");
        UNIT_ASSERT_VALUES_EQUAL(candidates.size(), 1);
        UNIT_ASSERT(candidates.contains(2));
    }

    // Commit the DLQ head; group A unblocks and its next message (offset 1) becomes eligible.
    UNIT_ASSERT(storage.Commit(0) == EOperationResult::Success);
    {
        auto candidates = ProbeCandidates(storage);
        UNIT_ASSERT_C(candidates.contains(1), "group A must be eligible after DLQ head committed");
    }
}

Y_UNIT_TEST(FairnessWithSkipMessageGroups) {
    // Groups listed in skipMessageGroups must never be returned by that Next call, while other groups are still
    // served respecting FIFO. When all eligible groups are skipped, Next returns nothing.
    TStorage storage(CreateDefaultTimeProvider(), TStorage::TStorageSettings{.KeepMessageOrder = true});

    storage.AddMessage(0, true, 1, TInstant::Now());
    storage.AddMessage(1, true, 1, TInstant::Now());
    storage.AddMessage(2, true, 2, TInstant::Now());
    storage.AddMessage(3, true, 3, TInstant::Now());

    {
        auto candidates = ProbeCandidates(storage, {1, 2});
        UNIT_ASSERT_VALUES_EQUAL(candidates.size(), 1);
        UNIT_ASSERT(candidates.contains(3));
    }

    // Skipping every group yields nothing.
    {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position, {1, 2, 3});
        UNIT_ASSERT_C(!result.has_value(), "all eligible groups skipped");
    }

    // Skipping only A leaves heads of B and C eligible; the skipped-group offsets 0/1 must not appear.
    {
        auto candidates = ProbeCandidates(storage, {1});
        UNIT_ASSERT_C(!candidates.contains(0) && !candidates.contains(1), "skipped group A must not be returned");
        UNIT_ASSERT(candidates.contains(2));
        UNIT_ASSERT(candidates.contains(3));
    }
}

Y_UNIT_TEST(FairnessSkipThenNoSkip) {
    // Groups skipped in earlier Next calls must eventually be returnable once Next is called with an empty skip
    // set (not necessarily on the first such call), preserving FIFO within each group and losing no message.
    TStorage storage(CreateDefaultTimeProvider(), TStorage::TStorageSettings{.KeepMessageOrder = true});

    // Groups: A=1 {0,1}, B=2 {2,3}, C=3 {4}.
    storage.AddMessage(0, true, 1, TInstant::Now());
    storage.AddMessage(1, true, 1, TInstant::Now());
    storage.AddMessage(2, true, 2, TInstant::Now());
    storage.AddMessage(3, true, 2, TInstant::Now());
    storage.AddMessage(4, true, 3, TInstant::Now());

    TMap<ui64, ui32> group = {{0, 1}, {1, 1}, {2, 2}, {3, 2}, {4, 3}};
    TMap<ui32, ui64> nextExpectedInGroup = {{1, 0}, {2, 2}, {3, 4}};

    auto consume = [&](ui64 offset) {
        ui32 g = group.at(offset);
        UNIT_ASSERT_VALUES_EQUAL_C(offset, nextExpectedInGroup.at(g), "FIFO within group violated");
        ++nextExpectedInGroup[g];
        UNIT_ASSERT(storage.Commit(offset) == EOperationResult::Success);
    };

    // Phase 1: skip groups A and B, so only group C is served.
    {
        const absl::flat_hash_set<ui32> skip = {1, 2};
        while (true) {
            TStorage::TPosition position;
            auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position, skip);
            if (!result.has_value()) {
                break;
            }
            ui32 g = group.at(result->Offset);
            UNIT_ASSERT_C(g != 1 && g != 2, "skipped group must not be returned");
            consume(result->Offset);
        }
    }
    UNIT_ASSERT_VALUES_EQUAL_C(nextExpectedInGroup.at(3), 5, "group C fully drained in phase 1");

    // Phase 2: no skip. The previously-skipped groups A and B must all be returned eventually, FIFO preserved.
    TSet<ui64> remaining = {0, 1, 2, 3};
    for (int guard = 0; guard < 100 && !remaining.empty(); ++guard) {
        TStorage::TPosition position;
        auto result = storage.Next(TInstant::Now() + TDuration::Seconds(1), position);
        UNIT_ASSERT_C(result.has_value(), "remaining messages must eventually be returned with empty skip");
        UNIT_ASSERT_C(remaining.contains(result->Offset), LabeledOutput(result->Offset));
        remaining.erase(result->Offset);
        consume(result->Offset);
    }
    UNIT_ASSERT_C(remaining.empty(), "all previously-skipped messages must be returned");
}

static void DrainWithCommit(TFairnessModel& model, size_t readSize = 3) {
    for (int guard = 0; guard < 200; ++guard) {
        auto batch = model.Next(readSize);
        if (batch.empty()) {
            break;
        }
        for (auto& m : batch) {
            model.Commit(m.Offset);
        }
    }
}

Y_UNIT_TEST(FairnessAddMessageAfterNext_SameGroup) {
    TFairnessModel model;
    model.AddMessage(1);
    model.AddMessage(2);

    auto first = model.Next(1);
    UNIT_ASSERT_VALUES_EQUAL(first.size(), 1);

    model.AddMessage(first[0].Group);

    DrainWithCommit(model);
    model.Commit(first[0].Offset);
    DrainWithCommit(model);

    UNIT_ASSERT_VALUES_EQUAL(model.Committed.size(), model.Offset);
}

Y_UNIT_TEST(FairnessAddMessageAfterNext_DifferentGroup) {
    TFairnessModel model;
    model.AddMessage(1);
    model.AddMessage(1);
    model.AddMessage(2);

    auto first = model.Next(1);
    UNIT_ASSERT_VALUES_EQUAL(first.size(), 1);

    model.AddMessage(999);

    DrainWithCommit(model);
    model.Commit(first[0].Offset);
    DrainWithCommit(model);

    UNIT_ASSERT_VALUES_EQUAL(model.Committed.size(), model.Offset);
}

Y_UNIT_TEST(FairnessAddMessageAfterCommit_SameGroup) {
    TFairnessModel model;
    model.AddMessage(1);
    model.AddMessage(2);

    auto first = model.Next(1);
    UNIT_ASSERT_VALUES_EQUAL(first.size(), 1);
    model.Commit(first[0].Offset);

    model.AddMessage(first[0].Group);

    DrainWithCommit(model);
    UNIT_ASSERT_VALUES_EQUAL(model.Committed.size(), model.Offset);
}

Y_UNIT_TEST(FairnessAddMessageAfterCommit_DifferentGroup) {
    TFairnessModel model;
    model.AddMessage(1);
    model.AddMessage(1);
    model.AddMessage(2);

    auto first = model.Next(1);
    UNIT_ASSERT_VALUES_EQUAL(first.size(), 1);
    model.Commit(first[0].Offset);

    model.AddMessage(500);
    model.AddMessage(500);

    DrainWithCommit(model);
    UNIT_ASSERT_VALUES_EQUAL(model.Committed.size(), model.Offset);
}

Y_UNIT_TEST(FairnessAddMessageDuringReadMixed) {
    TFairnessModel model;
    for (int i = 0; i < 6; ++i) {
        model.AddMessage(i % 3);
    }

    ui32 newGroup = 100;
    for (int round = 0; round < 20; ++round) {
        auto batch = model.Next(1);
        if (batch.empty()) {
            continue;
        }
        model.AddMessage(batch[0].Group);
        model.AddMessage(newGroup++);
        model.AddMessage(newGroup++, /*hasGroup*/ false);
        model.Commit(batch[0].Offset);
    }

    DrainWithCommit(model);
    UNIT_ASSERT_VALUES_EQUAL(model.Committed.size(), model.Offset);
}

std::vector<ui64> GetReadOffsets(const std::deque<TReadMessage>& messages) {
    std::vector<ui64> offsets;
    offsets.reserve(messages.size());
    for (const auto& message : messages) {
        offsets.push_back(message.Offset);
    }
    return offsets;
}

Y_UNIT_TEST(ReadAttemptReplayReturnsSameMessages) {
    TUtils utils;
    utils.Storage.SetReceiveAttemptIdPeriod(TDuration::Seconds(30));
    for (ui32 group = 0; group < 5; ++group) {
        utils.AddMessageWithGroup(group, group);
    }

    const TString attemptId = "my_attempt";
    const auto first = utils.ReadMessages(10, attemptId);
    UNIT_ASSERT_VALUES_EQUAL(first.size(), 5);

    const auto second = utils.ReadMessages(10, attemptId);
    UNIT_ASSERT_VALUES_EQUAL(second.size(), 5);
    UNIT_ASSERT_VALUES_EQUAL(GetReadOffsets(first), GetReadOffsets(second));
}

Y_UNIT_TEST(ReadAttemptDifferentIdsDoNotShareState) {
    TUtils utils;
    utils.Storage.SetReceiveAttemptIdPeriod(TDuration::Seconds(30));
    for (ui32 group = 0; group < 5; ++group) {
        utils.AddMessageWithGroup(group, group);
    }

    const auto first = utils.ReadMessages(1, "attempt_a");
    UNIT_ASSERT_VALUES_EQUAL(first.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(first.front().Offset, 0u);

    const auto second = utils.ReadMessages(10, "attempt_b");
    UNIT_ASSERT_VALUES_EQUAL(second.size(), 4);
    UNIT_ASSERT_VALUES_EQUAL(GetReadOffsets(second), (std::vector<ui64>{1, 2, 3, 4}));
}

Y_UNIT_TEST(ReadAttemptExpiresAfterPeriod) {
    TUtils utils;
    utils.Storage.SetReceiveAttemptIdPeriod(TDuration::Seconds(2));
    for (ui32 group = 0; group < 5; ++group) {
        utils.AddMessageWithGroup(group, group);
    }

    const TString attemptId = "my_attempt";
    const auto first = utils.ReadMessages(1, attemptId);
    UNIT_ASSERT_VALUES_EQUAL(first.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(first.front().Offset, 0u);

    utils.TimeProvider->Tick(TDuration::Seconds(3));
    utils.Storage.ProccessDeadlines();

    const auto second = utils.ReadMessages(10, attemptId);
    UNIT_ASSERT(second.size() > 0);
    UNIT_ASSERT_VALUES_UNEQUAL(GetReadOffsets(first), GetReadOffsets(second));
}

Y_UNIT_TEST(ReadAttemptReplayExtendsExpiry) {
    TUtils utils;
    utils.Storage.SetReceiveAttemptIdPeriod(TDuration::Seconds(2));
    utils.AddMessageWithGroup(0, 0);
    utils.AddMessageWithGroup(1, 1);

    const TString attemptId = "my_attempt";
    const auto first = utils.ReadMessages(2, attemptId);
    UNIT_ASSERT_VALUES_EQUAL(GetReadOffsets(first), (std::vector<ui64>{0, 1}));

    utils.TimeProvider->Tick(TDuration::Seconds(1));
    const auto replay = utils.ReadMessages(2, attemptId);
    UNIT_ASSERT_VALUES_EQUAL(GetReadOffsets(replay), (std::vector<ui64>{0, 1}));

    utils.TimeProvider->Tick(TDuration::Seconds(2));
    utils.CheckNoNext();
}

Y_UNIT_TEST(ReadAttemptCommitInvalidatesReplay) {
    TUtils utils;
    utils.Storage.SetReceiveAttemptIdPeriod(TDuration::Seconds(30));
    for (ui32 group = 0; group < 3; ++group) {
        utils.AddMessageWithGroup(group, group);
    }

    const TString attemptId = "my_attempt";
    const auto first = utils.ReadMessages(3, attemptId);
    UNIT_ASSERT_VALUES_EQUAL(GetReadOffsets(first), (std::vector<ui64>{0, 1, 2}));

    UNIT_ASSERT(utils.Commit(1));
    utils.Storage.ProccessDeadlines();

    const auto second = utils.ReadMessages(3, attemptId);
    UNIT_ASSERT_VALUES_EQUAL(second.size(), 0);
}

Y_UNIT_TEST(ReadAttemptChangeMessageDeadlineInvalidatesReplay) {
    TUtils utils;
    utils.Storage.SetReceiveAttemptIdPeriod(TDuration::Seconds(30));
    for (ui32 group = 0; group < 3; ++group) {
        utils.AddMessageWithGroup(group, group);
    }

    const TString attemptId = "my_attempt";
    const auto first = utils.ReadMessages(3, attemptId);
    UNIT_ASSERT_VALUES_EQUAL(GetReadOffsets(first), (std::vector<ui64>{0, 1, 2}));

    UNIT_ASSERT(
        utils.Storage.ChangeMessageDeadline(1, utils.TimeProvider->Now() + TDuration::Seconds(15))
        == EOperationResult::Success);
    utils.Storage.ProccessDeadlines();

    const auto second = utils.ReadMessages(3, attemptId);
    UNIT_ASSERT_VALUES_EQUAL(second.size(), 0);
}

Y_UNIT_TEST(ReadAttemptUnlockInvalidatesReplay) {
    TUtils utils;
    utils.Storage.SetReceiveAttemptIdPeriod(TDuration::Seconds(30));
    for (ui32 group = 0; group < 3; ++group) {
        utils.AddMessageWithGroup(group, group);
    }

    const TString attemptId = "my_attempt";
    const auto first = utils.ReadMessages(3, attemptId);
    UNIT_ASSERT_VALUES_EQUAL(GetReadOffsets(first), (std::vector<ui64>{0, 1, 2}));

    UNIT_ASSERT(utils.Unlock(1));
    utils.Storage.ProccessDeadlines();

    const auto second = utils.ReadMessages(3, attemptId);
    UNIT_ASSERT_VALUES_EQUAL(second.size(), 0);
}

Y_UNIT_TEST(ReadAttemptPurgeClearsReplayState) {
    TUtils utils;
    utils.Storage.SetReceiveAttemptIdPeriod(TDuration::Seconds(30));
    for (ui32 group = 0; group < 3; ++group) {
        utils.AddMessageWithGroup(group, group);
    }

    const TString attemptId = "my_attempt";
    const auto first = utils.ReadMessages(3, attemptId);
    UNIT_ASSERT_VALUES_EQUAL(GetReadOffsets(first), (std::vector<ui64>{0, 1, 2}));

    UNIT_ASSERT(utils.Storage.Purge(3));

    for (ui32 group = 0; group < 3; ++group) {
        utils.AddMessageWithGroup(group + 10, group);
    }

    const auto second = utils.ReadMessages(3, attemptId);
    UNIT_ASSERT_VALUES_EQUAL(GetReadOffsets(second), (std::vector<ui64>{10, 11, 12}));
}

Y_UNIT_TEST(ReadAttemptSnapshotPersistence) {
    TUtils utils;
    utils.Storage.SetReceiveAttemptIdPeriod(TDuration::Seconds(30));
    for (ui32 group = 0; group < 3; ++group) {
        utils.AddMessageWithGroup(group, group);
    }

    const TString attemptId = "my_attempt";
    const auto first = utils.ReadMessages(3, attemptId);
    UNIT_ASSERT_VALUES_EQUAL(GetReadOffsets(first), (std::vector<ui64>{0, 1, 2}));

    const auto snapshot = utils.CreateSnapshot();

    TUtils loaded(TStorage::TStorageSettings{.MinMessages = 1, .MaxMessages = 8, .KeepMessageOrder = true});
    loaded.Storage.SetReceiveAttemptIdPeriod(TDuration::Seconds(30));
    loaded.LoadSnapshot(snapshot);

    const auto second = loaded.ReadMessages(3, attemptId);
    UNIT_ASSERT_VALUES_EQUAL(GetReadOffsets(second), GetReadOffsets(first));
}

Y_UNIT_TEST(ReadAttemptWALPersistence) {
    TUtils utils;
    utils.Storage.SetReceiveAttemptIdPeriod(TDuration::Seconds(30));
    for (ui32 group = 0; group < 3; ++group) {
        utils.AddMessageWithGroup(group, group);
    }

    utils.Begin();
    const TString attemptId = "my_attempt";
    const auto first = utils.ReadMessages(3, attemptId);
    UNIT_ASSERT_VALUES_EQUAL(GetReadOffsets(first), (std::vector<ui64>{0, 1, 2}));
    utils.End();

    TUtils loaded(TStorage::TStorageSettings{.MinMessages = 1, .MaxMessages = 8, .KeepMessageOrder = true});
    loaded.Storage.SetReceiveAttemptIdPeriod(TDuration::Seconds(30));
    loaded.LoadSnapshot(utils.BeginSnapshot);
    loaded.LoadWAL(utils.WAL);

    const auto second = loaded.ReadMessages(3, attemptId);
    UNIT_ASSERT_VALUES_EQUAL(GetReadOffsets(second), GetReadOffsets(first));
}

Y_UNIT_TEST(ReadWithZeroVisibilityTimeoutUnlocksImmediately) {
    // SQS VisibilityTimeout=0: messages are handed to the client but their lock is released right away,
    // so they stay Unprocessed and can be received again immediately (with a growing receive count).
    TUtils utils;
    utils.Storage.SetMaxMessageProcessingCount(100);
    utils.Storage.SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_UNSPECIFIED);
    for (ui32 group = 0; group < 3; ++group) {
        utils.AddMessageWithGroup(group, group);
    }

    const auto first = utils.ReadMessages(10, {}, TDuration::Zero());
    UNIT_ASSERT_VALUES_EQUAL(GetReadOffsets(first), (std::vector<ui64>{0, 1, 2}));
    UNIT_ASSERT_VALUES_EQUAL(first.front().ApproximateReceiveCount, 1u);

    // Nothing is left in flight after the read.
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().LockedMessageCount, 0);
    for (ui64 offset = 0; offset < 3; ++offset) {
        auto message = utils.GetMessage(offset);
        UNIT_ASSERT(message.has_value());
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(message->Status),
            static_cast<int>(TStorage::EMessageStatus::Unprocessed), offset);
    }

    // The same messages are immediately available again; the receive count keeps growing.
    const auto second = utils.ReadMessages(10, {}, TDuration::Zero());
    auto secondOffsets = GetReadOffsets(second);
    std::sort(secondOffsets.begin(), secondOffsets.end());
    UNIT_ASSERT_VALUES_EQUAL(secondOffsets, (std::vector<ui64>{0, 1, 2}));
    for (const auto& message : second) {
        UNIT_ASSERT_VALUES_EQUAL(message.ApproximateReceiveCount, 2u);
    }
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().LockedMessageCount, 0);
}

Y_UNIT_TEST(ReadWithZeroVisibilityTimeoutMovesToDLQ) {
    // Even with immediate unlock, the receive count still advances the message towards the DLQ once the
    // configured receive limit is reached. The read that reaches the limit still returns the message.
    TUtils utils;
    utils.Storage.SetMaxMessageProcessingCount(3);
    utils.Storage.SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
    utils.AddMessageWithGroup(0, 0);

    for (ui32 attempt = 1; attempt <= 3; ++attempt) {
        const auto read = utils.ReadMessages(10, {}, TDuration::Zero());
        UNIT_ASSERT_VALUES_EQUAL_C(GetReadOffsets(read), (std::vector<ui64>{0}), attempt);
        UNIT_ASSERT_VALUES_EQUAL_C(read.front().ApproximateReceiveCount, attempt, attempt);
    }

    // After hitting the receive limit the message is scheduled to the DLQ and is no longer delivered.
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetDLQMessages().size(), 1);
    {
        auto message = utils.GetMessage(0);
        UNIT_ASSERT(message.has_value());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(message->Status),
            static_cast<int>(TStorage::EMessageStatus::DLQ));
    }

    const auto afterDlq = utils.ReadMessages(10, {}, TDuration::Zero());
    UNIT_ASSERT_VALUES_EQUAL(afterDlq.size(), 0);
}

Y_UNIT_TEST(SkipAddByRetentionBatchedAdvancesFirstOffset) {
    TUtils utils;
    const auto writeTimestamp = utils.TimeProvider->Now() - TDuration::Seconds(13);

    utils.Begin();
    UNIT_ASSERT(utils.Storage.AddMessage(10, true, 5, writeTimestamp, TDuration::Zero(), /*logicalMessageCount=*/4));
    utils.End();

    UNIT_ASSERT(utils.Storage.begin() == utils.Storage.end());
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetFirstOffset(), 14);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetLastOffset(), 14);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetFirstUncommittedOffset(), 14);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetFirstUnlockedOffset(), 14);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().TotalDeletedByRetentionMessageCount, 4);

    utils.AssertLoad();
}

Y_UNIT_TEST(SkipAddByRetentionThenAddFresh) {
    TUtils utils;
    const auto expiredTimestamp = utils.TimeProvider->Now() - TDuration::Seconds(13);

    UNIT_ASSERT(utils.Storage.AddMessage(3, true, 5, expiredTimestamp));
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetFirstOffset(), 4);

    utils.Begin();
    UNIT_ASSERT(utils.Storage.AddMessage(4, true, 7, utils.TimeProvider->Now()));
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetFirstOffset(), 4);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetLastOffset(), 5);
    UNIT_ASSERT_VALUES_EQUAL(utils.Next(), 4);
    UNIT_ASSERT(utils.Commit(4));
    utils.End();

    utils.CheckNoNext();
    utils.AssertLoad();
}

Y_UNIT_TEST(RetentionExpiredNotSkippedWhenFastZoneNonEmpty) {
    // Retention skip applies only when Messages.empty(); otherwise the message is stored
    // and later removed by Compact / Next retention handling.
    TUtils utils;
    utils.Storage.AddMessage(0, true, 1, utils.TimeProvider->Now());
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMessageCount(), 1);

    const auto expiredTimestamp = utils.TimeProvider->Now() - TDuration::Seconds(13);
    UNIT_ASSERT(utils.Storage.AddMessage(1, true, 2, expiredTimestamp));
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMessageCount(), 2);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().TotalDeletedByRetentionMessageCount, 0);

    auto expired = utils.GetMessage(1);
    UNIT_ASSERT(expired.has_value());
    UNIT_ASSERT_VALUES_EQUAL(expired->Status, TStorage::EMessageStatus::Unprocessed);
}

Y_UNIT_TEST(PurgeClearsStateAndAdvancesOffsets) {
    TUtils utils;
    utils.AddMessage(3);
    UNIT_ASSERT_VALUES_EQUAL(utils.Next(), 0);
    UNIT_ASSERT(utils.Unlock(0)); // schedule to DLQ with MaxProcessingCount=1 + MOVE
    utils.Storage.ProccessDeadlines();
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetDLQMessages().size(), 1);

    utils.Begin();
    UNIT_ASSERT(utils.Storage.Purge(10));
    utils.End();

    UNIT_ASSERT(utils.Storage.begin() == utils.Storage.end());
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetDLQMessages().size(), 0);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetLockedMessageGroupsId().size(), 0);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetFirstOffset(), 10);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetLastOffset(), 10);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetFirstUncommittedOffset(), 10);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetFirstUnlockedOffset(), 10);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().InflightMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().UnprocessedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().LockedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().DLQMessageCount, 0);
    UNIT_ASSERT_GE(utils.Storage.GetMetrics().TotalPurgedMessageCount, 1u);

    utils.CheckNoNext();

    utils.Storage.AddMessage(10, true, 3, utils.TimeProvider->Now());
    UNIT_ASSERT_VALUES_EQUAL(utils.Next(), 10);

    // Snapshot after purge must restore empty storage at the purged offset.
    {
        TUtils loaded(TStorage::TStorageSettings{.MinMessages = 1, .MaxMessages = 8, .KeepMessageOrder = true});
        loaded.LoadSnapshot(utils.EndSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(loaded.Storage.GetFirstOffset(), 10);
        UNIT_ASSERT_VALUES_EQUAL(loaded.Storage.GetLastOffset(), 10);
        UNIT_ASSERT(loaded.Storage.begin() == loaded.Storage.end());
        UNIT_ASSERT_VALUES_EQUAL(loaded.Storage.GetDLQMessages().size(), 0);
    }
}

Y_UNIT_TEST(ProccessDeadlinesVacuumIntervalSkipsWork) {
    TUtils utils;
    utils.Storage.SetMaxMessageProcessingCount(100);
    utils.Storage.SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_UNSPECIFIED);
    utils.Storage.SetRetentionPeriod(std::nullopt);
    UNIT_ASSERT(utils.Storage.AddMessage(0, true, 1, utils.TimeProvider->Now()));
    UNIT_ASSERT(utils.Storage.AddMessage(1, true, 2, utils.TimeProvider->Now()));

    UNIT_ASSERT_VALUES_EQUAL(utils.Next(TDuration::Seconds(1)), 0);
    UNIT_ASSERT_VALUES_EQUAL(utils.Next(TDuration::Seconds(1)), 1);

    utils.TimeProvider->Tick(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.ProccessDeadlines(), 2); // unlock both, arm NextVacuumRun
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().LockedMessageCount, 0);

    // Re-lock with deadline==now → DeadlineDelta=0 (already expired), but vacuum interval still blocks.
    UNIT_ASSERT_VALUES_EQUAL(utils.Next(TDuration::Zero()), 0);
    UNIT_ASSERT_VALUES_EQUAL(utils.Next(TDuration::Zero()), 1);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().LockedMessageCount, 2);

    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.ProccessDeadlines(), 0);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().LockedMessageCount, 2);

    utils.TimeProvider->Tick(TDuration::Seconds(1));
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.ProccessDeadlines(), 2);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().LockedMessageCount, 0);
}

Y_UNIT_TEST(MoveBaseDeadlineNoopOnEmpty) {
    auto timeProvider = TIntrusivePtr<MockTimeProvider>(new MockTimeProvider());
    TStorage storage(timeProvider, {});
    const auto baseDeadline = storage.GetBaseDeadline();
    const auto baseWrite = storage.GetBaseWriteTimestamp();

    storage.MoveBaseDeadline();

    UNIT_ASSERT_VALUES_EQUAL(storage.GetBaseDeadline(), baseDeadline);
    UNIT_ASSERT_VALUES_EQUAL(storage.GetBaseWriteTimestamp(), baseWrite);
    UNIT_ASSERT(storage.IsBatchEmpty());
}

Y_UNIT_TEST(CommitDelayedMessage) {
    TUtils utils;
    utils.Begin();
    UNIT_ASSERT(utils.Storage.AddMessage(0, true, 1, utils.TimeProvider->Now(), TDuration::Seconds(30)));
    {
        auto message = utils.GetMessage(0);
        UNIT_ASSERT(message.has_value());
        UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Delayed);
    }
    UNIT_ASSERT(utils.Storage.Commit(0) == EOperationResult::Success);
    utils.End();

    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().DelayedMessageCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().CommittedMessageCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(utils.Storage.GetMetrics().TotalCommittedMessageCount, 1);
    utils.CheckNoNext();
    utils.AssertLoad();
}

Y_UNIT_TEST(UnlockDelayedReturnsFailed) {
    TUtils utils;
    UNIT_ASSERT(utils.Storage.AddMessage(0, true, 1, utils.TimeProvider->Now(), TDuration::Seconds(30)));
    UNIT_ASSERT(utils.Storage.Unlock(0) == EOperationResult::Failed);
    {
        auto message = utils.GetMessage(0);
        UNIT_ASSERT(message.has_value());
        UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Delayed);
    }
}

Y_UNIT_TEST(ChangeDeadlineOnDelayedMessage) {
    TUtils utils;
    UNIT_ASSERT(utils.Storage.AddMessage(0, true, 1, utils.TimeProvider->Now(), TDuration::Seconds(30)));
    const auto newDeadline = utils.TimeProvider->Now() + TDuration::Seconds(5);
    UNIT_ASSERT(utils.Storage.ChangeMessageDeadline(0, newDeadline) == EOperationResult::Success);

    auto message = utils.GetMessage(0);
    UNIT_ASSERT(message.has_value());
    UNIT_ASSERT_VALUES_EQUAL(message->Status, TStorage::EMessageStatus::Delayed);
    UNIT_ASSERT_VALUES_EQUAL(message->ProcessingDeadline, TInstant::Seconds(newDeadline.Seconds()));
}

Y_UNIT_TEST(HasRetentionExpiredMessages) {
    TUtils utils;
    utils.Storage.AddMessage(0, true, 1, utils.TimeProvider->Now());
    UNIT_ASSERT(!utils.Storage.HasRetentionExpiredMessages());

    utils.TimeProvider->Tick(TDuration::Seconds(11));
    UNIT_ASSERT(utils.Storage.HasRetentionExpiredMessages());

    utils.Storage.Compact();
    UNIT_ASSERT(!utils.Storage.HasRetentionExpiredMessages());
    UNIT_ASSERT(utils.Storage.begin() == utils.Storage.end());
}

}

} // namespace NKikimr::NPQ::NMLP
