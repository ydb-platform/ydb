#include <ydb/library/yql/dq/runtime/dq_input_channel.h>
#include <ydb/library/yql/dq/runtime/dq_input_producer.h>
#include <ydb/library/yql/dq/runtime/dq_input_ready.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

#include <deque>
#include <thread>

using namespace NKikimr;
using namespace NMiniKQL;
using namespace NYql;
using namespace NYql::NDq;

namespace {

// An input channel of i32 rows. With readiness it marks its slot as IDqInput::BindReadySet asks: on a push, on
// the finish, on a resume, and when Pop returns false while it holds more (an empty chunk, as a finish chunk is)
class TFakeInput : public IDqInputChannel {
public:
    TFakeInput(TType* type, ui64 channelId, bool readiness)
        : Type(type), ChannelId(channelId), Readiness(readiness)
    {}

    void PushRow(i32 value) {
        Items.push_back(value);
        Hook.Mark();
    }

    // a chunk without rows: Pop returns false for it and the input marks itself, as it may hold more
    void PushEmptyChunk() {
        Items.push_back(std::nullopt);
        Hook.Mark();
    }

    void FinishInput() {
        Finished = true;
        Hook.Mark();
    }

    // IDqInput
    const TDqInputStats& GetPopStats() const override { return PopStats; }
    i64 GetFreeSpace() const override { return 0; }
    ui64 GetStoredBytes() const override { return 0; }
    bool Empty() const override { return Items.empty(); }

    bool Pop(TUnboxedValueBatch& batch, TMaybe<TInstant>&) override {
        ++PopCalls;
        if (Paused || Items.empty()) {
            return false;
        }
        auto item = Items.front();
        Items.pop_front();
        if (!item) {
            Hook.Mark();
            return false;
        }
        batch.emplace_back(NUdf::TUnboxedValuePod(*item));
        return true;
    }

    bool IsFinished() const override { return Finished && Items.empty(); }
    TType* GetInputType() const override { return Type; }

    void PauseByCheckpoint() override { Paused = true; }
    void ResumeByCheckpoint() override {
        Paused = false;
        Hook.Mark();
    }
    bool IsPausedByCheckpoint() const override { return Paused; }

    bool BindReadySet(const std::shared_ptr<TDqInputReadySet>& set, ui32 slot) override {
        if (!Readiness) {
            return false;
        }
        Hook = TDqInputReadyHook{set, slot};
        Hook.Mark();
        return true;
    }

    // IDqInputChannel
    ui64 GetChannelId() const override { return ChannelId; }
    const TDqInputChannelStats& GetPushStats() const override { return PushStats; }
    void Push(TDqSerializedBatch&&) override { Y_ABORT(); }
    void Push(TInstant) override { Y_ABORT(); }
    void Finish() override { FinishInput(); }
    void Bind(NActors::TActorId, NActors::TActorId) override {}
    bool IsLocal() const override { return true; }

    ui64 PopCalls = 0;

private:
    TType* Type;
    const ui64 ChannelId;
    const bool Readiness;
    TDqInputReadyHook Hook;
    std::deque<std::optional<i32>> Items;
    bool Finished = false;
    bool Paused = false;
    TDqInputStats PopStats;
    TDqInputChannelStats PushStats;
};

struct TUnionTest {
    TScopedAlloc Alloc;
    TTypeEnvironment TypeEnv;
    TMemoryUsageInfo MemInfo;
    THolderFactory HolderFactory;
    TType* RowType;
    TInstant StartTs;
    ui64 InputsConsumed = 0;
    TVector<TIntrusivePtr<TFakeInput>> Inputs;
    NUdf::TUnboxedValue Union;

    // `readiness` per input: true supports TDqInputReadySet; `useReadySet` is what the task runner asks for
    explicit TUnionTest(const std::vector<bool>& readiness, bool useReadySet = true)
        : Alloc(__LOCATION__)
        , TypeEnv(Alloc)
        , MemInfo("Mem")
        , HolderFactory(Alloc.Ref(), MemInfo)
        , RowType(TDataType::Create(NUdf::TDataType<i32>::Id, TypeEnv))
    {
        TVector<IDqInput::TPtr> inputs;
        for (size_t i = 0; i < readiness.size(); ++i) {
            Inputs.push_back(MakeIntrusive<TFakeInput>(RowType, i + 1, readiness[i]));
            inputs.push_back(Inputs.back());
        }
        Union = CreateInputUnionValue(RowType, std::move(inputs), HolderFactory, {}, StartTs, InputsConsumed, nullptr, nullptr, useReadySet);
    }

    ~TUnionTest() {
        Union = {};
        Inputs.clear();
    }

    // fetches until the union yields or finishes: the rows, and whether it finished
    std::pair<std::multiset<i32>, bool> Drain() {
        std::multiset<i32> rows;
        for (;;) {
            NUdf::TUnboxedValue row;
            auto status = Union.Fetch(row);
            if (status == NUdf::EFetchStatus::Ok) {
                rows.insert(row.Get<i32>());
                continue;
            }
            return {rows, status == NUdf::EFetchStatus::Finish};
        }
    }

    ui64 PopCalls(size_t input) const {
        return Inputs[input]->PopCalls;
    }
};

} // namespace

Y_UNIT_TEST_SUITE(DqInputReadySet) {

    Y_UNIT_TEST(ConsumerApi) {
        TDqInputReadySet set(4);

        set.Collect();
        UNIT_ASSERT_VALUES_EQUAL(set.ReadyCount(), 0);

        // marked twice, queued once
        set.Mark(2);
        set.Mark(2);
        set.Mark(0);
        set.Collect();
        UNIT_ASSERT_VALUES_EQUAL(set.ReadyCount(), 2);

        // had data: back to the tail; marked while taken: not queued a 2nd time
        auto first = set.Next();
        UNIT_ASSERT_VALUES_EQUAL(first, 2);
        set.Mark(first);
        set.Keep(first);
        set.Collect();
        UNIT_ASSERT_VALUES_EQUAL(set.ReadyCount(), 2);

        // found empty: out until marked again
        auto second = set.Next();
        UNIT_ASSERT_VALUES_EQUAL(second, 0);
        set.Release(second);
        UNIT_ASSERT_VALUES_EQUAL(set.ReadyCount(), 1);
        set.Mark(second);
        set.Collect();
        UNIT_ASSERT_VALUES_EQUAL(set.ReadyCount(), 2);

        // finished: its marks are ignored
        auto third = set.Next();
        UNIT_ASSERT_VALUES_EQUAL(third, 2);
        set.Retire(third);
        set.Mark(third);
        set.Collect();
        UNIT_ASSERT_VALUES_EQUAL(set.ReadyCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(set.Next(), 0);
    }

    // Producers make an item visible and then mark its slot, the consumer visits what the set hands out, one item
    // per visit as a union pops a chunk: no item may be left behind with its slot neither marked nor ready
    Y_UNIT_TEST(NoLostMarkUnderRace) {
        constexpr ui32 slotCount = 8;
        constexpr ui32 producerCount = 4;
        constexpr ui64 itemsPerProducer = 200000;

        TDqInputReadySet set(slotCount);
        std::vector<std::atomic<ui64>> available(slotCount);
        std::atomic<ui32> producersDone = 0;

        std::vector<std::thread> producers;
        for (ui32 p = 0; p < producerCount; ++p) {
            producers.emplace_back([&, p]() {
                for (ui64 i = 0; i < itemsPerProducer; ++i) {
                    auto slot = (p + i) % slotCount;
                    available[slot]++;
                    set.Mark(slot);
                }
                producersDone++;
            });
        }

        const ui64 total = producerCount * itemsPerProducer;
        ui64 consumed = 0;
        TInstant stuckSince;
        while (consumed < total) {
            set.Collect();
            if (set.ReadyCount() == 0) {
                if (producersDone.load() == producerCount) {
                    // every mark is in by now: nothing ready means an item was left behind
                    if (!stuckSince) {
                        stuckSince = TInstant::Now();
                    }
                    UNIT_ASSERT_C(TInstant::Now() - stuckSince < TDuration::Seconds(5),
                        TStringBuilder() << "consumed " << consumed << " of " << total << " and nothing is ready");
                }
                continue;
            }
            stuckSince = TInstant::Zero();
            auto slot = set.Next();
            auto value = available[slot].load();
            if (value > 0 && available[slot].compare_exchange_strong(value, value - 1)) {
                consumed++;
                set.Keep(slot);
            } else if (value > 0) {
                set.Keep(slot);
            } else {
                set.Release(slot);
            }
        }

        for (auto& producer : producers) {
            producer.join();
        }
        UNIT_ASSERT_VALUES_EQUAL(consumed, total);
    }
}

Y_UNIT_TEST_SUITE(DqInputUnionReadiness) {

    // the point: a notified input which has nothing is not polled again until it marks itself
    Y_UNIT_TEST(IdleInputsNotPolled) {
        TUnionTest test({true, true, true});

        auto [rows, finished] = test.Drain();
        UNIT_ASSERT(rows.empty());
        UNIT_ASSERT(!finished);
        auto idlePops = test.PopCalls(2);

        for (i32 i = 0; i < 100; ++i) {
            test.Inputs[0]->PushRow(i);
            test.Inputs[1]->PushRow(1000 + i);
            auto [got, finished] = test.Drain();
            UNIT_ASSERT_VALUES_EQUAL(got.size(), 2);
            UNIT_ASSERT(got.contains(i));
            UNIT_ASSERT(got.contains(1000 + i));
        }
        UNIT_ASSERT_VALUES_EQUAL_C(test.PopCalls(2), idlePops, "an idle notified input was polled");
    }

    Y_UNIT_TEST(DeliverAllAndFinish) {
        TUnionTest test({true, true, true, true});

        std::multiset<i32> expected;
        for (i32 i = 0; i < 50; ++i) {
            test.Inputs[i % 4]->PushRow(i);
            expected.insert(i);
        }
        auto [rows, finished] = test.Drain();
        UNIT_ASSERT(rows == expected);
        UNIT_ASSERT(!finished);

        for (size_t i = 0; i < 3; ++i) {
            test.Inputs[i]->FinishInput();
        }
        std::tie(rows, finished) = test.Drain();
        UNIT_ASSERT(rows.empty());
        UNIT_ASSERT_C(!finished, "finished with an input alive");

        test.Inputs[3]->PushRow(7);
        test.Inputs[3]->FinishInput();
        std::tie(rows, finished) = test.Drain();
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
        UNIT_ASSERT(finished);
    }

    // busy inputs share the fetches
    Y_UNIT_TEST(Fairness) {
        TUnionTest test({true, true, true});
        for (i32 i = 0; i < 30; ++i) {
            test.Inputs[0]->PushRow(i);
            test.Inputs[1]->PushRow(100 + i);
            test.Inputs[2]->PushRow(200 + i);
        }
        std::array<ui32, 3> counts = {};
        for (int i = 0; i < 30; ++i) {
            NUdf::TUnboxedValue row;
            UNIT_ASSERT(test.Union.Fetch(row) == NUdf::EFetchStatus::Ok);
            counts[row.Get<i32>() / 100]++;
        }
        for (auto count : counts) {
            UNIT_ASSERT_C(count >= 9, "counts " << counts[0] << ", " << counts[1] << ", " << counts[2]);
        }
    }

    // an input which does not support the ready set leaves the whole union polled: every input is looked at
    Y_UNIT_TEST(FallbackToPolling) {
        TUnionTest test({true, false, true});
        test.Drain();
        auto idlePops = test.PopCalls(2);
        test.Inputs[1]->PushRow(1);
        auto [rows, finished] = test.Drain();
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
        UNIT_ASSERT_C(test.PopCalls(2) > idlePops, "an idle input was not polled in a polled union");
        for (auto& input : test.Inputs) {
            input->FinishInput();
        }
        std::tie(rows, finished) = test.Drain();
        UNIT_ASSERT(finished);
    }

    // Pop returned false while the input held more: the input marks itself and the union comes back for the rest
    Y_UNIT_TEST(EmptyChunkThenData) {
        TUnionTest test({true});
        test.Inputs[0]->PushEmptyChunk();
        test.Inputs[0]->PushRow(1);
        test.Inputs[0]->PushRow(2);

        std::multiset<i32> rows;
        for (int round = 0; round < 3 && rows.size() < 2; ++round) {
            auto [got, finished] = test.Drain();
            rows.insert(got.begin(), got.end());
        }
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 2);
    }

    Y_UNIT_TEST(ResumeAfterCheckpoint) {
        TUnionTest test({true, true});
        test.Inputs[0]->PauseByCheckpoint();
        test.Inputs[0]->PushRow(1);
        test.Inputs[1]->PushRow(2);

        auto [rows, finished] = test.Drain();
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
        UNIT_ASSERT(rows.contains(2));

        test.Inputs[0]->ResumeByCheckpoint();
        std::tie(rows, finished) = test.Drain();
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
        UNIT_ASSERT(rows.contains(1));
    }

    // the task runner does not ask for the ready set: the inputs are polled as before, though they support it
    Y_UNIT_TEST(PolledOnly) {
        TUnionTest test({true, true}, false);
        test.Drain();
        auto idlePops = test.PopCalls(1);
        test.Inputs[0]->PushRow(1);
        auto [rows, finished] = test.Drain();
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
        UNIT_ASSERT_C(test.PopCalls(1) > idlePops, "an idle input was not polled in a polled union");
        test.Inputs[0]->FinishInput();
        test.Inputs[1]->FinishInput();
        std::tie(rows, finished) = test.Drain();
        UNIT_ASSERT(finished);
    }
}
