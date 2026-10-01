#include <ydb/core/fq/libs/row_dispatcher/format_handler/data_packer.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/ut/common/ut_common.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/common/rope_over_buffer.h>
#include <yql/essentials/minikql/mkql_string_util.h>

#include <library/cpp/testing/unittest/registar.h>

#include <thread>

namespace NFq::NRowDispatcher::NTests {

namespace {

constexpr size_t PageSize = NKikimr::NMiniKQL::TBufferPage::DefaultPageAllocSize;

class TCountingQuotaManager : public NYql::NDq::TGuaranteeQuotaManager {
public:
    using TGuaranteeQuotaManager::TGuaranteeQuotaManager;

    bool AllocateQuota(ui64 size, bool isOptional) override {
        ++Requests;
        return TGuaranteeQuotaManager::AllocateQuota(size, isOptional);
    }

    void FreeQuota(ui64 size) override {
        ++Releases;
        TGuaranteeQuotaManager::FreeQuota(size);
    }

    ui64 Requests = 0;
    ui64 Releases = 0;
};

class TDataPackerFixture : public TBaseFixture {
public:
    const NKikimr::NMiniKQL::TType* MakeRowType(bool withUint64 = false) {
        TVector<NKikimr::NMiniKQL::TType* const> columns{CheckSuccess(ParseTypeYson("[DataType; String]"))};
        if (withUint64) {
            columns.push_back(CheckSuccess(ParseTypeYson("[DataType; Uint64]")));
        }
        with_lock(Alloc) {
            return ProgramBuilder->NewMultiType(columns);
        }
    }
};

} // namespace

Y_UNIT_TEST_SUITE(RowDispatcherMemoryQuota) {
    Y_UNIT_TEST(ReservationFailureAndRelease) {
        auto manager = std::make_shared<TCountingQuotaManager>(2_MB, 2_MB);
        {
            TMemoryQuota second(manager, "test buffer");
            second.Resize(1);
            {
                TMemoryQuota first(manager);
                first.Resize(1_MB);
                first.Resize(0);
                UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 2_MB);
                UNIT_ASSERT_EXCEPTION_SATISFIES(second.Resize(1_MB + 1), NKikimr::TMemoryLimitExceededException,
                    [](const auto& error) {
                        UNIT_ASSERT_VALUES_EQUAL(GetMemoryLimitExceededMessage(error),
                            "Row dispatcher memory limit exceeded: failed to reserve 1048576 bytes for test buffer (already reserved: 1048576 bytes, actually used bytes 1)");
                        return true;
                    });
                UNIT_ASSERT_VALUES_EQUAL(second.GetSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(manager->Releases, 0);
            }
            second.Add(1_MB);
            UNIT_ASSERT_VALUES_EQUAL(second.GetSize(), 1_MB + 1);
            UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 2_MB);
        }
        UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 0);
        UNIT_ASSERT_VALUES_EQUAL(manager->Releases, 2);
    }

    Y_UNIT_TEST(RepeatedResizeAndAddReuseReservation) {
        auto manager = std::make_shared<TCountingQuotaManager>(2_MB, 2_MB);
        {
            TMemoryQuota memory(manager);
            memory.Resize(1_MB + 1);
            for (size_t i = 0; i < 1000; ++i) {
                memory.Resize(0);
                memory.Add(17);
                memory.Reserve(10_KB);
                memory.Reserve(1);
                memory.Add(1);
                UNIT_ASSERT_VALUES_EQUAL(memory.GetSize(), 10_KB + 1);
            }
            UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 2_MB);
            UNIT_ASSERT_VALUES_EQUAL(manager->Requests, 1);
            UNIT_ASSERT_VALUES_EQUAL(manager->Releases, 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 0);
        UNIT_ASSERT_VALUES_EQUAL(manager->Releases, 1);
    }

    Y_UNIT_TEST(NamedCountersTrackRetainedReservations) {
        auto manager = std::make_shared<TCountingQuotaManager>(4_MB, 4_MB);
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        {
            TMemoryQuota first(manager, "FirstMemory", counters);
            auto second = std::make_unique<TMemoryQuota>(manager, "FirstMemory", counters);
            TMemoryQuota other(manager, "OtherMemory", counters);
            const auto group = counters->FindSubgroup("component", "MemoryQuota");
            UNIT_ASSERT(group);
            const auto firstCounter = group->FindCounter("FirstMemory");
            const auto otherCounter = group->FindCounter("OtherMemory");
            UNIT_ASSERT(firstCounter && otherCounter);

            first.Resize(1);
            second->Resize(1_MB + 1);
            other.Resize(1);
            UNIT_ASSERT_VALUES_EQUAL(firstCounter->Val(), 3_MB);
            UNIT_ASSERT_VALUES_EQUAL(otherCounter->Val(), 1_MB);
            first.Resize(0);
            second->Resize(0);
            first.Reserve(1_MB);
            UNIT_ASSERT_VALUES_EQUAL(firstCounter->Val(), 3_MB);
            UNIT_ASSERT_VALUES_EQUAL(manager->Requests, 3);
            UNIT_ASSERT_VALUES_EQUAL(manager->Releases, 0);

            UNIT_ASSERT_EXCEPTION(other.Resize(1_MB + 1), NKikimr::TMemoryLimitExceededException);
            UNIT_ASSERT_VALUES_EQUAL(otherCounter->Val(), 1_MB);
            UNIT_ASSERT_VALUES_EQUAL(firstCounter->Val() + otherCounter->Val(), manager->GetCurrentQuota());

            std::thread destroyQuota([quota = std::move(second)]() mutable { quota.reset(); });
            destroyQuota.join();
            UNIT_ASSERT_VALUES_EQUAL(firstCounter->Val(), 1_MB);
            UNIT_ASSERT_VALUES_EQUAL(otherCounter->Val(), 1_MB);
        }
        const auto group = counters->FindSubgroup("component", "MemoryQuota");
        UNIT_ASSERT_VALUES_EQUAL(group->FindCounter("FirstMemory")->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(group->FindCounter("OtherMemory")->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 0);
    }

    Y_UNIT_TEST(MemoryWithoutManagerIsReported) {
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        {
            TMemoryQuota memory({}, "TestMemory", counters);
            memory.Resize(1_MB + 1);
            const auto sensor = counters->GetSubgroup("component", "MemoryQuota")->FindCounter("TestMemory");
            UNIT_ASSERT(sensor);
            UNIT_ASSERT_VALUES_EQUAL(sensor->Val(), 2_MB);
            memory.Resize(0);
            UNIT_ASSERT_VALUES_EQUAL(sensor->Val(), 2_MB);
        }
        UNIT_ASSERT_VALUES_EQUAL(counters->GetSubgroup("component", "MemoryQuota")->GetCounter("TestMemory")->Val(), 0);
    }

    Y_UNIT_TEST(MiniKqlWithoutManagerReportsGrowth) {
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        {
            NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
            LimitAllocator(alloc, {}, "TestAlloc", counters);
            const auto sensor = counters->GetSubgroup("component", "MemoryQuota")->FindCounter("TestAlloc");
            UNIT_ASSERT(sensor);
            UNIT_ASSERT_VALUES_EQUAL(sensor->Val(), 1_MB);
            NYql::NUdf::TUnboxedValue value = NKikimr::NMiniKQL::MakeStringNotFilled(2_MB);
            UNIT_ASSERT_GE(sensor->Val(), static_cast<i64>(2_MB));
            const auto reservation = sensor->Val();
            value = {};
            UNIT_ASSERT_VALUES_EQUAL(sensor->Val(), reservation);
        }
        UNIT_ASSERT_VALUES_EQUAL(counters->GetSubgroup("component", "MemoryQuota")->GetCounter("TestAlloc")->Val(), 0);
    }

    Y_UNIT_TEST(MiniKqlGrowthIsLimited) {
        auto manager = std::make_shared<NYql::NDq::TGuaranteeQuotaManager>(1_MB, 1_MB);
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        {
            NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
            LimitAllocator(alloc, manager, "TestAlloc", counters);
            UNIT_ASSERT_EXCEPTION(NKikimr::NMiniKQL::MakeStringNotFilled(2_MB), NKikimr::TMemoryLimitExceededException);
            NYql::NUdf::TUnboxedValue value = NKikimr::NMiniKQL::MakeStringNotFilled(1_KB);
            // MKQL may preallocate spare pages outside the checked allocation size.
            UNIT_ASSERT_GE(manager->GetCurrentQuota(), alloc.GetUsed());
            UNIT_ASSERT_LE(manager->GetCurrentQuota(), 1_MB);
            UNIT_ASSERT_VALUES_EQUAL(counters->GetSubgroup("component", "MemoryQuota")->GetCounter("TestAlloc")->Val(), 1_MB);
        }
        UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters->GetSubgroup("component", "MemoryQuota")->GetCounter("TestAlloc")->Val(), 0);
    }

    Y_UNIT_TEST(ZeroQuotaDoesNotDisableAllocatorLimit) {
        auto manager = std::make_shared<NYql::NDq::TGuaranteeQuotaManager>(0, 0);
        NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        UNIT_ASSERT_EXCEPTION(LimitAllocator(alloc, manager), NKikimr::TMemoryLimitExceededException);
        UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 0);
    }

    Y_UNIT_TEST_F(RepeatedBatchesReusePackingQuota, TDataPackerFixture) {
        auto manager = std::make_shared<TCountingQuotaManager>(1_MB, 1_MB);
        const auto* type = MakeRowType();
        const TString data(2 * PageSize, 'a');
        TRope payload;
        with_lock(Alloc) {
            TMemoryLimitedDataPacker packer(manager, sizeof(ui64));
            packer.SetPackerType(type);
            NYql::NUdf::TUnboxedValue value = NKikimr::NMiniKQL::MakeString(data);
            for (size_t i = 0; i < 100; ++i) {
                packer.AddWideItem(&value, 1);
                auto [buffer, size] = packer.Finish();
                UNIT_ASSERT_VALUES_EQUAL(size, 3 * PageSize + sizeof(ui64));
                payload = NYql::MakeReadOnlyRope(std::move(buffer));
                UNIT_ASSERT(packer.IsEmpty());
                UNIT_ASSERT_VALUES_EQUAL(packer.PackedSizeEstimate(), 0);
                std::thread destroyPayload([copy = payload]() mutable { copy.clear(); });
                destroyPayload.join();
            }
            UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 1_MB);
            UNIT_ASSERT_VALUES_EQUAL(manager->Requests, 1);
            UNIT_ASSERT_VALUES_EQUAL(manager->Releases, 0);
        }
        CheckMessageBatch(payload, TBatch().AddRow(TRow().AddString(data)));
        UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 0);
        UNIT_ASSERT_VALUES_EQUAL(manager->Releases, 1);
    }

    Y_UNIT_TEST_F(RejectedPushDiscardsRowsAndRetainsReservation, TDataPackerFixture) {
        auto manager = std::make_shared<TCountingQuotaManager>(1_MB, 1_MB);
        const auto* type = MakeRowType();
        TRope payload;
        with_lock(Alloc) {
            TMemoryLimitedDataPacker packer(manager, sizeof(ui64));
            packer.SetPackerType(type);
            NYql::NUdf::TUnboxedValue value = NKikimr::NMiniKQL::MakeStringNotFilled(1_KB);
            packer.AddWideItem(&value, 1);
            value = NKikimr::NMiniKQL::MakeStringNotFilled(2_MB);
            UNIT_ASSERT_EXCEPTION(packer.AddWideItem(&value, 1), NKikimr::TMemoryLimitExceededException);
            UNIT_ASSERT(packer.IsEmpty());
            UNIT_ASSERT_VALUES_EQUAL(packer.PackedSizeEstimate(), 0);
            UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 1_MB);
            UNIT_ASSERT_VALUES_EQUAL(manager->Releases, 0);

            value = NKikimr::NMiniKQL::MakeString("after rejection");
            packer.AddWideItem(&value, 1);
            auto [buffer, size] = packer.Finish();
            UNIT_ASSERT_VALUES_EQUAL(size, PageSize + sizeof(ui64));
            payload = NYql::MakeReadOnlyRope(std::move(buffer));
        }
        CheckMessageBatch(payload, TBatch().AddRow(TRow().AddString("after rejection")));
        UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 0);
    }

    Y_UNIT_TEST_F(EmptyBatchHeaderIsCharged, TDataPackerFixture) {
        auto manager = std::make_shared<TCountingQuotaManager>(1_MB, 1_MB);
        const auto* type = MakeRowType();
        with_lock(Alloc) {
            TMemoryLimitedDataPacker packer(manager, sizeof(ui64));
            packer.SetPackerType(type);
            for (size_t i = 0; i < 10; ++i) {
                auto [buffer, size] = packer.Finish();
                UNIT_ASSERT_VALUES_EQUAL(buffer.Size(), sizeof(ui64));
                UNIT_ASSERT_VALUES_EQUAL(size, PageSize);
            }
            UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 1_MB);
            UNIT_ASSERT_VALUES_EQUAL(manager->Requests, 1);
            UNIT_ASSERT_VALUES_EQUAL(manager->Releases, 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 0);
    }

    Y_UNIT_TEST_F(ScalarCrossingPageBoundaryIsCharged, TDataPackerFixture) {
        auto manager = std::make_shared<NYql::NDq::TGuaranteeQuotaManager>(1_MB, 1_MB);
        const auto* type = MakeRowType(true);
        // Leave four bytes on the first page: the Uint64 column needs a new one.
        const TString data(PageSize - sizeof(NKikimr::NMiniKQL::TBufferPage) - sizeof(ui64) - sizeof(ui32) - 4, 'a');
        TRope payload;
        with_lock(Alloc) {
            TMemoryLimitedDataPacker packer(manager, sizeof(ui64));
            packer.SetPackerType(type);
            NYql::NUdf::TUnboxedValue row[] = {
                NKikimr::NMiniKQL::MakeString(data),
                NYql::NUdf::TUnboxedValuePod(ui64{42})
            };
            packer.AddWideItem(row, 2);
            UNIT_ASSERT_LT(packer.PackedSizeEstimate(), PageSize);
            auto [buffer, size] = packer.Finish();
            UNIT_ASSERT_VALUES_EQUAL(size, 2 * PageSize + sizeof(ui64));
            payload = NYql::MakeReadOnlyRope(std::move(buffer));
        }
        CheckMessageBatch(payload, TBatch().AddRow(TRow().AddString(data).AddUint64(42)));
        UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 0);
    }
}

} // namespace NFq::NRowDispatcher::NTests
