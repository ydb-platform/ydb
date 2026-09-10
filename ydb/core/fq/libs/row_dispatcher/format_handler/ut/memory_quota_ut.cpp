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
        auto manager = std::make_shared<NYql::NDq::TGuaranteeQuotaManager>(100, 100);
        {
            TMemoryQuota first(manager);
            TMemoryQuota second(manager, "test buffer");
            first.Resize(60);
            UNIT_ASSERT_EXCEPTION_SATISFIES(second.Resize(50), NKikimr::TMemoryLimitExceededException,
                [](const auto& error) {
                    UNIT_ASSERT_VALUES_EQUAL(GetMemoryLimitExceededMessage(error),
                        "Row dispatcher memory limit exceeded: failed to reserve 50 bytes for test buffer (already reserved: 0 bytes)");
                    return true;
                });
            UNIT_ASSERT_VALUES_EQUAL(second.GetSize(), 0);
            UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 60);
            first.Resize(40);
            second.Resize(50);
            UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 90);
            UNIT_ASSERT_EXCEPTION_SATISFIES(second.Resize(70), NKikimr::TMemoryLimitExceededException,
                [](const auto& error) {
                    UNIT_ASSERT_VALUES_EQUAL(GetMemoryLimitExceededMessage(error, "while parsing or filtering messages"),
                        "Row dispatcher memory limit exceeded while parsing or filtering messages: failed to reserve 20 bytes for test buffer (already reserved: 50 bytes)");
                    return true;
                });
            UNIT_ASSERT_VALUES_EQUAL(second.GetSize(), 50);
            UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 90);
        }
        UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 0);
    }

    Y_UNIT_TEST(MiniKqlGrowthIsLimited) {
        auto manager = std::make_shared<NYql::NDq::TGuaranteeQuotaManager>(1_MB, 1_MB);
        {
            NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
            LimitAllocator(alloc, manager);
            UNIT_ASSERT_EXCEPTION(NKikimr::NMiniKQL::MakeStringNotFilled(2_MB), NKikimr::TMemoryLimitExceededException);
            NYql::NUdf::TUnboxedValue value = NKikimr::NMiniKQL::MakeStringNotFilled(1_KB);
            UNIT_ASSERT_GE(manager->GetCurrentQuota(), alloc.GetAllocated());
            UNIT_ASSERT_LE(manager->GetCurrentQuota(), 1_MB);
        }
        UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 0);
    }

    Y_UNIT_TEST(ZeroQuotaDoesNotDisableAllocatorLimit) {
        auto manager = std::make_shared<NYql::NDq::TGuaranteeQuotaManager>(0, 0);
        NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        UNIT_ASSERT_EXCEPTION(LimitAllocator(alloc, manager), NKikimr::TMemoryLimitExceededException);
        UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 0);
    }

    Y_UNIT_TEST_F(PackedBuffersRemainChargedWhilePayloadIsRetained, TDataPackerFixture) {
        auto manager = std::make_shared<NYql::NDq::TGuaranteeQuotaManager>(4 * PageSize, 4 * PageSize);
        auto* type = MakeRowType();
        const TString data(2 * PageSize, 'a');
        TRope payload;
        with_lock(Alloc) {
            TMemoryLimitedDataPacker packer(type, manager);
            NYql::NUdf::TUnboxedValue value = NKikimr::NMiniKQL::MakeString(data);
            packer.AddWideItem(&value, 1);
            UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 3 * PageSize);
            auto offsets = std::make_shared<TMemoryQuota>(manager);
            offsets->Resize(100);
            payload = NYql::MakeReadOnlyRope(HoldMemoryQuota(packer.Finish(), offsets));
            UNIT_ASSERT(packer.IsEmpty());
            UNIT_ASSERT_VALUES_EQUAL(packer.PackedSizeEstimate(), 0);
        }
        CheckMessageBatch(payload, TBatch().AddRow(TRow().AddString(data)));
        auto retry = payload;
        payload.clear();
        UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 3 * PageSize + 100);
        std::thread destroyPayload([retry = std::move(retry)]() mutable { retry.clear(); });
        destroyPayload.join();
        UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 0);
    }

    Y_UNIT_TEST_F(RejectedPushDiscardsBufferedRows, TDataPackerFixture) {
        auto manager = std::make_shared<NYql::NDq::TGuaranteeQuotaManager>(PageSize, PageSize);
        auto* type = MakeRowType();
        TRope payload;
        with_lock(Alloc) {
            TMemoryLimitedDataPacker packer(type, manager);
            NYql::NUdf::TUnboxedValue value = NKikimr::NMiniKQL::MakeStringNotFilled(1_KB);
            packer.AddWideItem(&value, 1);
            UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), PageSize);

            value = NKikimr::NMiniKQL::MakeStringNotFilled(PageSize);
            UNIT_ASSERT_EXCEPTION(packer.AddWideItem(&value, 1), NKikimr::TMemoryLimitExceededException);
            UNIT_ASSERT(packer.IsEmpty());
            UNIT_ASSERT_VALUES_EQUAL(packer.PackedSizeEstimate(), 0);
            UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 0);

            value = NKikimr::NMiniKQL::MakeString("after rejection");
            packer.AddWideItem(&value, 1);
            payload = NYql::MakeReadOnlyRope(packer.Finish());
        }
        CheckMessageBatch(payload, TBatch().AddRow(TRow().AddString("after rejection")));
        payload.clear();
        UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 0);
    }

    Y_UNIT_TEST_F(PackedOutputCompetesWithNewMessages, TDataPackerFixture) {
        auto manager = std::make_shared<NYql::NDq::TGuaranteeQuotaManager>(PageSize, PageSize);
        auto* type = MakeRowType();
        with_lock(Alloc) {
            TMemoryLimitedDataPacker packer(type, manager);
            NYql::NUdf::TUnboxedValue value = NKikimr::NMiniKQL::MakeStringNotFilled(1_KB);
            packer.AddWideItem(&value, 1);
            auto payload = NYql::MakeReadOnlyRope(packer.Finish());
            UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), PageSize);
            UNIT_ASSERT_EXCEPTION(packer.AddWideItem(&value, 1), NKikimr::TMemoryLimitExceededException);
            UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), PageSize);
            payload.clear();
            UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 0);
            packer.AddWideItem(&value, 1);
            UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), PageSize);
        }
        UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 0);
    }

    Y_UNIT_TEST_F(EmptyBatchHeaderIsCharged, TDataPackerFixture) {
        auto manager = std::make_shared<NYql::NDq::TGuaranteeQuotaManager>(PageSize, PageSize);
        auto* type = MakeRowType();
        with_lock(Alloc) {
            TMemoryLimitedDataPacker packer(type, manager);
            auto payload = packer.Finish();
            UNIT_ASSERT_VALUES_EQUAL(payload.Size(), sizeof(ui64));
            UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), PageSize);
            UNIT_ASSERT_EXCEPTION(packer.Finish(), NKikimr::TMemoryLimitExceededException);
            UNIT_ASSERT(packer.IsEmpty());
            UNIT_ASSERT_VALUES_EQUAL(packer.PackedSizeEstimate(), 0);
            UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), PageSize);
            payload.Clear();
            UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 0);
        }
    }

    Y_UNIT_TEST_F(ScalarCrossingPageBoundaryIsCharged, TDataPackerFixture) {
        auto manager = std::make_shared<NYql::NDq::TGuaranteeQuotaManager>(2 * PageSize, 2 * PageSize);
        auto* type = MakeRowType(true);
        // Leave four bytes on the first page: the Uint64 column needs a new one.
        const TString data(PageSize - sizeof(NKikimr::NMiniKQL::TBufferPage) - sizeof(ui64) - sizeof(ui32) - 4, 'a');
        TRope payload;
        with_lock(Alloc) {
            TMemoryLimitedDataPacker packer(type, manager);
            NYql::NUdf::TUnboxedValue row[] = {
                NKikimr::NMiniKQL::MakeString(data),
                NYql::NUdf::TUnboxedValuePod(ui64{42})
            };
            packer.AddWideItem(row, 2);
            UNIT_ASSERT_LT(packer.PackedSizeEstimate(), PageSize);
            UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 2 * PageSize);
            payload = NYql::MakeReadOnlyRope(packer.Finish());
        }
        CheckMessageBatch(payload, TBatch().AddRow(TRow().AddString(data).AddUint64(42)));
        UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 2 * PageSize);
        payload.clear();
        UNIT_ASSERT_VALUES_EQUAL(manager->GetCurrentQuota(), 0);
    }
}

} // namespace NFq::NRowDispatcher::NTests
