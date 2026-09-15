#include "events.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace {

constexpr ui32 EventBase = EventSpaceBegin(TKikimrEvents::ES_TX_COLUMNSHARD);

static_assert(TEvColumnShard::EvProposeTransaction == EventBase);
static_assert(TEvColumnShard::EvCancelTransactionProposal == EventBase + 1);
static_assert(TEvColumnShard::EvProposeTransactionResult == EventBase + 2);
static_assert(TEvColumnShard::EvNotifyTxCompletion == EventBase + 3);
static_assert(TEvColumnShard::EvNotifyTxCompletionResult == EventBase + 4);
static_assert(TEvColumnShard::EvReadBlobRanges == EventBase + 5);
static_assert(TEvColumnShard::EvReadBlobRangesResult == EventBase + 6);
static_assert(TEvColumnShard::EvCheckPlannedTransaction == EventBase + 7);
static_assert(TEvColumnShard::EvWrite == EventBase + 256);
static_assert(TEvColumnShard::EvOverloadReady == EventBase + 275);
static_assert(TEvColumnShard::EvOverloadUnsubscribe == EventBase + 276);

Y_UNIT_TEST_SUITE(TColumnShardPublicProtocol) {
    Y_UNIT_TEST(PreserveResultWireFormat) {
        // tx_columnshard.proto wire fields: status=1, kind=2, origin=3,
        // txId=4, minStep=5, message=7. Explicit zero MinStep and absent
        // empty StatusMessage are part of the existing constructor contract.
        const TString wire("\x08\x02\x10\x02\x18\x2a\x20\x96\x01\x28\x00", 11);
        TEvColumnShard::TEvProposeTransactionResult result(42, NKikimrTxColumnShard::TX_KIND_COMMIT, 150, NKikimrTxColumnShard::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result.Record.SerializeAsString(), wire);

        NKikimrTxColumnShard::TEvProposeTransactionResult parsed;
        UNIT_ASSERT(parsed.ParseFromString(wire));
        UNIT_ASSERT_VALUES_EQUAL(parsed.GetOrigin(), 42);
        UNIT_ASSERT_VALUES_EQUAL(parsed.GetTxId(), 150);
        UNIT_ASSERT(parsed.HasMinStep());
        UNIT_ASSERT_VALUES_EQUAL(parsed.GetMinStep(), 0);
        UNIT_ASSERT(!parsed.HasStatusMessage());

        const TString errorWire("\x08\x05\x10\x02\x18\x2a\x20\x96\x01\x28\x00\x3a\x07timeout", 20);
        TEvColumnShard::TEvProposeTransactionResult error(
            42, NKikimrTxColumnShard::TX_KIND_COMMIT, 150, NKikimrTxColumnShard::TIMEOUT, "timeout");
        UNIT_ASSERT_VALUES_EQUAL(error.Record.SerializeAsString(), errorWire);
    }

    Y_UNIT_TEST(PreserveStatusMapping) {
        using namespace NKikimrTxColumnShard;
        using TStatus = Ydb::StatusIds;
        const std::pair<EResultStatus, TStatus::StatusCode> cases[] = {
            { UNSPECIFIED, TStatus::STATUS_CODE_UNSPECIFIED },
            { PREPARED, TStatus::SUCCESS },
            { SUCCESS, TStatus::SUCCESS },
            { ABORTED, TStatus::ABORTED },
            { ERROR, TStatus::GENERIC_ERROR },
            { TIMEOUT, TStatus::TIMEOUT },
            { OUTDATED, TStatus::GENERIC_ERROR },
            { SCHEMA_ERROR, TStatus::SCHEME_ERROR },
            { SCHEMA_CHANGED, TStatus::SCHEME_ERROR },
            { OVERLOADED, TStatus::OVERLOADED },
            { STORAGE_ERROR, TStatus::UNAVAILABLE },
            { UNKNOWN, TStatus::GENERIC_ERROR },
            { static_cast<EResultStatus>(127), TStatus::GENERIC_ERROR },
        };
        for (const auto& [columnShardStatus, ydbStatus] : cases) {
            UNIT_ASSERT_VALUES_EQUAL(NColumnShard::ConvertToYdbStatus(columnShardStatus), ydbStatus);
        }
    }
}

}   // namespace
}   // namespace NKikimr
