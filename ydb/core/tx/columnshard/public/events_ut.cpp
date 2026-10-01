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
    Y_UNIT_TEST(ProposeConstructorsPreservePresenceAndPayload) {
        using namespace NKikimrTxColumnShard;
        const TActorId source(17, TStringBuf("protocol", 8));
        const TString body("a\0b", 3);
        for (ui32 flags : { 0u, 17u }) {
            TEvColumnShard::TEvProposeTransaction basic(TX_KIND_SCHEMA, source, 150, body, flags);
            UNIT_ASSERT_VALUES_EQUAL(basic.GetSource(), source);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(basic.Record.GetTxKind()), static_cast<ui32>(TX_KIND_SCHEMA));
            UNIT_ASSERT_VALUES_EQUAL(basic.Record.GetTxId(), 150);
            UNIT_ASSERT_VALUES_EQUAL(basic.Record.GetTxBody(), body);
            UNIT_ASSERT(basic.Record.HasFlags());
            UNIT_ASSERT_VALUES_EQUAL(basic.Record.GetFlags(), flags);
            UNIT_ASSERT(!basic.Record.HasSchemeShardId());
            UNIT_ASSERT(!basic.Record.HasSubDomainPathId());
            UNIT_ASSERT(!basic.Record.HasSeqNo());
            UNIT_ASSERT(!basic.Record.HasProcessingParams());
            UNIT_ASSERT(&Proto(&basic) == &basic.Record);

            for (ui64 schemeShard : { 0ull, 42ull }) {
                for (ui64 subDomain : { 0ull, 77ull }) {
                    TEvColumnShard::TEvProposeTransaction schema(TX_KIND_SCHEMA, schemeShard, source, 150, body, flags, subDomain);
                    UNIT_ASSERT(schema.Record.HasSchemeShardId());
                    UNIT_ASSERT_VALUES_EQUAL(schema.Record.GetSchemeShardId(), schemeShard);
                    UNIT_ASSERT_VALUES_EQUAL(schema.Record.HasSubDomainPathId(), subDomain != 0);
                    UNIT_ASSERT_VALUES_EQUAL(schema.Record.GetSubDomainPathId(), subDomain);
                    UNIT_ASSERT(!schema.Record.HasSeqNo());
                    UNIT_ASSERT(!schema.Record.HasProcessingParams());
                    UNIT_ASSERT_VALUES_EQUAL(schema.GetSource(), source);
                    UNIT_ASSERT_VALUES_EQUAL(schema.Record.GetTxBody(), body);

                    NKikimrSubDomains::TProcessingParams params;
                    params.SetVersion(9);
                    params.SetPlanResolution(11);
                    params.AddCoordinators(101);
                    params.AddCoordinators(202);
                    params.SetSchemeShard(schemeShard);
                    TEvColumnShard::TEvProposeTransaction sequenced(
                        TX_KIND_SCHEMA, schemeShard, source, 150, body, TMessageSeqNo(3, 8), params, flags, subDomain);
                    UNIT_ASSERT(sequenced.Record.HasProcessingParams());
                    UNIT_ASSERT_VALUES_EQUAL(sequenced.Record.GetProcessingParams().SerializeAsString(), params.SerializeAsString());
                    UNIT_ASSERT(sequenced.Record.HasSeqNo());
                    UNIT_ASSERT_VALUES_EQUAL(sequenced.Record.GetSeqNo().GetGeneration(), 3);
                    UNIT_ASSERT_VALUES_EQUAL(sequenced.Record.GetSeqNo().GetRound(), 8);
                    UNIT_ASSERT_VALUES_EQUAL(sequenced.GetSource(), source);
                    // Clearing the two optional extensions must restore the exact old wire message.
                    auto withoutExtensions = sequenced.Record;
                    withoutExtensions.ClearProcessingParams();
                    withoutExtensions.ClearSeqNo();
                    UNIT_ASSERT_VALUES_EQUAL(withoutExtensions.SerializeAsString(), schema.Record.SerializeAsString());
                    auto withoutSchema = schema.Record;
                    withoutSchema.ClearSchemeShardId();
                    withoutSchema.ClearSubDomainPathId();
                    UNIT_ASSERT_VALUES_EQUAL(withoutSchema.SerializeAsString(), basic.Record.SerializeAsString());
                    TEvColumnShard::TEvProposeTransaction roundtrip;
                    UNIT_ASSERT(roundtrip.Record.ParseFromString(sequenced.Record.SerializeAsString()));
                    UNIT_ASSERT_VALUES_EQUAL(roundtrip.GetSource(), source);
                    UNIT_ASSERT_VALUES_EQUAL(roundtrip.Record.SerializeAsString(), sequenced.Record.SerializeAsString());
                }
            }
        }
        NKikimrTxColumnShard::TSchemaSeqNo schemaSeqNo;
        UNIT_ASSERT(SeqNoFromProto(schemaSeqNo) == TMessageSeqNo(0, 0));
        UNIT_ASSERT(schemaSeqNo.ParseFromString(TString("\x08\x03\x10\x08", 4)));
        UNIT_ASSERT(SeqNoFromProto(schemaSeqNo) == TMessageSeqNo(3, 8));
        TEvColumnShard::TEvProposeTransaction empty;
        UNIT_ASSERT_VALUES_EQUAL(empty.Record.SerializeAsString(), TString());
    }

    Y_UNIT_TEST(PreserveCompletionAndOverloadWireContracts) {
        TEvColumnShard::TEvNotifyTxCompletion notify(150);
        UNIT_ASSERT_VALUES_EQUAL(notify.Record.SerializeAsString(), TString("\x08\x96\x01", 3));
        TEvColumnShard::TEvNotifyTxCompletionResult done(42, 150);
        UNIT_ASSERT_VALUES_EQUAL(done.Record.SerializeAsString(), TString("\x08\x2a\x10\x96\x01", 5));
        TEvColumnShard::TEvOverloadReady ready(42, 150);
        UNIT_ASSERT_VALUES_EQUAL(ready.Record.SerializeAsString(), TString("\x08\x2a\x10\x96\x01", 5));
        TEvColumnShard::TEvOverloadUnsubscribe unsubscribe(150);
        UNIT_ASSERT_VALUES_EQUAL(unsubscribe.Record.SerializeAsString(), TString("\x08\x96\x01", 3));

        const TActorId source(17, TStringBuf("protocol", 8));
        TEvColumnShard::TEvCheckPlannedTransaction planned(source, 7, 150);
        UNIT_ASSERT_VALUES_EQUAL(planned.GetSource(), source);
        UNIT_ASSERT(planned.Record.HasStep());
        UNIT_ASSERT_VALUES_EQUAL(planned.Record.GetStep(), 7);
        UNIT_ASSERT(planned.Record.HasTxId());
        UNIT_ASSERT_VALUES_EQUAL(planned.Record.GetTxId(), 150);
        UNIT_ASSERT(&Proto(&planned) == &planned.Record);
        auto withoutSource = planned.Record;
        withoutSource.ClearSource();
        UNIT_ASSERT_VALUES_EQUAL(withoutSource.SerializeAsString(), TString("\x10\x07\x18\x96\x01", 5));
        TEvColumnShard::TEvCheckPlannedTransaction restored;
        UNIT_ASSERT(restored.Record.ParseFromString(planned.Record.SerializeAsString()));
        UNIT_ASSERT_VALUES_EQUAL(restored.GetSource(), source);
    }

    Y_UNIT_TEST(PreserveResultWireFormat) {
        // tx_columnshard.proto wire fields: status=1, kind=2, origin=3,
        // txId=4, minStep=5, message=7. Explicit zero MinStep and absent
        // empty StatusMessage are part of the existing constructor contract.
        const TString wire("\x08\x02\x10\x02\x18\x2a\x20\x96\x01\x28\x00", 11);
        TEvColumnShard::TEvProposeTransactionResult result(42, NKikimrTxColumnShard::TX_KIND_COMMIT, 150, NKikimrTxColumnShard::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result.Record.SerializeAsString(), wire);
        UNIT_ASSERT(&Proto(&result) == &result.Record);

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
