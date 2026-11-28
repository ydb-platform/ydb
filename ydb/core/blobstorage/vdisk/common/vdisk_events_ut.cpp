#include "vdisk_events.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TVDiskEventsToString) {

        Y_UNIT_TEST(VPutWithoutPayload) {
            TEvBlobStorage::TEvVPut event;

            UNIT_ASSERT_STRING_CONTAINS(event.ToString(), "invalid payload count# 0");
        }

        Y_UNIT_TEST(VMultiPutWithoutPayload) {
            TEvBlobStorage::TEvVMultiPut event;
            event.Record.AddItems();

            UNIT_ASSERT_STRING_CONTAINS(event.ToString(), "missing payload");
        }

        Y_UNIT_TEST(VGetWithoutQuery) {
            TEvBlobStorage::TEvVGet event;

            UNIT_ASSERT(!event.ToString().empty());
        }

        Y_UNIT_TEST(VGetWithMalformedQueries) {
            TEvBlobStorage::TEvVGet event;
            event.Record.MutableRangeQuery();
            event.Record.AddExtremeQueries();

            UNIT_ASSERT_STRING_CONTAINS(event.ToString(), "<missing>");
        }
    }

    Y_UNIT_TEST_SUITE(TVPutResultOfRequest) {

        const TLogoBlobID BlobId(1000, 2, 3, 4, 5, 6, 1);
        const TVDiskID RequestVDiskId(7, 8, 0, 1, 2);
        const TVDiskID VDiskId(7, 9, 0, 1, 2);
        const ui64 Cookie = 10;
        const ui64 IncarnationGuid = 11;
        const TInstant Now = TInstant::Seconds(1000);

        std::unique_ptr<TEvBlobStorage::TEvVPut> CreateRequest() {
            std::unique_ptr<TEvBlobStorage::TEvVPut> put = std::make_unique<TEvBlobStorage::TEvVPut>(BlobId,
                TRope(TString("12345")), RequestVDiskId, false, &Cookie, TInstant::Seconds(2000),
                NKikimrBlobStorage::TabletLog, false);
            NKikimrBlobStorage::TMsgQoS *qos = put->Record.MutableMsgQoS();
            qos->SetExtQueueId(NKikimrBlobStorage::EVDiskQueueId::PutTabletLog);
            qos->MutableMsgId()->SetMsgId(12);
            qos->MutableMsgId()->SetSequenceId(13);
            qos->SetSendMeCostSettings(true);
            qos->SetInternalMessageId(14);
            qos->MutableExecTimeStats()->SetReceivedTimestamp(15);
            put->Record.MutableTimestamps()->SetSentByDSProxyUs(16);
            return put;
        }

        std::unique_ptr<TEvBlobStorage::TEvVPutResult> CreateConsuming(TEvBlobStorage::TEvVPut &put) {
            return TEvBlobStorage::TEvVPutResult::CreateOkConsumingRequest(BlobId, VDiskId, &Cookie,
                TOutOfSpaceStatus(0, 0.5), Now, put.GetCachedByteSize(), put.Record, nullptr, nullptr, nullptr,
                put.GetBufferBytes(), IncarnationGuid);
        }

        std::unique_ptr<TEvBlobStorage::TEvVPutResult> CreateOrdinary(TEvBlobStorage::TEvVPut &put,
                NKikimrProto::EReplyStatus status) {
            return std::make_unique<TEvBlobStorage::TEvVPutResult>(status, BlobId, VDiskId, &Cookie,
                TOutOfSpaceStatus(0, 0.5), Now, put.GetCachedByteSize(), &put.Record, nullptr, nullptr, nullptr,
                put.GetBufferBytes(), IncarnationGuid, TString("reason"));
        }

        void CheckResult(const NKikimrBlobStorage::TEvVPutResult &record) {
            UNIT_ASSERT_VALUES_EQUAL(LogoBlobIDFromLogoBlobID(record.GetBlobID()), BlobId);
            UNIT_ASSERT_VALUES_EQUAL(VDiskIDFromVDiskID(record.GetVDiskID()), VDiskId);
            UNIT_ASSERT_VALUES_EQUAL(record.GetCookie(), Cookie);
            UNIT_ASSERT_VALUES_EQUAL(record.GetTimestamps().GetSentByDSProxyUs(), 16);
            const NKikimrBlobStorage::TMsgQoS &qos = record.GetMsgQoS();
            UNIT_ASSERT_EQUAL(qos.GetExtQueueId(), NKikimrBlobStorage::EVDiskQueueId::PutTabletLog);
            UNIT_ASSERT_VALUES_EQUAL(qos.GetMsgId().GetMsgId(), 12);
            UNIT_ASSERT_VALUES_EQUAL(qos.GetMsgId().GetSequenceId(), 13);
            UNIT_ASSERT_VALUES_EQUAL(qos.GetExecTimeStats().GetReceivedTimestamp(), 15);
            UNIT_ASSERT(!qos.HasDeadlineSeconds());
            UNIT_ASSERT(!qos.HasSendMeCostSettings());
            UNIT_ASSERT(!qos.HasInternalMessageId());
        }

        NKikimrBlobStorage::TEvVPutResult SerializeAndParse(const TEvBlobStorage::TEvVPutResult &result) {
            TString data;
            UNIT_ASSERT(result.Record.SerializeToString(&data));
            NKikimrBlobStorage::TEvVPutResult parsed;
            UNIT_ASSERT(parsed.ParseFromString(data));
            return parsed;
        }

        Y_UNIT_TEST(ConsumingResultOutlivesRequest) {
            std::unique_ptr<TEvBlobStorage::TEvVPut> put = CreateRequest();
            std::unique_ptr<TEvBlobStorage::TEvVPutResult> result = CreateConsuming(*put);
            put.reset();

            UNIT_ASSERT_EQUAL(result->Record.GetStatus(), NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetIncarnationGuid(), IncarnationGuid);
            CheckResult(result->Record);
            CheckResult(SerializeAndParse(*result));
            UNIT_ASSERT_STRING_CONTAINS(result->ToString(), BlobId.ToString());
        }

        Y_UNIT_TEST(ConsumingResultBecomesErrorAfterRequest) {
            // a huge put gets its result before the data is written, and the write may fail
            std::unique_ptr<TEvBlobStorage::TEvVPut> put = CreateRequest();
            std::unique_ptr<TEvBlobStorage::TEvVPutResult> result = CreateConsuming(*put);
            put.reset();
            result->UpdateStatus(NKikimrProto::OUT_OF_SPACE, "huge chunk reservation refused");

            const NKikimrBlobStorage::TEvVPutResult parsed = SerializeAndParse(*result);
            UNIT_ASSERT_EQUAL(parsed.GetStatus(), NKikimrProto::OUT_OF_SPACE);
            UNIT_ASSERT_VALUES_EQUAL(parsed.GetErrorReason(), "huge chunk reservation refused");
            CheckResult(parsed);
        }

        Y_UNIT_TEST(ConsumingResultTakesOnlyDocumentedFields) {
            std::unique_ptr<TEvBlobStorage::TEvVPut> put = CreateRequest();
            NKikimrBlobStorage::TEvVPut expected = put->Record;
            expected.ClearMsgQoS();
            expected.ClearBlobID();
            expected.ClearVDiskID();
            expected.ClearTimestamps();

            CreateConsuming(*put);
            UNIT_ASSERT_VALUES_EQUAL(put->Record.ShortDebugString(), expected.ShortDebugString());
        }

        Y_UNIT_TEST(OrdinaryResultKeepsRequestExceptMsgQoS) {
            for (const NKikimrProto::EReplyStatus status : {NKikimrProto::OK, NKikimrProto::RACE,
                    NKikimrProto::OUT_OF_SPACE}) {
                std::unique_ptr<TEvBlobStorage::TEvVPut> put = CreateRequest();
                NKikimrBlobStorage::TEvVPut expected = put->Record;
                expected.MutableMsgQoS()->Clear();

                std::unique_ptr<TEvBlobStorage::TEvVPutResult> result = CreateOrdinary(*put, status);
                UNIT_ASSERT_EQUAL(result->Record.GetStatus(), status);
                CheckResult(result->Record);
                UNIT_ASSERT_VALUES_EQUAL(put->Record.ShortDebugString(), expected.ShortDebugString());
            }
        }
    }

} // NKikimr
