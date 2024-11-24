#include "query_spacetracker.h"

#include <ydb/core/protos/base.pb.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/blobstorage_disk.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

    NKikimrBlobStorage::TVDiskID GetMaxTVDiskID() {
        NKikimrBlobStorage::TVDiskID diskId;
        diskId.SetGroupID(Max<ui32>());
        diskId.SetGroupGeneration(Max<ui32>());
        diskId.SetRing(Max<ui32>());
        diskId.SetDomain(Max<ui32>());
        diskId.SetVDisk(Max<ui32>());
        return diskId;
    }

    NKikimrBlobStorage::TMessageId GetMaxTMessageId() {
        NKikimrBlobStorage::TMessageId msgId;
        msgId.SetSequenceId(Max<ui64>());
        msgId.SetMsgId(Max<ui64>());
        return msgId;
    }

    NKikimrBlobStorage::TVDiskCostSettings GetMaxTVDiskCostSettings() {
        NKikimrBlobStorage::TVDiskCostSettings costSettings;
        costSettings.SetSeekTimeUs(Max<ui64>());
        costSettings.SetReadSpeedBps(Max<ui64>());
        costSettings.SetWriteSpeedBps(Max<ui64>());
        costSettings.SetReadBlockSize(Max<ui64>());
        costSettings.SetWriteBlockSize(Max<ui64>());
        costSettings.SetMinREALHugeBlobInBytes(Max<ui32>());
        return costSettings;
    }

    NKikimrBlobStorage::TWindowFeedback GetMaxTWindowFeedback() {
        NKikimrBlobStorage::TWindowFeedback feedback;
        feedback.SetStatus(NKikimrBlobStorage::TWindowFeedback_EStatus_EStatus_MAX);
        feedback.SetActualWindowSize(Max<ui64>());
        feedback.SetMaxWindowSize(Max<ui64>());
        *feedback.MutableExpectedMsgId() = GetMaxTMessageId();
        *feedback.MutableFailedMsgId() = GetMaxTMessageId();
        return feedback;
    }

    NKikimrBlobStorage::TExecTimeStats GetMaxTExecTimeStats() {
        NKikimrBlobStorage::TExecTimeStats stats;
        stats.SetSubmitTimestamp(Max<ui64>());
        stats.SetInSenderQueue(Max<ui64>());
        stats.SetReceivedTimestamp(Max<ui64>());
        stats.SetTotal(Max<ui64>());
        stats.SetInQueue(Max<ui64>());
        stats.SetExecution(Max<ui64>());
        stats.SetHugeWriteTime(Max<ui64>());
        return stats;
    }

    NKikimrBlobStorage::TMsgQoS GetMaxTMsgQoS() {
        NKikimrBlobStorage::TMsgQoS msgQoS;
        msgQoS.SetDeadlineSeconds(Max<ui32>());
        *msgQoS.MutableMsgId() = GetMaxTMessageId();
        msgQoS.SetCost(Max<ui64>());
        msgQoS.SetExtQueueId(NKikimrBlobStorage::EVDiskQueueId_MAX);
        msgQoS.SetIntQueueId(NKikimrBlobStorage::EVDiskInternalQueueId_MAX);
        *msgQoS.MutableCostSettings() = GetMaxTVDiskCostSettings();
        msgQoS.SetSendMeCostSettings(true);
        *msgQoS.MutableWindow() = GetMaxTWindowFeedback();
        msgQoS.SetVDiskLoadId(Max<ui64>());
        *msgQoS.MutableExecTimeStats() = GetMaxTExecTimeStats();
        return msgQoS;
    }

    NKikimrBlobStorage::TTimestamps GetMaxTTimestamps() {
        NKikimrBlobStorage::TTimestamps ts;
        ts.SetSentByDSProxyUs(Max<ui64>());
        ts.SetReceivedByDSProxyUs(Max<ui64>());
        ts.SetSentByVDiskUs(Max<ui64>());
        ts.SetReceivedByVDiskUs(Max<ui64>());
        return ts;
    }

    constexpr ui64 DefaultDataSize = 200'000;
    constexpr ui64 DefaultQueryMaxCount = MaxProtobufSize / DefaultDataSize;

    NKikimrBlobStorage::TQueryResult GetMaxTQueryResult(ui64 size = DefaultDataSize) {
        static TString data;
        if (data.size() != size) {
            data.resize(size, 'a');
        }
        NKikimrBlobStorage::TQueryResult res;
        res.SetStatus(NKikimrProto::EReplyStatus_MAX);
        res.MutableBlobID(); // in only fixuint64
        res.SetShift(Max<ui64>());
        res.SetSize(Max<ui64>());
        res.SetBufferData(data);
        res.SetCookie(Max<ui64>());
        res.SetFullDataSize(Max<ui64>());
        res.SetIngress(Max<ui64>());
        return res;
    }

    Y_UNIT_TEST_SUITE(TQueryResultSizeTrackerTest) {
        Y_UNIT_TEST(CheckWithoutQueryResult) {
            TQueryResultSizeTracker resultSize;
            NKikimrBlobStorage::TEvVGetResult result;

            // check without QueryResult
            resultSize.Init();

            result.SetStatus(NKikimrProto::EReplyStatus_MAX);
            *result.MutableVDiskID() = GetMaxTVDiskID();
            result.SetCookie(Max<ui64>());
            *result.MutableMsgQoS() = GetMaxTMsgQoS();
            result.SetBlockedGeneration(Max<ui32>());
            *result.MutableTimestamps() = GetMaxTTimestamps();

            UNIT_ASSERT_LE(result.ByteSizeLong(), resultSize.GetSize());
            UNIT_ASSERT(!resultSize.IsOverflow());
        }

        Y_UNIT_TEST(CheckOnlyQueryResult) {
            TQueryResultSizeTracker resultSize;
            NKikimrBlobStorage::TEvVGetResult result;

            constexpr ui64 queryCount = (MaxProtobufSize + DefaultDataSize - 1) / DefaultDataSize;
            UNIT_ASSERT_LE(result.ByteSizeLong(), resultSize.GetSize());

            for (ui64 idx = 0; idx < queryCount; ++idx) {
                UNIT_ASSERT(!resultSize.IsOverflow());
                resultSize.AddLogoBlobIndex();
                resultSize.AddLogoBlobData(DefaultDataSize, 0, 0);
                *result.AddResult() = GetMaxTQueryResult();
                UNIT_ASSERT_LE(result.ByteSizeLong(), resultSize.GetSize());
            }

            UNIT_ASSERT(resultSize.IsOverflow());
        }

        Y_UNIT_TEST(CheckAll) {
            TQueryResultSizeTracker resultSize;
            NKikimrBlobStorage::TEvVGetResult result;

            // check without QueryResult
            resultSize.Init();

            result.SetStatus(NKikimrProto::EReplyStatus_MAX);
            *result.MutableVDiskID() = GetMaxTVDiskID();
            result.SetCookie(Max<ui64>());
            *result.MutableMsgQoS() = GetMaxTMsgQoS();
            result.SetBlockedGeneration(Max<ui32>());
            *result.MutableTimestamps() = GetMaxTTimestamps();

            constexpr ui64 queryCount = (MaxProtobufSize + DefaultDataSize - 1) / DefaultDataSize;
            UNIT_ASSERT_LE(result.ByteSizeLong(), resultSize.GetSize());

            for (ui64 idx = 0; idx < queryCount; ++idx) {
                UNIT_ASSERT(!resultSize.IsOverflow());
                resultSize.AddLogoBlobIndex();
                resultSize.AddLogoBlobData(DefaultDataSize, 0, 0);
                *result.AddResult() = GetMaxTQueryResult();
                UNIT_ASSERT_LE(result.ByteSizeLong(), resultSize.GetSize());
            }

            UNIT_ASSERT(resultSize.IsOverflow());
        }

        void MakeTestSerializeDeserialize(ui64 defaultQueryCount, ui64 lastDataSize, ui64 protobufSize) {
            NKikimrBlobStorage::TEvVGetResult result;

            result.SetStatus(NKikimrProto::EReplyStatus_MAX);
            *result.MutableVDiskID() = GetMaxTVDiskID();
            result.SetCookie(Max<ui64>());
            *result.MutableMsgQoS() = GetMaxTMsgQoS();
            result.SetBlockedGeneration(Max<ui32>());
            *result.MutableTimestamps() = GetMaxTTimestamps();

            for (ui64 idx = 0; idx < defaultQueryCount; ++idx) {
                Y_PROTOBUF_SUPPRESS_NODISCARD result.ParseFromString(result.SerializeAsString());
                *result.AddResult() = GetMaxTQueryResult();
            }
            *result.AddResult() = GetMaxTQueryResult(lastDataSize);
            UNIT_ASSERT(result.ByteSizeLong() == protobufSize);

            NKikimrBlobStorage::TEvVGetResult desResult;
            TArrayHolder<char> buffer(new char[protobufSize]);
            Y_PROTOBUF_SUPPRESS_NODISCARD result.SerializeToArray(buffer.Get(), protobufSize);
            UNIT_ASSERT(desResult.ParseFromArray(buffer.Get(), protobufSize));
        }

        Y_UNIT_TEST(SerializeDeserializeMaxPtotobufSizeMinusOne) {
            MakeTestSerializeDeserialize(DefaultQueryMaxCount, 84'775, MaxProtobufSize - 1);
        }

        Y_UNIT_TEST(SerializeDeserializeMaxPtotobufSize) {
            MakeTestSerializeDeserialize(DefaultQueryMaxCount, 84'776, MaxProtobufSize);
        }

        Y_UNIT_TEST(SerializeDeserializeMaxPtotobufSizePlusOne) {
            MakeTestSerializeDeserialize(DefaultQueryMaxCount, 84'777, MaxProtobufSize + 1);
        }
    }

} // NKikimr
