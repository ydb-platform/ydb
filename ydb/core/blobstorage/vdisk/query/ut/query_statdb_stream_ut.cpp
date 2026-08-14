#include <ydb/core/blobstorage/vdisk/query/query_statdb_stream.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/protos/events.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace {

    TKeyLogoBlob MakeKey(ui64 tabletId, ui32 step, ui8 channel = 0) {
        return TKeyLogoBlob(TLogoBlobID(tabletId, 1, step, channel, 1, 0));
    }

    TMemRecLogoBlob MakeMemRec(ui32 dataSize) {
        TMemRecLogoBlob memRec;
        memRec.SetDiskBlob(TDiskPart(1, 0, dataSize));
        return memRec;
    }

    void AssertSingleChannelTablet(
            const NKikimrVDisk::TabletInfo& tablet,
            ui64 tabletId,
            ui64 count,
            ui64 dataSize)
    {
        UNIT_ASSERT_VALUES_EQUAL(tablet.tablet_id(), tabletId);
        UNIT_ASSERT_VALUES_EQUAL(tablet.channels_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(tablet.channels(0).count(), count);
        UNIT_ASSERT_VALUES_EQUAL(tablet.channels(0).data_size(), dataSize);
    }

    Y_UNIT_TEST_SUITE(TLogoBlobIndexStatStreamAccumulatorTest) {
        Y_UNIT_TEST(DoesNotSplitTabletsAndRetainsTotalsAcrossExtractions) {
            TLogoBlobIndexStatStreamAccumulator accumulator(1);

            // A tablet may exceed the requested batch size, but it remains the
            // current tablet and is not exposed until the next tablet starts.
            accumulator.Update(MakeKey(3, 30), MakeMemRec(10));
            accumulator.Update(MakeKey(3, 20), MakeMemRec(20));
            UNIT_ASSERT(!accumulator.IsBatchReady());

            accumulator.Update(MakeKey(2, 30), MakeMemRec(30));
            UNIT_ASSERT(accumulator.IsBatchReady());

            NKikimrVDisk::LogoBlobIndexStat batch;
            accumulator.ExtractBatch(&batch);
            UNIT_ASSERT(!accumulator.IsBatchReady());
            UNIT_ASSERT_VALUES_EQUAL(batch.tablets_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(batch.channels_size(), 0);
            AssertSingleChannelTablet(batch.tablets(0), 3, 2, 30);
            UNIT_ASSERT_VALUES_EQUAL(
                batch.tablets(0).channels(0).min_id(), MakeKey(3, 20).LogoBlobID().ToString());
            UNIT_ASSERT_VALUES_EQUAL(
                batch.tablets(0).channels(0).max_id(), MakeKey(3, 30).LogoBlobID().ToString());

            // The first record for tablet 2 was already retained as current
            // state while the previous batch was extracted.
            accumulator.Update(MakeKey(2, 20), MakeMemRec(40));
            UNIT_ASSERT(!accumulator.IsBatchReady());
            accumulator.Update(MakeKey(1, 30), MakeMemRec(50));
            UNIT_ASSERT(accumulator.IsBatchReady());

            accumulator.ExtractBatch(&batch);
            UNIT_ASSERT_VALUES_EQUAL(batch.tablets_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(batch.channels_size(), 0);
            AssertSingleChannelTablet(batch.tablets(0), 2, 2, 70);

            accumulator.Update(MakeKey(1, 20), MakeMemRec(60));
            accumulator.Finish();
            UNIT_ASSERT(accumulator.IsBatchReady());

            accumulator.ExtractBatch(&batch);
            UNIT_ASSERT_VALUES_EQUAL(batch.tablets_size(), 1);
            AssertSingleChannelTablet(batch.tablets(0), 1, 2, 110);

            // Global channel totals are emitted only by Finish, after all
            // intermediate batches have been extracted.
            UNIT_ASSERT_VALUES_EQUAL(batch.channels_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(batch.channels(0).count(), 6);
            UNIT_ASSERT_VALUES_EQUAL(batch.channels(0).data_size(), 210);
            UNIT_ASSERT_VALUES_EQUAL(
                batch.channels(0).min_id(), MakeKey(1, 20).LogoBlobID().ToString());
            UNIT_ASSERT_VALUES_EQUAL(
                batch.channels(0).max_id(), MakeKey(3, 30).LogoBlobID().ToString());
        }

        Y_UNIT_TEST(EmptyDatabaseProducesEmptyTerminalBatch) {
            TLogoBlobIndexStatStreamAccumulator accumulator(1024);
            accumulator.Finish();

            NKikimrVDisk::LogoBlobIndexStat batch;
            accumulator.ExtractBatch(&batch);
            UNIT_ASSERT_VALUES_EQUAL(batch.tablets_size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(batch.channels_size(), 0);
        }

        Y_UNIT_TEST(EmptyChannelsDoNotContainSyntheticIdBounds) {
            TLogoBlobIndexStatStreamAccumulator accumulator(1024);
            const TKeyLogoBlob key = MakeKey(42, 10, 2);
            accumulator.Update(key, MakeMemRec(17));
            accumulator.Finish();

            NKikimrVDisk::LogoBlobIndexStat batch;
            accumulator.ExtractBatch(&batch);
            UNIT_ASSERT_VALUES_EQUAL(batch.tablets_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(batch.tablets(0).channels_size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(batch.channels_size(), 3);

            for (ui32 channel = 0; channel < 2; ++channel) {
                for (const auto* channels : {
                        &batch.tablets(0).channels(),
                        &batch.channels()})
                {
                    UNIT_ASSERT_VALUES_EQUAL(channels->Get(channel).count(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(channels->Get(channel).data_size(), 0);
                    UNIT_ASSERT(channels->Get(channel).min_id().empty());
                    UNIT_ASSERT(channels->Get(channel).max_id().empty());
                }
            }

            UNIT_ASSERT_VALUES_EQUAL(batch.tablets(0).tablet_id(), 42);
            UNIT_ASSERT_VALUES_EQUAL(batch.tablets(0).channels(2).count(), 1);
            UNIT_ASSERT_VALUES_EQUAL(batch.tablets(0).channels(2).data_size(), 17);
            UNIT_ASSERT_VALUES_EQUAL(batch.tablets(0).channels(2).min_id(), key.LogoBlobID().ToString());
            UNIT_ASSERT_VALUES_EQUAL(batch.tablets(0).channels(2).max_id(), key.LogoBlobID().ToString());
            UNIT_ASSERT_VALUES_EQUAL(batch.channels(2).min_id(), key.LogoBlobID().ToString());
            UNIT_ASSERT_VALUES_EQUAL(batch.channels(2).max_id(), key.LogoBlobID().ToString());
        }
    }

    Y_UNIT_TEST_SUITE(TLogoBlobIndexStatStreamingProtocolTest) {
        Y_UNIT_TEST(DefaultsPreserveLegacySingleResponseProtocol) {
            NKikimrVDisk::GetLogoBlobIndexStatRequest request;
            UNIT_ASSERT(!request.stream());
            UNIT_ASSERT_VALUES_EQUAL(request.max_batch_bytes(), 0);

            TString requestWire;
            UNIT_ASSERT(request.SerializeToString(&requestWire));
            UNIT_ASSERT(requestWire.empty());

            // This response only uses fields that existed before streaming was
            // introduced. A streaming-aware reader must treat it as terminal.
            NKikimrVDisk::GetLogoBlobIndexStatResponse legacyResponse;
            legacyResponse.set_status("OK");
            legacyResponse.mutable_stat()->add_tablets()->set_tablet_id(42);

            TString responseWire;
            UNIT_ASSERT(legacyResponse.SerializeToString(&responseWire));

            NKikimrVDisk::GetLogoBlobIndexStatResponse parsedResponse;
            UNIT_ASSERT(parsedResponse.ParseFromString(responseWire));
            UNIT_ASSERT(!parsedResponse.has_more());
            UNIT_ASSERT_VALUES_EQUAL(parsedResponse.sequence_id(), 0);
            UNIT_ASSERT_VALUES_EQUAL(parsedResponse.stat().tablets_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(parsedResponse.stat().tablets(0).tablet_id(), 42);
        }

        Y_UNIT_TEST(ControlFieldsRoundTrip) {
            NKikimrVDisk::GetLogoBlobIndexStatRequest request;
            request.set_stream(true);
            request.set_max_batch_bytes(1 << 20);

            TString wire;
            UNIT_ASSERT(request.SerializeToString(&wire));

            NKikimrVDisk::GetLogoBlobIndexStatRequest parsedRequest;
            UNIT_ASSERT(parsedRequest.ParseFromString(wire));
            UNIT_ASSERT(parsedRequest.stream());
            UNIT_ASSERT_VALUES_EQUAL(parsedRequest.max_batch_bytes(), 1 << 20);

            NKikimrVDisk::GetLogoBlobIndexStatResponse response;
            response.set_has_more(true);
            response.set_sequence_id(7);
            UNIT_ASSERT(response.SerializeToString(&wire));

            NKikimrVDisk::GetLogoBlobIndexStatResponse parsedResponse;
            UNIT_ASSERT(parsedResponse.ParseFromString(wire));
            UNIT_ASSERT(parsedResponse.has_more());
            UNIT_ASSERT_VALUES_EQUAL(parsedResponse.sequence_id(), 7);

            TEvGetLogoBlobIndexStatResponseAck ack(7, true);
            UNIT_ASSERT(ack.Record.has_sequence_id());
            UNIT_ASSERT_VALUES_EQUAL(ack.Record.sequence_id(), 7);
            UNIT_ASSERT(ack.Record.has_cancel());
            UNIT_ASSERT(ack.Record.cancel());

            TEvGetLogoBlobIndexStatResponseAck regularAck(8);
            UNIT_ASSERT(regularAck.Record.has_sequence_id());
            UNIT_ASSERT_VALUES_EQUAL(regularAck.Record.sequence_id(), 8);
            UNIT_ASSERT(!regularAck.Record.has_cancel());
        }

        Y_UNIT_TEST(FieldNumbersAreAdditiveAndStable) {
            const auto* request = NKikimrVDisk::GetLogoBlobIndexStatRequest::descriptor();
            UNIT_ASSERT_VALUES_EQUAL(request->FindFieldByName("stream")->number(), 1);
            UNIT_ASSERT_VALUES_EQUAL(request->FindFieldByName("max_batch_bytes")->number(), 2);

            const auto* response = NKikimrVDisk::GetLogoBlobIndexStatResponse::descriptor();
            UNIT_ASSERT_VALUES_EQUAL(response->FindFieldByName("status")->number(), 1);
            UNIT_ASSERT_VALUES_EQUAL(response->FindFieldByName("stat")->number(), 2);
            UNIT_ASSERT_VALUES_EQUAL(response->FindFieldByName("has_more")->number(), 3);
            UNIT_ASSERT_VALUES_EQUAL(response->FindFieldByName("sequence_id")->number(), 4);

            const auto* ack = NKikimrVDisk::GetLogoBlobIndexStatResponseAck::descriptor();
            UNIT_ASSERT_VALUES_EQUAL(ack->FindFieldByName("sequence_id")->number(), 1);
            UNIT_ASSERT_VALUES_EQUAL(ack->FindFieldByName("cancel")->number(), 2);
        }
    }

} // anonymous namespace
} // namespace NKikimr
