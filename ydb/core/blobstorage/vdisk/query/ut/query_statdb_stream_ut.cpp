#include <ydb/core/blobstorage/vdisk/query/query_statdb_stream.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/protos/events.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace {

    TKeyLogoBlob MakeKey(ui64 tabletId, ui32 step) {
        return TKeyLogoBlob(TLogoBlobID(tabletId, 1, step, 0, 1, 0));
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

            // A tablet may exceed the requested chunk size, but it remains the
            // current tablet and is not exposed until the next tablet starts.
            accumulator.Update(MakeKey(3, 30), MakeMemRec(10));
            accumulator.Update(MakeKey(3, 20), MakeMemRec(20));
            UNIT_ASSERT(!accumulator.IsChunkReady());

            accumulator.Update(MakeKey(2, 30), MakeMemRec(30));
            UNIT_ASSERT(accumulator.IsChunkReady());

            NKikimrVDisk::LogoBlobIndexStat chunk;
            accumulator.ExtractChunk(&chunk);
            UNIT_ASSERT(!accumulator.IsChunkReady());
            UNIT_ASSERT_VALUES_EQUAL(chunk.tablets_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(chunk.channels_size(), 0);
            AssertSingleChannelTablet(chunk.tablets(0), 3, 2, 30);
            UNIT_ASSERT_VALUES_EQUAL(
                chunk.tablets(0).channels(0).min_id(), MakeKey(3, 20).LogoBlobID().ToString());
            UNIT_ASSERT_VALUES_EQUAL(
                chunk.tablets(0).channels(0).max_id(), MakeKey(3, 30).LogoBlobID().ToString());

            // The first record for tablet 2 was already retained as current
            // state while the previous chunk was extracted.
            accumulator.Update(MakeKey(2, 20), MakeMemRec(40));
            UNIT_ASSERT(!accumulator.IsChunkReady());
            accumulator.Update(MakeKey(1, 30), MakeMemRec(50));
            UNIT_ASSERT(accumulator.IsChunkReady());

            accumulator.ExtractChunk(&chunk);
            UNIT_ASSERT_VALUES_EQUAL(chunk.tablets_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(chunk.channels_size(), 0);
            AssertSingleChannelTablet(chunk.tablets(0), 2, 2, 70);

            accumulator.Update(MakeKey(1, 20), MakeMemRec(60));
            accumulator.Finish();
            UNIT_ASSERT(accumulator.IsChunkReady());

            accumulator.ExtractChunk(&chunk);
            UNIT_ASSERT_VALUES_EQUAL(chunk.tablets_size(), 1);
            AssertSingleChannelTablet(chunk.tablets(0), 1, 2, 110);

            // Global channel totals are emitted only by Finish, after all
            // intermediate chunks have been extracted.
            UNIT_ASSERT_VALUES_EQUAL(chunk.channels_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(chunk.channels(0).count(), 6);
            UNIT_ASSERT_VALUES_EQUAL(chunk.channels(0).data_size(), 210);
            UNIT_ASSERT_VALUES_EQUAL(
                chunk.channels(0).min_id(), MakeKey(1, 20).LogoBlobID().ToString());
            UNIT_ASSERT_VALUES_EQUAL(
                chunk.channels(0).max_id(), MakeKey(3, 30).LogoBlobID().ToString());
        }

        Y_UNIT_TEST(EmptyDatabaseProducesEmptyTerminalChunk) {
            TLogoBlobIndexStatStreamAccumulator accumulator(1024);
            accumulator.Finish();

            NKikimrVDisk::LogoBlobIndexStat chunk;
            accumulator.ExtractChunk(&chunk);
            UNIT_ASSERT_VALUES_EQUAL(chunk.tablets_size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(chunk.channels_size(), 0);
        }
    }

    Y_UNIT_TEST_SUITE(TLogoBlobIndexStatStreamingProtocolTest) {
        Y_UNIT_TEST(DefaultsPreserveLegacySingleResponseProtocol) {
            NKikimrVDisk::GetLogoBlobIndexStatRequest request;
            UNIT_ASSERT(!request.stream());
            UNIT_ASSERT_VALUES_EQUAL(request.max_chunk_bytes(), 0);

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
            request.set_max_chunk_bytes(1 << 20);

            TString wire;
            UNIT_ASSERT(request.SerializeToString(&wire));

            NKikimrVDisk::GetLogoBlobIndexStatRequest parsedRequest;
            UNIT_ASSERT(parsedRequest.ParseFromString(wire));
            UNIT_ASSERT(parsedRequest.stream());
            UNIT_ASSERT_VALUES_EQUAL(parsedRequest.max_chunk_bytes(), 1 << 20);

            NKikimrVDisk::GetLogoBlobIndexStatResponse response;
            response.set_has_more(true);
            response.set_sequence_id(7);
            UNIT_ASSERT(response.SerializeToString(&wire));

            NKikimrVDisk::GetLogoBlobIndexStatResponse parsedResponse;
            UNIT_ASSERT(parsedResponse.ParseFromString(wire));
            UNIT_ASSERT(parsedResponse.has_more());
            UNIT_ASSERT_VALUES_EQUAL(parsedResponse.sequence_id(), 7);

            TEvGetLogoBlobIndexStatResponseAck ack(7, true);
            UNIT_ASSERT_VALUES_EQUAL(ack.Record.sequence_id(), 7);
            UNIT_ASSERT(ack.Record.cancel());
        }

        Y_UNIT_TEST(FieldNumbersAreAdditiveAndStable) {
            const auto* request = NKikimrVDisk::GetLogoBlobIndexStatRequest::descriptor();
            UNIT_ASSERT_VALUES_EQUAL(request->FindFieldByName("stream")->number(), 1);
            UNIT_ASSERT_VALUES_EQUAL(request->FindFieldByName("max_chunk_bytes")->number(), 2);

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
