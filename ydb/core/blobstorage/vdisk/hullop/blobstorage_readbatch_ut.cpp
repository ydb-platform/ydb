#include "blobstorage_readbatch.h"
#include <library/cpp/testing/unittest/registar.h>
#include <util/random/fast.h>
#include <util/generic/bitmap.h>
#include <util/stream/null.h>

using namespace NKikimr;

#define STR Cnull

Y_UNIT_TEST_SUITE(ReadBatcher) {
    Y_UNIT_TEST(Range) {
        TVector<std::pair<ui32, ui32>> ranges;
        const ui32 len = 8;
        for (ui32 start = 0; start <= len; ++start) {
            for (ui32 end = start; end <= len; ++end) {
                ranges.emplace_back(start, end);
            }
        }
        for (ui32 a = 0; a < ranges.size(); ++a) {
            for (ui32 b = 0; b < ranges.size(); ++b) {
                for (ui32 c = 0; c < ranges.size(); ++c) {
                    for (ui32 d = 0; d < ranges.size(); ++d) {
                        TDynBitMap bm;
                        TRangeSet<ui32> range;
                        for (const auto& r : {ranges[a], ranges[b], ranges[c], ranges[d]}) {
                            bm.Set(r.first, r.second);
                            range.Set(r.first, r.second);
                        }
                        TDynBitMap ref;
                        range.ToBitMap(ref);
                        UNIT_ASSERT_EQUAL(ref, bm);
                        UNIT_ASSERT_VALUES_EQUAL(range.GetCount(), bm.Count());
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(ReadBatcher) {
        struct TPayload {
            TChunkIdx ChunkIdx;
            ui32 Offset;
            ui32 Size;
        };

        const ui32 chunkSize = 16 << 20;
        TMap<TChunkIdx, TString> chunks;
        for (TChunkIdx chunkIdx = 1; chunkIdx <= 10; ++chunkIdx) {
            TString data(chunkSize, ' ');
            ui64 pattern = RandomNumber<ui64>();
            for (ui32 pos = 0; pos + sizeof(ui64) <= chunkSize; pos += sizeof(ui64)) {
                *reinterpret_cast<ui64 *>(const_cast<char *>(data.data()) + pos) = pattern;
            }
            chunks.emplace(chunkIdx, data);
        }

        TReallyFastRng32 rng(100500);
        TCompactReadBatcher<TPayload> batcher("", 4 << 20, 4 << 20, 0.5);

        // generate requests
        const ui32 numRequests = 10000;
        for (ui32 i = 0; i < numRequests; ++i) {
            decltype(chunks)::iterator it = chunks.begin();
            std::advance(it, rng() % chunks.size());
            const ui32 offset = rng() % chunkSize;
            const ui32 len = 1 + rng() % Min<ui32>(100000, chunkSize + 1 - offset);
            TPayload payload{it->first, offset, len};
            batcher.AddReadItem({it->first, offset, len}, std::move(payload));
        }
        batcher.Start();

        // start processing
        TVector<std::unique_ptr<NPDisk::TEvChunkRead>> pendingReads;
        ui32 expectedSerial = 0;
        for (;;) {
            ui32 action = rng() % 100;
            if (pendingReads && action < 20) {
                ui32 index = rng() % pendingReads.size();
                std::unique_ptr<NPDisk::TEvChunkRead> msg = std::move(pendingReads[index]);
                pendingReads.erase(pendingReads.begin() + index);
                NPDisk::TEvChunkReadResult result(NKikimrProto::OK, msg->ChunkIdx, msg->Offset, msg->Cookie, 0, "");
                UNIT_ASSERT(msg->Offset + msg->Size <= chunkSize);
                result.Data.SetData(TRcBuf::Copy(chunks.at(msg->ChunkIdx).substr(msg->Offset, msg->Size)));
                batcher.Apply(&result);
            } else if (auto msg = batcher.GetPendingMessage(0, 0, 0)) {
                pendingReads.push_back(std::move(msg));
            } else if (!pendingReads) {
                UNIT_ASSERT_VALUES_EQUAL(expectedSerial, numRequests);
                break;
            }

            ui64 serial;
            TPayload payload;
            NKikimrProto::EReplyStatus status;
            TRcBuf data;
            while (batcher.GetResultItem(&serial, &payload, &status, &data)) {
                STR << serial << Endl;
                UNIT_ASSERT_VALUES_EQUAL(serial, expectedSerial);
                ++expectedSerial;
                UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::OK);
                UNIT_ASSERT_EQUAL(chunks.at(payload.ChunkIdx).substr(payload.Offset, payload.Size), TStringBuf(data));
            }
        }
    }
}
