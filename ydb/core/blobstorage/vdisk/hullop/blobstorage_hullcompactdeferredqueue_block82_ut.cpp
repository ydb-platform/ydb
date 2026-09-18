#include "blobstorage_hullcompactdeferredqueue.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_arena.h>
#include <util/random/fast.h>
#include <library/cpp/testing/unittest/registar.h>
#include <bit>

namespace NKikimr {
namespace {
class TBlock82DeferredQueue : public TDeferredItemQueueBase<TBlock82DeferredQueue> {
    friend class TDeferredItemQueueBase<TBlock82DeferredQueue>;
    void StartImpl() {}
    void FinishImpl() {}
    void ProcessItemImpl(const TDiskPart&, const TRope& rope, bool isInline) {
        Results.emplace_back(rope, isInline);
    }
public:
    std::vector<std::pair<TRope, bool>> Results;
    explicit TBlock82DeferredQueue(TRopeArena& arena)
        : TDeferredItemQueueBase<TBlock82DeferredQueue>(arena,
            TBlobStorageGroupType(TErasureType::Erasure8Plus2Block), false)
    {}
};
}

Y_UNIT_TEST_SUITE(HullDeferredQueueBlock82) {
    Y_UNIT_TEST(BoundedWideMasksAndHighPartReads) {
        const TBlobStorageGroupType type(TErasureType::Erasure8Plus2Block);
        TRopeArena arena(TRopeArenaBackend::Allocate);
        TReallyFastRng32 random(0x82def);
        for (const auto crc : {TErasureType::CrcModeNone, TErasureType::CrcModeWholePart}) {
            const TString data(8193, 'x');
            const TLogoBlobID id(1, 1, 1, 0, data.size(), 0, 0, crc);
            std::array<TRope, 10> parts;
            ErasureSplit(crc, type, TRope(data), parts, nullptr, GetDefaultRcBufAllocator());
            std::vector<std::pair<ui32, ui32>> corpus;
            for (ui32 first = 0; first < 10; ++first) {
                corpus.emplace_back(0, 1u << first);
                corpus.emplace_back(1u << first, 0);
                for (ui32 second = first + 1; second < 10; ++second) {
                    corpus.emplace_back(1u << first, 1u << second);
                    corpus.emplace_back(0, (1u << first) | (1u << second));
                }
            }
            corpus.emplace_back(0, 1023);
            corpus.emplace_back(1023, 1023);
            for (ui32 i = 0; i < 1000; ++i) {
                corpus.emplace_back(random() & 1023, (random() & 1023) | 512);
            }
            TBlock82DeferredQueue queue(arena);
            std::vector<TRope> expected;
            ui64 index = 0;
            for (const auto& [memory, disk] : corpus) {
                TDiskBlobMerger initial, combined;
                for (ui8 part = 0; part < 10; ++part) {
                    if (memory & (1u << part)) {
                        initial.AddPart(TRope(parts[part]), type, TLogoBlobID(id, part + 1));
                    }
                    if ((memory | disk) & (1u << part)) {
                        combined.AddPart(TRope(parts[part]), type, TLogoBlobID(id, part + 1));
                    }
                }
                queue.Put(index, std::popcount(disk), TDiskPart(), initial, id, (index & 1) == 0);
                expected.push_back(combined.CreateDiskBlob(arena, false));
                ++index;
            }
            queue.Start();
            index = 0;
            for (const auto& [memory, disk] : corpus) {
                Y_UNUSED(memory);
                for (ui8 part = 0; part < 10; ++part) {
                    if (disk & (1u << part)) {
                        queue.AddReadDiskBlob(index, TRope(parts[part]), part);
                    }
                }
                ++index;
            }
            UNIT_ASSERT(queue.AllProcessed());
            queue.Finish();
            UNIT_ASSERT_VALUES_EQUAL(queue.Results.size(), expected.size());
            for (size_t i = 0; i < expected.size(); ++i) {
                UNIT_ASSERT_EQUAL(queue.Results[i].first, expected[i]);
                UNIT_ASSERT_VALUES_EQUAL(queue.Results[i].second, (i & 1) == 0);
            }
        }
    }
}
}
