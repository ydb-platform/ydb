#include "blobstorage_blob.h"
#include "hullds_arena.h"
#include <ydb/core/erasure/erasure.h>
#include <library/cpp/testing/unittest/registar.h>

#if defined(_unix_)
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#include <cerrno>
#include <csignal>
#endif

namespace NKikimr {
namespace {
#if defined(_unix_)
template<typename TCallback>
void AssertHeaderRejected(TCallback callback) {
    const pid_t child = fork();
    UNIT_ASSERT(child >= 0);
    if (!child) {
        const rlimit limit{0, 0};
        setrlimit(RLIMIT_CORE, &limit);
        signal(SIGABRT, SIG_DFL);
        if (!freopen("/dev/null", "w", stderr)) {
            _exit(100);
        }
        callback();
        _exit(0);
    }
    int status = 0;
    pid_t result;
    do {
        result = waitpid(child, &status, 0);
    } while (result == -1 && errno == EINTR);
    UNIT_ASSERT_VALUES_EQUAL(result, child);
    UNIT_ASSERT(WIFSIGNALED(status));
    UNIT_ASSERT_VALUES_EQUAL(WTERMSIG(status), SIGABRT);
}
#endif
}

Y_UNIT_TEST_SUITE(DiskBlobBlock82) {
    Y_UNIT_TEST(AllPartMasksCreateIterateAndMerge) {
        const TBlobStorageGroupType type(TErasureType::Erasure8Plus2Block);
        TRopeArena arena(TRopeArenaBackend::Allocate);
        for (const auto crc : {TErasureType::CrcModeNone, TErasureType::CrcModeWholePart}) {
            for (const ui32 size : {1u, 255u, 256u, 257u, 8193u}) {
                TString data = TString::Uninitialized(size);
                for (ui32 i = 0; i < size; ++i) {
                    data.Detach()[i] = (i * 37 + i / 7) & 255;
                }
                const TLogoBlobID id(1, 1, 1, 0, size, 0, 0, crc);
                std::array<TRope, 10> encoded;
                ErasureSplit(crc, type, TRope(data), encoded, nullptr, GetDefaultRcBufAllocator());
                for (ui32 mask = 1; mask < (1u << 10); ++mask) {
                    NMatrix::TVectorType parts(0, 10);
                    TVector<TRope> selected;
                    TDiskBlobMerger merger;
                    ui32 expectedSize = 0;
                    for (ui8 p = 0; p < 10; ++p) {
                        if (mask & (1u << p)) {
                            parts.Set(p);
                            selected.push_back(encoded[p]);
                            expectedSize += encoded[p].GetSize();
                            TRope record = TDiskBlob::Create(size, p + 1, 10, TRope(encoded[p]), arena, false);
                            TDiskBlob blob(&record, NMatrix::TVectorType::MakeOneHot(p, 10), type, id);
                            merger.Add(blob);
                        }
                    }
                    const TRope record = TDiskBlob::CreateFromDistinctParts(selected.begin(), selected.end(),
                        parts, size, arena, false);
                    UNIT_ASSERT_VALUES_EQUAL(record.GetSize(), expectedSize);
                    UNIT_ASSERT_VALUES_EQUAL(TDiskBlob::CalculateBlobSize(type, id, parts, false), expectedSize);
                    UNIT_ASSERT_EQUAL(record, merger.CreateDiskBlob(arena, false));
                    TDiskBlob blob(&record, parts, type, id);
                    UNIT_ASSERT_VALUES_EQUAL(blob.GetFullDataSize(), size);
                    UNIT_ASSERT_VALUES_EQUAL(blob.GetBlobSize(false), expectedSize);
                    ui32 found = 0;
                    for (auto p = blob.begin(); p != blob.end(); ++p) {
                        UNIT_ASSERT_EQUAL(p.GetPart(), encoded[p.GetPartId() - 1]);
                        found |= 1u << (p.GetPartId() - 1);
                    }
                    UNIT_ASSERT_VALUES_EQUAL(found, mask);
                    if (parts.Get(9)) {
                        merger.ClearPart(9);
                        UNIT_ASSERT(!merger.GetDiskBlob().GetParts().Get(9));
                    }
                }
            }
        }
    }

#if defined(_unix_)
    Y_UNIT_TEST(HeaderfulCreationAndReadingAreRejected) {
        const TBlobStorageGroupType type(TErasureType::Erasure8Plus2Block);
        const TLogoBlobID id(1, 1, 1, 0, 256, 0);
        TRopeArena arena(TRopeArenaBackend::Allocate);
        const auto parts = NMatrix::TVectorType::MakeOneHot(0, 10);
        AssertHeaderRejected([&] { TDiskBlob::Create(256, 1, 10, TRope(TString(32, 'a')), arena, true); });
        AssertHeaderRejected([&] { TDiskBlob::CalculateBlobSize(type, id, parts, true); });
        AssertHeaderRejected([&] {
            TDiskBlobMerger merger;
            merger.AddPart(TRope(TString(32, 'a')), type, TLogoBlobID(id, 1));
            merger.CreateDiskBlob(arena, true);
        });
        AssertHeaderRejected([&] {
            TRope record(TString(32 + TDiskBlob::HeaderSize, 'a'));
            TDiskBlob blob(&record, parts, type, id);
        });
    }
#endif
}
}
