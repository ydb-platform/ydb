#include "func.h"
#include "variants.h"

#include <ydb/core/blobstorage/crypto/chacha.h>

// File mode (mode 2) needs POSIX mmap plus a portable XXH3_64bits/CityHash64
// -- all available on Linux and macOS (ARM64 included), just not Windows.
// This block must come before the x86-only block below: on x86 the plain
// xxhash.h include here is redundant (xxh_x86dispatch.h below re-includes
// it and macro-overrides XXH3_64bits -> XXH3_64bits_dispatch); on ARM it's
// the only place XXH3_64bits comes from. XXH3_64bits here is only ever used
// for internal record/header/manifest digests (never cross-checked against
// a specific variant), and every XXH3 implementation is bit-identical for
// the same input, so it's safe to use regardless of which ISA path a given
// machine's default build picks.
#if !defined(_win_)
#include "xxHash/xxhash.h"
#include <contrib/restricted/cityhash-1.0.2/city.h>
#include <util/system/cpu_id.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cerrno>
#endif

#if !(defined(_win_) || defined(_arm64_))
#include <ydb/core/blobstorage/crypto/chacha_vec.h>
#include <ydb/core/blobstorage/crypto/chacha_512/chacha_512.h>
#include "xxHash/xxh_x86dispatch.h"
#endif

#include <util/generic/algorithm.h>
#include <util/generic/array_size.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>
#include <util/string/printf.h>
#include <util/system/align.h>
#include <util/system/hp_timer.h>
#include <util/system/types.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

namespace {

// ---------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------

struct TSplitMix64 {
    ui64 State;

    explicit TSplitMix64(ui64 seed)
        : State(seed)
    {}

    ui64 Next() {
        ui64 z = (State += 0x9E3779B97F4A7C15ULL);
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
        return z ^ (z >> 31);
    }
};

void FillRandom(ui8* data, size_t len, TSplitMix64& rng) {
    size_t i = 0;
    while (i + 8 <= len) {
        const ui64 v = rng.Next();
        memcpy(data + i, &v, 8);
        i += 8;
    }
    if (i < len) {
        const ui64 v = rng.Next();
        memcpy(data + i, &v, len - i);
    }
}

// Buffer whose data pointer is 64-byte aligned, with room to also read a
// misaligned sub-range (e.g. base+1, base+63) -- see ai_func.md §5.1.
class TAlignedBuffer {
public:
    explicit TAlignedBuffer(size_t size)
        : Raw_(new ui8[size + 64])
    {
        Data_ = reinterpret_cast<ui8*>(AlignUp<intptr_t>(intptr_t(Raw_.get()), size_t(64)));
    }

    ui8* Data() const {
        return Data_;
    }

private:
    std::unique_ptr<ui8[]> Raw_;
    ui8* Data_ = nullptr;
};

// Fixed key/IV for every correctness check in this file (sweep and file
// mode alike) -- same pattern bench.cpp's self-checks already use.
constexpr ui8 kFixedKey[32] = {
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
    0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
};
constexpr ui8 kFixedIv[8] = {0, 0, 0, 0, 0, 0, 0, 0};

TString Hex64(ui64 v) {
    return Sprintf("0x%016llx", static_cast<unsigned long long>(v));
}

bool VariantUsable(const TCipherVariant& variant, const void* plaintext) {
    return !variant.IsAlignmentOk || variant.IsAlignmentOk(plaintext);
}

#if !(defined(_win_) || defined(_arm64_))
// Mirrors variants.cpp's IsChaChaVecAlignmentOk for the direct ChaChaVec
// class used by the streaming-split check below (which bypasses the
// TCipherVariant abstraction to keep one cipher instance alive across
// segments).
bool IsChaChaVecAlignmentOk(const void* p) {
    const ui32 a = static_cast<ui32>(reinterpret_cast<intptr_t>(p) % 16);
    return a == 0 || a == 8;
}
#endif

// ---------------------------------------------------------------------
// Mode 1: in-memory equivalence sweep (ai_func.md §5)
// ---------------------------------------------------------------------

struct TSweepShared {
    std::atomic<bool> Failed{false};
    std::atomic<ui64> Iterations{0};
    std::mutex PrintMutex;
};

void ReportSweepFailure(TSweepShared& shared, const TString& message, ui32 threadIdx, ui64 seed, ui32 threads) {
    std::lock_guard<std::mutex> lock(shared.PrintMutex);
    if (!shared.Failed.exchange(true)) {
        Cerr << "functional sweep FAILED\n"
             << "  " << message << "\n"
             << "  thread=" << threadIdx << " seed=" << seed << " threads=" << threads << "\n";
    }
}

// Streaming-split equivalence (ai_func.md §5.2 item 5): encipher `data` as
// a sequence of segments on one persistent cipher instance and compare the
// concatenation against a single-call reference. Segments are constrained
// to multiples of 64 bytes except possibly the last -- empirically the
// only contract the vectorized implementations actually honor, matching
// what the perf kernels rely on (documented in README.md).
template <class TCipherClass>
bool CheckStreamingSplitImpl(const ui8* data, size_t len, TSplitMix64& rng) {
    // Output buffers must be exactly 16-byte aligned for ChaChaVec
    // regardless of the input's alignment (chacha_vec.cpp aborts
    // otherwise) -- TAlignedBuffer's 64-byte alignment satisfies that for
    // every cipher class.
    TAlignedBuffer refOutBuf(len);
    ui8* refOut = refOutBuf.Data();
    {
        TCipherClass cipher;
        cipher.SetKey(kFixedKey, TCipherClass::KEY_SIZE);
        cipher.SetIV(kFixedIv);
        cipher.Encipher(data, refOut, len);
    }

    TAlignedBuffer streamOutBuf(len);
    ui8* streamOut = streamOutBuf.Data();
    {
        TCipherClass cipher;
        cipher.SetKey(kFixedKey, TCipherClass::KEY_SIZE);
        cipher.SetIV(kFixedIv);
        size_t pos = 0;
        while (pos < len) {
            const size_t remaining = len - pos;
            size_t seg;
            if (remaining <= 64) {
                seg = remaining;
            } else {
                const size_t maxBlocks = Min<size_t>(remaining / 64, 16);
                const size_t blocks = 1 + (rng.Next() % maxBlocks);
                seg = blocks * 64;
            }
            cipher.Encipher(data + pos, streamOut + pos, seg);
            pos += seg;
        }
    }

    return len == 0 || memcmp(refOut, streamOut, len) == 0;
}

bool CheckChaChaIntStreamingSplit(const ui8* data, size_t len, TSplitMix64& rng) {
    return CheckStreamingSplitImpl<ChaCha>(data, len, rng);
}

#if !(defined(_win_) || defined(_arm64_))
bool CheckChaChaVecStreamingSplit(const ui8* data, size_t len, TSplitMix64& rng) {
    return CheckStreamingSplitImpl<ChaChaVec>(data, len, rng);
}

bool CheckChaCha512StreamingSplit(const ui8* data, size_t len, TSplitMix64& rng) {
    return CheckStreamingSplitImpl<ChaCha512>(data, len, rng);
}
#endif

// One randomized iteration's worth of checks over `data[0, len)`, which the
// caller has already filled with random bytes (ai_func.md §5.2).
bool CheckIteration(
    ui8* data, size_t len,
    TSplitMix64& rng,
    const THashVariant* xxhVariants, size_t xxhCount,
    const TCipherVariant* chachaVariants, size_t chachaCount,
    ui32 threadIdx, ui64 seed, ui32 threads,
    TSweepShared& shared)
{
    // 1. XXH3 family: every supported variant must agree with the
    // reference (scalar, index 0).
    if (xxhCount > 0) {
        const ui64 refHash = xxhVariants[0].Hash(data, len);
        for (size_t v = 1; v < xxhCount; ++v) {
            if (xxhVariants[v].IsSupported && !xxhVariants[v].IsSupported()) {
                continue;
            }
            const ui64 h = xxhVariants[v].Hash(data, len);
            if (h != refHash) {
                ReportSweepFailure(shared, Sprintf(
                    "xxh3 mismatch: %s=%s vs reference %s=%s (len=%zu)",
                    xxhVariants[v].Name, Hex64(h).c_str(),
                    xxhVariants[0].Name, Hex64(refHash).c_str(), len),
                    threadIdx, seed, threads);
                return false;
            }
        }
    }

    if (chachaCount == 0) {
        return true;
    }

    const ui64 counter64 = rng.Next();

    // 2. ChaCha out-of-place equivalence across variants. Output buffers
    // use TAlignedBuffer: ChaChaVec requires an exactly 16-byte-aligned
    // ciphertext pointer regardless of the plaintext's alignment.
    TAlignedBuffer refCtBuf(len);
    ui8* refCt = refCtBuf.Data();
    chachaVariants[0].Encipher(kFixedKey, kFixedIv, counter64, data, refCt, len);

    TAlignedBuffer altCtBuf(len);
    ui8* altCt = altCtBuf.Data();
    for (size_t v = 1; v < chachaCount; ++v) {
        if (chachaVariants[v].IsSupported && !chachaVariants[v].IsSupported()) {
            continue;
        }
        if (!VariantUsable(chachaVariants[v], data)) {
            continue;
        }
        chachaVariants[v].Encipher(kFixedKey, kFixedIv, counter64, data, altCt, len);
        if (len > 0 && memcmp(altCt, refCt, len) != 0) {
            ReportSweepFailure(shared, Sprintf(
                "chacha out-of-place mismatch: %s vs reference %s (len=%zu, counter=%llu)",
                chachaVariants[v].Name, chachaVariants[0].Name, len,
                static_cast<unsigned long long>(counter64)),
                threadIdx, seed, threads);
            return false;
        }
    }

    auto pickSupported = [&](const void* plaintextPtr) -> size_t {
        for (int attempt = 0; attempt < 8; ++attempt) {
            const size_t idx = rng.Next() % chachaCount;
            const bool supported = !chachaVariants[idx].IsSupported || chachaVariants[idx].IsSupported();
            if (supported && VariantUsable(chachaVariants[idx], plaintextPtr)) {
                return idx;
            }
        }
        return 0; // chacha-int is always supported and alignment-agnostic
    };

    // 3. In-place must match the out-of-place ciphertext. Copied into an
    // aligned scratch buffer first, so every variant is exercised here
    // regardless of `data`'s (deliberately varied) alignment.
    {
        TAlignedBuffer inplaceBuf(len);
        ui8* inplace = inplaceBuf.Data();
        memcpy(inplace, data, len);
        const size_t vi = pickSupported(inplace);
        chachaVariants[vi].Encipher(kFixedKey, kFixedIv, counter64, inplace, inplace, len);
        if (len > 0 && memcmp(inplace, refCt, len) != 0) {
            ReportSweepFailure(shared, Sprintf(
                "chacha in-place mismatch: %s (len=%zu, counter=%llu)",
                chachaVariants[vi].Name, len, static_cast<unsigned long long>(counter64)),
                threadIdx, seed, threads);
            return false;
        }
    }

    // 4. Round-trip (stream cipher symmetry; ChaCha512 has no Decipher).
    {
        const size_t vi = pickSupported(refCt);
        TAlignedBuffer roundTripBuf(len);
        ui8* roundTrip = roundTripBuf.Data();
        chachaVariants[vi].Encipher(kFixedKey, kFixedIv, counter64, refCt, roundTrip, len);
        if (len > 0 && memcmp(roundTrip, data, len) != 0) {
            ReportSweepFailure(shared, Sprintf(
                "chacha round-trip mismatch: %s (len=%zu, counter=%llu)",
                chachaVariants[vi].Name, len, static_cast<unsigned long long>(counter64)),
                threadIdx, seed, threads);
            return false;
        }
    }

    // 5. Streaming-split equivalence -- the mode the perf kernels actually
    // use, and the one the old self-checks never covered.
    if (!CheckChaChaIntStreamingSplit(data, len, rng)) {
        ReportSweepFailure(shared, Sprintf("chacha-int streaming-split mismatch (len=%zu)", len),
            threadIdx, seed, threads);
        return false;
    }
#if !(defined(_win_) || defined(_arm64_))
    if (IsChaChaVecAlignmentOk(data) && !CheckChaChaVecStreamingSplit(data, len, rng)) {
        ReportSweepFailure(shared, Sprintf("chacha-sse streaming-split mismatch (len=%zu)", len),
            threadIdx, seed, threads);
        return false;
    }
    if (NX86::CachedHaveAVX512F() && !CheckChaCha512StreamingSplit(data, len, rng)) {
        ReportSweepFailure(shared, Sprintf("chacha-avx512 streaming-split mismatch (len=%zu)", len),
            threadIdx, seed, threads);
        return false;
    }
#endif

    return true;
}

// Boundary lengths (ai_func.md §5.1): union of the XXH3 and ChaCha
// code-path edges, swept deterministically in Phase A and used as one of
// the Phase B random buckets.
constexpr size_t BoundaryLengths[] = {
    0, 1, 3, 4, 8, 9, 16, 17, 32, 63, 64, 65, 96, 127, 128, 129,
    160, 191, 192, 193, 240, 241, 255, 256, 257, 320, 321, 512, 1024, 4096,
};
constexpr size_t BoundaryOffsets[] = {0, 1, 8, 63};
constexpr size_t kMaxOffset = 63;
constexpr size_t kMaxLargeLen = 8 * 1024 * 1024;

void SweepWorker(
    ui32 threadIdx, ui32 threads, ui64 seed, ui32 durationS,
    const THashVariant* xxhVariants, size_t xxhCount,
    const TCipherVariant* chachaVariants, size_t chachaCount,
    TSweepShared& shared)
{
    TSplitMix64 rng(seed * 0x9E3779B97F4A7C15ULL + threadIdx);

    TAlignedBuffer buffer(kMaxOffset + kMaxLargeLen + 64);
    ui8* base = buffer.Data();

    auto runOne = [&](size_t offset, size_t len) -> bool {
        FillRandom(base + offset, len, rng);
        return CheckIteration(base + offset, len, rng, xxhVariants, xxhCount,
            chachaVariants, chachaCount, threadIdx, seed, threads, shared);
    };

    // Phase A: deterministic boundary sweep (cheap, run it in full).
    for (size_t len : BoundaryLengths) {
        for (size_t offset : BoundaryOffsets) {
            if (shared.Failed.load(std::memory_order_relaxed)) {
                return;
            }
            if (!runOne(offset, len)) {
                return;
            }
            shared.Iterations.fetch_add(1, std::memory_order_relaxed);
        }
    }

    // Phase B: randomized fuzz until the deadline.
    THPTimer timer;
    while (timer.Passed() < durationS && !shared.Failed.load(std::memory_order_relaxed)) {
        const ui32 bucket = static_cast<ui32>(rng.Next() % 100);
        size_t len;
        if (bucket < 40) {
            const size_t edge = BoundaryLengths[rng.Next() % Y_ARRAY_SIZE(BoundaryLengths)];
            const i64 jitter = static_cast<i64>(rng.Next() % 7) - 3;
            const i64 jittered = static_cast<i64>(edge) + jitter;
            len = jittered < 0 ? 0 : static_cast<size_t>(jittered);
        } else if (bucket < 80) {
            len = rng.Next() % 4097;
        } else {
            const double u = static_cast<double>(rng.Next() >> 11) / 9007199254740992.0; // 2^53
            const double lo = std::log(4096.0);
            const double hi = std::log(static_cast<double>(kMaxLargeLen));
            len = static_cast<size_t>(std::exp(lo + u * (hi - lo)));
        }
        const size_t offset = rng.Next() % (kMaxOffset + 1);

        if (!runOne(offset, len)) {
            return;
        }
        shared.Iterations.fetch_add(1, std::memory_order_relaxed);
    }
}

} // namespace

bool RunFunctionalSweep(ui32 threads, ui64 seed, ui32 durationS) {
    size_t xxhCount = 0;
    const THashVariant* xxhVariants = GetXxh3Variants(&xxhCount);
    size_t chachaCount = 0;
    const TCipherVariant* chachaVariants = GetChaChaVariants(&chachaCount);

    Cout << "Functional sweep: " << threads << " threads, seed=" << seed
         << ", duration=" << durationS << "s, "
         << xxhCount << " xxh3 variant(s), " << chachaCount << " chacha variant(s)\n";

    TSweepShared shared;
    TVector<std::thread> workers;
    workers.reserve(threads);
    for (ui32 t = 0; t < threads; ++t) {
        workers.emplace_back(SweepWorker, t, threads, seed, durationS,
            xxhVariants, xxhCount, chachaVariants, chachaCount, std::ref(shared));
    }
    for (auto& w : workers) {
        w.join();
    }

    if (shared.Failed.load()) {
        return false;
    }

    Cout << "functional sweep passed: " << shared.Iterations.load()
         << " iterations, " << threads << " threads, " << durationS
         << "s, seed " << seed << "\n";
    return true;
}

// ---------------------------------------------------------------------
// Mode 2: file format (ai_func.md §6), writer (§7) and verifier (§8)
// ---------------------------------------------------------------------

namespace {

constexpr size_t kDataBlockSize = 4096;
constexpr size_t kRecordSize = 32;
constexpr size_t kRecordsPerGroup = kDataBlockSize / kRecordSize; // 128
constexpr size_t kGroupSize = kDataBlockSize + kRecordsPerGroup * kDataBlockSize; // 528384
constexpr size_t kHeaderSize = 4096;
constexpr ui32 kFileVersion = 1;
constexpr char kMagic[8] = {'Y', 'D', 'B', 'H', 'A', 'S', 'H', '1'};

struct THashFileHeader {
    char Magic[8];
    ui32 Version;
    ui32 DataBlockSize;
    ui32 RecordSize;
    ui32 RecordsPerGroup;
    ui64 GroupCount;
    ui64 TotalDataBlocks;
    ui64 Seed;
    ui64 HeaderHash;
};

struct TBlockRecord {
    ui64 Xxh3;
    ui64 City64;
    ui64 ChaChaDigest;
    ui64 BlockIndex;
};
static_assert(sizeof(TBlockRecord) == 32);

ui64 ComputeGroupCount(double outputSizeGiB) {
    const double totalBytes = outputSizeGiB * static_cast<double>(1ULL << 30);
    const double usable = totalBytes - static_cast<double>(kHeaderSize);
    i64 groups = static_cast<i64>(usable / static_cast<double>(kGroupSize));
    if (groups < 1) {
        groups = 1;
    }
    return static_cast<ui64>(groups);
}

} // namespace

#if !defined(_win_)

namespace {

// Small RAII helper around open+fallocate/ftruncate+mmap (ai_func.md §7
// note: plain POSIX is fine; works on Linux and macOS, including ARM64 --
// see OpenForWrite for the macOS fallocate() gap).
class TMappedFile {
public:
    TMappedFile() = default;
    ~TMappedFile() {
        Close();
    }

    TMappedFile(const TMappedFile&) = delete;
    TMappedFile& operator=(const TMappedFile&) = delete;

    bool OpenForWrite(const TString& path, ui64 size) {
        Fd_ = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (Fd_ < 0) {
            Cerr << "Failed to open " << path << " for writing: " << strerror(errno) << "\n";
            return false;
        }
#if defined(_darwin_)
        // fallocate() doesn't exist on macOS at all (not even declared);
        // go straight to ftruncate. This means ENOSPC surfaces lazily as
        // SIGBUS during a write instead of eagerly here -- an accepted
        // platform gap, since there's no macOS equivalent that's simpler
        // than F_PREALLOCATE juggling for a test tool.
        if (ftruncate(Fd_, static_cast<off_t>(size)) != 0) {
            Cerr << "ftruncate failed: " << strerror(errno) << "\n";
            Close();
            return false;
        }
#else
        if (fallocate(Fd_, 0, 0, static_cast<off_t>(size)) != 0) {
            Cerr << "Warning: fallocate failed (" << strerror(errno) << "), falling back to ftruncate\n";
            if (ftruncate(Fd_, static_cast<off_t>(size)) != 0) {
                Cerr << "ftruncate failed: " << strerror(errno) << "\n";
                Close();
                return false;
            }
        }
#endif
        return MapCommon(size, PROT_READ | PROT_WRITE);
    }

    bool OpenForRead(const TString& path) {
        Fd_ = open(path.c_str(), O_RDONLY);
        if (Fd_ < 0) {
            Cerr << "Failed to open " << path << " for reading: " << strerror(errno) << "\n";
            return false;
        }
        struct stat st;
        if (fstat(Fd_, &st) != 0) {
            Cerr << "fstat failed: " << strerror(errno) << "\n";
            Close();
            return false;
        }
        return MapCommon(static_cast<ui64>(st.st_size), PROT_READ);
    }

    ui8* Data() const {
        return Data_;
    }

    ui64 Size() const {
        return Size_;
    }

    void Sync() {
        if (Data_) {
            msync(Data_, Size_, MS_SYNC);
        }
    }

    void Close() {
        if (Data_) {
            munmap(Data_, Size_);
            Data_ = nullptr;
        }
        if (Fd_ >= 0) {
            close(Fd_);
            Fd_ = -1;
        }
    }

private:
    bool MapCommon(ui64 size, int prot) {
        if (size == 0) {
            Cerr << "File is empty\n";
            Close();
            return false;
        }
        void* p = mmap(nullptr, size, prot, MAP_SHARED, Fd_, 0);
        if (p == MAP_FAILED) {
            Cerr << "mmap failed: " << strerror(errno) << "\n";
            Close();
            return false;
        }
        Data_ = reinterpret_cast<ui8*>(p);
        Size_ = size;
        return true;
    }

    int Fd_ = -1;
    ui8* Data_ = nullptr;
    ui64 Size_ = 0;
};

// ---- Writer (ai_func.md §7) ----

struct TWriterSharedState {
    std::atomic<bool> Failed{false};
    std::atomic<ui64> GroupsDone{0};
    std::mutex PrintMutex;
};

void WriterThreadFunc(
    ui32 /*threadIdx*/,
    ui8* mapping,
    ui64 groupStart, ui64 groupEnd,
    ui64 seed,
    const THashVariant* xxhVariants, size_t xxhCount,
    const TCipherVariant* chachaVariants, size_t chachaCount,
    TWriterSharedState& shared,
    ui64* outPartialDigest)
{
    // Aligned like the sweep's scratch buffers: ChaChaVec requires an
    // exactly 16-byte-aligned ciphertext output pointer.
    TAlignedBuffer scratchBuf(kDataBlockSize);
    TAlignedBuffer altScratchBuf(kDataBlockSize);
    ui8* scratch = scratchBuf.Data();
    ui8* altScratch = altScratchBuf.Data();
    ui64 partialDigest = 0;

    for (ui64 g = groupStart; g < groupEnd; ++g) {
        if (shared.Failed.load(std::memory_order_relaxed)) {
            break;
        }
        ui8* hashPage = mapping + kHeaderSize + g * kGroupSize;
        ui8* dataArea = hashPage + kDataBlockSize;

        for (ui32 r = 0; r < kRecordsPerGroup; ++r) {
            const ui64 blockIndex = g * kRecordsPerGroup + r;
            ui8* block = dataArea + r * kDataBlockSize;

            TSplitMix64 rng(seed ^ (blockIndex * 0xff51afd7ed558ccdULL));
            FillRandom(block, kDataBlockSize, rng);

            TString mismatchMsg;

            const ui64 refXxh = xxhVariants[0].Hash(block, kDataBlockSize);
            for (size_t v = 1; v < xxhCount && mismatchMsg.empty(); ++v) {
                if (xxhVariants[v].IsSupported && !xxhVariants[v].IsSupported()) {
                    continue;
                }
                const ui64 h = xxhVariants[v].Hash(block, kDataBlockSize);
                if (h != refXxh) {
                    mismatchMsg = Sprintf("xxh3 mismatch at block %llu: %s=%s vs %s=%s",
                        static_cast<unsigned long long>(blockIndex),
                        xxhVariants[0].Name, Hex64(refXxh).c_str(),
                        xxhVariants[v].Name, Hex64(h).c_str());
                }
            }

            const ui64 city = CityHash_v1_0_2::CityHash64(reinterpret_cast<const char*>(block), kDataBlockSize);

            const ui64 counter64 = blockIndex * (kDataBlockSize / ChaCha::BLOCK_SIZE);
            chachaVariants[0].Encipher(kFixedKey, kFixedIv, counter64, block, scratch, kDataBlockSize);
            const ui64 refChachaDigest = xxhVariants[0].Hash(scratch, kDataBlockSize);

            for (size_t v = 1; v < chachaCount && mismatchMsg.empty(); ++v) {
                if (chachaVariants[v].IsSupported && !chachaVariants[v].IsSupported()) {
                    continue;
                }
                chachaVariants[v].Encipher(kFixedKey, kFixedIv, counter64, block, altScratch, kDataBlockSize);
                if (memcmp(altScratch, scratch, kDataBlockSize) != 0) {
                    mismatchMsg = Sprintf("chacha mismatch at block %llu: %s vs %s",
                        static_cast<unsigned long long>(blockIndex),
                        chachaVariants[0].Name, chachaVariants[v].Name);
                }
            }

            if (!mismatchMsg.empty()) {
                std::lock_guard<std::mutex> lock(shared.PrintMutex);
                if (!shared.Failed.exchange(true)) {
                    Cerr << "File write FAILED: " << mismatchMsg << "\n";
                }
                return;
            }

            TBlockRecord record{refXxh, city, refChachaDigest, blockIndex};
            memcpy(hashPage + r * kRecordSize, &record, sizeof(record));
            partialDigest ^= XXH3_64bits(&record, sizeof(record));
        }

        shared.GroupsDone.fetch_add(1, std::memory_order_relaxed);
    }

    *outPartialDigest = partialDigest;
}

// ---- Verifier (ai_func.md §8) ----

struct TVerifierSharedState {
    std::atomic<ui64> MismatchCount{0};
    std::atomic<ui64> BlocksVerified{0};
    std::atomic<bool> StopEarly{false};
    std::mutex PrintMutex;
    ui32 PrintedMismatches = 0; // guarded by PrintMutex
};

void ReportVerifyMismatch(TVerifierSharedState& shared, const TString& message) {
    const ui64 count = shared.MismatchCount.fetch_add(1, std::memory_order_relaxed) + 1;
    {
        std::lock_guard<std::mutex> lock(shared.PrintMutex);
        if (shared.PrintedMismatches < 10) {
            Cerr << "mismatch #" << count << ": " << message << "\n";
            ++shared.PrintedMismatches;
        }
    }
    if (count >= 1000) {
        shared.StopEarly.store(true, std::memory_order_relaxed);
    }
}

void VerifierThreadFunc(
    ui32 /*threadIdx*/,
    const ui8* mapping,
    ui64 groupStart, ui64 groupEnd,
    ui64 seed, bool verifyData,
    const THashVariant* xxhVariants, size_t xxhCount,
    const TCipherVariant* chachaVariants, size_t chachaCount,
    TVerifierSharedState& shared,
    ui64* outPartialDigest)
{
    TAlignedBuffer scratchBuf(kDataBlockSize);
    ui8* scratch = scratchBuf.Data();
    TVector<ui8> expectedData(kDataBlockSize);
    ui64 partialDigest = 0;

    for (ui64 g = groupStart; g < groupEnd; ++g) {
        if (shared.StopEarly.load(std::memory_order_relaxed)) {
            break;
        }
        const ui8* hashPage = mapping + kHeaderSize + g * kGroupSize;
        const ui8* dataArea = hashPage + kDataBlockSize;

        for (ui32 r = 0; r < kRecordsPerGroup; ++r) {
            const ui64 blockIndex = g * kRecordsPerGroup + r;
            const ui8* block = dataArea + r * kDataBlockSize;

            TBlockRecord record;
            memcpy(&record, hashPage + r * kRecordSize, sizeof(record));

            if (record.BlockIndex != blockIndex) {
                ReportVerifyMismatch(shared, Sprintf(
                    "block=%llu field=BlockIndex expected=%llu actual=%llu",
                    static_cast<unsigned long long>(blockIndex),
                    static_cast<unsigned long long>(blockIndex),
                    static_cast<unsigned long long>(record.BlockIndex)));
            }

            for (size_t v = 0; v < xxhCount; ++v) {
                if (xxhVariants[v].IsSupported && !xxhVariants[v].IsSupported()) {
                    continue;
                }
                const ui64 h = xxhVariants[v].Hash(block, kDataBlockSize);
                if (h != record.Xxh3) {
                    ReportVerifyMismatch(shared, Sprintf(
                        "block=%llu field=Xxh3 variant=%s expected=%s actual=%s",
                        static_cast<unsigned long long>(blockIndex), xxhVariants[v].Name,
                        Hex64(record.Xxh3).c_str(), Hex64(h).c_str()));
                }
            }

            const ui64 city = CityHash_v1_0_2::CityHash64(reinterpret_cast<const char*>(block), kDataBlockSize);
            if (city != record.City64) {
                ReportVerifyMismatch(shared, Sprintf(
                    "block=%llu field=City64 expected=%s actual=%s",
                    static_cast<unsigned long long>(blockIndex),
                    Hex64(record.City64).c_str(), Hex64(city).c_str()));
            }

            const ui64 counter64 = blockIndex * (kDataBlockSize / ChaCha::BLOCK_SIZE);
            for (size_t v = 0; v < chachaCount; ++v) {
                if (chachaVariants[v].IsSupported && !chachaVariants[v].IsSupported()) {
                    continue;
                }
                chachaVariants[v].Encipher(kFixedKey, kFixedIv, counter64, block, scratch, kDataBlockSize);
                const ui64 digest = xxhVariants[0].Hash(scratch, kDataBlockSize);
                if (digest != record.ChaChaDigest) {
                    ReportVerifyMismatch(shared, Sprintf(
                        "block=%llu field=ChaChaDigest variant=%s expected=%s actual=%s",
                        static_cast<unsigned long long>(blockIndex), chachaVariants[v].Name,
                        Hex64(record.ChaChaDigest).c_str(), Hex64(digest).c_str()));
                }
            }

            if (verifyData) {
                TSplitMix64 rng(seed ^ (blockIndex * 0xff51afd7ed558ccdULL));
                FillRandom(expectedData.data(), kDataBlockSize, rng);
                if (memcmp(expectedData.data(), block, kDataBlockSize) != 0) {
                    size_t diffIdx = kDataBlockSize;
                    for (size_t i = 0; i < kDataBlockSize; ++i) {
                        if (expectedData[i] != block[i]) {
                            diffIdx = i;
                            break;
                        }
                    }
                    ReportVerifyMismatch(shared, Sprintf(
                        "block=%llu field=RawData first_diff_byte=%zu",
                        static_cast<unsigned long long>(blockIndex), diffIdx));
                }
            }

            partialDigest ^= XXH3_64bits(&record, sizeof(record));
            shared.BlocksVerified.fetch_add(1, std::memory_order_relaxed);
        }
    }

    *outPartialDigest = partialDigest;
}

} // namespace

bool RunFileWrite(const TString& path, double outputSizeGiB, ui32 threads, ui64 seed) {
    if (outputSizeGiB <= 0) {
        Cerr << "--output-size must be positive\n";
        return false;
    }

    const ui64 groupCount = ComputeGroupCount(outputSizeGiB);
    const ui64 totalDataBlocks = groupCount * kRecordsPerGroup;
    const ui64 fileSize = kHeaderSize + groupCount * kGroupSize;

    Cout << "Writing " << path << ": " << groupCount << " groups, "
         << totalDataBlocks << " data blocks, " << fileSize << " bytes\n";

    TMappedFile file;
    if (!file.OpenForWrite(path, fileSize)) {
        return false;
    }

    size_t xxhCount = 0;
    const THashVariant* xxhVariants = GetXxh3Variants(&xxhCount);
    size_t chachaCount = 0;
    const TCipherVariant* chachaVariants = GetChaChaVariants(&chachaCount);
    if (xxhCount == 0 || chachaCount == 0) {
        Cerr << "File mode requires at least one xxh3 and one chacha variant\n";
        return false;
    }

    TWriterSharedState shared;
    TVector<std::thread> workers;
    TVector<ui64> partialDigests(threads, 0);
    std::atomic<bool> progressDone{false};

    std::thread progressThread([&]() {
        ui32 elapsedMs = 0;
        while (!progressDone.load(std::memory_order_relaxed)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            elapsedMs += 200;
            if (progressDone.load(std::memory_order_relaxed)) {
                break;
            }
            if (elapsedMs >= 5000) {
                elapsedMs = 0;
                Cout << "  progress: " << shared.GroupsDone.load(std::memory_order_relaxed)
                     << "/" << groupCount << " groups\n";
            }
        }
    });

    for (ui32 t = 0; t < threads; ++t) {
        const ui64 start = groupCount * t / threads;
        const ui64 end = groupCount * (t + 1) / threads;
        workers.emplace_back(WriterThreadFunc, t, file.Data(), start, end, seed,
            xxhVariants, xxhCount, chachaVariants, chachaCount,
            std::ref(shared), &partialDigests[t]);
    }
    for (auto& w : workers) {
        w.join();
    }
    progressDone.store(true, std::memory_order_relaxed);
    progressThread.join();

    if (shared.Failed.load()) {
        Cerr << "File write FAILED (cross-variant mismatch); file left on disk for inspection\n";
        return false;
    }

    THashFileHeader header{};
    memcpy(header.Magic, kMagic, sizeof(kMagic));
    header.Version = kFileVersion;
    header.DataBlockSize = static_cast<ui32>(kDataBlockSize);
    header.RecordSize = static_cast<ui32>(kRecordSize);
    header.RecordsPerGroup = static_cast<ui32>(kRecordsPerGroup);
    header.GroupCount = groupCount;
    header.TotalDataBlocks = totalDataBlocks;
    header.Seed = seed;
    header.HeaderHash = 0;
    header.HeaderHash = XXH3_64bits(&header, sizeof(header));

    memset(file.Data(), 0, kHeaderSize);
    memcpy(file.Data(), &header, sizeof(header));

    ui64 manifestDigest = 0;
    for (ui64 d : partialDigests) {
        manifestDigest ^= d;
    }

    file.Sync();

    Cout << "Write complete: " << groupCount << " groups, " << totalDataBlocks
         << " blocks, manifest digest: " << Hex64(manifestDigest) << "\n";
    return true;
}

bool RunFileVerify(const TString& path, ui32 threads, bool verifyData) {
    TMappedFile file;
    if (!file.OpenForRead(path)) {
        return false;
    }

    if (file.Size() < sizeof(THashFileHeader)) {
        Cerr << "File too small to contain a header\n";
        return false;
    }

    THashFileHeader header;
    memcpy(&header, file.Data(), sizeof(header));

    if (memcmp(header.Magic, kMagic, sizeof(kMagic)) != 0) {
        Cerr << "Bad magic: not a hash_test functional file\n";
        return false;
    }
    if (header.Version != kFileVersion) {
        Cerr << "Unsupported file version " << header.Version << "\n";
        return false;
    }
    if (header.RecordSize != kRecordSize || header.DataBlockSize != kDataBlockSize
        || header.RecordsPerGroup != kRecordsPerGroup)
    {
        Cerr << "Header field mismatch (record/block size differs from this build)\n";
        return false;
    }

    THashFileHeader headerCheck = header;
    const ui64 storedHash = headerCheck.HeaderHash;
    headerCheck.HeaderHash = 0;
    const ui64 computedHash = XXH3_64bits(&headerCheck, sizeof(headerCheck));
    if (computedHash != storedHash) {
        Cerr << "Header hash mismatch (file corrupted or truncated)\n";
        return false;
    }

    const ui64 expectedFileSize = kHeaderSize + header.GroupCount * kGroupSize;
    if (file.Size() != expectedFileSize) {
        Cerr << "File size mismatch: header implies " << expectedFileSize
             << " bytes, file is " << file.Size() << " bytes\n";
        return false;
    }

    size_t xxhCount = 0;
    const THashVariant* xxhVariants = GetXxh3Variants(&xxhCount);
    size_t chachaCount = 0;
    const TCipherVariant* chachaVariants = GetChaChaVariants(&chachaCount);
    if (xxhCount == 0 || chachaCount == 0) {
        Cerr << "File mode requires at least one xxh3 and one chacha variant\n";
        return false;
    }

    Cout << "Verifying " << path << ": " << header.GroupCount << " groups, "
         << header.TotalDataBlocks << " data blocks, seed=" << header.Seed << "\n";
    Cout << "  xxh3 variants on this machine:";
    for (size_t v = 0; v < xxhCount; ++v) {
        if (!xxhVariants[v].IsSupported || xxhVariants[v].IsSupported()) {
            Cout << ' ' << xxhVariants[v].Name;
        }
    }
    Cout << "\n  chacha variants on this machine:";
    for (size_t v = 0; v < chachaCount; ++v) {
        if (!chachaVariants[v].IsSupported || chachaVariants[v].IsSupported()) {
            Cout << ' ' << chachaVariants[v].Name;
        }
    }
    Cout << "\n";

    TVerifierSharedState shared;
    TVector<std::thread> workers;
    TVector<ui64> partialDigests(threads, 0);

    for (ui32 t = 0; t < threads; ++t) {
        const ui64 start = header.GroupCount * t / threads;
        const ui64 end = header.GroupCount * (t + 1) / threads;
        workers.emplace_back(VerifierThreadFunc, t, file.Data(), start, end,
            header.Seed, verifyData, xxhVariants, xxhCount, chachaVariants, chachaCount,
            std::ref(shared), &partialDigests[t]);
    }
    for (auto& w : workers) {
        w.join();
    }

    ui64 manifestDigest = 0;
    for (ui64 d : partialDigests) {
        manifestDigest ^= d;
    }

    const ui64 mismatches = shared.MismatchCount.load();
    Cout << "Verify complete: " << shared.BlocksVerified.load() << " blocks checked, "
         << mismatches << " mismatches, manifest digest: " << Hex64(manifestDigest) << "\n";

    if (shared.StopEarly.load()) {
        Cerr << "Stopped early: mismatch count exceeded 1000 (file likely corrupt or incompatible)\n";
    }

    return mismatches == 0;
}

#else // defined(_win_)

bool RunFileWrite(const TString&, double, ui32, ui64) {
    Cerr << "File mode is not supported on this platform\n";
    return false;
}

bool RunFileVerify(const TString&, ui32, bool) {
    Cerr << "File mode is not supported on this platform\n";
    return false;
}

#endif
