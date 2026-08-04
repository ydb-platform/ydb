#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_ut_env.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

namespace {

using namespace NKikimr;
using namespace NKikimr::NPDisk;

constexpr ui32 MaxOps = 96;
constexpr ui32 MaxTrackedChunks = 32;
constexpr ui32 MinWriteSize = 1024;
constexpr ui32 MaxWriteSize = 8 * 1024;

struct TFuzzChunkState {
    TChunkIdx Chunk = 0;
    TString Data;
};

TString MakePayload(FuzzedDataProvider& provider) {
    const ui32 size = provider.ConsumeIntegralInRange<ui32>(MinWriteSize, MaxWriteSize);
    TString data = TString::Uninitialized(size);
    char* out = data.Detach();
    const ui8 salt = provider.ConsumeIntegral<ui8>();
    for (ui32 i = 0; i < size; ++i) {
        out[i] = static_cast<char>(salt + i * 17);
    }
    return data;
}

void ClearFaults(TActorTestContext& ctx) {
    auto* probs = ctx.TestCtx.SectorMap->GetFailureProbabilities();
    probs->ReadErrorProbability = 0.0;
    probs->WriteErrorProbability = 0.0;
    probs->SilentWriteFailProbability = 0.0;
    probs->ReadReplayProbability = 0.0;
    ctx.TestCtx.SectorMap->IoErrorEveryNthRequests = 0;
    ctx.TestCtx.SectorMap->ReadIoErrorEveryNthRequests = 0;
}

void SetReadCorruption(TActorTestContext& ctx, ui8 mode) {
    ClearFaults(ctx);
    auto* probs = ctx.TestCtx.SectorMap->GetFailureProbabilities();
    switch (mode % 4) {
        case 0:
            probs->ReadReplayProbability = 0.25;
            break;
        case 1:
            probs->ReadErrorProbability = 0.10;
            break;
        case 2:
            ctx.TestCtx.SectorMap->ReadIoErrorEveryNthRequests = 3;
            break;
        case 3:
            break;
    }
}

void InitAfterRestart(TActorTestContext& ctx, TVDiskMock& vdisk) {
    ClearFaults(ctx);
    ctx.RestartPDiskSync();
    vdisk.Init();
    vdisk.ReadLog(true);
}

TVector<TChunkIdx> CommittedChunks(const TVDiskMock& vdisk) {
    TVector<TChunkIdx> chunks;
    auto it = vdisk.Chunks.find(EChunkState::COMMITTED);
    if (it != vdisk.Chunks.end()) {
        chunks.assign(it->second.begin(), it->second.end());
    }
    return chunks;
}

void WriteChunk(TActorTestContext& ctx, TVDiskMock& vdisk, THashMap<TChunkIdx, TFuzzChunkState>& model,
        FuzzedDataProvider& provider)
{
    const TVector<TChunkIdx> chunks = CommittedChunks(vdisk);
    if (chunks.empty()) {
        return;
    }
    const TChunkIdx chunk = chunks[provider.ConsumeIntegralInRange<size_t>(0, chunks.size() - 1)];
    TString data = MakePayload(provider);
    auto parts = MakeIntrusive<TEvChunkWrite::TAlignedParts>(TString(data));
    auto res = ctx.TestResponse<TEvChunkWriteResult>(new TEvChunkWrite(vdisk.PDiskParams->Owner,
        vdisk.PDiskParams->OwnerRound, chunk, 0, parts, nullptr, false, 0));
    if (res->Status == NKikimrProto::OK) {
        model[chunk] = {chunk, std::move(data)};
    }
}

void ReadChunk(TActorTestContext& ctx, TVDiskMock& vdisk, const THashMap<TChunkIdx, TFuzzChunkState>& model,
        FuzzedDataProvider& provider)
{
    if (model.empty()) {
        return;
    }
    auto it = model.begin();
    std::advance(it, provider.ConsumeIntegralInRange<size_t>(0, model.size() - 1));
    const TFuzzChunkState& expected = it->second;
    auto res = ctx.TestResponse<TEvChunkReadResult>(new TEvChunkRead(vdisk.PDiskParams->Owner,
        vdisk.PDiskParams->OwnerRound, expected.Chunk, 0, expected.Data.size(), 0, nullptr));
    if (res->Status == NKikimrProto::OK) {
        Y_ABORT_UNLESS(res->Data.ToString() == expected.Data);
    }
}

void RecoverAndCheck(TActorTestContext& ctx, TVDiskMock& vdisk, const THashMap<TChunkIdx, TFuzzChunkState>& model,
        FuzzedDataProvider& provider)
{
    SetReadCorruption(ctx, provider.ConsumeIntegral<ui8>());
    vdisk.ReadLog(true);
    InitAfterRestart(ctx, vdisk);
    for (const auto& [chunk, expected] : model) {
        Y_UNUSED(chunk);
        auto res = ctx.TestResponse<TEvChunkReadResult>(new TEvChunkRead(vdisk.PDiskParams->Owner,
            vdisk.PDiskParams->OwnerRound, expected.Chunk, 0, expected.Data.size(), 0, nullptr));
        if (res->Status == NKikimrProto::OK) {
            Y_ABORT_UNLESS(res->Data.ToString() == expected.Data);
        }
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);

    TActorTestContext::TSettings settings;
    settings.UseSectorMap = true;
    settings.DiskSize = ui64(MIN_CHUNK_SIZE) * 256;
    settings.ChunkSize = MIN_CHUNK_SIZE;
    settings.SmallDisk = true;
    settings.PlainDataChunks = provider.ConsumeBool();
    settings.EnableFormatAndMetadataEncryption = false;
    settings.EnableSectorEncryption = false;
    settings.RandomizeMagic = false;

    TActorTestContext ctx(settings);
    TVDiskMock vdisk(&ctx);
    vdisk.InitFull(1);

    THashMap<TChunkIdx, TFuzzChunkState> model;

    const ui32 ops = provider.ConsumeIntegralInRange<ui32>(1, MaxOps);
    for (ui32 i = 0; i < ops && provider.remaining_bytes(); ++i) {
        ClearFaults(ctx);
        switch (provider.ConsumeIntegralInRange<ui8>(0, 7)) {
            case 0:
                if (vdisk.Chunks[EChunkState::RESERVED].size() + vdisk.Chunks[EChunkState::COMMITTED].size()
                        < MaxTrackedChunks) {
                    vdisk.ReserveChunk(provider.ConsumeIntegralInRange<ui32>(1, 2));
                }
                break;
            case 1:
                if (!vdisk.Chunks[EChunkState::RESERVED].empty()) {
                    vdisk.CommitReservedChunks();
                }
                break;
            case 2:
                vdisk.SendEvLogSync(provider.ConsumeIntegralInRange<ui64>(1, 4096));
                break;
            case 3:
                WriteChunk(ctx, vdisk, model, provider);
                break;
            case 4:
                ReadChunk(ctx, vdisk, model, provider);
                break;
            case 5:
                InitAfterRestart(ctx, vdisk);
                break;
            case 6:
                RecoverAndCheck(ctx, vdisk, model, provider);
                break;
            case 7:
                if (!vdisk.Chunks[EChunkState::COMMITTED].empty() && provider.ConsumeBool()) {
                    for (TChunkIdx chunk : vdisk.Chunks[EChunkState::COMMITTED]) {
                        model.erase(chunk);
                    }
                    vdisk.MarkCommitedChunksDirty();
                    vdisk.DeleteCommitedChunks();
                }
                break;
        }
    }

    ClearFaults(ctx);
    InitAfterRestart(ctx, vdisk);
    for (const auto& [_, expected] : model) {
        auto res = ctx.TestResponse<TEvChunkReadResult>(new TEvChunkRead(vdisk.PDiskParams->Owner,
            vdisk.PDiskParams->OwnerRound, expected.Chunk, 0, expected.Data.size(), 0, nullptr));
        if (res->Status == NKikimrProto::OK) {
            Y_ABORT_UNLESS(res->Data.ToString() == expected.Data);
        }
    }

    return 0;
}
