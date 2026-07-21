#include <ydb/core/blobstorage/incrhuge/incrhuge_keeper_log.h>
#include <ydb/core/protos/blobstorage_vdisk_internal.pb.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <tuple>

namespace {

using namespace NKikimr;
using namespace NKikimr::NIncrHuge;

constexpr ui32 MaxOps = 256;
constexpr ui32 MaxChunks = 64;
constexpr ui32 MaxItems = 64;

struct TChunkModel {
    TChunkSerNum SerNum;
    NKikimrVDiskData::TIncrHugeChunks::EChunkState State;
};

using TChunkState = TMap<TChunkIdx, std::pair<ui64, int>>;
using TDeleteState = TMap<ui64, TVector<ui32>>;
using TOwnerState = TMap<ui32, ui64>;

TChunkState NormalizeChunks(const NKikimrVDiskData::TIncrHugeChunks& record) {
    TChunkState state;
    for (const auto& chunk : record.GetChunks()) {
        Y_ABORT_UNLESS(chunk.HasChunkIdx() && chunk.HasChunkSerNum() && chunk.HasState());
        state[chunk.GetChunkIdx()] = {chunk.GetChunkSerNum(), chunk.GetState()};
    }
    return state;
}

TDynBitMap DecodeDeletes(const NKikimrVDiskData::TIncrHugeDelete::TChunkInfo& chunk) {
    TDynBitMap deleted;
    switch (chunk.DeletedData_case()) {
        case NKikimrVDiskData::TIncrHugeDelete::TChunkInfo::kDeletedItems:
            for (ui32 index : chunk.GetDeletedItems().GetIndexes()) {
                deleted.Set(index);
            }
            for (const auto& range : chunk.GetDeletedItems().GetRanges()) {
                Y_ABORT_UNLESS(range.HasFirst() && range.HasCount());
                deleted.Set(range.GetFirst(), range.GetFirst() + range.GetCount());
            }
            break;
        case NKikimrVDiskData::TIncrHugeDelete::TChunkInfo::kBits: {
            TStringStream stream(chunk.GetBits());
            deleted.Load(&stream);
            break;
        }
        default:
            Y_ABORT("unexpected delete bitmap encoding");
    }
    return deleted;
}

TDeleteState NormalizeDeletes(const NKikimrVDiskData::TIncrHugeDelete& record) {
    TDeleteState state;
    for (const auto& chunk : record.GetChunks()) {
        Y_ABORT_UNLESS(chunk.HasChunkSerNum());
        TDynBitMap bitmap = DecodeDeletes(chunk);
        TVector<ui32>& indexes = state[chunk.GetChunkSerNum()];
        Y_FOR_EACH_BIT(index, bitmap) {
            indexes.push_back(index);
        }
    }
    return state;
}

TOwnerState NormalizeOwners(const NKikimrVDiskData::TIncrHugeDelete& record) {
    TOwnerState state;
    for (const auto& owner : record.GetOwners()) {
        Y_ABORT_UNLESS(owner.HasOwner() && owner.HasSeqNo());
        state[owner.GetOwner()] = owner.GetSeqNo();
    }
    return state;
}

void CheckChunkState(const TChunkRecordMerger& direct, const TChunkRecordMerger& replay) {
    Y_ABORT_UNLESS(NormalizeChunks(direct.GetCurrentState()) == NormalizeChunks(replay.GetCurrentState()));
}

void CheckDeleteState(const TDeleteRecordMerger& direct, const TDeleteRecordMerger& replay) {
    const auto directState = direct.GetCurrentState();
    const auto replayState = replay.GetCurrentState();
    Y_ABORT_UNLESS(NormalizeDeletes(directState) == NormalizeDeletes(replayState));
    Y_ABORT_UNLESS(NormalizeOwners(directState) == NormalizeOwners(replayState));
}

template <class TRecord>
void ApplySerializedChunk(const TRecord& op, TChunkRecordMerger& replay) {
    const TString data = TChunkRecordMerger::Serialize(op);
    NKikimrVDiskData::TIncrHugeChunks parsed;
    Y_ABORT_UNLESS(parsed.ParseFromString(data));
    replay(parsed);
}

void ApplySerializedDelete(const TDeleteRecordMerger::TBlobDeletes& op, TDeleteRecordMerger& replay) {
    const TString data = TDeleteRecordMerger::Serialize(op);
    NKikimrVDiskData::TIncrHugeDelete parsed;
    Y_ABORT_UNLESS(parsed.ParseFromString(data));
    replay(parsed);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);

    TChunkRecordMerger directChunks;
    TChunkRecordMerger replayChunks;
    TDeleteRecordMerger directDeletes;
    TDeleteRecordMerger replayDeletes;

    THashMap<TChunkIdx, TChunkModel> chunks;
    THashMap<TChunkSerNum, TDynBitMap> deleted;
    THashMap<ui8, ui64> ownerSeqNo;
    TChunkSerNum nextSerNum(1000);

    const ui32 ops = provider.ConsumeIntegralInRange<ui32>(0, MaxOps);
    for (ui32 i = 0; i < ops && provider.remaining_bytes(); ++i) {
        switch (provider.ConsumeIntegralInRange<ui8>(0, 5)) {
            case 0: {
                TVector<TChunkIdx> newChunks;
                const ui32 count = provider.ConsumeIntegralInRange<ui32>(1, 4);
                for (ui32 j = 0; j < count; ++j) {
                    const TChunkIdx chunkIdx = provider.ConsumeIntegralInRange<TChunkIdx>(1, MaxChunks);
                    if (!chunks.contains(chunkIdx) && Find(newChunks.begin(), newChunks.end(), chunkIdx) == newChunks.end()) {
                        newChunks.push_back(chunkIdx);
                    }
                }
                if (newChunks.empty()) {
                    break;
                }
                const TChunkSerNum baseSerNum = nextSerNum;
                nextSerNum.Advance(newChunks.size());
                TChunkRecordMerger::TChunkAllocation op{newChunks, baseSerNum, nextSerNum};
                directChunks(op);
                ApplySerializedChunk(op, replayChunks);
                for (size_t j = 0; j < newChunks.size(); ++j) {
                    chunks[newChunks[j]] = {baseSerNum.Add(j), NKikimrVDiskData::TIncrHugeChunks::WriteIntent};
                }
                break;
            }

            case 1: {
                TVector<TChunkIdx> candidates;
                for (const auto& [chunkIdx, chunk] : chunks) {
                    if (chunk.State == NKikimrVDiskData::TIncrHugeChunks::WriteIntent) {
                        candidates.push_back(chunkIdx);
                    }
                }
                if (candidates.empty()) {
                    break;
                }
                const TChunkIdx chunkIdx = candidates[provider.ConsumeIntegralInRange<size_t>(0, candidates.size() - 1)];
                auto& chunk = chunks[chunkIdx];
                TChunkRecordMerger::TCompleteChunk op{chunkIdx, chunk.SerNum};
                directChunks(op);
                ApplySerializedChunk(op, replayChunks);
                chunk.State = NKikimrVDiskData::TIncrHugeChunks::Complete;
                break;
            }

            case 2: {
                if (chunks.empty()) {
                    break;
                }
                const ui32 index = provider.ConsumeIntegralInRange<ui32>(0, chunks.size() - 1);
                auto it = chunks.begin();
                std::advance(it, index);
                const TChunkIdx chunkIdx = it->first;
                const TChunkSerNum serNum = it->second.SerNum;
                TChunkRecordMerger::TChunkDeletion op{chunkIdx, serNum, provider.ConsumeIntegralInRange<ui32>(0, MaxItems)};
                directChunks(op);
                ApplySerializedChunk(op, replayChunks);
                chunks.erase(it);
                deleted.erase(serNum);
                break;
            }

            case 3: {
                if (chunks.empty()) {
                    break;
                }
                TVector<TBlobDeleteLocator> locators;
                const ui32 count = provider.ConsumeIntegralInRange<ui32>(1, 8);
                for (ui32 j = 0; j < count; ++j) {
                    const ui32 chunkPos = provider.ConsumeIntegralInRange<ui32>(0, chunks.size() - 1);
                    auto it = chunks.begin();
                    std::advance(it, chunkPos);
                    const ui32 item = provider.ConsumeIntegralInRange<ui32>(0, MaxItems - 1);
                    TDynBitMap& bitmap = deleted[it->second.SerNum];
                    if (!bitmap.Get(item)) {
                        bitmap.Set(item);
                        locators.push_back(TBlobDeleteLocator{it->first, it->second.SerNum, provider.ConsumeIntegral<ui64>(), item, 1});
                    }
                }
                if (locators.empty()) {
                    break;
                }
                Sort(locators);
                const ui8 owner = provider.ConsumeIntegralInRange<ui8>(0, 15);
                ui64 seqNo = 0;
                if (owner) {
                    seqNo = ownerSeqNo[owner] + 1 + provider.ConsumeIntegralInRange<ui64>(0, 8);
                    ownerSeqNo[owner] = seqNo;
                }
                TDeleteRecordMerger::TBlobDeletes op{owner, seqNo, locators};
                directDeletes(op);
                ApplySerializedDelete(op, replayDeletes);
                break;
            }

            case 4: {
                TVector<TChunkSerNum> candidates;
                for (const auto& [serNum, bitmap] : deleted) {
                    bool prefix = true;
                    ui32 count = 0;
                    while (count < MaxItems && bitmap.Get(count)) {
                        ++count;
                    }
                    Y_FOR_EACH_BIT(index, bitmap) {
                        if (index >= count) {
                            prefix = false;
                            break;
                        }
                    }
                    if (prefix && count > 0) {
                        candidates.push_back(serNum);
                    }
                }
                if (candidates.empty()) {
                    break;
                }
                const TChunkSerNum serNum = candidates[provider.ConsumeIntegralInRange<size_t>(0, candidates.size() - 1)];
                const ui32 numItems = deleted[serNum].Count();
                TDeleteRecordMerger::TDeleteChunk op{serNum, numItems};
                directDeletes(op);
                replayDeletes(op);
                deleted.erase(serNum);
                break;
            }

            case 5: {
                NKikimrVDiskData::TIncrHugeChunks state = directChunks.GetCurrentState();
                directChunks(state);
                ApplySerializedChunk(state, replayChunks);
                break;
            }
        }

        CheckChunkState(directChunks, replayChunks);
        CheckDeleteState(directDeletes, replayDeletes);
    }

    return 0;
}
