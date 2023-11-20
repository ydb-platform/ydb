#include "basic_test.h"
#include "objectwithstate.h"
#include "state_manager.h"
#include <util/generic/set.h>
#include <util/generic/bitmap.h>
#include <util/system/unaligned_mem.h>
#include <library/cpp/digest/crc32c/crc32c.h>

class TFakeVDisk
    : public TActor<TFakeVDisk>
    , public TObjectWithState
{
    const TVDiskID VDiskId;
    const TActorId PDiskServiceId;
    const ui64 PDiskGuid;
    TStateManager *StateManager;
    TIntrusivePtr<TPDiskParams> PDiskParams;
    TFakeVDiskParams Params;

    NKikimr::NPDisk::TLogPosition ReadLogPosition{0, 0};

    ui64 Lsn = 0;

    struct TLogRecord {
        ui64 Lsn;
        TLogSignature Signature;
        ui32 DataLen;
        ui32 Checksum;
        TVector<TChunkIdx> CommitChunks;
        TVector<TChunkIdx> DeleteChunks;

        friend bool operator <(const TLogRecord& x, const TLogRecord& y) {
            return x.Lsn < y.Lsn;
        }
    };

    struct TWriteRecord {
        TChunkIdx ChunkIdx;
        ui32 OffsetInBlocks;
        ui32 SizeInBlocks;
        TVector<ui32> Checksums;
        void *Cookie;
    };

    enum class ECommitState {
        RESERVED,
        COMMIT_IN_PROGRESS,
        COMMITTED,
        DELETE_IN_PROGRESS,
        DELETED,
    };

    struct TState {
        class TChunkInfo {
            TDynBitMap UsedBlocks;
            TVector<ui32> Checksums;
            ECommitState CommitState = ECommitState::RESERVED;

        public:
            TChunkInfo() = delete;

            TChunkInfo(const TChunkInfo& other) = default;
            TChunkInfo(TChunkInfo&& other) = default;

            TChunkInfo(ui32 numBlocks) {
                Y_ABORT_UNLESS(numBlocks > 0);
                UsedBlocks.Reserve(numBlocks);
                UsedBlocks.Reset(0, numBlocks);
                Checksums.resize(numBlocks);
            }

            TChunkInfo(ui32 numBlocks, const NPDiskFIT::TFakeVDiskState::TChunk& pb)
                : TChunkInfo(numBlocks)
            {
                Y_ABORT_UNLESS(pb.HasCommitState());
                for (const auto& block : pb.GetBlocks()) {
                    Y_ABORT_UNLESS(block.HasIndex());
                    Y_ABORT_UNLESS(block.HasChecksum());
                    const ui32 index = block.GetIndex();
                    Y_ABORT_UNLESS(index < numBlocks);
                    UsedBlocks.Set(index);
                    Checksums[index] = block.GetChecksum();
                }
                CommitState = static_cast<ECommitState>(pb.GetCommitState());
            }

            void SetChecksums(ui32 index, const TVector<ui32>& checksums) {
                const ui32 num = checksums.size();
                Y_ABORT_UNLESS(index + num <= Checksums.size() && index + num <= UsedBlocks.Size());
                UsedBlocks.Set(index, index + num);
                std::copy(checksums.begin(), checksums.end(), Checksums.begin() + index);
            }

            ui32 GetChecksum(ui32 index) const {
                Y_ABORT_UNLESS(index < Checksums.size());
                return Checksums[index];
            }

            bool VerifyChecksum(ui32 index, ui32 checksum) const {
                Y_ABORT_UNLESS(index < Checksums.size());
                return UsedBlocks[index] && Checksums[index] == checksum;
            }

            bool IsUsed(ui32 index) const {
                return UsedBlocks[index];
            }

            void SerializeToProto(NPDiskFIT::TFakeVDiskState::TChunk& pb) const {
                Y_FOR_EACH_BIT(index, UsedBlocks) {
                    Y_ABORT_UNLESS(index < Checksums.size(), "index# %zu +Checksums# %zu UsedBlocks# %zu", index, Checksums.size(),
                            UsedBlocks.Size());
                    auto& block = *pb.AddBlocks();
                    block.SetIndex(index);
                    block.SetChecksum(Checksums[index]);
                }
                pb.SetCommitState(static_cast<ui32>(CommitState));
            }

            ECommitState GetCommitState() const {
                return CommitState;
            }

            void SetCommitState(ECommitState x) {
                CommitState = x;
            }
        };

        TSet<TLogRecord> Confirmed;
        TSet<TLogRecord> InFlight;
        ui64 FirstLsnToKeep = 0;
        TMap<TChunkIdx, TChunkInfo> Chunks;
        TList<TWriteRecord> WritesInFlight;
        ui32 BlocksInChunk = 0;

        TString ToString() const {
            TStringStream str;
            str << "{Confirmed# [";
            bool first = true;
            for (const TLogRecord& x : Confirmed) {
                if (first) {
                    first = false;
                } else {
                    str << " ";
                }
                str << x.Lsn << ":" << x.Signature.ToString() << ":" << x.DataLen;
            }
            str << "] InFlight# [";
            first = true;
            for (const TLogRecord& x : InFlight) {
                if (first) {
                    first = false;
                } else {
                    str << " ";
                }
                str << x.Lsn << ":" << x.Signature.ToString() << ":" << x.DataLen;
            }
            str << "] FirstLsnToKeep# " << FirstLsnToKeep;
            str << " Chunks# {";
            first = true;
            for (const auto& [chunkIdx, chunk] : Chunks) {
                if (first) {
                    first = false;
                } else {
                    str << ' ';
                }
                str << chunkIdx << ":";
                switch (chunk.GetCommitState()) {
                    case ECommitState::RESERVED:
                        str << 'R';
                        break;

                    case ECommitState::COMMIT_IN_PROGRESS:
                        str << 'c';
                        break;

                    case ECommitState::COMMITTED:
                        str << 'C';
                        break;

                    case ECommitState::DELETE_IN_PROGRESS:
                        str << 'd';
                        break;

                    case ECommitState::DELETED:
                        Y_ABORT();
                }
            }
            str << '}';
            str << '}';
            return str.Str();
        }

        TChunkInfo& GetChunk(TChunkIdx chunkIdx) {
            auto it = Chunks.find(chunkIdx);
            Y_ABORT_UNLESS(it != Chunks.end());
            return it->second;
        }

        const TChunkInfo& GetChunk(TChunkIdx chunkIdx) const {
            auto it = Chunks.find(chunkIdx);
            Y_ABORT_UNLESS(it != Chunks.end());
            return it->second;
        }

        void SerializeToProto(NPDiskFIT::TFakeVDiskState& pb) const {
            for (const auto& item : Confirmed) {
                Serialize(item, *pb.AddLogItems());
            }
            for (const auto& item : InFlight) {
                Serialize(item, *pb.AddInFlightItems());
            }
            pb.SetFirstLsnToKeep(FirstLsnToKeep);
            pb.SetBlocksInChunk(BlocksInChunk);
            for (const auto& pair : Chunks) {
                auto& chunk = *pb.AddChunks();
                chunk.SetChunkIdx(pair.first);
                pair.second.SerializeToProto(chunk);
            }
            for (const TWriteRecord& write : WritesInFlight) {
                auto& pbItem = *pb.AddWritesInFlight();
                pbItem.SetChunkIdx(write.ChunkIdx);
                pbItem.SetOffsetInBlocks(write.OffsetInBlocks);
                pbItem.SetSizeInBlocks(write.SizeInBlocks);
                for (ui32 checksum : write.Checksums) {
                    pbItem.AddChecksums(checksum);
                }
            }
        }

        void DeserializeFromProto(const NPDiskFIT::TFakeVDiskState& pb) {
            for (const auto& pbItem : pb.GetLogItems()) {
                TLogRecord item;
                Deserialize(pbItem, item);
                Confirmed.insert(item);
            }
            for (const auto& pbItem : pb.GetInFlightItems()) {
                TLogRecord item;
                Deserialize(pbItem, item);
                InFlight.insert(item);
            }
            FirstLsnToKeep = pb.GetFirstLsnToKeep();

            // extract number of blocks per single chunk
            Y_ABORT_UNLESS(pb.HasBlocksInChunk());
            BlocksInChunk = pb.GetBlocksInChunk();

            for (const auto& pbItem : pb.GetChunks()) {
                Y_ABORT_UNLESS(pbItem.HasChunkIdx());
                const TChunkIdx chunkIdx = pbItem.GetChunkIdx();
                Chunks.emplace(chunkIdx, TChunkInfo(BlocksInChunk, pbItem));
            }
            for (const auto& pbItem : pb.GetWritesInFlight()) {
                Y_ABORT_UNLESS(pbItem.HasChunkIdx());
                Y_ABORT_UNLESS(pbItem.HasOffsetInBlocks());
                Y_ABORT_UNLESS(pbItem.HasSizeInBlocks());
                Y_ABORT_UNLESS(pbItem.ChecksumsSize() == pbItem.GetSizeInBlocks());
                TWriteRecord write;
                write.ChunkIdx = pbItem.GetChunkIdx();
                write.OffsetInBlocks = pbItem.GetOffsetInBlocks();
                write.SizeInBlocks = pbItem.GetSizeInBlocks();
                write.Checksums = {pbItem.GetChecksums().begin(), pbItem.GetChecksums().end()};
                WritesInFlight.push_back(std::move(write));
            }
        }

    private:
        static void Serialize(const TLogRecord& item, NPDiskFIT::TFakeVDiskState::TLogItem& pbItem) {
            pbItem.SetLsn(item.Lsn);
            pbItem.SetSignature(item.Signature);
            pbItem.SetDataLen(item.DataLen);
            pbItem.SetChecksum(item.Checksum);
            for (TChunkIdx chunk : item.CommitChunks) {
                pbItem.AddCommitChunks(chunk);
            }
            for (TChunkIdx chunk : item.DeleteChunks) {
                pbItem.AddDeleteChunks(chunk);
            }
        }

        static void Deserialize(const NPDiskFIT::TFakeVDiskState::TLogItem& pbItem, TLogRecord& item) {
            Y_ABORT_UNLESS(pbItem.HasLsn());
            Y_ABORT_UNLESS(pbItem.HasSignature());
            Y_ABORT_UNLESS(pbItem.HasDataLen());
            Y_ABORT_UNLESS(pbItem.HasChecksum());
            item.Lsn = pbItem.GetLsn();
            item.Signature = pbItem.GetSignature();
            item.DataLen = pbItem.GetDataLen();
            item.Checksum = pbItem.GetChecksum();
            item.CommitChunks = {pbItem.GetCommitChunks().begin(), pbItem.GetCommitChunks().end()};
            item.DeleteChunks = {pbItem.GetDeleteChunks().begin(), pbItem.GetDeleteChunks().end()};
        }
    };

    const TState Recovered;

    TState State;

    ui32 InFlightLog = 0;

    ui32 LogsSent = 0;

    ui64 NextWriteCookie = 1;

    ui32 ReadMsgPending = 0;

    bool StateVerified = false;

    std::vector<ui32> ChunksToForget;
    THashSet<ui32> OwnedChunks;

public:
    TFakeVDisk(const TVDiskID& vdiskId, const TActorId& pdiskServiceId, ui64 pdiskGuid, TStateManager *stateManager,
            TFakeVDiskParams params)
        : TActor<TFakeVDisk>(&TFakeVDisk::StateFunc)
        , TObjectWithState(Sprintf("vdisk[%s]", vdiskId.ToString().data()))
        , VDiskId(vdiskId)
        , PDiskServiceId(pdiskServiceId)
        , PDiskGuid(pdiskGuid)
        , StateManager(stateManager)
        , Params(params)
        , Recovered(DeserializeRecoveredState())
    {
//        TStringStream str;
//        str << "VDiskId# " << vdiskId.ToString() << " Recovered# " << Recovered.ToString() << Endl;
//        Cerr << str.Str();

        // actually register this VDisk after all initialization procedures are finished
        TObjectWithState::Register();
    }

    ~TFakeVDisk() {
    }

    TState DeserializeRecoveredState() {
        TState recovered;

        if (TString state = GetState()) {
            NPDiskFIT::TFakeVDiskState pb;
            bool status = pb.ParseFromString(state);
            Y_ABORT_UNLESS(status);
            recovered.DeserializeFromProto(pb);
        }

        return recovered;
    }

    template<typename TFunc>
    void SendPDiskRequest(const TActorContext& ctx, IEventBase *msg, TFunc&& stateUpdate) {
        // we must execute this action consistently with state manager because if this action is executed after generating
        // failure condition, then state may become inconsistent, because PDisk may actually execute this request after
        // state collect
        StateManager->ExecuteConsistentAction([&] {
            ctx.Send(PDiskServiceId, msg);
            stateUpdate();
        });
    }

    TString SelfInfo() const {
        return TStringBuilder() << "VDiskId# " << VDiskId.ToStringWOGeneration() << " Owner# " << PDiskParams->Owner;
    }

    void Bootstrap(const TActorContext& ctx) {
        Become(&TFakeVDisk::StateFunc);
        SendPDiskRequest(ctx, new NPDisk::TEvYardInit(2, VDiskId, PDiskGuid), [] {});
    }

    void Handle(NPDisk::TEvYardInitResult::TPtr& ev, const TActorContext& ctx) {
        auto *msg = ev->Get();
        Y_ABORT_UNLESS(msg->Status == NKikimrProto::OK);
        PDiskParams = msg->PDiskParams;
        State.BlocksInChunk = PDiskParams->ChunkSize / PDiskParams->AppendBlockSize;

        TStringStream str;
        str << SelfInfo() << " starting, owned chunks# " << FormatList(msg->OwnedChunks) << Endl;
        Cerr << str.Str();

        OwnedChunks = {msg->OwnedChunks.begin(), msg->OwnedChunks.end()};
        IssueReadLogRequest(ctx);
    }

    void VerifyOwnedChunks(const TActorContext& /*ctx*/) {
        for (const auto& [idx, info] : Recovered.Chunks) {
            if (info.GetCommitState() == ECommitState::COMMITTED) {
                Y_VERIFY_S(OwnedChunks.count(idx), SelfInfo() << " has commited chunk# " << idx << " from Recovered.Chunks,"
                        << " but can't find it in OwnedChunks list from PDisk");
                OwnedChunks.erase(idx);
            }
        }
        for (auto idx : OwnedChunks) {
            auto it = Recovered.Chunks.find(idx);
            Y_VERIFY_S(it != Recovered.Chunks.end(), SelfInfo() << " has owned chunk# " << idx
                    << " from PDisks's OwnedChunks, but can't find it in Recovered list"
                    << " Recovered# " << Recovered.ToString());

            auto& info = it->second;
            Y_ABORT_UNLESS(info.GetCommitState() == ECommitState::COMMIT_IN_PROGRESS ||
                info.GetCommitState() == ECommitState::DELETE_IN_PROGRESS);
        }
    }

    void IssueReadLogRequest(const TActorContext& ctx) {
        SendPDiskRequest(ctx, new NPDisk::TEvReadLog(PDiskParams->Owner, PDiskParams->OwnerRound, ReadLogPosition), [] {});
    }

    void Handle(NPDisk::TEvReadLogResult::TPtr& ev, const TActorContext& ctx) {
        auto *msg = ev->Get();
        Y_ABORT_UNLESS(msg->Status == NKikimrProto::OK);
        Y_ABORT_UNLESS(msg->Position == ReadLogPosition);

        for (const auto& item : msg->Results) {
            Y_ABORT_UNLESS(Lsn < item.Lsn);
            Lsn = item.Lsn;

            TStringStream str;
            str << "TEvReadLogResult " << SelfInfo() << " Lsn# " << item.Lsn << " Len# " << item.Data.size()
                << " ChunkCommitSignature# " << (item.Signature.HasCommitRecord() ? "true" : "false")
                << Endl;
            Cerr << str.Str();

            State.Confirmed.insert(TLogRecord{item.Lsn, item.Signature, (ui32)item.Data.size(),
                    Crc32c(item.Data.data(), item.Data.size()), {}, {}});
        }

        if (msg->IsEndOfLog) {
            VerifyRecoveredLog(ctx);
            VerifyOwnedChunks(ctx);
        } else {
            ReadLogPosition = msg->NextPosition;
            IssueReadLogRequest(ctx);
        }
    }

    void ProcessItem(const TLogRecord *current, const TLogRecord *prev) {
        if (current && prev) {
            Y_ABORT_UNLESS(current->Lsn == prev->Lsn);
            Y_ABORT_UNLESS(current->Signature == prev->Signature);
            Y_ABORT_UNLESS(current->DataLen == prev->DataLen);
            Y_ABORT_UNLESS(current->Checksum == prev->Checksum);
        } else if (current) {
            // there is a record in current set (recovered from PDisk), but not in previous set (stored state); there
            // may be an item in flight?
            auto it = Recovered.InFlight.find(*current);
            Y_VERIFY_S(it != Recovered.InFlight.end() || current->Lsn < Recovered.FirstLsnToKeep,
                "unexpected log record " << SelfInfo() << " Lsn# " << current->Lsn
                << " Signature# " << current->Signature.ToString());
            if (it != Recovered.InFlight.end()) {
                Y_VERIFY_S(it->DataLen == current->DataLen && it->Checksum == current->Checksum &&
                    it->Signature == current->Signature, SelfInfo() << " Lsn# " << it->Lsn
                    << " InFlightData# " << it->DataLen << " StoredData# " << current->DataLen);
            }
        } else if (prev) {
            // lost item
            if (prev->Lsn >= Recovered.FirstLsnToKeep) {
                Y_FAIL_S("lost item Owner# " << PDiskParams->Owner << " Lsn# " << prev->Lsn);
            }
        }
    }

    void VerifyRecoveredLog(const TActorContext& ctx) {
//        TStringStream str;
//        str << "Owner# " << (int)PDiskParams->Owner << " Confirmed#";
//        for (const auto& item : Recovered.Confirmed) {
//            str << " [" << item.Lsn << " " << item.DataLen << "]";
//        }
//        str << " InFlight#";
//        for (const auto& item : Recovered.InFlight) {
//            str << " [" << item.Lsn << " " << item.DataLen << "]";
//        }
//        str << " State#";
//        for (const auto& item : State.Confirmed) {
//            str << " [" << item.Lsn << " " << item.DataLen << "]";
//        }
//        str << Endl;
//        Cerr << str.Str();

        auto curIt = State.Confirmed.begin();
        auto prevIt = Recovered.Confirmed.begin();
        for (;;) {
            const bool curEnd = curIt == State.Confirmed.end();
            const bool prevEnd = prevIt == Recovered.Confirmed.end();
            if (curEnd && prevEnd) {
                break;
            } else if (!curEnd && (prevEnd || *curIt < *prevIt)) {
                ProcessItem(&*curIt++, nullptr);
            } else if (!prevEnd && (curEnd || *prevIt < *curIt)) {
                ProcessItem(nullptr, &*prevIt++);
            } else {
                ProcessItem(&*curIt++, &*prevIt++);
            }
        }

        // advance LSN to point to next free index
        State.FirstLsnToKeep = Recovered.FirstLsnToKeep;
        ++Lsn;

        for (const auto& pair : Recovered.Chunks) {
            SendPDiskRequest(ctx, new NPDisk::TEvChunkRead(PDiskParams->Owner, PDiskParams->OwnerRound,
                    pair.first, 0, PDiskParams->ChunkSize, NPriRead::HullLoad, nullptr), [] {});
            ++ReadMsgPending;
        }

        CheckReadMsgPending(ctx);
    }

    void CheckReadMsgPending(const TActorContext& ctx) {
        if (!ReadMsgPending) {
            auto action = [this] {
                StateVerified = true;
            };
            StateManager->ExecuteConsistentAction(action);
            Activity(ctx);
        }
    }

    void Activity(const TActorContext& ctx) {
        while (InFlightLog < 10) {
            ui64 writeLogScore = 100;
            ui64 allocateScore = State.Chunks.size() < 20 ? 10 : 0;
            ui64 writeScore = State.Chunks.empty() ? 0 : 5;
            ui64 forgetScore = ChunksToForget.empty() ? 0 : ChunksToForget.size() < 100 ? 10 : 1000;

            ui64 totalScore = writeLogScore + allocateScore + writeScore + forgetScore;
            if (!totalScore) {
                // nothing to do
                break;
            }

            ui64 option = RandomNumber<ui64>(totalScore);
            if (Params.LogsToBeSent && LogsSent >= Params.LogsToBeSent) {
                break;
            } else if (option < writeLogScore) {
                IssueLogMessage(1, ctx);
            } else if ((option -= writeLogScore) < allocateScore) {
                IssueAllocateRequest(ctx);
            } else if ((option -= allocateScore) < writeScore) {
                IssueWriteRequest(ctx);
            } else if ((option -= writeScore) < forgetScore) {
                const size_t index = RandomNumber(ChunksToForget.size());
                ui32& chunk = ChunksToForget[index];
                SendPDiskRequest(ctx, new NPDisk::TEvChunkForget(PDiskParams->Owner, PDiskParams->OwnerRound, {chunk}), [] {});
                std::swap(chunk, ChunksToForget.back());
                ChunksToForget.pop_back();
            } else {
                Y_ABORT("unexpected option");
            }
        }
    }

    void IssueLogMessage(TLogSignature signature, const TActorContext& ctx) {
        ui32 size = Params.SizeMin + RandomNumber<ui32>(Params.SizeMax - Params.SizeMin + 1);
        TRcBuf data = TRcBuf(GenerateRandomDataBuffer(size));

        auto *info = new TLogRecord;
        info->Signature = signature;
        info->DataLen = data.size();
        info->Lsn = Lsn;
        info->Checksum = Crc32c(data.data(), data.size());

        // find out chunks we can commit or delete now
        for (auto& pair : State.Chunks) {
            TChunkIdx chunkIdx = pair.first;
            TState::TChunkInfo& chunk = pair.second;
            switch (chunk.GetCommitState()) {
                case ECommitState::RESERVED:
                    // reserved chunk is a subject for commit; 10% chance to commit chunk
                    if (RandomNumber<double>() < 0.1) {
                        info->CommitChunks.push_back(chunkIdx);
                        chunk.SetCommitState(ECommitState::COMMIT_IN_PROGRESS);
                        break;
                    }
                    [[fallthrough]];
                case ECommitState::COMMITTED:
                    // committed chunk is a subject for deletion; 10% chance to delete chunk
                    if (RandomNumber<double>() < 0.1) {
                        for (auto it = State.WritesInFlight.begin(); it != State.WritesInFlight.end(); ) {
                            if (it->ChunkIdx == chunkIdx) {
                                it = State.WritesInFlight.erase(it);
                            } else {
                                ++it;
                            }
                        }
                        info->DeleteChunks.push_back(chunkIdx);
                        chunk.SetCommitState(ECommitState::DELETE_IN_PROGRESS);
                    }
                    break;

                default:
                    break;
            }
        }

        NPDisk::TCommitRecord cr;

        // advance LSN every 500 items avg
        if (Lsn > Params.LsnToKeepCount && RandomNumber<double>() < Params.LogCutProbability) {
            cr.FirstLsnToKeep = Lsn - Params.LsnToKeepCount;
            cr.IsStartingPoint = true; // make starting point if we cut log
        }

        // fill in commit/delete records
        cr.CommitChunks = info->CommitChunks;
        cr.DeleteChunks = info->DeleteChunks;
        cr.DeleteToDecommitted = RandomNumber(2u);
        cr.IsStartingPoint = cr.IsStartingPoint || cr.CommitChunks || cr.DeleteChunks;

        if (cr.FirstLsnToKeep) {
            TStringStream str;
            str << SelfInfo() << " FirstLsnToKeep# " << cr.FirstLsnToKeep << Endl;
            Cerr << str.Str();
        }

        if (cr.DeleteToDecommitted) {
            ChunksToForget.insert(ChunksToForget.end(), cr.DeleteChunks.begin(), cr.DeleteChunks.end());
        }

        if (cr.IsStartingPoint || cr.CommitChunks || cr.DeleteChunks) {
            TStringStream msg;
            msg << "TEvLog " << SelfInfo() << " Lsn# " << Lsn << " Size# " << info->DataLen
                << " Commit# " << FormatList(cr.CommitChunks) << " Delete# " << FormatList(cr.DeleteChunks)
                << " IsStartingPoint# " << (cr.IsStartingPoint ? "true" : "false")
                << " DeleteToDecommitted# " << (cr.DeleteToDecommitted ? "true" : "false")
                << Endl;
            Cerr << msg.Str();
        }

        auto lsn = Lsn++;
        SendPDiskRequest(ctx, new NPDisk::TEvLog(PDiskParams->Owner, PDiskParams->OwnerRound, signature, cr, data,
            TLsnSeg(lsn, lsn), info), [&] {
                State.InFlight.insert(*info);
                if (cr.FirstLsnToKeep) {
                    Y_ABORT_UNLESS(cr.FirstLsnToKeep >= State.FirstLsnToKeep);
                    State.FirstLsnToKeep = cr.FirstLsnToKeep;
                }
            });

        ++LogsSent;
        ++InFlightLog;
    }

    void Handle(NPDisk::TEvLogResult::TPtr& ev, const TActorContext& ctx) {
        auto *msg = ev->Get();
        Y_ABORT_UNLESS(msg->Results, "No results in TEvLogResult, Owner# %" PRIu32 " Status# %s",
                (ui32)PDiskParams->Owner, NKikimrProto::EReplyStatus_Name(msg->Status).c_str());
        InFlightLog -= msg->Results.size();
        for (const auto& result : msg->Results) {
            std::unique_ptr<TLogRecord> info(static_cast<TLogRecord *>(result.Cookie));
            Y_ABORT_UNLESS(info);

            StateManager->ExecuteConsistentAction([&] {
                auto it = State.InFlight.find(*info);
                Y_ABORT_UNLESS(it != State.InFlight.end());
                State.InFlight.erase(it);

                const bool success = msg->Status == NKikimrProto::OK;

                if (success) {
                    State.Confirmed.insert(*info);
                }

                // apply chunk commits
                for (TChunkIdx chunkIdx : info->CommitChunks) {
                    TState::TChunkInfo& chunk = State.GetChunk(chunkIdx);
                    Cerr << (TStringBuilder() << SelfInfo() << " Chunk COMMITTED ChunkIdx# " << chunkIdx << Endl);
                    chunk.SetCommitState(success ? ECommitState::COMMITTED : ECommitState::RESERVED);
                }

                // apply chunk deletes
                for (TChunkIdx chunkIdx : info->DeleteChunks) {
                    auto it = State.Chunks.find(chunkIdx);
                    Y_ABORT_UNLESS(it != State.Chunks.end());
                    TState::TChunkInfo& chunk = it->second;
                    chunk.SetCommitState(success ? ECommitState::DELETED : ECommitState::COMMITTED);
                    if (chunk.GetCommitState() == ECommitState::DELETED) {
                        Cerr << (TStringBuilder() << SelfInfo() << " Chunk DELETED ChunkIdx# " << chunkIdx << Endl);
                        State.Chunks.erase(it);
                    }
                }
            });

            TStringStream str;
            str << "TEvLogResult " << SelfInfo() << " Lsn# " << result.Lsn <<
                " Status# " << NKikimrProto::EReplyStatus_Name(msg->Status) << Endl;
            Cerr << str.Str();
        }

        Activity(ctx);
    }

    void IssueAllocateRequest(const TActorContext& ctx) {
        SendPDiskRequest(ctx, new NPDisk::TEvChunkReserve(PDiskParams->Owner, PDiskParams->OwnerRound, 1), [] {});
    }

    void Handle(NPDisk::TEvChunkReserveResult::TPtr& ev, const TActorContext& ctx) {
        auto *msg = ev->Get();
        Y_ABORT_UNLESS(msg->Status == NKikimrProto::OK);

        StateManager->ExecuteConsistentAction([&] {
            for (TChunkIdx chunk : msg->ChunkIds) {
                Y_ABORT_UNLESS(State.Chunks.count(chunk) == 0);
                State.Chunks.emplace(chunk, TState::TChunkInfo(State.BlocksInChunk));
            }
        });

        Activity(ctx);
    }

    void IssueWriteRequest(const TActorContext& ctx) {
        // generate list of possible ranges we can select
        TMap<TChunkIdx, TDynBitMap> maps;
        for (const auto& pair : State.Chunks) {
            if (pair.second.GetCommitState() != ECommitState::DELETE_IN_PROGRESS) {
                TDynBitMap bm;
                bm.Reserve(State.BlocksInChunk);
                bm.Set(0, State.BlocksInChunk);
                maps.emplace(pair.first, std::move(bm));
            }
        }

        // exclude any pending writes from that list
        for (const TWriteRecord& write : State.WritesInFlight) {
            auto it = maps.find(write.ChunkIdx);
            if (it == maps.end()) {
                continue;
            }

            TDynBitMap& bm = it->second;
            bm.Reset(write.OffsetInBlocks, write.OffsetInBlocks + write.SizeInBlocks);
        }

        // generate set of ranges suitable for writing
        TMultiMap<TChunkIdx, std::pair<ui32, ui32>> ranges;
        for (const auto& p : maps) {
            bool valid = false;
            for (ui32 i = 0, begin; i <= State.BlocksInChunk; ++i) {
                if (i != State.BlocksInChunk && p.second[i]) {
                    // mark start of segment
                    if (!valid) {
                        valid = true;
                        begin = i;
                    }
                } else if (valid) {
                    ranges.emplace(p.first, std::make_pair(begin, i));
                    valid = false;
                }
            }
        }

        // if there are no valid sets, return
        if (!ranges) {
            return;
        }

        // select random range from that set
        auto it = ranges.begin();
        std::advance(it, RandomNumber(ranges.size()));

        // extract range offset and size
        const ui32 rangeOffset = it->second.first;
        const ui32 rangeSize = it->second.second - rangeOffset;

        // pick up number of blocks to write
        const ui32 numBlocks = 1 + RandomNumber(Min(rangeSize, 20U));

        // pick up block offset
        const ui32 offsetInBlocks = rangeOffset + RandomNumber(rangeSize - numBlocks + 1);

        // create data buffer
        TString data = GenerateRandomDataBuffer(numBlocks * PDiskParams->AppendBlockSize);

        // calculate checksums for written blocks
        TVector<ui32> checksums;
        for (ui32 i = 0; i < numBlocks; ++i) {
            const char *ptr = data.data() + i * PDiskParams->AppendBlockSize;
            checksums.push_back(Crc32c(ptr, PDiskParams->AppendBlockSize));
        }

        TStringStream str;
        str << "TEvChunkWrite ChunkIdx# " << it->first << " OffsetInBlocks# " << offsetInBlocks
            << " SizeInBlocks# " << numBlocks << " Checksums# [";
        bool first = true;
        for (ui32 checksum : checksums) {
            str << (first ? first = false, "" : " ") << Sprintf("%08" PRIx32, checksum);
        }
        str << "]" << Endl;
        Cerr << str.Str();

        void *cookie = reinterpret_cast<void *>(NextWriteCookie++);

        SendPDiskRequest(ctx, new NPDisk::TEvChunkWrite(PDiskParams->Owner, PDiskParams->OwnerRound, it->first,
                offsetInBlocks * PDiskParams->AppendBlockSize, new NPDisk::TEvChunkWrite::TStrokaBackedUpParts(data),
                cookie, true, NPriWrite::HullHugeAsyncBlob), [&] {
            State.WritesInFlight.push_back(TWriteRecord{it->first, offsetInBlocks, numBlocks, std::move(checksums),
                    cookie});
        });
    }

    void Handle(NPDisk::TEvChunkWriteResult::TPtr& ev, const TActorContext& ctx) {
        auto *msg = ev->Get();
        if (const auto it = State.Chunks.find(msg->ChunkIdx); it == State.Chunks.end() ||
                it->second.GetCommitState() == ECommitState::DELETE_IN_PROGRESS) {
            return Activity(ctx); // ignore this write result
        }

        Y_ABORT_UNLESS(msg->Status == NKikimrProto::OK);

        StateManager->ExecuteConsistentAction([&] {
            // find write in flight record
            decltype(State.WritesInFlight)::iterator it;
            for (it = State.WritesInFlight.begin(); it != State.WritesInFlight.end(); ++it) {
                if (msg->Cookie == it->Cookie) {
                    break;
                }
            }
            Y_ABORT_UNLESS(it != State.WritesInFlight.end());
            Y_ABORT_UNLESS(it->ChunkIdx == msg->ChunkIdx);
            Y_ABORT_UNLESS(it->Checksums.size() == it->SizeInBlocks);

            TStringStream s;
            s << "TEvChunkWriteResult ChunkIdx# " << it->ChunkIdx << " OffsetInBlocks# " << it->OffsetInBlocks
                << " SizeInBlocks# " << it->SizeInBlocks << Endl;
            Cerr << s.Str();

            // move data into confirmed state -- set used blocks bits and fill in actual checksums
            TState::TChunkInfo& chunk = State.GetChunk(it->ChunkIdx);
            Y_ABORT_UNLESS(it->OffsetInBlocks + it->SizeInBlocks <= State.BlocksInChunk);
            chunk.SetChecksums(it->OffsetInBlocks, it->Checksums);

            // drop write in flight record
            State.WritesInFlight.erase(it);
        });

        // more action!
        Activity(ctx);
    }

    void Handle(NPDisk::TEvChunkReadResult::TPtr& ev, const TActorContext& ctx) {
        auto *msg = ev->Get();
        TState::TChunkInfo chunk = Recovered.GetChunk(msg->ChunkIdx);

        TStringStream str;
        str << "TEvChunkReadResult ChunkIdx# " << msg->ChunkIdx << " Status# "
            << NKikimrProto::EReplyStatus_Name(msg->Status);
//        if (msg->Status == NKikimrProto::OK) {
//            for (ui32 i = 0; i < Recovered.BlocksInChunk; ++i) {
//                const ui32 offset = i * PDiskParams->AppendBlockSize;
//                const bool readable = msg->Data.IsReadable(offset, PDiskParams->AppendBlockSize);
//                const char *data = readable ? msg->Data.DataPtr<const char>(offset) : nullptr;
//                const ui32 checksum = data ? Crc32c(data, PDiskParams->AppendBlockSize) : 0;
//                str << " " << i << ":" << (data ? Sprintf("%08" PRIx32, checksum) : "XXXXXXXX");
//            }
//        }
        str << Endl;
        Cerr << str.Str();

        if (msg->Status != NKikimrProto::OK) {
            // this chunk can't be read at all; this means that we don't own it
            Y_ABORT_UNLESS(chunk.GetCommitState() != ECommitState::COMMITTED);
        } else {
            // this chunk is readable, so it is committed somehow
            Y_ABORT_UNLESS(chunk.GetCommitState() == ECommitState::COMMITTED
                    || chunk.GetCommitState() == ECommitState::COMMIT_IN_PROGRESS
                    || chunk.GetCommitState() == ECommitState::DELETE_IN_PROGRESS,
                    "ChunkIdx# %" PRIu32 " CommitState# %" PRIu32, msg->ChunkIdx,
                    static_cast<ui32>(chunk.GetCommitState()));
            const bool wasDeleteInProgress = chunk.GetCommitState() == ECommitState::DELETE_IN_PROGRESS;
            chunk.SetCommitState(ECommitState::COMMITTED);

            ui32 offset = 0;
            for (ui32 i = 0; i < Recovered.BlocksInChunk; ++i, offset += PDiskParams->AppendBlockSize) {
                const bool readable = msg->Data.IsReadable(offset, PDiskParams->AppendBlockSize);
                const char *data = readable ? msg->Data.DataPtr<const char>(offset) : nullptr;
                const ui32 checksum = data ? Crc32c(data, PDiskParams->AppendBlockSize) : 0;

                if (readable) {
                    TStringStream s;
                    bool first = true;
                    for (const auto& w : Recovered.WritesInFlight) {
                        s << (first ? first = false, "" : " ") << "{ChunkIdx# " << w.ChunkIdx << " OffsetInBlocks# "
                            << w.OffsetInBlocks << " SizeInBlocks# " << w.SizeInBlocks << " Checksums# [";
                        bool firstChecksum = true;
                        for (ui32 checksum : w.Checksums) {
                            s << (firstChecksum ? firstChecksum = false, "" : " ") << Sprintf("%08" PRIx32, checksum);
                        }
                        s << "]}";
                    }

                    decltype(TState::WritesInFlight)::const_iterator it;
                    for (it = Recovered.WritesInFlight.begin(); it != Recovered.WritesInFlight.end(); ++it) {
                        if (it->ChunkIdx == msg->ChunkIdx && i >= it->OffsetInBlocks && i < it->OffsetInBlocks + it->SizeInBlocks &&
                                it->Checksums[i - it->OffsetInBlocks] == checksum) {
                            break;
                        }
                    }
                    Y_ABORT_UNLESS(it != Recovered.WritesInFlight.end() || chunk.VerifyChecksum(i, checksum) || wasDeleteInProgress,
                            "inconsistent chunk data ChunkIdx# %" PRIu32 " OffsetInBlocks# %" PRIu32 " Used# %s"
                            " Checksum# %08" PRIx32 " StoredChecksum# %08" PRIx32 " WritesInFlight# %s", msg->ChunkIdx, i,
                            chunk.IsUsed(i) ? "true" : "false", checksum, chunk.GetChecksum(i), s.Str().data());
                    chunk.SetChecksums(i, {checksum});
                } else {
                    Y_ABORT_UNLESS(!chunk.IsUsed(i), "unexpected data ChunkIdx# %" PRIu32 " OffsetInBlocks# %" PRIu32,
                            msg->ChunkIdx, i);
                }
            }

            State.Chunks.emplace(msg->ChunkIdx, std::move(chunk));
        }

        --ReadMsgPending;
        CheckReadMsgPending(ctx);
    }

    TString SerializeState() override {
        NPDiskFIT::TFakeVDiskState pb;
//        TStringStream str;
//        str << "VDiskId# " << VDiskId.ToString() << " Owner# " << (PDiskParams ? (int)PDiskParams->Owner : 0)
//            << " StateVerified# " << (StateVerified ? "true" : "false") << Endl;
//        Cerr << str.Str();
        (StateVerified ? State : Recovered).SerializeToProto(pb);
        TString data;
        bool status = pb.SerializeToString(&data);
        Y_ABORT_UNLESS(status);
        return data;
    }

    TString GenerateRandomDataBuffer(size_t len) {
        TString data(len, ' ');
        char *mutableData = data.Detach();
        ui64 pattern = RandomNumber<ui64>();
        if (!pattern) {
            pattern = ~pattern;
        }
        size_t i;
        for (i = 0; i + 8 <= len; ++i) {
            WriteUnaligned<ui64>(mutableData + i, pattern);
            pattern = (pattern ^ pattern << 1 ^ pattern << 3 ^ pattern << 4) >> 63 | pattern << 1;
        }
        while (i < len) {
            *(char *)(mutableData + i) = pattern;
            pattern >>= 8;
            ++i;
        }
        return data;
    }

    void Handle(NPDisk::TEvChunkForgetResult::TPtr& ev, const TActorContext& /*ctx*/) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            Cerr << (TStringBuilder() << VDiskId << " TEvChunkForgetResult# " << ev->Get()->ToString() << Endl);
        }
    }

    STFUNC(StateFunc) {
        switch (const ui32 type = ev->GetTypeRewrite()) {
            CFunc(TEvents::TSystem::Bootstrap, Bootstrap);
            HFunc(NPDisk::TEvYardInitResult, Handle);
            HFunc(NPDisk::TEvReadLogResult, Handle);
            HFunc(NPDisk::TEvLogResult, Handle);
            HFunc(NPDisk::TEvChunkReserveResult, Handle);
            HFunc(NPDisk::TEvChunkWriteResult, Handle);
            HFunc(NPDisk::TEvChunkReadResult, Handle);
            HFunc(NPDisk::TEvChunkForgetResult, Handle);
            default: Y_ABORT("unexpected message 0x%08" PRIx32, type);
        }
    }
};

IActor *CreateFakeVDisk(const TVDiskID& vdiskId, const TActorId& pdiskServiceId, ui64 pdiskGuid,
        TStateManager *stateManager, TFakeVDiskParams params) {
    return new TFakeVDisk(vdiskId, pdiskServiceId, pdiskGuid, stateManager, params);
}
