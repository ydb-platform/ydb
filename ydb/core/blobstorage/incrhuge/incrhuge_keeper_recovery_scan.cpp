#include "incrhuge_keeper_recovery_scan.h"
#include <library/cpp/digest/crc32c/crc32c.h>

namespace NKikimr {
    namespace NIncrHuge {

        class TScanActor : public TActor<TScanActor> {
            const TChunkIdx ChunkIdx;
            const bool IndexOnly;
            const TChunkSerNum ChunkSerNum;
            const ui64 Cookie;
            const TKeeperCommonState& State;
            TActorId KeeperActorId;

        public:
            static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
                return NKikimrServices::TActivity::BS_INCR_HUGE_KEEPER_RECOVERY_SCAN;
            }

            TScanActor(TChunkIdx chunkIdx, bool indexOnly, TChunkSerNum chunkSerNum, ui64 cookie,
                    const TKeeperCommonState& state)
                : TActor<TScanActor>(&TScanActor::StateFunc)
                , ChunkIdx(chunkIdx)
                , IndexOnly(indexOnly)
                , ChunkSerNum(chunkSerNum)
                , Cookie(cookie)
                , State(state)
            {}

        private:
            TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parentId) override {
                return new IEventHandle(self, parentId, new TEvents::TEvBootstrap);
            }

            void Handle(TEvents::TEvBootstrap::TPtr& ev, const TActorContext& ctx) {
                KeeperActorId = ev->Sender;

                // in index-only mode read just index; otherwise read the full block
                ui32 offset = IndexOnly ? State.BlocksInDataSection * State.BlockSize : 0;
                ui32 size = IndexOnly ? State.BlocksInIndexSection * State.BlockSize : State.PDiskParams->ChunkSize;
                ctx.Send(State.Settings.PDiskActorId, new NPDisk::TEvChunkRead(State.PDiskParams->Owner,
                        State.PDiskParams->OwnerRound, ChunkIdx, offset, size, NPriRead::HullLoad, nullptr));
            }

            void SendReplyAndDie(NKikimrProto::EReplyStatus status, TVector<TBlobIndexRecord>&& index, bool indexValid,
                    const TActorContext& ctx) {
                auto result = std::make_unique<TEvIncrHugeScanResult>();
                result->Status = status;
                result->Index = std::move(index);
                result->ChunkIdx = ChunkIdx;
                result->IndexOnly = IndexOnly;
                result->ChunkSerNum = ChunkSerNum;
                result->IndexValid = indexValid;
                ctx.Send(KeeperActorId, result.release(), 0, Cookie);
                Die(ctx);
            }

            void Handle(NPDisk::TEvChunkReadResult::TPtr& ev, const TActorContext& ctx) {
                NPDisk::TEvChunkReadResult *msg = ev->Get();
                if (msg->Status != NKikimrProto::OK) {
                    return SendReplyAndDie(msg->Status, {}, {}, ctx);
                }
                bool success = false;
                if (IndexOnly) {
                    success = ParseIndex(*msg, ctx);
                } else {
                    success = ParseWholeChunk(*msg, ctx);
                }
                if (!success) {
                    return SendReplyAndDie(NKikimrProto::ERROR, {}, {}, ctx);
                }
            }

            bool ParseIndex(NPDisk::TEvChunkReadResult& msg, const TActorContext& ctx) {
                // try to get index
                TVector<TBlobIndexRecord> index;
                if (!GetIndex(msg.Data, 0, index)) {
                    return false;
                }

                // send reply & die
                SendReplyAndDie(NKikimrProto::OK, std::move(index), true, ctx);
                return true;
            }

            bool GetIndex(TBufferWithGaps& data, ui32 offset, TVector<TBlobIndexRecord>& index) {
                // check that we can read header
                if (!data.IsReadable(offset, sizeof(TBlobIndexHeader))) {
                    return false;
                }

                // get a reference to header and ensure we can read all declared records
                const TBlobIndexHeader& header = *data.DataPtr<const TBlobIndexHeader>(offset);
                const ui32 dataSize = header.NumItems * sizeof(TBlobIndexRecord);
                if (header.NumItems > State.MaxBlobsPerChunk || !data.IsReadable(offset + sizeof(TBlobIndexHeader), dataSize)) {
                    return false;
                }

                // validate id; id mismatch means that consistency of this chunk is broken
                if (header.ChunkSerNum != ChunkSerNum) {
                    return false;
                }

                // validate checksum
                const ui32 checksum = Crc32c(&header.ChunkSerNum, sizeof(TBlobIndexHeader) - sizeof(ui32) + dataSize);
                if (checksum != header.Checksum) {
                    return false;
                }

                index.assign(header.InplaceIndexBegin(), header.InplaceIndexEnd());
                return true;
            }

            bool ParseWholeChunk(NPDisk::TEvChunkReadResult& msg, const TActorContext& ctx) {
                TVector<TBlobIndexRecord> index;

                for (ui32 block = 0; block < State.BlocksInDataSection; ) {
                    ui32 offset = block * State.BlockSize;

                    if (!msg.Data.IsReadable(offset, sizeof(TBlobHeader))) {
                        break;
                    }

                    const TBlobHeader& header = *msg.Data.DataPtr<const TBlobHeader>(offset);

                    const ui32 sizeInBlocks = (sizeof(TBlobHeader) + header.IndexRecord.PayloadSize + State.BlockSize -
                            1) / State.BlockSize;
                    if (!msg.Data.IsReadable(offset, sizeInBlocks * State.BlockSize)) {
                        break;
                    }

                    const ui32 checksum = Crc32c(&header.IndexRecord, sizeof(TBlobHeader) - sizeof(ui32) +
                            header.IndexRecord.PayloadSize);
                    if (checksum != header.Checksum) {
                        break;
                    }

                    // on id mismatch we stop, because the only reason is appearance of old data inside new chunk
                    if (header.ChunkSerNum != ChunkSerNum) {
                        break;
                    }

                    index.push_back(header.IndexRecord);

                    block += sizeInBlocks;
                }

                // try to get stored index and check if it the same as computed one
                TVector<TBlobIndexRecord> storedIndex;
                const bool indexValid = GetIndex(msg.Data, State.BlocksInDataSection * State.BlockSize, storedIndex) &&
                    index == storedIndex;

                SendReplyAndDie(index ? NKikimrProto::OK : NKikimrProto::NODATA, std::move(index), indexValid, ctx);
                return true;
            }

            void HandlePoison(const TActorContext& ctx) {
                Die(ctx);
            }

            STRICT_STFUNC(StateFunc,
                HFunc(TEvents::TEvBootstrap, Handle)
                HFunc(NPDisk::TEvChunkReadResult, Handle)
                CFunc(TEvents::TSystem::PoisonPill, HandlePoison);
            )
        };

        IActor *CreateRecoveryScanActor(TChunkIdx chunkIdx, bool indexOnly, TChunkSerNum chunkSerNum, ui64 cookie,
                const TKeeperCommonState& state) {
            return new TScanActor(chunkIdx, indexOnly, chunkSerNum, cookie, state);
        }

    } // NIncrHuge
} // NKikimr
