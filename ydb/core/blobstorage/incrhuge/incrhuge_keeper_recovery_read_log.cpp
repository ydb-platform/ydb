#include "incrhuge_keeper_recovery_read_log.h"
#include "incrhuge_keeper_common.h"
#include "incrhuge_keeper_log.h"

namespace NKikimr {
    namespace NIncrHuge {

        class TReadLogActor : public TActor<TReadLogActor> {
            const TActorId PDiskActorId;
            const ui8 Owner;
            const NPDisk::TOwnerRound OwnerRound;
            TActorId KeeperActorId;
            TMaybe<ui64> ChunksEntrypointLsn;
            TChunkRecordMerger ChunkMerger;
            TMaybe<ui64> DeletesEntrypointLsn;
            TDeleteRecordMerger DeleteMerger;

        public:
            static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
                return NKikimrServices::TActivity::BS_INCR_HUGE_KEEPER_RECOVERY_READ_LOG;
            }

            TReadLogActor(const TActorId& pdiskActorId, ui8 owner, NPDisk::TOwnerRound ownerRound,
                    TMaybe<ui64> chunksEntrypointLsn, TMaybe<ui64> deletesEntrypointLsn)
                : TActor<TReadLogActor>(&TReadLogActor::StateFunc)
                , PDiskActorId(pdiskActorId)
                , Owner(owner)
                , OwnerRound(ownerRound)
                , ChunksEntrypointLsn(chunksEntrypointLsn)
                , DeletesEntrypointLsn(deletesEntrypointLsn)
            {}

        private:
            TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parentId) override {
                return new IEventHandle(self, parentId, new TEvents::TEvBootstrap);
            }

            void Handle(TEvents::TEvBootstrap::TPtr& ev, const TActorContext& ctx) {
                KeeperActorId = ev->Sender;
                ctx.Send(PDiskActorId, new NPDisk::TEvReadLog(Owner, OwnerRound));
            }

            void Handle(NPDisk::TEvReadLogResult::TPtr& ev, const TActorContext& ctx) {
                NPDisk::TEvReadLogResult *msg = ev->Get();
                if (msg->Status != NKikimrProto::OK) {
                    auto result = std::make_unique<TEvIncrHugeReadLogResult>();
                    result->Status = msg->Status;
                    ctx.Send(KeeperActorId, result.release());
                    return Die(ctx);
                }

                // send next log read request if this is not the end
                if (!msg->IsEndOfLog) {
                    ctx.Send(PDiskActorId, new NPDisk::TEvReadLog(Owner, OwnerRound, msg->NextPosition));
                }

                // parse log items
                TMaybe<ui64> maxLsn;
                for (const auto& item : msg->Results) {
                    Y_ABORT_UNLESS(!maxLsn || item.Lsn > *maxLsn);
                    maxLsn = item.Lsn;

                    switch (item.Signature) {
                        case TLogSignature::SignatureIncrHugeChunks:
                            if (!ChunksEntrypointLsn || item.Lsn >= *ChunksEntrypointLsn) {
                                NKikimrVDiskData::TIncrHugeChunks record;
                                bool status = record.ParseFromArray(item.Data.GetData(), item.Data.GetSize());
                                Y_ABORT_UNLESS(status);
                                ChunkMerger(record);
                            }
                            break;

                        case TLogSignature::SignatureIncrHugeDeletes:
                            if (!DeletesEntrypointLsn || item.Lsn >= *DeletesEntrypointLsn) {
                                NKikimrVDiskData::TIncrHugeDelete record;
                                bool status = record.ParseFromArray(item.Data.GetData(), item.Data.GetSize());
                                Y_ABORT_UNLESS(status);

                                TStringStream s;
                                s << "Chunks# [";
                                for (size_t i = 0; i < record.ChunksSize(); ++i) {
                                    const auto& chunk = record.GetChunks(i);
                                    s << (i ? " " : "");
                                    s << "Id# " << chunk.GetChunkSerNum() << " Items# [";
                                    TDynBitMap deletedItems;
                                    DeserializeDeletes(deletedItems, chunk);
                                    bool first = true;
                                    Y_FOR_EACH_BIT(j, deletedItems) {
                                        s << (first ? first = false, "" : " ") << j;
                                    }
                                    s << "]";
                                }
                                s << "] Owners# [";
                                for (size_t i = 0; i < record.OwnersSize(); ++i) {
                                    const auto& o = record.GetOwners(i);
                                    s << (i ? " " : "") << o.GetOwner() << ": " << o.GetSeqNo();
                                }
                                s << "]";
                                LOG_DEBUG(ctx, NKikimrServices::BS_INCRHUGE, "IncrHugeDelete# Lsn# %" PRIu64 " %s",
                                        item.Lsn, s.Str().data());

                                DeleteMerger(record);
                            }
                            break;

                        default:
                            Y_FAIL_S("unexpected log record " << item.Signature.ToString());
                    }
                }

                // if this is the end, reply to parent and die
                if (msg->IsEndOfLog) {
                    auto result = std::make_unique<TEvIncrHugeReadLogResult>();
                    result->Status = NKikimrProto::OK;
                    result->Chunks = ChunkMerger.GetCurrentState();
                    result->Deletes = DeleteMerger.GetCurrentState();
                    result->NextLsn = maxLsn.GetOrElse(0) + 1;
                    result->IssueInitialStartingPoints = !maxLsn;
                    ctx.Send(KeeperActorId, result.release());
                    Die(ctx);
                }
            }

            void HandlePoison(const TActorContext& ctx) {
                Die(ctx);
            }

            STRICT_STFUNC(StateFunc,
                HFunc(TEvents::TEvBootstrap, Handle)
                HFunc(NPDisk::TEvReadLogResult, Handle)
                CFunc(TEvents::TSystem::PoisonPill, HandlePoison);
            )
        };

        IActor *CreateRecoveryReadLogActor(const TActorId& pdiskActorId, ui8 owner, NPDisk::TOwnerRound ownerRound,
                TMaybe<ui64> chunksEntrypointLsn, TMaybe<ui64> deletesEntrypointLsn)  {
            return new TReadLogActor(pdiskActorId, owner, ownerRound, chunksEntrypointLsn, deletesEntrypointLsn);
        }

    } // NIncrHuge
} // NKikimr
