#pragma once
#include "defs.h"
#include "query_statalgo.h"
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>
#include <library/cpp/streams/bzip2/bzip2.h>

namespace NKikimr {

    class TLevelIndexStreamActor : public TActorBootstrapped<TLevelIndexStreamActor> {
        static constexpr TDuration LifetimeAfterQuery = TDuration::Seconds(30);
        static constexpr size_t BlockSize = 4 << 20;

        TIntrusivePtr<THullCtx> HullCtx;
        std::optional<THullDsSnap> FullSnap;
        TActorId SkeletonId;
        const TString StreamId;
        TInstant Deadline;
        ui64 SequenceId = 1;

    public:
        enum class EDatabase {
            LOGOBLOBS,
            BLOCKS,
            BARRIERS,
        };

        enum class ETableType : ui32 {
            FCUR,
            FDREG,
            FOLD,
            LEVEL,
        };

#pragma pack(push, 1)
        struct TOutputLogoBlob {
            TKeyLogoBlob Key;
            ui64 Ingress;
        };

        struct TOutputBlock {
            TKeyBlock Key;
            TMemRecBlock MemRec;
        };

        struct TOutputBarrier {
            TKeyBarrier Key;
            TMemRecBarrier MemRec;
        };

        struct TOutputBlockHeader {
            ui32 NumRecs;
            ui32 Database : 2;
            ui32 TableType : 2;
            ui32 Level : 6;
            ui32 Reserved : 22;
            ui64 SstId;
        };
#pragma pack(pop)

    private:
        static constexpr size_t GetRecLen(EDatabase database) {
            switch (database) {
                case EDatabase::LOGOBLOBS:
                    return sizeof(TOutputLogoBlob);

                case EDatabase::BLOCKS:
                    return sizeof(TOutputBlock);

                case EDatabase::BARRIERS:
                    return sizeof(TOutputBarrier);
            }
        }

        class TDumpProcessor {
            class TTableId {
                ETableType Type;
                ui32 Level = 0;
                ui64 SstId = 0;

            public:
                TTableId(const char *segName) {
                    Y_DEBUG_ABORT_UNLESS(!strcmp(segName, "FCur") || !strcmp(segName, "FDreg") || !strcmp(segName, "FOld"));
                    switch (segName[1]) {
                        case 'C':
                            Type = ETableType::FCUR;
                            break;

                        case 'D':
                            Type = ETableType::FDREG;
                            break;

                        case 'O':
                            Type = ETableType::FOLD;
                            break;

                        default:
                            Y_ABORT();
                    }
                }

                template<typename T>
                TTableId(const T& sst)
                    : Type(ETableType::LEVEL)
                    , Level(sst.Level)
                    , SstId(sst.SstPtr->AssignedSstId)
                {}

                ui32 GetType() const { return static_cast<ui32>(Type); }
                ui32 GetLevel() const { return Level; }
                ui64 GetSstId() const { return SstId; }

                friend bool operator !=(const TTableId& x, const TTableId& y) {
                    return x.Type != y.Type || x.Level != y.Level || x.SstId != y.SstId;
                }
            };

       private:
            struct TBlock {
                EDatabase Database;
                TTableId TableId;
                char Data[65536];
                size_t Len = 0;

                TBlock(EDatabase database, TTableId tableId)
                    : Database(database)
                    , TableId(tableId)
                {}

                void Append(const void *data, size_t len) {
                    memcpy(Data + Len, data, len);
                    Len += len;
                }

                size_t Capacity() const {
                    return sizeof(Data) - Len;
                }

                const void *GetData() const {
                    return Data;
                }

                size_t GetLen() const {
                    return Len;
                }
            };

       private:
            std::deque<TBlock> Blocks;

        public:
            template<typename TKey, typename TMemRec>
            void UpdateFresh(const char *segName, const TKey& key, const TMemRec& memRec) {
                Update(TTableId(segName), key, memRec);
            }

            template<typename T, typename TKey, typename TMemRec>
            void UpdateLevel(const T& sst, const TKey& key, const TMemRec& memRec) {
                Update(TTableId(sst), key, memRec);
            }

            void Finish()
            {}

            void Update(TTableId tableId, const TKeyLogoBlob& key, const TMemRecLogoBlob& memRec) {
                Update(EDatabase::LOGOBLOBS, tableId, &key, sizeof(key), &memRec, sizeof(memRec));
            }

            void Update(TTableId tableId, const TKeyBlock& key, const TMemRecBlock& memRec) {
                Update(EDatabase::BLOCKS, tableId, &key, sizeof(key), &memRec, sizeof(memRec));
            }

            void Update(TTableId tableId, const TKeyBarrier& key, const TMemRecBarrier& memRec) {
                Update(EDatabase::BARRIERS, tableId, &key, sizeof(key), &memRec, sizeof(memRec));
            }

            void Update(EDatabase database, TTableId tableId, const void *key, size_t keyLen, const void *memRec, size_t memRecLen) {
                if (Blocks.empty() || Blocks.back().Database != database || Blocks.back().TableId != tableId ||
                        Blocks.back().Capacity() < keyLen + memRecLen) {
                    Blocks.emplace_back(database, tableId);
                }
                Blocks.back().Append(key, keyLen);
                Blocks.back().Append(memRec, memRecLen);
            }

            ui64 GetNumBlocks() const {
                return Blocks.size();
            }

            TString ReadIntoString(size_t maxLen) {
                TString s = TString::Uninitialized(maxLen);
                char *begin = s.Detach();
                char *end = begin + s.size();
                char *p = begin;

                for (; !Blocks.empty(); Blocks.pop_front()) {
                    TBlock& front = Blocks.front();

                    const size_t outputRecLen = GetRecLen(front.Database);

                    const size_t inputRecLen = front.Database == EDatabase::LOGOBLOBS
                        ? sizeof(TKeyLogoBlob) + sizeof(TMemRecLogoBlob)
                        : outputRecLen; // the same for other record types

                    const ui32 numRecs = front.GetLen() / inputRecLen;
                    Y_DEBUG_ABORT_UNLESS(front.GetLen() % inputRecLen == 0);

                    if (static_cast<size_t>(end - p) < sizeof(TOutputBlockHeader) + outputRecLen * numRecs) {
                        break; // this block does not fit into the output, exit
                    }

                    const TOutputBlockHeader header{
                        .NumRecs = numRecs,
                        .Database = static_cast<ui32>(front.Database),
                        .TableType = front.TableId.GetType(),
                        .Level = front.TableId.GetLevel(),
                        .Reserved = 0,
                        .SstId = front.TableId.GetSstId(),
                    };
                    memcpy(p, &header, sizeof(header));
                    p += sizeof(header);

                    switch (front.Database) {
                        case EDatabase::LOGOBLOBS: {
                            const void *input = front.GetData();
                            for (ui32 i = 0; i < numRecs; ++i) {
                                auto *key = static_cast<const TKeyLogoBlob*>(input);
                                auto *memRec = reinterpret_cast<const TMemRecLogoBlob*>(key + 1);
                                input = memRec + 1;

                                *reinterpret_cast<TOutputLogoBlob*>(p) = {
                                    .Key = *key,
                                    .Ingress = memRec->GetIngress().Raw(),
                                };
                                p += sizeof(TOutputLogoBlob);
                            }
                            Y_DEBUG_ABORT_UNLESS(input == static_cast<const char*>(front.GetData()) + front.GetLen());
                            break;
                        }

                        case EDatabase::BLOCKS:
                        case EDatabase::BARRIERS:
                            // no conversion
                            memcpy(p, front.GetData(), front.GetLen());
                            p += front.GetLen();
                            break;
                    }
                }

                s.resize(p - begin);
                return s;
            }

            bool Done() const {
                return Blocks.empty();
            }
        };

        TDumpProcessor Processor;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_MONSTREAM_ACTOR;
        }

        TLevelIndexStreamActor(THullDsSnap&& fullSnap, TEvBlobStorage::TEvMonStreamQuery::TPtr& ev)
            : FullSnap(std::move(fullSnap))
            , StreamId(std::move(ev->Get()->StreamId))
        {}

        void Bootstrap(const TActorId& parentId, const TActorContext& ctx) {
            Become(&TLevelIndexStreamActor::StateFunc);

            // record all snapped data into the processor and release snapshot
            TraverseDbWithoutMerge(FullSnap->HullCtx, &Processor, FullSnap->LogoBlobsSnap);
            TraverseDbWithoutMerge(FullSnap->HullCtx, &Processor, FullSnap->BlocksSnap);
            TraverseDbWithoutMerge(FullSnap->HullCtx, &Processor, FullSnap->BarriersSnap);
            FullSnap.reset();

            SkeletonId = parentId;
            Deadline = ctx.Now() + LifetimeAfterQuery;
            HandleWakeup(ctx);
        }

        STRICT_STFUNC(StateFunc, {
            HFunc(TEvBlobStorage::TEvMonStreamQuery, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            CFunc(TEvents::TSystem::PoisonPill, Die);
        })

        void Handle(TEvBlobStorage::TEvMonStreamQuery::TPtr& ev, const TActorContext& ctx) {
            TStringStream s;
            if (!Processor.Done()) {
                TBZipCompress comp(&s, 6, 65536);
                Save(&comp, SelfId().RawX1());
                Save(&comp, SelfId().RawX2());
                Save(&comp, SequenceId++);
                Save(&comp, Processor.GetNumBlocks());
                comp << Processor.ReadIntoString(BlockSize);
                comp.Finish();
            }
            ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(s.Str()), 0, ev->Cookie);
            Deadline = ctx.Now() + LifetimeAfterQuery;
        }

        void HandleWakeup(const TActorContext& ctx) {
            const TInstant now = ctx.Now();
            if (now < Deadline) {
                ctx.Schedule(Deadline - now, new TEvents::TEvWakeup);
            } else {
                Die(ctx);
            }
        }

        void Die(const TActorContext& ctx) override {
            ctx.Send(SkeletonId, new TEvBlobStorage::TEvMonStreamActorDeathNote(std::move(StreamId)));
            TActorBootstrapped<TLevelIndexStreamActor>::Die(ctx);
        }
    };

} // NKikimr
