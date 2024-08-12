#pragma once

#include "defs.h"
#include "blobstorage_hullreplwritesst.h"
#include "blobstorage_repl.h"

namespace NKikimr {

    namespace NRepl {

        enum class ETimeState : ui32 {
            PREPARE_PLAN,
            TOKEN_WAIT,
            PROXY_WAIT,
            MERGE,
            PDISK_OP,
            COMMIT,
            OTHER,
            PHANTOM,
            COUNT
        };

        struct TTimeAccount {
            TTimeAccount()
                : CurrentState(ETimeState::COUNT)
            {}

            void SetState(ETimeState state) {
                if (CurrentState != state) {
                    TInstant timestamp = TAppData::TimeProvider->Now();
                    if (CurrentState != ETimeState::COUNT)
                        Durations[static_cast<ui32>(CurrentState)] += timestamp - PrevTimestamp;
                    CurrentState = state;
                    PrevTimestamp = timestamp;
                }
            }

            void UpdateInfo(TEvReplFinished::TInfo& replInfo) const {
                replInfo.PreparePlanDuration = Durations[static_cast<ui32>(ETimeState::PREPARE_PLAN)];
                replInfo.TokenWaitDuration = Durations[static_cast<ui32>(ETimeState::TOKEN_WAIT)];
                replInfo.ProxyWaitDuration = Durations[static_cast<ui32>(ETimeState::PROXY_WAIT)];
                replInfo.MergeDuration = Durations[static_cast<ui32>(ETimeState::MERGE)];
                replInfo.PDiskDuration = Durations[static_cast<ui32>(ETimeState::PDISK_OP)];
                replInfo.CommitDuration = Durations[static_cast<ui32>(ETimeState::COMMIT)];
                replInfo.OtherDuration = Durations[static_cast<ui32>(ETimeState::OTHER)];
                replInfo.PhantomDuration = Durations[static_cast<ui32>(ETimeState::PHANTOM)];
            }

        private:
            ETimeState CurrentState;
            TInstant PrevTimestamp;
            TDuration Durations[static_cast<ui32>(ETimeState::COUNT)];
        };

        ////////////////////////////////////////////////////////////////////////////
        // TRecoveryMachine
        ////////////////////////////////////////////////////////////////////////////
        class TRecoveryMachine {
        public:
            using TRecoveredBlobInfo = TReplSstStreamWriter::TRecoveredBlobInfo;
            using TRecoveredBlobsQueue = TQueue<TRecoveredBlobInfo>;

            struct TPartSet {
                const TLogoBlobID Id;
                TStackVec<TRope, 8> Parts;
                ui32 PartsMask = 0;
                ui32 DisksRepliedOK = 0;
                ui32 DisksRepliedNODATA = 0;
                ui32 DisksRepliedNOT_YET = 0;
                ui32 DisksRepliedOther = 0;

                TPartSet(TLogoBlobID id, TBlobStorageGroupType gtype)
                    : Id(id)
                    , Parts(gtype.TotalPartCount())
                {}

                void AddData(ui32 diskIdx, const TLogoBlobID& id, NKikimrProto::EReplyStatus status, TRope&& data) {
                    Y_ABORT_UNLESS(id.FullID() == Id);
                    switch (status) {
                        case NKikimrProto::OK: {
                            const ui8 partIdx = id.PartId() - 1;
                            Y_ABORT_UNLESS(partIdx < Parts.size());
                            PartsMask |= 1 << partIdx;
                            Parts[partIdx] = std::move(data);
                            DisksRepliedOK |= 1 << diskIdx;
                            break;
                        }

                        case NKikimrProto::NODATA:
                            DisksRepliedNODATA |= 1 << diskIdx;
                            break;

                        case NKikimrProto::NOT_YET:
                            DisksRepliedNOT_YET |= 1 << diskIdx;
                            break;

                        default:
                            DisksRepliedOther |= 1 << diskIdx;
                            break;
                    }
                }

                TString ToString() const {
                    return TStringBuilder() << "{DisksRepliedOK# " << DisksRepliedOK
                        << " DisksRepliedNODATA# " << DisksRepliedNODATA
                        << " DisksRepliedNOT_YET# " << DisksRepliedNOT_YET
                        << " DisksRepliedOther# " << DisksRepliedOther
                        << "}";
                }
            };

        public:
            TRecoveryMachine(
                    std::shared_ptr<TReplCtx> replCtx,
                    TEvReplFinished::TInfoPtr replInfo)
                : ReplCtx(std::move(replCtx))
                , ReplInfo(replInfo)
                , LostVec(TMemoryConsumer(ReplCtx->VCtx->Replication))
                , Arena(&TRopeArenaBackend::Allocate)
            {}

            template<typename TBlobProcessor>
            void Recover(TPartSet& item, TRecoveredBlobsQueue& rbq, TBlobProcessor&& processor) {
                const TLogoBlobID& id = item.Id;
                Y_ABORT_UNLESS(!id.PartId());
                Y_ABORT_UNLESS(!LastRecoveredId || *LastRecoveredId < id);
                LastRecoveredId = id;

                RecoverMetadata(id, rbq);

                while (!LostVec.empty() && LostVec.front().Id < id) {
                    SkipItem(LostVec.front(), processor);
                    LostVec.pop_front();
                }

                if (LostVec.empty() || LostVec.front().Id != id) {
                    STLOG(PRI_ERROR, BS_REPL, BSVR27, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "blob not in LostVec"),
                        (BlobId, id));
                    return;
                }

                const TLost& lost = LostVec.front();
                Y_ABORT_UNLESS(lost.Id == id);

                const NMatrix::TVectorType parts = lost.PartsToRecover;

                const TBlobStorageGroupType groupType = ReplCtx->VCtx->Top->GType;

                ui32 partsSize = 0;
                bool hasExactParts = false;
                bool needToRestore = false;
                for (ui8 i = parts.FirstPosition(); i != parts.GetSize(); i = parts.NextPosition(i)) {
                    if (item.PartsMask & (1 << i)) {
                        hasExactParts = true;
                    } else {
                        needToRestore = true;
                    }
                }

                Y_DEBUG_ABORT_UNLESS((item.PartsMask >> groupType.TotalPartCount()) == 0);
                const ui32 presentParts = PopCount(item.PartsMask);
                bool canRestore = presentParts >= groupType.MinimalRestorablePartCount();

                // first of all, count present parts and recover only if there are enough of these parts
                if (!canRestore && needToRestore && !hasExactParts) {
                    if (lost.PossiblePhantom) {
                        processor.AddPhantomBlobRecord(item, lost.Ingress, lost.PartsToRecover);
                    } else {
                        STLOG(PRI_INFO, BS_REPL, BSVR28, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "not enough data parts to recover"),
                            (BlobId, id), (NumPresentParts, presentParts), (MinParts, groupType.DataParts()),
                            (PartSet, item.ToString()), (Ingress, lost.Ingress.ToString(ReplCtx->VCtx->Top.get(),
                            ReplCtx->VCtx->ShortSelfVDisk, id)));
                        BlobDone(item, false /*success*/, true /*unrecovered*/, &TEvReplFinished::TInfo::ItemsNotRecovered,
                            lost.Ingress, false /*looksLikePhantom*/, processor);
                    }
                } else {
                    // recover
                    try {
                        // PartSet contains some data, other data will be restored and written in the same PartSet
                        if (canRestore && needToRestore) {
                            ui32 restoreMask = 0;
                            for (ui8 i = parts.FirstPosition(); i != parts.GetSize(); i = parts.NextPosition(i)) {
                                restoreMask |= 1 << i;
                            }
                            restoreMask &= ~item.PartsMask;

                            ErasureRestore((TErasureType::ECrcMode)id.CrcMode(), groupType, id.BlobSize(), nullptr,
                                item.Parts, restoreMask);
                            item.PartsMask |= restoreMask;
                        }

                        ui32 numSmallParts = 0, numMissingParts = 0, numHuge = 0;
                        std::array<TRope, 8> partData; // part data for small blobs
                        NMatrix::TVectorType small(0, parts.GetSize());

                        for (ui8 i = parts.FirstPosition(); i != parts.GetSize(); i = parts.NextPosition(i)) {
                            if (~item.PartsMask & (1 << i)) {
                                ++numMissingParts; // ignore this missing part
                                continue;
                            }
                            const TLogoBlobID partId(id, i + 1);
                            const ui32 partSize = groupType.PartSize(partId);
                            Y_ABORT_UNLESS(partSize); // no metadata here
                            partsSize += partSize;
                            TRope& data = item.Parts[i];
                            Y_ABORT_UNLESS(data.GetSize() == partSize);
                            if (ReplCtx->HugeBlobCtx->IsHugeBlob(groupType, id, ReplCtx->MinREALHugeBlobInBytes)) {
                                AddBlobToQueue(partId, TDiskBlob::Create(id.BlobSize(), i + 1, groupType.TotalPartCount(),
                                    std::move(data), Arena, ReplCtx->GetAddHeader()), {}, true, rbq);
                                ++numHuge;
                            } else {
                                partData[numSmallParts++] = std::move(data);
                                small.Set(i);
                            }
                        }

                        if (numSmallParts) {
                            // fill in disk blob buffer
                            AddBlobToQueue(id, TDiskBlob::CreateFromDistinctParts(&partData[0], &partData[numSmallParts],
                                small, id.BlobSize(), Arena, ReplCtx->GetAddHeader()), small, false, rbq);
                        }

                        ReplInfo->LogoBlobsRecovered += !!numSmallParts;
                        ReplInfo->HugeLogoBlobsRecovered += !!numHuge;
                        ReplInfo->BytesRecovered += partsSize;

                        if (!numMissingParts) {
                            BlobDone(item, true /*success*/, false /*unrecovered*/, &TEvReplFinished::TInfo::ItemsRecovered,
                                {}, false /*looksLikePhantom*/, processor);
                            if (lost.PossiblePhantom) {
                                ++ReplCtx->MonGroup.ReplPhantomLikeRecovered();
                            }
                        } else if (lost.PossiblePhantom) {
                            processor.AddPhantomBlobRecord(item, lost.Ingress, lost.PartsToRecover);
                        } else {
                            BlobDone(item, false /*success*/, true /*unrecovered*/, &TEvReplFinished::TInfo::ItemsPartiallyRecovered,
                                lost.Ingress, false /*looksLikePhantom*/, processor);
                        }
                    } catch (const std::exception& ex) {
                        ++ReplCtx->MonGroup.ReplRecoveryGroupTypeErrors();
                        STLOG(PRI_ERROR, BS_REPL, BSVR29, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "recovery exception"),
                            (BlobId, id), (Error, TString(ex.what())));
                        BlobDone(item, false /*success*/, true /*unrecovered*/, &TEvReplFinished::TInfo::ItemsException,
                            lost.Ingress, false /*looksLikePhantom*/, processor);
                    }
                }

                LostVec.pop_front();
            }

            template<typename TBlobProcessor>
            void ProcessPhantomBlob(const TPartSet& item, NMatrix::TVectorType parts, bool isPhantom, bool looksLikePhantom,
                    TIngress ingress, TBlobProcessor&& processor) {
                STLOG(PRI_INFO, BS_REPL, BSVR00, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "phantom check completed"),
                    (BlobId, item.Id), (Parts, parts), (IsPhantom, isPhantom), (LooksLikePhantom, looksLikePhantom));

                const bool success = isPhantom; // confirmed phantom blob
                const bool unrecovered = !looksLikePhantom; // if blob doesn't look like phantom, then it is ordinary unrecovered blob
                Y_DEBUG_ABORT_UNLESS(isPhantom <= looksLikePhantom);

                ++(success
                    ? ReplCtx->MonGroup.ReplPhantomLikeDropped()
                    : ReplCtx->MonGroup.ReplPhantomLikeUnrecovered());

                auto counter = success ? &TEvReplFinished::TInfo::ItemsPhantom :
                    looksLikePhantom ? &TEvReplFinished::TInfo::ItemsNonPhantom :
                    &TEvReplFinished::TInfo::ItemsNotRecovered;

                BlobDone(item, success, unrecovered, counter, ingress, looksLikePhantom, processor);
            }

            template<typename TBlobProcessor>
            void BlobDone(const TPartSet& item, bool success, bool unrecovered, ui64 TEvReplFinished::TInfo::*counter,
                    TIngress ingress, bool looksLikePhantom, TBlobProcessor&& processor) {
                STLOG(PRI_DEBUG, BS_REPL, BSVR35, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "BlobDone"), (BlobId, item.Id),
                    (Success, success));

                const ui32 size = item.Id.BlobSize();
                if (success) {
                    ReplInfo->WorkUnitsPerformed += size;
                    ReplCtx->MonGroup.ReplWorkUnitsDone() += size;
                    ReplCtx->MonGroup.ReplWorkUnitsRemaining() -= size;
                    ++ReplCtx->MonGroup.ReplItemsDone();
                    --ReplCtx->MonGroup.ReplItemsRemaining();
                    processor.DropUnreplicatedBlobRecord(item.Id);
                } else {
                    processor.AddUnreplicatedBlobRecord(item, ingress, looksLikePhantom);
                }

                ++((*ReplInfo).*counter);
                ReplInfo->UnrecoveredNonphantomBlobs |= unrecovered;
            }

            // finish work
            template<typename TBlobProcessor>
            void Finish(TRecoveredBlobsQueue& rbq, TBlobProcessor&& processor) {
                RecoverMetadata(TLogoBlobID(Max<ui64>(), Max<ui64>(), Max<ui64>()), rbq);
                for (auto&& item : LostVec) {
                    SkipItem(item, processor);
                }
                LostVec.clear();
            }

            // add next task during preparation phase
            void AddTask(const TLogoBlobID &id, const NMatrix::TVectorType &partsToRecover, bool possiblePhantom,
                    TIngress ingress) {
                Y_ABORT_UNLESS(!id.PartId());
                Y_ABORT_UNLESS(LostVec.empty() || LostVec.back().Id < id);
                LostVec.push_back(TLost(id, partsToRecover, possiblePhantom, ingress));
            }

            void AddMetadataPart(const TLogoBlobID& id) {
                MetadataParts.push_back(id);
            }

            bool FullOfTasks() const {
                return LostVec.size() >= ReplCtx->VDiskCfg->ReplMaxLostVecSize;
            }

            bool NoTasks() const {
                return LostVec.empty() && MetadataParts.empty();
            }

            void ClearPossiblePhantom() {
                for (TLost& item : LostVec) {
                    item.PossiblePhantom = false;
                }
            }

            template<typename TCallback>
            void ForEach(TCallback&& callback) {
                for (const TLost& item : LostVec) {
                    callback(item.Id, item.PartsToRecover, item.Ingress);
                }
            }

        private:
            // structure for a lost part
            struct TLost {
                const TLogoBlobID Id;
                const NMatrix::TVectorType PartsToRecover;
                bool PossiblePhantom;
                const TIngress Ingress;

                TLost(const TLogoBlobID &id, const NMatrix::TVectorType &partsToRecover, const bool possiblePhantom,
                        TIngress ingress)
                    : Id(id)
                    , PartsToRecover(partsToRecover)
                    , PossiblePhantom(possiblePhantom)
                    , Ingress(ingress)
                {}
            };

            // vector of lost parts, we are goind to recover them during this job
            typedef TTrackableDeque<TLost> TLostVec;

            std::shared_ptr<TReplCtx> ReplCtx;
            TEvReplFinished::TInfoPtr ReplInfo;
            TLostVec LostVec;
            TDeque<TLogoBlobID> MetadataParts;
            TRopeArena Arena;
            std::optional<TLogoBlobID> LastRecoveredId;

            void AddBlobToQueue(const TLogoBlobID& id, TRope blob, NMatrix::TVectorType parts, bool isHugeBlob,
                    TRecoveredBlobsQueue& rbq) {
                if (!rbq.empty() && rbq.back().Id == id && !isHugeBlob) {
                    auto& last = rbq.back();
                    TDiskBlobMerger merger;
                    merger.Add(TDiskBlob(&last.Data, last.LocalParts, ReplCtx->VCtx->Top->GType, id));
                    merger.Add(TDiskBlob(&blob, parts, ReplCtx->VCtx->Top->GType, id));
                    last.LocalParts = merger.GetDiskBlob().GetParts();
                    last.Data = merger.CreateDiskBlob(Arena, ReplCtx->GetAddHeader());
                } else {
                    rbq.emplace(id, std::move(blob), isHugeBlob, parts);
                }
            }

            void RecoverMetadata(const TLogoBlobID& id, TRecoveredBlobsQueue& rbq) {
                while (!MetadataParts.empty() && MetadataParts.front().FullID() <= id) {
                    const TLogoBlobID id = MetadataParts.front();
                    const bool isHugeBlob = ReplCtx->HugeBlobCtx->IsHugeBlob(ReplCtx->VCtx->Top->GType, id.FullID(), ReplCtx->MinREALHugeBlobInBytes);
                    MetadataParts.pop_front();
                    STLOG(PRI_DEBUG, BS_REPL, BSVR30, VDISKP(ReplCtx->VCtx->VDiskLogPrefix,
                        "TRecoveryMachine::RecoverMetadata"), (BlobId, id));
                    const TBlobStorageGroupType gtype = ReplCtx->VCtx->Top->GType;
                    if (isHugeBlob) {
                        // huge metadata blob contains ID with designated part id and no data at all (and no parts vector)
                        AddBlobToQueue(id, TRope(), {}, true, rbq);
                    } else {
                        // small metadata blob contains only header without data, but its ID has PartId = 0 and parts
                        // vector is filled accordingly
                        const NMatrix::TVectorType parts = NMatrix::TVectorType::MakeOneHot(id.PartId() - 1,
                            gtype.TotalPartCount());
                        AddBlobToQueue(id.FullID(), TDiskBlob::Create(id.BlobSize(), parts, TRope(),
                            Arena, ReplCtx->GetAddHeader()), parts, isHugeBlob, rbq);
                    }
                    ++ReplInfo->MetadataBlobs;
                }
            }

            template<typename TBlobProcessor>
            void SkipItem(const TLost& item, TBlobProcessor&& processor) {
                STLOG(PRI_INFO, BS_REPL, BSVR31, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "TRecoveryMachine::SkipItem"),
                    (BlobId, item.Id));
                ++ReplInfo->ItemsNotRecovered;
                if (item.PossiblePhantom) {
                    ++ReplCtx->MonGroup.ReplPhantomLikeUnrecovered();
                }
                processor.AddUnreplicatedBlobRecord({item.Id, ReplCtx->VCtx->Top->GType}, item.Ingress, item.PossiblePhantom);
            }
        };

    } // NRepl

} // NKikimr
