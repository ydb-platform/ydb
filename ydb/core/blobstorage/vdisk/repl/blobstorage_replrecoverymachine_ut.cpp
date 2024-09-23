#include "blobstorage_replrecoverymachine.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_iter.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/random/fast.h>

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TBlobStorageReplRecoveryMachine) {

        TMap<TLogoBlobID, TVector<TString>> GenerateData(ui32 numBlobs,
                                                        ui32 maxLen,
                                                        const TIntrusivePtr<TBlobStorageGroupInfo> &info,
                                                        const TVector<TVDiskID>& vdisks) {
            TMap<TLogoBlobID, TVector<TString>> rv;
            TReallyFastRng32 rng(1);
            ui32 step = 1;

            while (numBlobs--) {
                ui32 len = 1 + rng() % maxLen;
                TString data;
                data.reserve(len);
                for (ui32 i = 0; i < len; ++i) {
                    data.push_back(rng());
                }

                TLogoBlobID id(1, 1, step, 0, len, 0);
                ++step;

                TBlobStorageGroupInfo::TVDiskIds varray;
                TBlobStorageGroupInfo::TServiceIds services;
                info->PickSubgroup(id.Hash(), &varray, &services);

                TDataPartSet parts;
                info->Type.SplitData((TErasureType::ECrcMode)id.CrcMode(), data, parts);

                TVector<TString> diskvec(vdisks.size());

                for (ui32 i = 0; i < info->Type.TotalPartCount(); ++i) {
                    for (ui32 k = 0; k < vdisks.size(); ++k) {
                        if (varray[i] == vdisks[k]) {
                            diskvec[k] = parts.Parts[i].OwnedString.ConvertToString();
                            break;
                        }
                    }
                }

                rv.emplace(id, std::move(diskvec));
            }

            return rv;
        }

        std::shared_ptr<TReplCtx> CreateReplCtx(
            TVector<TVDiskID>& vdisks,
            const TIntrusivePtr<TBlobStorageGroupInfo> &info)
        {
            for (const auto& vdisk : info->GetVDisks()) {
                vdisks.push_back(info->GetVDiskId(vdisk.OrderNumber));
            }

            auto baseInfo = TVDiskConfig::TBaseInfo::SampleForTests();
            baseInfo.VDiskIdShort = TVDiskIdShort(vdisks[0]);
            auto vdiskCfg = MakeIntrusive<TVDiskConfig>(baseInfo);
            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            auto vctx = MakeIntrusive<TVDiskContext>(TActorId(), info->PickTopology(), counters, TVDiskID(TGroupId::FromValue(0), 1, 0, 0, 0),
                nullptr, NPDisk::DEVICE_TYPE_UNKNOWN);
            auto hugeBlobCtx = std::make_shared<THugeBlobCtx>(nullptr, true);
            auto replCtx = std::make_shared<TReplCtx>(
                vctx,
                nullptr, // HullCtx
                nullptr, // PDiskCtx
                hugeBlobCtx,
                4097,
                nullptr,
                info,
                TActorId(),
                vdiskCfg,
                std::make_unique<std::atomic_uint64_t>());

            return replCtx;
        }

        Y_UNIT_TEST(BasicFunctionality) {
            TRopeArena arena(&TRopeArenaBackend::Allocate);
            TVector<TVDiskID> vdisks;
            auto groupInfo = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::Erasure4Plus2Block);
            auto replCtx = CreateReplCtx(vdisks, groupInfo);
            auto info = MakeIntrusive<TEvReplFinished::TInfo>();
            info->WorkUnitsPlanned = Max<ui64>();
            TBlobIdQueuePtr unreplicatedBlobsPtr = std::make_shared<TBlobIdQueue>();
            NRepl::TRecoveryMachine m(replCtx, info);
            TMap<TLogoBlobID, TVector<TString>> data = GenerateData(10000, 1024, groupInfo, vdisks);
            for (const auto& pair : data) {
                const TLogoBlobID& id = pair.first;

                // make ingress for every disk other than SelfVDisk
                TIngress ingress;
                TBlobStorageGroupInfo::TVDiskIds varray;
                TBlobStorageGroupInfo::TServiceIds services;
                groupInfo->PickSubgroup(id.Hash(), &varray, &services);
                for (ui32 i = 0; i < groupInfo->Type.TotalPartCount(); ++i) {
                    TIngress otherDiskIngress(*TIngress::CreateIngressWithLocal(&groupInfo->GetTopology(),
                                                                                varray[i],
                                                                                TLogoBlobID(id, i + 1)));
                    ingress.Merge(otherDiskIngress);
                }
                ingress = ingress.CopyWithoutLocal(groupInfo->Type);

                ui8 partIndex = 0;
                for (partIndex = 0; partIndex < groupInfo->Type.BlobSubgroupSize(); ++partIndex) {
                    if (varray[partIndex] == groupInfo->GetVDiskId(replCtx->VCtx->ShortSelfVDisk)) {
                        break;
                    }
                }
                UNIT_ASSERT(partIndex != groupInfo->Type.BlobSubgroupSize());
                if (partIndex >= groupInfo->Type.TotalPartCount()) {
                    continue;
                }

                auto partsToRecover = ingress.PartsWeMustHaveLocally(&groupInfo->GetTopology(),
                    replCtx->VCtx->ShortSelfVDisk, id) - ingress.LocalParts(groupInfo->Type);
                UNIT_ASSERT(!partsToRecover.Empty());
                UNIT_ASSERT(partsToRecover.Get(partIndex));
                m.AddTask(id, partsToRecover, false, ingress);
            }
            for (const auto& pair : data) {
                const TLogoBlobID& id = pair.first;
                const TVector<TString>& v = pair.second;
                if (v[0].empty()) {
                    continue; // nothing to recover on this disk
                }

                TBlobStorageGroupInfo::TVDiskIds varray;
                TBlobStorageGroupInfo::TServiceIds services;
                groupInfo->PickSubgroup(id.Hash(), &varray, &services);

                NRepl::TRecoveryMachine::TPartSet p(id, groupInfo->Type);
                for (ui32 i = 1; i < v.size(); ++i) {
                    if (v[i].empty()) {
                        continue;
                    }
                    ui8 partIndex;
                    for (partIndex = 0; partIndex < groupInfo->Type.BlobSubgroupSize(); ++partIndex) {
                        if (varray[partIndex] == vdisks[i]) {
                            break;
                        }
                    }
                    UNIT_ASSERT(partIndex != groupInfo->Type.BlobSubgroupSize());
                    p.AddData(0, TLogoBlobID(id, partIndex + 1), NKikimrProto::OK, TRope(v[i]));
                }
                NRepl::TRecoveryMachine::TRecoveredBlobsQueue rbq;
                struct {
                    void AddUnreplicatedBlobRecord(const NRepl::TRecoveryMachine::TPartSet& /*item*/, TIngress /*ingress*/,
                        bool /*looksLikePhantom*/) {}
                    void DropUnreplicatedBlobRecord(const TLogoBlobID& /*id*/) {}
                    void AddPhantomBlobRecord(const NRepl::TRecoveryMachine::TPartSet& /*item*/, TIngress /*ingress*/,
                            NMatrix::TVectorType /*partsToRecover*/) {
                        Y_ABORT();
                    }
                } processor;
                m.Recover(p, rbq, processor);

                ui8 partIndex;
                for (partIndex = 0; partIndex < groupInfo->Type.BlobSubgroupSize(); ++partIndex) {
                    if (varray[partIndex] == groupInfo->GetVDiskId(replCtx->VCtx->ShortSelfVDisk)) {
                        break;
                    }
                }
                UNIT_ASSERT(partIndex != groupInfo->Type.BlobSubgroupSize());

                UNIT_ASSERT_EQUAL(rbq.size(), 1);
                auto& item = rbq.front();
                UNIT_ASSERT_EQUAL(item.Id, id);

                TRope buf = TDiskBlob::Create(id.BlobSize(), partIndex + 1, groupInfo->Type.TotalPartCount(), TRope(v[0]), arena, true);

                UNIT_ASSERT_EQUAL(item.Data, buf);
            }
        }
    }

} // NKikimr
