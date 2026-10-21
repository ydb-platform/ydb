#include "blobstorage_groupinfo.h"
#include "blobstorage_groupinfo_iter.h"
#include "blobstorage_groupinfo_sets.h"
#include "blobstorage_groupinfo_partlayout.h"
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>
#include <ydb/core/protos/bridge.pb.h>
#include <library/cpp/testing/unittest/registar.h>

#include <bit>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TBlobStorageGroupInfoTest) {

    Y_UNIT_TEST(ParseEncryptedGroupWithoutKeyReportsError) {
        NKikimrBlobStorage::TGroupInfo group;
        group.SetGroupID(0);
        group.SetGroupGeneration(1);
        group.SetErasureSpecies(TBlobStorageGroupType::ErasureNone);
        group.AddRings()->AddFailDomains()->AddVDiskLocations();
        group.SetEncryptionMode(TBlobStorageGroupInfo::EEM_ENC_V1);
        group.SetLifeCyclePhase(TBlobStorageGroupInfo::ELCP_IN_USE);
        TStringStream error;
        const auto info = TBlobStorageGroupInfo::Parse(group, nullptr, &error);
        UNIT_ASSERT(info);
        UNIT_ASSERT_VALUES_EQUAL(info->GetLifeCyclePhase(), TBlobStorageGroupInfo::ELCP_KEY_NOT_LOADED);
        UNIT_ASSERT(!error.Str().empty());
    }

    Y_UNIT_TEST(Mirror3dcStateContract) {
        using T = NKikimrBlobStorage::TMirror3dcGroupState;
        using TInfo = TBlobStorageGroupInfo;
        using TState = TInfo::TMirror3dcState;
        const TInfo::TTopology topology(TBlobStorageGroupType::ErasureMirror3dc, 3, 3, 1, true);
        const TInfo::TTopology other(TBlobStorageGroupType::ErasureNone, 1, 1, 1, true);
        const TInfo::TTopology empty(TBlobStorageGroupType::ErasureMirror3dc, 0, 0, 0);
        T proto;
        UNIT_ASSERT(!TState::Parse(proto, topology));
        proto.SetMode(T::NORMAL);
        UNIT_ASSERT(TState::Parse(proto, topology));
        UNIT_ASSERT(!TState::Parse(proto, other));
        UNIT_ASSERT(!TState::Parse(proto, empty));
        proto.SetFailedFailRealmIdx(0);
        UNIT_ASSERT(!TState::Parse(proto, topology));
        proto.SetMode(T::DC_FAILURE);
        UNIT_ASSERT(TState::Parse(proto, topology));
        proto.SetFailedFailRealmIdx(3);
        UNIT_ASSERT(!TState::Parse(proto, topology));
        proto.SetFailedFailRealmIdx(0);
        proto.SetMode(T::SOLO_DC_INV_1);
        UNIT_ASSERT(!TState::Parse(proto, topology));
        proto.SetSurvivingFailRealmIdx(0);
        UNIT_ASSERT(!TState::Parse(proto, topology));
        proto.SetSurvivingFailRealmIdx(2);
        UNIT_ASSERT(TState::Parse(proto, topology));

        TState normal;
        TState failed{T::DC_FAILURE, 0, {}};
        TState inv1{T::DC_FAILURE_INV_1, 0, {}};
        TState inv2{T::DC_FAILURE_INV_2, 0, {}};
        TState solo1{T::SOLO_DC_INV_1, 0, 2};
        TState solo2{T::SOLO_DC_INV_2, 0, 2};
        UNIT_ASSERT(!normal.CanTransitionTo(failed));
        UNIT_ASSERT(failed.CanTransitionTo(inv1));
        UNIT_ASSERT(!inv1.CanTransitionTo(inv2));
        TState strengthening{T::DC_FAILURE_INV_2_PENDING, 0, {}};
        UNIT_ASSERT(inv1.CanTransitionTo(strengthening));
        UNIT_ASSERT(strengthening.CanTransitionTo(inv2));
        UNIT_ASSERT(!strengthening.GetWritePolicy().CheckWriteOk({1, 1}));
        UNIT_ASSERT(strengthening.GetWritePolicy().CheckWriteOk({2, 2}));
        UNIT_ASSERT(!inv2.CanTransitionTo(inv1));
        UNIT_ASSERT(!inv1.CanTransitionTo(solo1));
        UNIT_ASSERT(!inv2.CanTransitionTo(solo2));
        UNIT_ASSERT(!solo1.CanTransitionTo(normal));
        UNIT_ASSERT(!solo2.CanTransitionTo(inv2));
        UNIT_ASSERT(solo1.CanTransitionTo(solo2));
        UNIT_ASSERT(!solo2.CanTransitionTo({T::SOLO_DC_INV_2, 0, 1}));
        UNIT_ASSERT(!inv1.CanTransitionTo({T::DC_FAILURE_INV_2, 1, {}}));
        UNIT_ASSERT(failed.GetWritePolicy().CheckWriteOk({1, 1}));
        UNIT_ASSERT(!inv2.GetWritePolicy().CheckWriteOk({1, 2}));
        UNIT_ASSERT(inv2.GetWritePolicy().CheckWriteOk({2, 2}));
        UNIT_ASSERT(!inv2.GetWritePolicy().CheckWriteDurability({1, 2}));
        UNIT_ASSERT(solo1.GetWritePolicy().CheckWriteOk({2}));
        UNIT_ASSERT(!solo2.GetWritePolicy().CheckWriteOk({1}));

        const TVector<TState> path = {
            normal, {T::DC_FAILURE_PENDING, 0, {}}, failed, inv1,
            {T::SOLO_DC_INV_1_PENDING, 0, 2}, solo1, solo2,
            {T::SOLO_DC_RECOVERY_PENDING, 0, 2}, {T::SOLO_DC_RECOVERY, 0, 2}, inv2,
            {T::DC_SYNC_PENDING, 0, {}}, {T::DC_SYNC, 0, {}}, {T::NORMAL_PENDING, 0, {}}, normal,
        };
        for (size_t i = 1; i < path.size(); ++i) {
            UNIT_ASSERT_C(path[i - 1].CanTransitionTo(path[i]), i);
        }
        for (int mode = T::NORMAL; mode <= T::DC_FAILURE_INV_2_PENDING; ++mode) {
            TState state;
            state.Mode = static_cast<T::EMode>(mode);
            if (mode != T::NORMAL) {
                state.FailedFailRealmIdx = 0;
            }
            if (state.RequiresSurvivingRealm()) {
                state.SurvivingFailRealmIdx = 2;
            }
            UNIT_ASSERT(TState::Parse(state.ToProto(), topology) == state);
            UNIT_ASSERT(TInfo::TWritePolicy::Parse(state.GetWritePolicy().ToProto(), topology) == state.GetWritePolicy());
            NKikimrBlobStorage::TGroupInfo group;
            group.SetGroupID(0);
            group.SetGroupGeneration(1);
            group.SetErasureSpecies(TBlobStorageGroupType::ErasureMirror3dc);
            for (ui32 r = 0; r < 3; ++r) {
                auto* ring = group.AddRings();
                for (ui32 d = 0; d < 3; ++d) {
                    ring->AddFailDomains()->AddVDiskLocations();
                }
            }
            group.MutableMirror3dcState()->CopyFrom(state.ToProto());
            group.MutableWritePolicy()->CopyFrom(state.GetWritePolicy().ToProto());
            for (const auto& realm : state.GetFailRealmStates()) {
                auto* item = group.MutableFailRealmStates()->AddFailRealm();
                item->SetFailRealmIdx(realm.FailRealmIdx);
                item->SetState(static_cast<NKikimrBlobStorage::TFailRealmStates::EState>(realm.State));
            }
            const auto info = TInfo::Parse(group, nullptr, nullptr);
            UNIT_ASSERT_C(info, mode);
            UNIT_ASSERT(info->Mirror3dcState == state);
            TInfo copy(info, TVDiskID(0, 2, 0, 0, 0), TActorId());
            UNIT_ASSERT(copy.Mirror3dcState == state);
            UNIT_ASSERT(!copy.HasStoragePolicyChanged(*info));
            group.ClearWritePolicy();
            UNIT_ASSERT(!TInfo::Parse(group, nullptr, nullptr));
        }
        UNIT_ASSERT(!TInfo::TWritePolicy::Parse(normal.GetWritePolicy().ToProto(), empty));
    }

    Y_UNIT_TEST(FailRealmStates) {
        using TInfo = TBlobStorageGroupInfo;
        using TProto = NKikimrBlobStorage::TFailRealmStates;
        NKikimrBlobStorage::TGroupInfo group;
        group.SetGroupID(0);
        group.SetGroupGeneration(1);
        group.SetErasureSpecies(TBlobStorageGroupType::ErasureMirror3dc);
        for (ui32 realm = 0; realm < 3; ++realm) {
            auto* ring = group.AddRings();
            for (ui32 domain = 0; domain < 3; ++domain) {
                ring->AddFailDomains()->AddVDiskLocations();
            }
        }
        const auto original = TInfo::Parse(group, nullptr, nullptr);
        UNIT_ASSERT(original);
        auto add = [&](ui32 realm, TProto::EState state) {
            auto* item = group.MutableFailRealmStates()->AddFailRealm();
            item->SetFailRealmIdx(realm);
            item->SetState(state);
        };
        add(1, TProto::Joining);
        add(0, TProto::Excluded);
        add(1, TProto::Joining);
        add(2, TProto::Included);
        const auto info = TInfo::Parse(group, nullptr, nullptr);
        UNIT_ASSERT(info);
        UNIT_ASSERT_VALUES_EQUAL(info->FailRealmStates.size(), 2);
        UNIT_ASSERT(info->GetFailRealmState(0) == TInfo::EFailRealmState::Excluded);
        UNIT_ASSERT(info->GetFailRealmState(1) == TInfo::EFailRealmState::Joining);
        UNIT_ASSERT(info->GetFailRealmState(2) == TInfo::EFailRealmState::Included);
        UNIT_ASSERT(info->HasStoragePolicyChanged(*original));
        TInfo copy(info, TVDiskID(0, 2, 0, 0, 0), TActorId());
        UNIT_ASSERT(copy.FailRealmStates == info->FailRealmStates);
        UNIT_ASSERT(!copy.HasStoragePolicyChanged(*info));
        auto* policy = group.MutableWritePolicy();
        policy->AddWriteOk()->AddPartsPerFailRealm(2);
        policy->AddWriteDurability()->AddPartsPerFailRealm(2);
        group.SetBridgeProxyGroupId(0);
        group.MutableBridgeGroupState()->AddPile()->SetGroupId(0);
        const auto inlineInfo = TInfo::Parse(group, nullptr, nullptr);
        UNIT_ASSERT(inlineInfo);
        const auto view = inlineInfo->MakeInlinePileView();
        UNIT_ASSERT(view->FailRealmStates == inlineInfo->FailRealmStates);
        UNIT_ASSERT(view->WritePolicy == inlineInfo->WritePolicy);
        add(1, TProto::Excluded);
        UNIT_ASSERT(!TInfo::Parse(group, nullptr, nullptr));
        group.ClearFailRealmStates();
        add(3, TProto::Excluded);
        UNIT_ASSERT(!TInfo::Parse(group, nullptr, nullptr));
        group.ClearFailRealmStates();
        group.MutableFailRealmStates()->AddFailRealm()->SetFailRealmIdx(0);
        UNIT_ASSERT(!TInfo::Parse(group, nullptr, nullptr));
        group.ClearFailRealmStates();
        group.MutableFailRealmStates()->AddFailRealm()->SetState(TProto::Included);
        UNIT_ASSERT(!TInfo::Parse(group, nullptr, nullptr));
        group.ClearFailRealmStates();
        auto* invalid = group.MutableFailRealmStates()->AddFailRealm();
        invalid->SetFailRealmIdx(0);
        invalid->GetReflection()->MutableUnknownFields(invalid)->AddVarint(2, 99);
        UNIT_ASSERT(!TInfo::Parse(group, nullptr, nullptr));
        group.ClearFailRealmStates();
        add(2, TProto::Leaving);
        UNIT_ASSERT(TInfo::Parse(group, nullptr, nullptr));
        group.ClearFailRealmStates();
        UNIT_ASSERT(TInfo::Parse(group, nullptr, nullptr)->FailRealmStates.empty());
    }

    Y_UNIT_TEST(WritePolicyLayouts) {
        const TBlobStorageGroupInfo::TTopology topology(TBlobStorageGroupType::ErasureMirror3dc, 3, 3, 1, true);
        struct TCase {
            TVector<ui32> Layout;
            TVector<ui32> Success;
            TVector<ui32> Failure;
        };
        for (const auto& item : TVector<TCase>{
                {{2}, {0, 2, 0}, {1, 0, 0}},
                {{2, 2}, {2, 0, 2}, {2, 1, 1}},
                {{1, 2}, {1, 0, 2}, {3, 0, 0}},
                {{1, 1}, {1, 0, 1}, {2, 0, 0}},
                {{1, 1, 1}, {1, 1, 1}, {3, 3, 0}},
                {{2, 2, 2}, {2, 3, 2}, {3, 1, 3}}}) {
            NKikimrBlobStorage::TGroupWritePolicy proto;
            auto* layout = proto.AddWriteOk();
            for (ui32 count : item.Layout) {
                layout->AddPartsPerFailRealm(count);
            }
            proto.AddWriteDurability()->CopyFrom(*layout);
            const auto policy = TBlobStorageGroupInfo::TWritePolicy::Parse(proto, topology);
            UNIT_ASSERT(policy);
            UNIT_ASSERT(policy->CheckWriteOk(item.Success));
            UNIT_ASSERT(policy->CheckWriteDurability(item.Success));
            UNIT_ASSERT(!policy->CheckWriteOk(item.Failure));
            UNIT_ASSERT(!policy->CheckWriteDurability(item.Failure));
            UNIT_ASSERT(TBlobStorageGroupInfo::TWritePolicy::Parse(policy->ToProto(), topology) == policy);
        }
    }

    Y_UNIT_TEST(WritePolicyAlternativesAndValidation) {
        const TBlobStorageGroupInfo::TTopology topology(TBlobStorageGroupType::ErasureMirror3dc, 3, 3, 1, true);
        NKikimrBlobStorage::TGroupWritePolicy proto;
        auto* layout = proto.AddWriteOk();
        layout->AddPartsPerFailRealm(2);
        layout->AddPartsPerFailRealm(2);
        layout = proto.AddWriteOk();
        for (ui32 i = 0; i < 3; ++i) {
            layout->AddPartsPerFailRealm(1);
        }
        layout = proto.AddWriteDurability();
        for (ui32 i = 0; i < 3; ++i) {
            layout->AddPartsPerFailRealm(2);
        }
        const auto policy = TBlobStorageGroupInfo::TWritePolicy::Parse(proto, topology);
        UNIT_ASSERT(policy);
        UNIT_ASSERT(policy->CheckWriteOk({2, 2, 0}));
        UNIT_ASSERT(policy->CheckWriteOk({1, 1, 1}));
        UNIT_ASSERT(!policy->CheckWriteOk({2, 1, 0}));
        UNIT_ASSERT(!policy->CheckWriteDurability({2, 2, 0}));
        UNIT_ASSERT(policy->CheckWriteDurability({2, 2, 2}));
        UNIT_ASSERT(!policy->CheckWriteOk({}));
        proto.AddWriteOk()->CopyFrom(proto.GetWriteOk(0));
        UNIT_ASSERT(TBlobStorageGroupInfo::TWritePolicy::Parse(proto, topology) == policy);
        proto.ClearWriteOk();
        UNIT_ASSERT(!TBlobStorageGroupInfo::TWritePolicy::Parse(proto, topology));
        for (const auto& invalid : TVector<TVector<ui32>>{{}, {0}, {4}, {1, 1, 1, 1}}) {
            auto malformed = policy->ToProto();
            auto* bad = malformed.MutableWriteOk(0);
            bad->ClearPartsPerFailRealm();
            for (ui32 count : invalid) {
                bad->AddPartsPerFailRealm(count);
            }
            UNIT_ASSERT(!TBlobStorageGroupInfo::TWritePolicy::Parse(malformed, topology));
        }
        auto malformed = policy->ToProto();
        malformed.ClearWriteDurability();
        UNIT_ASSERT(!TBlobStorageGroupInfo::TWritePolicy::Parse(malformed, topology));
    }

    Y_UNIT_TEST(WritePolicyBlock42) {
        const TBlobStorageGroupInfo::TTopology topology(TBlobStorageGroupType::Erasure4Plus2Block, 1, 8, 1, true);
        NKikimrBlobStorage::TGroupWritePolicy proto;
        proto.AddWriteOk()->AddPartsPerFailRealm(4);
        proto.AddWriteDurability()->AddPartsPerFailRealm(6);
        const auto policy = TBlobStorageGroupInfo::TWritePolicy::Parse(proto, topology);
        UNIT_ASSERT(policy);
        UNIT_ASSERT(policy->CheckWriteOk({4}));
        UNIT_ASSERT(!policy->CheckWriteOk({3}));
        UNIT_ASSERT(policy->CheckWriteDurability({6}));
        UNIT_ASSERT(!policy->CheckWriteDurability({5}));
        for (ui32 count : {3u, 9u}) {
            auto invalid = proto;
            invalid.MutableWriteOk(0)->SetPartsPerFailRealm(0, count);
            UNIT_ASSERT(!TBlobStorageGroupInfo::TWritePolicy::Parse(invalid, topology));
        }
        auto invalid = proto;
        invalid.MutableWriteOk(0)->AddPartsPerFailRealm(4);
        UNIT_ASSERT(!TBlobStorageGroupInfo::TWritePolicy::Parse(invalid, topology));
        NKikimrBlobStorage::TFailRealmStates excluded;
        auto* realm = excluded.AddFailRealm();
        realm->SetFailRealmIdx(0);
        realm->SetState(NKikimrBlobStorage::TFailRealmStates::Excluded);
        UNIT_ASSERT(TBlobStorageGroupInfo::ParseFailRealmStates(excluded, topology));
        realm->SetFailRealmIdx(1);
        UNIT_ASSERT(!TBlobStorageGroupInfo::ParseFailRealmStates(excluded, topology));
    }

    Y_UNIT_TEST(TestBelongsToSubgroup) {
        for (ui32 disks = 1; disks < 4; ++disks) {
            for (auto [erasureType, _] : TErasureType::ErasureNames) {
                if (erasureType == TBlobStorageGroupType::ErasureMirror3dc) {
                    continue;
                }

                const ui32 numFailDomains = TBlobStorageGroupType(erasureType).BlobSubgroupSize();
                TBlobStorageGroupInfo info(erasureType, disks, numFailDomains);

                for (ui32 hashIdx = 0; hashIdx < 1000; ++hashIdx) {
                    ui32 hash = 640480 + 13 * hashIdx;
                    TBlobStorageGroupInfo::TVDiskIds vdiskIds;
                    info.PickSubgroup(hash, &vdiskIds, nullptr);
                    TVector<bool> isMissingDomain;
                    auto realm0 = info.FailRealmsBegin();
                    isMissingDomain.resize(realm0.GetNumFailDomainsPerFailRealm());
                    for (const auto& domain : realm0.GetFailRealmFailDomains()) {
                        isMissingDomain[domain.FailDomainOrderNumber] = true;
                    }
                    for (ui32 i = 0; i < vdiskIds.size(); ++i) {
                        isMissingDomain[vdiskIds[i].FailDomain] = false;
                    }
                    for (auto it = realm0.FailRealmFailDomainsBegin(); it != realm0.FailRealmFailDomainsEnd(); ++it) {
                        if (isMissingDomain[it.GetFailDomainIdx()]) {
                            ui32 vDisksSz = it.GetNumVDisksPerFailDomain();
                            for (ui32 vdiskIdx = 0; vdiskIdx < vDisksSz; ++vdiskIdx) {
                                TVDiskID id(0, 1, 0, it.GetFailDomainIdx(), vdiskIdx);
                                bool isReplica = info.BelongsToSubgroup(id, hash);
                                UNIT_ASSERT(!isReplica);
                            }
                        }
                    }

                    for (ui32 i = 0; i < vdiskIds.size(); ++i) {
                        ui32 realmIdx = vdiskIds[i].FailRealm;
                        ui32 domainIdx = vdiskIds[i].FailDomain;
                        for (auto vdiskIt = realm0.FailRealmVDisksBegin(); vdiskIt != realm0.FailRealmVDisksEnd(); ++vdiskIt) {
                            if (vdiskIt.GetFailDomainIdx() != domainIdx) {
                                continue;
                            }
                            ui32 vdiskIdx = vdiskIt.GetVDiskIdx();
                            TVDiskID id(0, 1, realmIdx, domainIdx, vdiskIdx);
                            bool isReplica = info.BelongsToSubgroup(id, hash);
                            UNIT_ASSERT(isReplica == (vdiskIdx == vdiskIds[i].VDisk));
                        }
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(SubgroupPartLayout) {
        TLogoBlobID id(1, 1, 1, 0, 100, 0);

        for (auto [erasureType, _] : TErasureType::ErasureNames) {
            if (erasureType == TBlobStorageGroupType::ErasureMirror3dc || erasureType == TBlobStorageGroupType::ErasureMirror3of4) {
                continue;
            }

            const ui32 numFailDomains = TBlobStorageGroupType(erasureType).BlobSubgroupSize();
            TBlobStorageGroupInfo info(erasureType, 1, numFailDomains);

            TBlobStorageGroupInfo::TVDiskIds ids;
            info.PickSubgroup(id.Hash(), &ids, nullptr);

            const ui32 totalPartCount = info.Type.TotalPartCount();
            const ui32 handoff = info.Type.Handoff();
            const ui32 numHandoff1 = handoff >= 1 ? totalPartCount : 0;
            const ui32 numHandoff2 = handoff >= 2 ? totalPartCount : 0;

            for (ui32 main = 0; main < (1U << totalPartCount); ++main) {
                for (ui32 handoff1 = 0; handoff1 < (1U << numHandoff1); ++handoff1) {
                    for (ui32 handoff2 = 0; handoff2 < (1U << numHandoff2); ++handoff2) {
                        TIngress ingress;
                        for (ui32 i = 0; i < totalPartCount; ++i) {
                            if (main & (1 << i)) {
                                auto iOpt = TIngress::CreateIngressWithLocal(&info.GetTopology(),
                                                                             ids[i],
                                                                             TLogoBlobID(id, i + 1));
                                ingress.Merge(*iOpt);
                            }
                            if (handoff1 & (1 << i)) {
                                auto iOpt = TIngress::CreateIngressWithLocal(&info.GetTopology(),
                                                                             ids[totalPartCount],
                                                                             TLogoBlobID(id, i + 1));
                                ingress.Merge(*iOpt);
                            }
                            if (handoff2 & (1 << i)) {
                                auto iOpt = TIngress::CreateIngressWithLocal(&info.GetTopology(),
                                                                             ids[totalPartCount + 1],
                                                                             TLogoBlobID(id, i + 1));
                                ingress.Merge(*iOpt);
                            }
                        }
                        TSubgroupPartLayout layout;
                        for (ui32 i = 0; i < totalPartCount; ++i) {
                            if (main & (1 << i)) {
                                layout.AddItem(i, i, info.Type);
                            }
                            if (handoff1 & (1 << i)) {
                                layout.AddItem(totalPartCount, i, info.Type);
                            }
                            if (handoff2 & (1 << i)) {
                                layout.AddItem(totalPartCount + 1, i, info.Type);
                            }
                        }

                        UNIT_ASSERT_EQUAL(layout, TSubgroupPartLayout::CreateFromIngress(ingress, info.Type));

//                        Cerr << "main# " << main << " hanoff1# " << handoff1 << " handoff2# " << handoff2 << " num# " << num << " num2# " << num2 << Endl;
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(GroupQuorumCheckerOrdinary) {
        for (auto [erasure, _] : TErasureType::ErasureNames) {
            if (erasure == TBlobStorageGroupType::ErasureMirror3dc || erasure == TBlobStorageGroupType::ErasureMirror3of4) {
                // separate test for mirror-3-dc
                continue;
            }

            const ui32 numFailDomains = 10;
            const ui32 numVDisks = 3;
            TBlobStorageGroupInfo info(erasure, numVDisks, numFailDomains);

            // calculate number of handoff disks
            const ui32 numHandoff = info.Type.Handoff();
            const ui32 numMasks = 1U << numFailDomains;

            // calculate sets of disks
            TVector<ui32> goodMask, badMask;
            for (ui32 mask = 0; mask < numMasks; ++mask) {
                (static_cast<ui32>(std::popcount(mask)) <= numHandoff ? goodMask : badMask).push_back(mask);
            }

            auto createGroupVDisks = [&](ui32 domainMask, ui32 vdiskMask) {
                TBlobStorageGroupInfo::TGroupVDisks vdisks(&info.GetTopology());
                for (const auto& vdisk : info.GetVDisks()) {
                    if (((domainMask >> vdisk.FailDomainOrderNumber) & 1) && ((vdiskMask >> vdisk.VDiskIdShort.VDisk) & 1)) {
                        auto vd = info.GetVDiskId(vdisk.OrderNumber);
                        vdisks += TBlobStorageGroupInfo::TGroupVDisks(&info.GetTopology(), vd);
                    }
                }
                return vdisks;
            };

            const auto& checker = info.GetQuorumChecker();
            for (ui32 vdiskMask = 1; vdiskMask < (1U << numVDisks); ++vdiskMask) {
                for (ui32 domainMask : goodMask) {
                    UNIT_ASSERT(checker.CheckQuorumForGroup(~createGroupVDisks(domainMask, vdiskMask)));
                }
                for (ui32 domainMask : badMask) {
                    UNIT_ASSERT(!checker.CheckQuorumForGroup(~createGroupVDisks(domainMask, vdiskMask)));
                }
            }
        }
    }

    Y_UNIT_TEST(GroupQuorumCheckerMirror3dc) {
        const ui32 numFailRealms = 3;
        const ui32 numFailDomains = 4;
        TBlobStorageGroupInfo info(TBlobStorageGroupType::ErasureMirror3dc, 1, numFailDomains, numFailRealms);

        auto domainMask = [&](ui32 realm, ui32 domain) {
            return 1U << (realm * numFailDomains + domain);
        };

        TVector<ui32> goodMask;

        goodMask.push_back(0);

        for (ui32 realm = 0; realm < numFailRealms; ++realm) {
            for (ui32 mask = 1; mask < (1U << numFailDomains); ++mask) {
                // first, create failed domains mask for a single fail realm, iterating through all possible options
                // for failed fail domains
                ui32 domMask = 0;
                for (ui32 i = 0; i < numFailDomains; ++i) {
                    if ((mask >> i) & 1) {
                        domMask |= domainMask(realm, i);
                    }
                }

                // push that mask to good ones
                goodMask.push_back(domMask);

                // now iterate through all other realms (not including current one) and add one more failed fail domain
                // to the common mask
                for (ui32 i = 0; i < numFailRealms; ++i) {
                    if (i != realm) {
                        for (ui32 j = 0; j < numFailDomains; ++j) {
                            goodMask.push_back(domMask | domainMask(i, j));
                        }
                    }
                }
            }
        }

        // sort options
        std::sort(goodMask.begin(), goodMask.end());

        // create bad mask
        TVector<ui32> allMasks;
        for (ui32 i = 0; i < (1U << (numFailRealms * numFailDomains)); ++i) {
            allMasks.push_back(i);
        }

        TVector<ui32> badMask;
        std::set_difference(allMasks.begin(), allMasks.end(), goodMask.begin(), goodMask.end(), std::back_inserter(badMask));

        auto createGroupVDisks = [&](ui32 domainMask) {
            TBlobStorageGroupInfo::TGroupVDisks vdisks(&info.GetTopology());
            for (const auto& vdisk : info.GetVDisks()) {
                if ((domainMask >> vdisk.FailDomainOrderNumber) & 1) {
                    auto vd = info.GetVDiskId(vdisk.OrderNumber);
                    vdisks += TBlobStorageGroupInfo::TGroupVDisks(&info.GetTopology(), vd);
                }
            }
            return vdisks;
        };

        const auto& checker = info.GetQuorumChecker();
        for (ui32 domainMask : goodMask) {
            UNIT_ASSERT(checker.CheckQuorumForGroup(~createGroupVDisks(domainMask)));
        }
        for (ui32 domainMask : badMask) {
            UNIT_ASSERT(!checker.CheckQuorumForGroup(~createGroupVDisks(domainMask)));
        }
    }
}

} // namespace NKikimr

