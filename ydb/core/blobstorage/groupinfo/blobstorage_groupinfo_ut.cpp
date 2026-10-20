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

    Y_UNIT_TEST(VDiskStates) {
        NKikimrBlobStorage::TGroupInfo group;
        group.SetGroupID(0);
        group.SetGroupGeneration(1);
        group.SetErasureSpecies(TBlobStorageGroupType::ErasureMirror3dc);
        for (ui32 realm = 0; realm < 3; ++realm) {
            auto *ring = group.AddRings();
            for (ui32 domain = 0; domain < 3; ++domain) {
                auto *location = ring->AddFailDomains()->AddVDiskLocations();
                location->SetNodeID(1 + realm * 3 + domain);
                location->SetPDiskID(1);
                location->SetVDiskSlotID(1);
            }
        }
        const auto original = TBlobStorageGroupInfo::Parse(group, nullptr, nullptr);
        UNIT_ASSERT(original);
        UNIT_ASSERT(original->VDiskStates.empty());
        for (ui32 order : {2u, 0u, 1u, 2u}) {
            auto* diskState = group.MutableVDiskStates()->AddVDisk();
            diskState->SetOrderNumber(order);
            diskState->SetState(NKikimrBlobStorage::TVDiskStates::Excluded);
        }
        const auto info = TBlobStorageGroupInfo::Parse(group, nullptr, nullptr);
        UNIT_ASSERT(info);
        UNIT_ASSERT_VALUES_EQUAL(info->VDiskStates.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(info->VDiskStates[0].OrderNumber, 0);
        UNIT_ASSERT_VALUES_EQUAL(info->VDiskStates[1].OrderNumber, 1);
        UNIT_ASSERT(info->HasVDiskPolicyChanged(*original));
        for (ui32 orderNumber = 0; orderNumber < 9; ++orderNumber) {
            UNIT_ASSERT_VALUES_EQUAL(info->GetVDiskState(info->GetVDiskId(orderNumber)) == TBlobStorageGroupInfo::EVDiskState::Excluded,
                orderNumber < 3);
        }
        TBlobStorageGroupInfo copy(info, TVDiskID(0, 2, 0, 0, 0), TActorId());
        UNIT_ASSERT(copy.VDiskStates == info->VDiskStates);
        UNIT_ASSERT(!copy.HasVDiskPolicyChanged(*info));
        group.ClearVDiskStates();
        {
            auto* diskState = group.MutableVDiskStates()->AddVDisk();
            diskState->SetOrderNumber(1);
            diskState->SetState(NKikimrBlobStorage::TVDiskStates::Excluded);
        }
        UNIT_ASSERT(!TBlobStorageGroupInfo::Parse(group, nullptr, nullptr));
        group.ClearVDiskStates();
        const auto cleared = TBlobStorageGroupInfo::Parse(group, nullptr, nullptr);
        UNIT_ASSERT(cleared);
        UNIT_ASSERT(cleared->VDiskStates.empty());
        UNIT_ASSERT(cleared->HasVDiskPolicyChanged(*info));
        auto* writePolicy = group.MutableWritePolicy();
        writePolicy->AddWriteOk()->AddPartsPerFailRealm(2);
        auto* durability = writePolicy->AddWriteDurability();
        durability->AddPartsPerFailRealm(2);
        durability->AddPartsPerFailRealm(2);
        const auto withPolicy = TBlobStorageGroupInfo::Parse(group, nullptr, nullptr);
        UNIT_ASSERT(withPolicy);
        UNIT_ASSERT(withPolicy->WritePolicy);
        UNIT_ASSERT(withPolicy->HasVDiskPolicyChanged(*cleared));
        TBlobStorageGroupInfo policyCopy(withPolicy, TVDiskID(0, 3, 0, 0, 0), TActorId());
        UNIT_ASSERT(policyCopy.WritePolicy == withPolicy->WritePolicy);
        UNIT_ASSERT(!policyCopy.HasVDiskPolicyChanged(*withPolicy));
        for (ui32 i = 0; i < 3; ++i) {
            auto* diskState = group.MutableVDiskStates()->AddVDisk();
            diskState->SetOrderNumber(i);
            diskState->SetState(NKikimrBlobStorage::TVDiskStates::Excluded);
        }
        group.SetBridgeProxyGroupId(0);
        group.MutableBridgeGroupState()->AddPile()->SetGroupId(0);
        const auto inlineInfo = TBlobStorageGroupInfo::Parse(group, nullptr, nullptr);
        UNIT_ASSERT(inlineInfo);
        UNIT_ASSERT(inlineInfo->IsInlinePile());
        const auto view = inlineInfo->MakeInlinePileView();
        UNIT_ASSERT(view->WritePolicy == inlineInfo->WritePolicy);
        UNIT_ASSERT(view->VDiskStates == inlineInfo->VDiskStates);
        group.ClearVDiskStates();
        group.ClearBridgeProxyGroupId();
        group.ClearBridgeGroupState();
        group.ClearWritePolicy();
        UNIT_ASSERT(TBlobStorageGroupInfo::Parse(group, nullptr, nullptr)->HasVDiskPolicyChanged(*withPolicy));
        group.MutableWritePolicy();
        UNIT_ASSERT(!TBlobStorageGroupInfo::Parse(group, nullptr, nullptr));
        group.ClearWritePolicy();
        {
            auto* diskState = group.MutableVDiskStates()->AddVDisk();
            diskState->SetOrderNumber(9);
            diskState->SetState(NKikimrBlobStorage::TVDiskStates::Excluded);
        }
        TStringStream error;
        UNIT_ASSERT(!TBlobStorageGroupInfo::Parse(group, nullptr, &error));
        UNIT_ASSERT(!error.Str().empty());
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
        NKikimrBlobStorage::TVDiskStates excluded;
        {
            auto* diskState = excluded.AddVDisk();
            diskState->SetOrderNumber(0);
            diskState->SetState(NKikimrBlobStorage::TVDiskStates::Excluded);
        }
        UNIT_ASSERT(!TBlobStorageGroupInfo::ParseVDiskStates(excluded, topology));
        for (ui32 i = 1; i < 8; ++i) {
            auto* diskState = excluded.AddVDisk();
            diskState->SetOrderNumber(i);
            diskState->SetState(NKikimrBlobStorage::TVDiskStates::Excluded);
        }
        UNIT_ASSERT(TBlobStorageGroupInfo::ParseVDiskStates(excluded, topology));
    }

    Y_UNIT_TEST(VDiskStatesReplicating) {
        using TInfo = TBlobStorageGroupInfo;
        using TProto = NKikimrBlobStorage::TVDiskStates;
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
        auto add = [&](ui32 order, TProto::EState state) {
            auto* disk = group.MutableVDiskStates()->AddVDisk();
            disk->SetOrderNumber(order);
            disk->SetState(state);
        };
        for (ui32 order = 0; order < 3; ++order) {
            add(order, TProto::Excluded);
            add(order + 3, TProto::Replicating);
        }
        add(3, TProto::Replicating); // Identical duplicates are normalized.
        add(8, TProto::Included); // Explicit default is normalized away.
        const auto info = TInfo::Parse(group, nullptr, nullptr);
        UNIT_ASSERT(info);
        UNIT_ASSERT_VALUES_EQUAL(info->VDiskStates.size(), 6);
        for (ui32 order = 0; order < 9; ++order) {
            const auto expected = order < 3 ? TInfo::EVDiskState::Excluded
                : order < 6 ? TInfo::EVDiskState::Replicating : TInfo::EVDiskState::Included;
            UNIT_ASSERT(info->GetVDiskState(info->GetVDiskId(order)) == expected);
        }
        TInfo copy(info, TVDiskID(0, 2, 0, 0, 0), TActorId());
        UNIT_ASSERT(copy.VDiskStates == info->VDiskStates);
        UNIT_ASSERT(!copy.HasVDiskPolicyChanged(*info));
        add(3, TProto::Excluded);
        UNIT_ASSERT(!TInfo::Parse(group, nullptr, nullptr)); // Conflicting duplicate.
        group.MutableVDiskStates()->MutableVDisk()->RemoveLast();
        group.MutableVDiskStates()->MutableVDisk(3)->SetState(TProto::Included);
        UNIT_ASSERT(!TInfo::Parse(group, nullptr, nullptr)); // Mixed states in a realm.
        group.ClearVDiskStates();
        group.MutableVDiskStates()->AddVDisk()->SetOrderNumber(0);
        UNIT_ASSERT(!TInfo::Parse(group, nullptr, nullptr)); // Missing state.
        group.ClearVDiskStates();
        group.MutableVDiskStates()->AddVDisk()->SetState(TProto::Included);
        UNIT_ASSERT(!TInfo::Parse(group, nullptr, nullptr)); // Missing position.
        group.ClearVDiskStates();
        auto* invalid = group.MutableVDiskStates()->AddVDisk();
        invalid->SetOrderNumber(0);
        invalid->GetReflection()->MutableUnknownFields(invalid)->AddVarint(2, 99);
        UNIT_ASSERT(!TInfo::Parse(group, nullptr, nullptr)); // Unknown enum on the wire.
        group.ClearVDiskStates();
        const auto cleared = TInfo::Parse(group, nullptr, nullptr);
        UNIT_ASSERT(cleared);
        UNIT_ASSERT(cleared->HasVDiskPolicyChanged(*info));
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

