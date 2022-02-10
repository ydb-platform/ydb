#include "blobstorage_groupinfo.h"
#include "blobstorage_groupinfo_iter.h"
#include "blobstorage_groupinfo_sets.h"
#include "blobstorage_groupinfo_partlayout.h"
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TBlobStorageGroupInfoTest) {

    Y_UNIT_TEST(TestBelongsToSubgroup) {
        for (ui32 disks = 1; disks < 4; ++disks) {
            for (ui32 species = 0; species < TBlobStorageGroupType::ErasureSpeciesCount; ++species) {
                if (species == TBlobStorageGroupType::ErasureMirror3dc) {
                    continue;
                }

                const auto erasureType = TErasureType::EErasureSpecies(species);
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

        for (ui32 species = 0; species < TBlobStorageGroupType::ErasureSpeciesCount; ++species) {
            if (species == TBlobStorageGroupType::ErasureMirror3dc || species == TBlobStorageGroupType::ErasureMirror3of4) {
                continue;
            }

            const auto erasureType = TErasureType::EErasureSpecies(species);
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
        for (ui32 i = 0; i < TBlobStorageGroupType::ErasureSpeciesCount; ++i) {
            auto erasure = static_cast<TBlobStorageGroupType::EErasureSpecies>(i);
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
                (PopCount(mask) <= numHandoff ? goodMask : badMask).push_back(mask);
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

