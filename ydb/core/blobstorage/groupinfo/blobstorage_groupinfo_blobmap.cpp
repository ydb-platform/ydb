#include "blobstorage_groupinfo_blobmap.h"
#include <util/random/fast.h>

namespace NKikimr {
    namespace NBlobMapper {

        class TBasicMapper : public IBlobToDiskMapper {
            const TBlobStorageGroupInfo::TTopology *Topology;
            const ui8 BlobSubgroupSize;

        private:
            ui32 GetIdxInSubgroupImpl(const TVDiskIdShort& vdisk, ui32 hash) {
                const ui32 failDomainOrderNumber = Topology->GetFailDomainOrderNumber(vdisk);
                const ui32 numFailDomains = Topology->GetTotalFailDomainsNum();
                ui32 domainIdx = hash % numFailDomains;

                // get shift of this disk inside ring
                ui32 shift = failDomainOrderNumber + numFailDomains - domainIdx;
                if (shift >= numFailDomains) {
                    shift -= numFailDomains;
                }
                if (shift >= BlobSubgroupSize) {
                    return BlobSubgroupSize;
                }

                const TBlobStorageGroupInfo::TFailDomain& domain = Topology->GetFailDomain(vdisk);
                if (domain.VDisks.size() == 1) {
                    return shift;
                } else {
                    TReallyFastRng32 rng(hash);
                    for (ui32 i = 0; i < shift; ++i) {
                        rng();
                    }

                    return vdisk.VDisk == rng() % domain.VDisks.size() ? shift : BlobSubgroupSize;
                }
            }

        public:
            TBasicMapper(const TBlobStorageGroupInfo::TTopology *topology)
                : Topology(topology)
                , BlobSubgroupSize(Topology->GType.BlobSubgroupSize())
            {}

            void PickSubgroup(ui32 hash, TBlobStorageGroupInfo::TOrderNums &orderNums) override final {
                Y_ABORT_UNLESS(orderNums.empty());

                const ui32 numFailDomains = Topology->GetTotalFailDomainsNum();
                ui32 domainIdx = hash % numFailDomains;

                if (Topology->GetNumVDisksPerFailDomain() == 1) {
                    for (ui32 i = 0; i < BlobSubgroupSize; ++i) {
                        const TBlobStorageGroupInfo::TFailDomain& domain = Topology->GetFailDomain(domainIdx);
                        if (++domainIdx == numFailDomains) {
                            domainIdx = 0;
                        }
                        Y_DEBUG_ABORT_UNLESS(domain.VDisks.size() == 1);
                        const TBlobStorageGroupInfo::TVDiskInfo& vdisk = domain.VDisks[0];
                        orderNums.push_back(vdisk.OrderNumber);
                    }
                } else {
                    TReallyFastRng32 rng(hash);

                    for (ui32 i = 0; i < BlobSubgroupSize; ++i) {
                        const TBlobStorageGroupInfo::TFailDomain& domain = Topology->GetFailDomain(domainIdx);
                        if (++domainIdx == numFailDomains) {
                            domainIdx = 0;
                        }
                        const ui32 vdiskIdx = rng() % domain.VDisks.size();
                        const TBlobStorageGroupInfo::TVDiskInfo& vdisk = domain.VDisks[vdiskIdx];
                        orderNums.push_back(vdisk.OrderNumber);
                    }
                }
            }

            bool BelongsToSubgroup(const TVDiskIdShort& vdisk, ui32 hash) override final {
                return GetIdxInSubgroupImpl(vdisk, hash) < BlobSubgroupSize;
            }

            ui32 GetIdxInSubgroup(const TVDiskIdShort& vdisk, ui32 hash) override final {
                return GetIdxInSubgroupImpl(vdisk, hash);
            }

            TVDiskIdShort GetVDiskInSubgroup(ui32 idxInSubgroup, ui32 hash) override final {
                const ui32 numFailDomains = Topology->GetTotalFailDomainsNum();
                ui32 domainIdx = (static_cast<ui64>(hash) + idxInSubgroup) % numFailDomains;
                const TBlobStorageGroupInfo::TFailDomain& domain = Topology->GetFailDomain(domainIdx);

                if (domain.VDisks.size() == 1) {
                    return domain.VDisks[0].VDiskIdShort;
                } else {
                    TReallyFastRng32 rng(hash);
                    for (ui32 i = 0; i < idxInSubgroup; ++i) {
                        rng();
                    }

                    const ui32 vdiskIdx = rng() % domain.VDisks.size();
                    return domain.VDisks[vdiskIdx].VDiskIdShort;
                }
            }
        };

        class TMirror3dcMapper : public IBlobToDiskMapper {
            const TBlobStorageGroupInfo::TTopology *Topology;
            const ui32 NumFailRealms;
            const ui32 NumFailDomainsPerFailRealm;
            const ui32 NumVDisksPerFailDomain;

            // In mirror-3-dc blob mapper uses 9 disks to put blob on -- first three of them are main replicas and
            // other six are handoff replicas. Their positions in subgroup are as follows:
            //
            // <-DC->
            // 0 1 2 ^
            // 3 4 5 fail domain
            // 6 7 8 V

            const ui32 NumFailRealmsInSubgroup = 3;
            const ui32 NumFailDomainsPerFailRealmInSubgroup = 3;
            const ui32 BlobSubgroupSize = NumFailRealmsInSubgroup * NumFailDomainsPerFailRealmInSubgroup;

        private:
            void GetBaseCoordinates(ui32 hash, ui32 *baseRealm, ui32 *baseDomain, ui32 *baseVDisk) const {
                *baseRealm = hash % NumFailRealms;
                hash /= NumFailRealms;
                *baseDomain = hash % NumFailDomainsPerFailRealm;
                hash /= NumFailDomainsPerFailRealm;
                *baseVDisk = hash % NumVDisksPerFailDomain;
            }

            const TBlobStorageGroupInfo::TVDiskInfo& GetVDiskInfo(ui32 baseRealm, ui32 baseDomain, ui32 baseVDisk,
                    ui32 row, ui32 col) const {
                const ui32 realm = (baseRealm + col) % NumFailRealms;
                const ui32 domain = (baseDomain + row) % NumFailDomainsPerFailRealm;
                const ui32 vdisk = baseVDisk;
                return Topology->FailRealms[realm].FailDomains[domain].VDisks[vdisk];
            }

            void DecomposeIndex(ui32 idxInSubgroup, ui32 *row, ui32 *col) const {
                *row = idxInSubgroup / NumFailRealmsInSubgroup;
                *col = idxInSubgroup % NumFailRealmsInSubgroup;
            }

            ui32 ComposeIndex(ui32 row, ui32 col) const {
                return row * NumFailRealmsInSubgroup + col;
            }

            ui32 GetIdxInSubgroupImpl(const TVDiskIdShort& vdisk, ui32 hash) const {
                ui32 baseRealm;
                ui32 baseDomain;
                ui32 baseVDisk;
                GetBaseCoordinates(hash, &baseRealm, &baseDomain, &baseVDisk);

                ui32 offsRealm = (vdisk.FailRealm + NumFailRealms - baseRealm) % NumFailRealms;
                ui32 offsDomain = (vdisk.FailDomain + NumFailDomainsPerFailRealm - baseDomain) % NumFailDomainsPerFailRealm;
                ui32 offsVDisk = (vdisk.VDisk + NumVDisksPerFailDomain - baseVDisk) % NumVDisksPerFailDomain;

                if (offsRealm >= NumFailRealmsInSubgroup || offsDomain >= NumFailDomainsPerFailRealmInSubgroup || offsVDisk) {
                    return BlobSubgroupSize;
                }

                return ComposeIndex(offsDomain, offsRealm);
            }

        public:
            TMirror3dcMapper(const TBlobStorageGroupInfo::TTopology *topology)
                : Topology(topology)
                , NumFailRealms(Topology->FailRealms.size())
                , NumFailDomainsPerFailRealm(NumFailRealms ? Topology->FailRealms[0].FailDomains.size() : 0)
                , NumVDisksPerFailDomain(NumFailDomainsPerFailRealm ? Topology->FailRealms[0].FailDomains[0].VDisks.size() : 0)
            {
                if (NumFailRealms && NumFailDomainsPerFailRealm && NumVDisksPerFailDomain) {
                    Y_ABORT_UNLESS(NumFailRealms >= NumFailRealmsInSubgroup &&
                            NumFailDomainsPerFailRealm >= NumFailDomainsPerFailRealmInSubgroup,
                            "mirror-3-dc group tolopogy is invalid: %s", topology->ToString().data());
                }
            }

            void PickSubgroup(ui32 hash, TBlobStorageGroupInfo::TOrderNums &orderNums) override final {
                Y_ABORT_UNLESS(orderNums.empty());

                ui32 baseRealm;
                ui32 baseDomain;
                ui32 baseVDisk;
                GetBaseCoordinates(hash, &baseRealm, &baseDomain, &baseVDisk);

                // make output matrix; process in order of increasing index
                for (ui32 row = 0; row < NumFailDomainsPerFailRealmInSubgroup; ++row) {
                    for (ui32 col = 0; col < NumFailRealmsInSubgroup; ++col) {
                        const TBlobStorageGroupInfo::TVDiskInfo& vdiskInfo = GetVDiskInfo(baseRealm, baseDomain,
                                baseVDisk, row, col);
                        orderNums.push_back(vdiskInfo.OrderNumber);
                    }
                }
            }

            bool BelongsToSubgroup(const TVDiskIdShort& vdisk, ui32 hash) override final {
                return GetIdxInSubgroupImpl(vdisk, hash) < BlobSubgroupSize;
            }

            ui32 GetIdxInSubgroup(const TVDiskIdShort& vdisk, ui32 hash) override final {
                return GetIdxInSubgroupImpl(vdisk, hash);
            }

            TVDiskIdShort GetVDiskInSubgroup(ui32 idxInSubgroup, ui32 hash) override final {
                ui32 baseRealm;
                ui32 baseDomain;
                ui32 baseVDisk;
                GetBaseCoordinates(hash, &baseRealm, &baseDomain, &baseVDisk);
                ui32 row;
                ui32 col;
                DecomposeIndex(idxInSubgroup, &row, &col);
                const TBlobStorageGroupInfo::TVDiskInfo& vdiskInfo = GetVDiskInfo(baseRealm, baseDomain, baseVDisk,
                        row, col);
                return vdiskInfo.VDiskIdShort;
            }
        };

    } // NBlobMapper

    IBlobToDiskMapper *IBlobToDiskMapper::CreateBasicMapper(const TBlobStorageGroupInfo::TTopology *topology) {
        return new NBlobMapper::TBasicMapper(topology);
    }

    IBlobToDiskMapper *IBlobToDiskMapper::CreateMirror3dcMapper(const TBlobStorageGroupInfo::TTopology *topology) {
        return new NBlobMapper::TMirror3dcMapper(topology);
    }

} // NKikimr
