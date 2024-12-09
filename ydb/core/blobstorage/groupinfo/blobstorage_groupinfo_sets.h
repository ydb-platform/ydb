#pragma once

#include "defs.h"
#include "blobstorage_groupinfo_iter.h"

#include <bit>

namespace NKikimr {

    // There are basically three types of sets introduced in blob storage groups: a set of disks inside the whole group
    // (referred to as a TGroupVDisks), a set of disks inside a single blob's decomposition (subgroup, TSubgroupVDisks),
    // and a set of fail domains inside the group.
    //
    // Consider the following example group:
    //
    // +---------------+  +---------------+  +---------------+
    // |  FAIL REALM 0 |  |  FAIL REALM 1 |  |  FAIL REALM 2 |
    // |               |  |               |  |               |
    // | +-----------+ |  | +-----------+ |  | +-----------+ |
    // | |  DOMAIN 0 | |  | |  DOMAIN 0 | |  | |  DOMAIN 0 | |
    // | |     #0    | |  | |     #3    | |  | |     #6    | |
    // | |           | |  | |           | |  | |           | |
    // | | +-------+ | |  | | +-------+ | |  | | +-------+ | |
    // | | |VDISK 0| | |  | | |VDISK 0| | |  | | |VDISK 0| | |
    // | | |  #0   | | |  | | |  #6   | | |  | | |  #12  | | |
    // | | +-------+ | |  | | +-------+ | |  | | +-------+ | |
    // | |           | |  | |           | |  | |           | |
    // | | +-------+ | |  | | +-------+ | |  | | +-------+ | |
    // | | |VDISK 1| | |  | | |VDISK 1| | |  | | |VDISK 1| | |
    // | | |  #1   | | |  | | |  #7   | | |  | | |  #13  | | |
    // | | +-------+ | |  | | +-------+ | |  | | +-------+ | |
    // | +-----------+ |  | +-----------+ |  | +-----------+ |
    // |               |  |               |  |               |
    // | +-----------+ |  | +-----------+ |  | +-----------+ |
    // | |  DOMAIN 1 | |  | |  DOMAIN 1 | |  | |  DOMAIN 1 | |
    // | |     #1    | |  | |     #4    | |  | |     #7    | |
    // | |           | |  | |           | |  | |           | |
    // | | +-------+ | |  | | +-------+ | |  | | +-------+ | |
    // | | |VDISK 0| | |  | | |VDISK 0| | |  | | |VDISK 0| | |
    // | | |  #2   | | |  | | |  #8   | | |  | | |  #14  | | |
    // | | +-------+ | |  | | +-------+ | |  | | +-------+ | |
    // | |           | |  | |           | |  | |           | |
    // | | +-------+ | |  | | +-------+ | |  | | +-------+ | |
    // | | |VDISK 1| | |  | | |VDISK 1| | |  | | |VDISK 1| | |
    // | | |  #3   | | |  | | |  #9   | | |  | | |  #15  | | |
    // | | +-------+ | |  | | +-------+ | |  | | +-------+ | |
    // | +-----------+ |  | +-----------+ |  | +-----------+ |
    // |               |  |               |  |               |
    // | +-----------+ |  | +-----------+ |  | +-----------+ |
    // | |  DOMAIN 2 | |  | |  DOMAIN 2 | |  | |  DOMAIN 2 | |
    // | |     #2    | |  | |     #5    | |  | |     #8    | |
    // | |           | |  | |           | |  | |           | |
    // | | +-------+ | |  | | +-------+ | |  | | +-------+ | |
    // | | |VDISK 0| | |  | | |VDISK 0| | |  | | |VDISK 0| | |
    // | | |  #4   | | |  | | |  #10  | | |  | | |  #16  | | |
    // | | +-------+ | |  | | +-------+ | |  | | +-------+ | |
    // | |           | |  | |           | |  | |           | |
    // | | +-------+ | |  | | +-------+ | |  | | +-------+ | |
    // | | |VDISK 1| | |  | | |VDISK 1| | |  | | |VDISK 1| | |
    // | | |  #5   | | |  | | |  #11  | | |  | | |  #17  | | |
    // | | +-------+ | |  | | +-------+ | |  | | +-------+ | |
    // | +-----------+ |  | +-----------+ |  | +-----------+ |
    // +---------------+  +---------------+  +---------------+
    //
    // In any BS group a single VDisk can be referred to as a tuple of (group id, group generation, fail realm, fail
    // domain, vdisk) -- provided by the TVDiskID structure, or by the order number of VDisk inside the continuously
    // numbered group. E.g. in the example group we have VDisk with order number #14 (inside the VDisk box) matching the
    // TVDiskID(id, gen, 2, 1, 0). This order number #14 can be obtained by Info->GetOrderNumber(vdisk) function.
    // Also, each fail domain inside the whole group has its own continuously numbered FailDomainOrderNumber. E.g.
    // fail domain 2 in fail realm 1 has the order number of 5.
    //
    // Internally each set contains information if the corresponding entity is in set or not. This is achieved by using
    // bitfields, in which indexes represent the order number of entities inside the group -- VDisk order numbers for
    // TGroupVDisks, fail domain order numbers for TGroupFailDomains, and IdxInSubgroup for subgroups.
    //
    // TGroupVDisks set is used in commands that span over the whole group; usually it is used to track erroneous replies
    // from VDisks in quorum tracker to determine whether the fail model was exceeded or not; also it is used in quorum
    // tracker to remember successful replies and to get quorum condition.
    //
    // TGroupFailDomains is used internally by TBlobStorageGroupInfo class to match fail model, because the fail model
    // mentions fail domains as basic entities and works solely with them. There is a simple way to create
    // TGroupFailDomains instance from TGroupVDisks class -- by using TGroupFailDomains::CreateFromGroupDiskSet method.
    // There are two ways of creating domain set from disk set. First of them uses ANY predicate and adds domain to set
    // in case if ANY disk inside that domain is in source disk set. Second one uses ALL predicate and adds only these
    // domains where all of VDisks are in source disk set. When working with failed disks set fail domain is considered
    // failed one if ANY disk inside that domain has failed.
    //
    // TSubgroupVDisks set is used to track status of disks comprising blob subgroup. These disks are returned by
    // Top->PickSubgroup() methods and their ordinal indexes inside TVDiskIds array match their bit positions in set.
    //
    // The common methods of manipulating the sets are (Top is std::shared_ptr<TTopology>):
    //
    // 1. Construct empty set:
    //    TGroupVDisks set(Top.get());
    //
    // 2. Add NEW disk to the set (function asserts if the vdisk is already in set):
    //    set += TGroupVDisks(Top.get(), TVDiskID(vdisk));
    //    set += TSubgroupVDisks(Top.get(), Top->GetIdxInSubgroup(id.Hash(), vdisk));
    //
    // 3. Check if disk is in set:
    //    if (set & TGroupVDisks(Top.get(), vdisk)) { ... }
    //
    // 4. Check if disk is not in set:
    //    if (~set & TGroupVDisks(Top.get(), vdisk)) { ... }
    //
    // 5. Calculate union of two sets:
    //    TGroupVDisks res = set1 | set2;
    //
    // 6. Remove disks of one set from another one:
    //    set1 -= set2; /* shortcut for set1 &= ~set2; */
    //
    // 7. Count number of disks in set:
    //    ui32 num = set.GetNumSetItems();
    //
    // 8. Get the maximum possible number of disks in set:
    //    ui32 num = set.GetNumBits();
    //
    // 9. Check if set is not empty
    //    if (set) { ... }
    //
    // 10. Check if the group fail model allows failure of provided disks set:
    //     if (Top->GetQuorumChecker().CheckFailModelForGroup(failedDisksSet)) { ... }
    //
    // 11. Check for quorum of disks in the group (providing set of successfully answered disks; quorum is obtained when
    //     all disks answer except those who may fail):
    //     if (Top->GetQuorumChecker().CheckQuorumForGroup(successfulDisksSet)) { ... }

    template<typename TDerived>
    class TBlobStorageGroupInfo::TDomainSetBase {
        ui64 Mask;

    protected:
        const TBlobStorageGroupInfo::TTopology *Top;

        TDomainSetBase(const TBlobStorageGroupInfo::TTopology *top)
            : Mask(0)
            , Top(top)
        {}

        TDomainSetBase(const TBlobStorageGroupInfo::TTopology *top, ui32 bitIndex)
            : Mask((ui64)1 << bitIndex)
            , Top(top)
        {}

        TDomainSetBase(const TBlobStorageGroupInfo::TTopology *top, const ui64 *mask)
            : Mask(*mask)
            , Top(top)
        {}

    public:
        // combine this set with the other one and store the result inplace
        friend TDerived& operator |=(TDerived& x, const TDerived& y) {
            Y_ABORT_UNLESS(x.Top == y.Top);
            x.Mask |= y.Mask;
            return x;
        }

        // combine two sets and return the result
        friend TDerived operator |(const TDerived& x, const TDerived& y) {
            TDerived res(x);
            return res |= y;
        }

        // union of two nonintersecting subsets
        friend TDerived& operator +=(TDerived& x, const TDerived& y) {
            Y_ABORT_UNLESS(!(x & y));
            return x |= y;
        }

        // inplace union of two nonintersecting subsets
        friend TDerived operator +(const TDerived& x, const TDerived& y) {
            Y_ABORT_UNLESS(!(x & y));
            return x | y;
        }

        // calculate intersection of two sets and store the result inplace
        friend TDerived& operator &=(TDerived& x, const TDerived& y) {
            Y_ABORT_UNLESS(x.Top == y.Top);
            x.Mask &= y.Mask;
            return x;
        }

        // calculate intersection of two sets and return the result
        friend TDerived operator &(const TDerived& x, const TDerived& y) {
            TDerived res(x);
            return res &= y;
        }

        // calculate set difference of two sets -- return items contained in left set, but not in right one; store result
        // inplace
        friend TDerived& operator -=(TDerived& x, const TDerived& y) {
            return x &= ~y;
        }

        // calculate set difference for two sets and return the result
        friend TDerived operator -(const TDerived& x, const TDerived& y) {
            TDerived res(x);
            return res -= y;
        }

        // calculate the inverse set for the provided group
        friend TDerived operator ~(const TDerived& x) {
            const ui64 mask = ((ui64)1 << x.GetNumBits()) - 1;
            TDerived res(x);
            res.Mask = ~res.Mask & mask;
            return res;
        }

        // checks if the set is not empty
        explicit operator bool() const {
            return Mask != 0;
        }

        // check for specific bit
        bool operator [](ui32 index) const {
            Y_ABORT_UNLESS(index < static_cast<const TDerived&>(*this).GetNumBits());
            return (Mask >> index) & 1;
        }

        // calculate number of set bits
        ui32 GetNumSetItems() const {
            return std::popcount(Mask);
        }

        const TBlobStorageGroupInfo::TTopology *GetTopology() const {
            return Top;
        }

        void Output(IOutputStream& str) const {
            const ui32 numBits = static_cast<const TDerived&>(*this).GetNumBits();
            for (ui32 i = 0; i < numBits; ++i) {
                str << ((Mask >> (numBits - i - 1)) & 1);
            }
        }

        TString ToString() const {
            TStringStream str;
            Output(str);
            return str.Str();
        }

        friend bool operator <(const TDerived& x, const TDerived& y) {
            return x.Mask < y.Mask;
        }

        friend bool operator ==(const TDerived& x, const TDerived& y) {
            return x.Mask == y.Mask;
        }

        friend bool operator !=(const TDerived& x, const TDerived& y) {
            return x.Mask != y.Mask;
        }
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TSubgroupVDisks -- a class that defines subset of a subgroup for a specific blob; it contains bit mask of set
    // disks that can be used to figure out some properties, e.g. failure model fitness

    class TBlobStorageGroupInfo::TSubgroupVDisks
        : public TDomainSetBase<TSubgroupVDisks>
    {
        friend class TDomainSetBase<TSubgroupVDisks>;

        TSubgroupVDisks(const TBlobStorageGroupInfo::TTopology *top, const ui64 *mask)
            : TDomainSetBase(top, mask)
        {}

    public:
        TSubgroupVDisks(const TBlobStorageGroupInfo::TTopology *top)
            : TDomainSetBase(top)
        {}

        // create a set of subgroup disks containing single disk identified by its sequential number inside subgroup
        // decomposition for specific blob; this number is referred to as "nodeId" in Ingress
        TSubgroupVDisks(const TBlobStorageGroupInfo::TTopology *top, ui32 nodeId)
            : TDomainSetBase(top, nodeId)
        {}

        static TSubgroupVDisks CreateFromMask(const TBlobStorageGroupInfo::TTopology *top, ui64 mask) {
            return TSubgroupVDisks(top, &mask);
        }

        // Get the maximum possible number of disks in set
        ui32 GetNumBits() const {
            return Top->GType.BlobSubgroupSize();
        }
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TGroupVDisks -- a class that stored the set of disks inside the whole group; it's like TSubgroupVDisks, but for
    // the whole group

    class TBlobStorageGroupInfo::TGroupVDisks
        : public TDomainSetBase<TGroupVDisks>
    {
        friend class TDomainSetBase<TGroupVDisks>;
        friend class TGroupFailDomains;

        TGroupVDisks(const TBlobStorageGroupInfo::TTopology *top, const ui64 *mask)
            : TDomainSetBase(top, mask)
        {}

    public:
        TGroupVDisks(const TBlobStorageGroupInfo::TTopology *top)
            : TDomainSetBase(top)
        {}

        TGroupVDisks(const TBlobStorageGroupInfo::TTopology *top, const TVDiskIdShort& vdiskId)
            : TDomainSetBase(top, top->GetOrderNumber(vdiskId))
        {}

        static TGroupVDisks CreateFromMask(const TBlobStorageGroupInfo::TTopology *top, ui64 mask) {
            return TGroupVDisks(top, &mask);
        }

        // Get the maximum possible number of disks in set
        ui32 GetNumBits() const {
            return Top->GetTotalVDisksNum();
        }
    };

    class TBlobStorageGroupInfo::TGroupFailDomains
        : public TDomainSetBase<TGroupFailDomains>
    {
        friend class TDomainSetBase<TGroupFailDomains>;

    public:
        enum class EDiskCondition {
            ALL, // the domain is selected when ALL disks of this domain are set
            ANY, // the domain is selected when ANY disk of this domain is set
        };

    private:
        TGroupFailDomains(const TBlobStorageGroupInfo::TTopology *top, ui32 domainOrderNumber)
            : TDomainSetBase(top, domainOrderNumber)
        {}

    public:
        TGroupFailDomains(const TBlobStorageGroupInfo::TTopology *top)
            : TDomainSetBase(top)
        {}

        // create a group domain set containing single domain for provided VDisk
        TGroupFailDomains(const TBlobStorageGroupInfo::TTopology *top, const TVDiskIdShort& vdiskId)
            : TDomainSetBase(top, top->GetFailDomainOrderNumber(vdiskId))
        {}

        // create a group domain set from the disk set; domain is selected in resulting group if the provided condition
        // triggers for disks of that domain
        static TGroupFailDomains CreateFromGroupDiskSet(const TGroupVDisks& disks, EDiskCondition condition) {
            const TBlobStorageGroupInfo::TTopology *top = disks.GetTopology();
            TGroupFailDomains res(top);

            for (auto domIt = top->FailDomainsBegin(), domEnd = top->FailDomainsEnd(); domIt != domEnd; ++domIt) {
                bool someSet = false;
                bool someNotSet = false;
                for (const auto& vdisk : domIt.GetFailDomainVDisks()) {
                    if (disks & TGroupVDisks(top, top->GetVDiskId(vdisk.OrderNumber))) {
                        someSet = true;
                    } else {
                        someNotSet = true;
                    }
                }
                bool match = false;
                switch (condition) {
                    case EDiskCondition::ANY:
                        match = someSet;
                        break;

                    case EDiskCondition::ALL:
                        match = !someNotSet;
                        break;
                }
                if (match) {
                    res += TGroupFailDomains(top, domIt->FailDomainOrderNumber);
                }
            }

            return res;
        }

        // Get the maximum possible number of fail domains in set
        ui32 GetNumBits() const {
            return Top->GetTotalFailDomainsNum();
        }
    };

} // NKikimr
