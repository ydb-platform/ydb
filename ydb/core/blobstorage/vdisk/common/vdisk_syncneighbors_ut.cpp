#include <library/cpp/testing/unittest/registar.h>
#include "vdisk_syncneighbors.h"

using namespace NKikimr;
using namespace NSync;

namespace {
    TIntrusivePtr<TBlobStorageGroupInfo> CreateTestGroup(ui32 numDomains, ui32 numFailRealms) {
        return new TBlobStorageGroupInfo(TBlobStorageGroupType::Erasure4Plus2Block, 5, numDomains, numFailRealms);
    }

    TVector<TVDiskID> GetVDisks(const TIntrusivePtr<TBlobStorageGroupInfo>& info) {
        TVector<TVDiskID> ret;
        for (const auto& x : info->GetVDisks()) {
            auto vd = info->GetVDiskId(x.OrderNumber);
            ret.push_back(vd);
        }
        return ret;
    }

    struct TPayload {
        ui32 Value = 0;

        TPayload() = default;

        TPayload(ui32 value)
            : Value(value)
        {}

        bool operator ==(const TPayload& other) const {
            return Value == other.Value;
        }
    };

    class TSer {
    public:
        TSer(IOutputStream &str)
            : Str(str)
        {}

        void operator() (const TVDiskInfo<TPayload> &val) {
            Save(&Str, val.Get().Value);
        }

        void Finish() {}

    private:
        IOutputStream &Str;
    };

    class TDes {
    public:
        TDes(IInputStream &str)
            : Str(str)
        {}

        void operator() (TVDiskInfo<TPayload> &val) {
            Load(&Str, val.Get().Value);
        }

        void Finish() {
            char c = '\0';
            if (Str.ReadChar(c))
                ythrow yexception() << "not eof";
        }

    private:
        IInputStream &Str;
    };

}

Y_UNIT_TEST_SUITE(TBlobStorageSyncNeighborsTest) {

    Y_UNIT_TEST(IterateOverAllDisks) {
        const ui32 numDomains = 8;
        TIntrusivePtr<TBlobStorageGroupInfo> info = CreateTestGroup(numDomains, 2);
        TVector<TVDiskID> vdisks = GetVDisks(info);
        const TVDiskID& self = vdisks[0];
        TVDiskNeighbors<TPayload> neighbors(self, info->PickTopology());

        THashSet<TVDiskID> temp(vdisks.begin(), vdisks.end());
        for (const auto& item : neighbors) {
            const size_t count = temp.erase(TVDiskID(info->GroupID, info->GroupGeneration, item.VDiskIdShort));
            UNIT_ASSERT_VALUES_EQUAL(count, 1);
        }
        UNIT_ASSERT_VALUES_EQUAL(temp.size(), 0);
    }

    Y_UNIT_TEST(CheckRevLookup) {
        const ui32 numDomains = 8;
        TIntrusivePtr<TBlobStorageGroupInfo> info = CreateTestGroup(numDomains, 2);
        TVector<TVDiskID> vdisks = GetVDisks(info);
        const TVDiskID& self = vdisks[0];
        TVDiskNeighbors<TPayload> neighbors(self, info->PickTopology());

        for (const TVDiskID& vdisk : vdisks) {
            UNIT_ASSERT_EQUAL(TVDiskIdShort(vdisk), neighbors[vdisk].VDiskIdShort);
        }
    }

    Y_UNIT_TEST(CheckIsMyDomain) {
        const ui32 numDomains = 8;
        TIntrusivePtr<TBlobStorageGroupInfo> info = CreateTestGroup(numDomains, 2);
        TVector<TVDiskID> vdisks = GetVDisks(info);
        const TVDiskID& self = vdisks[0];
        TVDiskNeighbors<TPayload> neighbors(self, info->PickTopology());

        for (ui32 i = 0; i < numDomains; ++i) {
            auto domains = neighbors.GetFailDomains();
            auto it = domains.begin();
            for (ui32 k = 0; k < i; ++k) {
                ++it;
            }
            UNIT_ASSERT_VALUES_EQUAL(neighbors.IsMyFailDomain(it), i == self.FailDomain);
        }
    }

    Y_UNIT_TEST(SerDes) {
        const ui32 numDomains = 8;
        TIntrusivePtr<TBlobStorageGroupInfo> info = CreateTestGroup(numDomains, 2);
        TVector<TVDiskID> vdisks = GetVDisks(info);
        const TVDiskID& self = vdisks[0];
        TVDiskNeighborsSerializable<TPayload> neighbors(self, info->PickTopology());

        ui32 index = 0;
        for (const TVDiskID& vdisk : vdisks) {
            neighbors[vdisk].Get() = index++;
        }

        // serialize
        TStringStream outputStream;
        TSer ser(outputStream);
        neighbors.GenericSerialize(ser);
        // parse
        TVDiskNeighborsSerializable<TPayload> n2(self, info->PickTopology());
        TStringInput inp(outputStream.Str());
        TDes des(inp);
        n2.GenericParse(des);
        for (const TVDiskID& vdisk : vdisks) {
            UNIT_ASSERT_EQUAL(neighbors[vdisk].Get(), n2[vdisk].Get());
        }
    }

    Y_UNIT_TEST(CheckVDiskIterators) {
        TBlobStorageGroupInfo info(TBlobStorageGroupType::ErasureMirror3dc, 2, 3, 3);
        const TBlobStorageGroupInfo::TTopology& topo = info.GetTopology();
        for (ui32 i = 0; i < topo.GetTotalVDisksNum(); ++i) {
            const TVDiskIdShort& self = topo.GetVDiskId(i);
            TVDiskNeighbors<TPayload> neighbors(self, info.PickTopology());

            auto it1 = info.VDisksBegin();
            auto it2 = neighbors.Begin();
            while (it1 != info.VDisksEnd() && it2 != neighbors.End()) {
                UNIT_ASSERT_EQUAL(it1->VDiskIdShort, it2->VDiskIdShort);
                ++it1, ++it2;
            }
            UNIT_ASSERT_EQUAL(it1, info.VDisksEnd());
            UNIT_ASSERT_EQUAL(it2, neighbors.End());
        }
    }

    Y_UNIT_TEST(CheckFailDomainsIterators) {
        TBlobStorageGroupInfo info(TBlobStorageGroupType::ErasureMirror3dc, 2, 3, 3);
        const TBlobStorageGroupInfo::TTopology& topo = info.GetTopology();
        for (ui32 i = 0; i < topo.GetTotalVDisksNum(); ++i) {
            const TVDiskIdShort& self = topo.GetVDiskId(i);
            TVDiskNeighbors<TPayload> neighbors(self, info.PickTopology());
            const auto& range = neighbors.GetFailDomains();
            size_t index = 0;
            for (auto it = range.begin(); it != range.end(); ++it, ++index) {
                auto fd = info.FailDomainsBegin();
                for (size_t i = 0; i < index; ++i) {
                    ++fd;
                }

                const auto& range = *it;
                auto it1 = fd.FailDomainVDisksBegin();
                auto it2 = range.begin();
                while (it1 != fd.FailDomainVDisksEnd() && it2 != range.end()) {
                    UNIT_ASSERT_EQUAL(it1->VDiskIdShort, it2->VDiskIdShort);
                    ++it1, ++it2;
                }
                UNIT_ASSERT_EQUAL(it1, fd.FailDomainVDisksEnd());
                UNIT_ASSERT_EQUAL(it2, range.end());
            }
        }
    }

    Y_UNIT_TEST(CheckVDiskDistance) {
        TBlobStorageGroupInfo info(TBlobStorageGroupType::ErasureMirror3dc, 2, 3, 3);
        const TBlobStorageGroupInfo::TTopology& topo = info.GetTopology();
        const TVDiskIdShort& self = topo.GetVDiskId(0);
        TVDiskNeighbors<TPayload> neighbors(self, info.PickTopology());

        for (auto it1 = neighbors.Begin(); it1 != neighbors.End(); ++it1) {
            size_t expectedDistance = 0;
            for (auto it2 = it1; it2 != neighbors.End(); ++it2, ++expectedDistance) {
                UNIT_ASSERT_VALUES_EQUAL(it2 - it1, expectedDistance);
            }
        }
    }

}
