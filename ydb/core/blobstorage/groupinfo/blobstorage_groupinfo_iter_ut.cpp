#include <library/cpp/testing/unittest/registar.h>
#include "blobstorage_groupinfo_iter.h"

using namespace NKikimr;

TIntrusivePtr<TBlobStorageGroupInfo> CreateTestGroupInfo(TVector<TVDiskID>& vdisks) {
    TIntrusivePtr<TBlobStorageGroupInfo> info = new TBlobStorageGroupInfo(TBlobStorageGroupType::ErasureNone, 5, 10, 5);
    for (const TBlobStorageGroupInfo::TVDiskInfo& vdiskInfo : info->GetVDisks()) {
        vdisks.push_back(info->GetVDiskId(vdiskInfo.OrderNumber));
    }
    return info;
}

Y_UNIT_TEST_SUITE(TBlobStorageGroupInfoIterTest) {
    Y_UNIT_TEST(IteratorForward) {
        TVector<TVDiskID> vdisks;
        TIntrusivePtr<TBlobStorageGroupInfo> info = CreateTestGroupInfo(vdisks);
        ui32 pos = 0;
        for (auto it = info->VDisksBegin(), end = info->VDisksEnd(); it != end; ++it, ++pos) {
            auto vd = info->GetVDiskId(it->OrderNumber);
            UNIT_ASSERT_EQUAL(vd, vdisks[pos]);
        }
    }

    Y_UNIT_TEST(IteratorBackward) {
        TVector<TVDiskID> vdisks;
        TIntrusivePtr<TBlobStorageGroupInfo> info = CreateTestGroupInfo(vdisks);
        auto end = info->VDisksEnd();
        auto it = end;
        for (ui32 pos = vdisks.size(); pos--; ) {
            --it;
            auto vd = info->GetVDiskId(it->OrderNumber);
            UNIT_ASSERT_EQUAL(vd, vdisks[pos]);
        }
    }

    Y_UNIT_TEST(IteratorForwardAndBackward) {
        TVector<TVDiskID> vdisks;
        TIntrusivePtr<TBlobStorageGroupInfo> info = CreateTestGroupInfo(vdisks);
        ui32 pos = 0;
        for (auto it = info->VDisksBegin(), end = info->VDisksEnd(); it != end; ++it, ++pos) {
            auto temp = it;
            for (ui32 i = 0; i <= pos; ++i) {
                UNIT_ASSERT_EQUAL(info->GetVDiskId(temp->OrderNumber), vdisks[pos - i]);
                if (i != pos) {
                    --temp;
                } else {
                    UNIT_ASSERT_EQUAL(temp, info->VDisksBegin());
                }
            }
        }
        UNIT_ASSERT_EQUAL(pos, vdisks.size());
    }

    Y_UNIT_TEST(PerRealmIterator) {
        TVector<TVDiskID> vdisks;
        TIntrusivePtr<TBlobStorageGroupInfo> info = CreateTestGroupInfo(vdisks);
        for (auto realmIt = info->FailRealmsBegin(); realmIt != info->FailRealmsEnd(); ++realmIt) {
            TVector<TVDiskID> vdisksInFailRealm;
            for (const TVDiskID& vdisk : vdisks) {
                if (vdisk.FailRealm == realmIt.GetFailRealmIdx()) {
                    vdisksInFailRealm.push_back(vdisk);
                }
            }

            ui32 pos = 0;
            for (auto it = realmIt.FailRealmVDisksBegin(), end = realmIt.FailRealmVDisksEnd(); it != end; ++it, ++pos) {
                auto temp = it;
                for (ui32 i = 0; i <= pos; ++i) {
                    UNIT_ASSERT_EQUAL(info->GetVDiskId(temp->OrderNumber), vdisksInFailRealm[pos - i]);
                    if (i != pos) {
                        --temp;
                    } else {
                        UNIT_ASSERT_EQUAL(temp, realmIt.FailRealmVDisksBegin());
                    }
                }
            }
            UNIT_ASSERT_EQUAL(pos, vdisksInFailRealm.size());
        }
    }

    Y_UNIT_TEST(PerFailDomainRange) {
        TVector<TVDiskID> vdisks;
        TIntrusivePtr<TBlobStorageGroupInfo> info = CreateTestGroupInfo(vdisks);
        for (auto it = info->FailRealmsBegin(); it != info->FailRealmsEnd(); ++it) {
            for (auto domIt = it.FailRealmFailDomainsBegin(); domIt != it.FailRealmFailDomainsEnd(); ++domIt) {
                TVector<TVDiskID> vdisksInDomain;
                for (const TVDiskID& vdisk : vdisks) {
                    if (vdisk.FailRealm == domIt.GetFailRealmIdx() && vdisk.FailDomain == domIt.GetFailDomainIdx()) {
                        vdisksInDomain.push_back(vdisk);
                    }
                }

                ui32 pos = 0;
                for (const auto& vdisk : domIt.GetFailDomainVDisks()) {
                    auto vd = info->GetVDiskId(vdisk.OrderNumber);
                    UNIT_ASSERT_EQUAL(vdisksInDomain[pos], vd);
                    ++pos;
                }
                UNIT_ASSERT_EQUAL(pos, vdisksInDomain.size());
            }
        }
    }

    Y_UNIT_TEST(Domains) {
        TVector<TVDiskID> vdisks;
        TIntrusivePtr<TBlobStorageGroupInfo> info = CreateTestGroupInfo(vdisks);
        TVector<std::pair<ui32, ui32>> x1, x2;
        for (auto it = info->FailRealmsBegin(); it != info->FailRealmsEnd(); ++it) {
            for (auto domIt = it.FailRealmFailDomainsBegin(); domIt != it.FailRealmFailDomainsEnd(); ++domIt) {
                x1.emplace_back(it.GetFailRealmIdx(), domIt.GetFailDomainIdx());
            }
        }
        for (auto domIt = info->FailDomainsBegin(); domIt != info->FailDomainsEnd(); ++domIt) {
            x2.emplace_back(domIt.GetFailRealmIdx(), domIt.GetFailDomainIdx());
        }
        UNIT_ASSERT_VALUES_EQUAL(x1, x2);
    }

    Y_UNIT_TEST(Indexes) {
        TVector<TVDiskID> vdisks;
        TIntrusivePtr<TBlobStorageGroupInfo> info = CreateTestGroupInfo(vdisks);
        for (auto it = info->VDisksBegin(); it != info->VDisksEnd(); ++it) {
            UNIT_ASSERT_EQUAL(it->VDiskIdShort.FailRealm, it.GetFailRealmIdx());
            UNIT_ASSERT_EQUAL(it->VDiskIdShort.FailDomain, it.GetFailDomainIdx());
            UNIT_ASSERT_EQUAL(it->VDiskIdShort.VDisk, it.GetVDiskIdx());
        }
    }

    Y_UNIT_TEST(WalkFailRealms) {
        TVector<TVDiskID> vdisks;
        TIntrusivePtr<TBlobStorageGroupInfo> info = CreateTestGroupInfo(vdisks);
        ui32 num = 0;
        for (auto it = info->FailRealmsBegin(); it != info->FailRealmsEnd(); ++it) {
            UNIT_ASSERT_EQUAL(num, it.GetFailRealmIdx());
            ++num;
        }
    }
}
