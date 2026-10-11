#include "vdisk_config.h"
#include <library/cpp/testing/unittest/registar.h>
#include <google/protobuf/text_format.h>


namespace NKikimr {

    Y_UNIT_TEST_SUITE(TVDiskConfigTest) {


        Y_UNIT_TEST(JustConfig) {
            TVDiskConfig cfg(TVDiskConfig::TBaseInfo::SampleForTests());
            UNIT_ASSERT_VALUES_EQUAL(cfg.FreshBufSizeLogoBlobs, 64 << 20);
        }

        TVDiskConfig::TBaseInfo GetDefaultBaseInfo(NKikimrBlobStorage::TVDiskKind::EVDiskKind kind) {
            return TVDiskConfig::TBaseInfo(TVDiskID(), TActorId(), 0x1234, 0x5678,
                    NPDisk::DEVICE_TYPE_ROT, 0x01, kind, 1, {});
        }

        Y_UNIT_TEST(Basic) {
            TString prototext = R"___(
            VDiskKinds {
                Kind: Test1
                BaseKind: Default
                Config {
                    FreshReadReadyMode: false
                }
            }
            VDiskKinds {
                Kind: Test2
                BaseKind: Default
                Config {
                    FreshReadReadyMode: true
                }
            }
            )___";

            TAllVDiskKinds kinds(prototext);
            TIntrusivePtr<TVDiskConfig> cfg;
            cfg = kinds.MakeVDiskConfig(GetDefaultBaseInfo(NKikimrBlobStorage::TVDiskKind::Test1));
            UNIT_ASSERT_VALUES_EQUAL(cfg->FreshReadReadyMode, false);
        }

        Y_UNIT_TEST(RtmrProblem1) {
            TString prototext = R"___(
            VDiskKinds {
            Kind: Log
                Config {
                FreshBufSizeLogoBlobs: 536870912
                FreshUseDreg: true
                }
            }
            )___";

            TAllVDiskKinds kinds(prototext);
            TIntrusivePtr<TVDiskConfig> cfg;
            cfg = kinds.MakeVDiskConfig(GetDefaultBaseInfo(NKikimrBlobStorage::TVDiskKind::Log));
            UNIT_ASSERT_VALUES_EQUAL(cfg->FreshBufSizeLogoBlobs, 536870912);
            UNIT_ASSERT_VALUES_EQUAL(cfg->FreshUseDreg, true);
        }

        Y_UNIT_TEST(RtmrProblem2) {
            TString prototext = R"___(
            VDiskKinds {
            Kind: Log
                Config {
                FreshBufSizeLogoBlobs: 536870912
                FreshUseDreg: true
                }
            }
            )___";

            NKikimrBlobStorage::TAllVDiskKinds allKindsConfig;
            bool result = google::protobuf::TextFormat::ParseFromString(prototext, &allKindsConfig);
            if (!result)
                ythrow yexception() << "failed to parse AllVDiskKinds config:\n" << prototext << "\n";


            TAllVDiskKinds kinds;
            kinds.Merge(allKindsConfig);

            TIntrusivePtr<TVDiskConfig> cfg;
            cfg = kinds.MakeVDiskConfig(GetDefaultBaseInfo(NKikimrBlobStorage::TVDiskKind::Log));
            UNIT_ASSERT_VALUES_EQUAL(cfg->FreshBufSizeLogoBlobs, 536870912);
            UNIT_ASSERT_VALUES_EQUAL(cfg->FreshUseDreg, true);
        }

        Y_UNIT_TEST(ThreeLevels) {
            TString prototext = R"___(
            VDiskKinds {
                Kind: Test1
                BaseKind: Default
                Config {
                    FreshBufSizeLogoBlobs: 32
                }
            }
            VDiskKinds {
                Kind: Test2
                BaseKind: Test1
                Config {
                    FreshBufSizeLogoBlobs: 64
                }
            }
            )___";

            TAllVDiskKinds kinds(prototext);
            TIntrusivePtr<TVDiskConfig> cfgBase;
            TIntrusivePtr<TVDiskConfig> cfgChild;
            TIntrusivePtr<TVDiskConfig> cfgGrand;

            cfgBase  = kinds.MakeVDiskConfig(GetDefaultBaseInfo(NKikimrBlobStorage::TVDiskKind::Default));
            cfgChild = kinds.MakeVDiskConfig(GetDefaultBaseInfo(NKikimrBlobStorage::TVDiskKind::Test1));
            cfgGrand = kinds.MakeVDiskConfig(GetDefaultBaseInfo(NKikimrBlobStorage::TVDiskKind::Test2));

            UNIT_ASSERT_VALUES_EQUAL(cfgBase->FreshBufSizeLogoBlobs, 64 << 20);
            UNIT_ASSERT_VALUES_EQUAL(cfgChild->FreshBufSizeLogoBlobs, 32);
            UNIT_ASSERT_VALUES_EQUAL(cfgGrand->FreshBufSizeLogoBlobs, 64);
        }

        Y_UNIT_TEST(NoMoneyNoHoney) {
            TString prototext;
            TAllVDiskKinds kinds(prototext);
            TIntrusivePtr<TVDiskConfig> cfg;
            cfg = kinds.MakeVDiskConfig(GetDefaultBaseInfo(NKikimrBlobStorage::TVDiskKind::Default));
            UNIT_ASSERT_VALUES_EQUAL(cfg->FreshBufSizeLogoBlobs, 64 << 20);
        }
    }

} // NKikimr
