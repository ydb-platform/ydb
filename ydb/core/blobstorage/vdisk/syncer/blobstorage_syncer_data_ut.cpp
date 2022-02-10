#include <library/cpp/testing/unittest/registar.h>
#include "blobstorage_syncer_data.h"
#include "blobstorage_syncer_dataserdes.h"

using namespace NKikimr;
using namespace NSync;


Y_UNIT_TEST_SUITE(TSyncNeighborsTests) {

    template<typename... TArgs>
    void SerDes(TArgs&&... args) {
        auto info = MakeIntrusive<TBlobStorageGroupInfo>(std::forward<TArgs>(args)...);
        auto it = info->VDisksBegin();
        ++it;
        auto vd = info->GetVDiskId(it->OrderNumber);
        const TVDiskID self = vd;

        // fill in values
        TSyncNeighbors n("Prefix", TActorId(), self, info->PickTopology());
        ui64 i = 123456u;
        for (auto &x: n) {
            x.Get().PeerSyncState.SchTime = TInstant::MicroSeconds(++i);
        }

        // old serialize/parse
        TSyncNeighbors n2("Prefix", TActorId(), self, info->PickTopology());
        {
            TStringStream output;
            n.OldSerialize(output, info.Get());
            n2.OldParse(output.Str());
            i = 123456u;
            for (const auto &x: n2) {
                UNIT_ASSERT_VALUES_EQUAL(x.Get().PeerSyncState.SchTime, TInstant::MicroSeconds(++i));
            }
        }

        // new serialize/parse
        TSyncNeighbors n3("Prefix", TActorId(), self, info->PickTopology());
        {
            TStringStream output;
            n2.Serialize(output, info.Get());
            n3.Parse(output.Str());
            i = 123456u;
            for (const auto &x: n3) {
                UNIT_ASSERT_VALUES_EQUAL(x.Get().PeerSyncState.SchTime, TInstant::MicroSeconds(++i));
            }
        }
    }

    Y_UNIT_TEST(SerDes1) {
        SerDes(TBlobStorageGroupType::ErasureMirror3, 2U, 4U);
    }

    Y_UNIT_TEST(SerDes2) {
        SerDes(TBlobStorageGroupType::Erasure4Plus2Block, 1U, 8U);
    }

    Y_UNIT_TEST(SerDes3) {
        SerDes(TBlobStorageGroupType::Erasure4Plus2Block, 2U, 8U);
    }
}
