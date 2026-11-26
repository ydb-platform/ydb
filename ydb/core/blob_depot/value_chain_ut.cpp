#include "types.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBlobDepot {

    Y_UNIT_TEST_SUITE(BlobDepotValueChainPayload) {
        Y_UNIT_TEST(PreservesPackedAndLegacyS3Ranges) {
            const TS3Locator packed{.Len = 8, .Generation = 1, .KeyId = 1};
            const TS3Locator legacy{.Len = 4, .Generation = 1, .KeyId = 2};
            TValueChain chain;
            auto *item = chain.Add();
            packed.ToProto(item->MutableS3Locator());
            item->SetSubrangeBegin(3);
            item->SetSubrangeEnd(8);
            legacy.ToProto(chain.Add()->MutableS3Locator());

            std::vector<std::tuple<TS3Locator, ui32, ui32>> ranges;
            UNIT_ASSERT(EnumerateValueChainPayload(chain, 1, TOverloaded{
                [](TLogoBlobID, ui32, ui32) { UNIT_FAIL("unexpected local blob"); },
                [&](TS3Locator locator, ui32 begin, ui32 end) { ranges.emplace_back(locator, begin, end); }
            }));
            UNIT_ASSERT_VALUES_EQUAL(ranges.size(), 2);
            UNIT_ASSERT(ranges[0] == std::make_tuple(packed, 3u, 8u));
            UNIT_ASSERT(ranges[1] == std::make_tuple(legacy, 0u, 4u));
        }

        Y_UNIT_TEST(ExcludesLocalFooterFromPayload) {
            const TBlobSeqId sequence{2, 1, 1, 0};
            TValueChain chain;
            auto *item = chain.Add();
            auto *locator = item->MutableBlobLocator();
            sequence.ToProto(locator->MutableBlobSeqId());
            locator->SetTotalDataLen(6);
            locator->SetFooterLen(2);
            item->SetSubrangeBegin(3);

            ui32 blobs = 0;
            UNIT_ASSERT(EnumerateValueChainPayload(chain, 1, TOverloaded{
                [&](TLogoBlobID id, ui32 begin, ui32 end) {
                    UNIT_ASSERT_EQUAL(id, sequence.MakeBlobId(1, EBlobType::VG_COMPOSITE_BLOB, 0, 8));
                    UNIT_ASSERT_VALUES_EQUAL(begin, 3);
                    UNIT_ASSERT_VALUES_EQUAL(end, 6);
                    ++blobs;
                },
                [](TS3Locator, ui32, ui32) { UNIT_FAIL("unexpected S3 object"); }
            }));
            UNIT_ASSERT_VALUES_EQUAL(blobs, 1);
        }

        Y_UNIT_TEST(RejectsRangeOutsideObject) {
            TValueChain chain;
            auto *item = chain.Add();
            TS3Locator{.Len = 8, .Generation = 1, .KeyId = 1}.ToProto(item->MutableS3Locator());
            item->SetSubrangeEnd(ui64{1} << 32);

            UNIT_ASSERT(!EnumerateValueChainPayload(chain, 1, [](auto, ui32, ui32) {
                UNIT_FAIL("unexpected payload");
            }));
        }
    }

} // NKikimr::NBlobDepot
