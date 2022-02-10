#include "blobstorage_hullstorageratio.h"
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>

namespace NKikimr {

#define STR Cnull

    using namespace NHullComp;

    Y_UNIT_TEST_SUITE(TBlobStorageHullStorageRatio) {

        Y_UNIT_TEST(Test) {
            TSstRatio Ratio;

            // item
            Ratio.IndexItemsTotal++;
            Ratio.IndexBytesTotal += 50;
            // item
            Ratio.IndexItemsTotal++;
            Ratio.IndexBytesTotal += 50;
            Ratio.InplacedDataTotal += 100;
            Ratio.IndexItemsKeep++;
            Ratio.IndexBytesKeep += 50;
            Ratio.InplacedDataKeep += 100;


            Ratio.Output(STR);
            STR << "\n";
            STR << Ratio.MonSummary() << "\n";
            UNIT_ASSERT_EQUAL(Ratio.MonSummary(), "50 / 50 / 100 / NA");
        }
    }

} // NKikimr
