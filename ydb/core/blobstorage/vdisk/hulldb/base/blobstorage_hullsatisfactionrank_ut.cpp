#include "blobstorage_hullsatisfactionrank.h"
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>

#define STR Cnull

namespace NKikimr {

    using namespace NSat;

    Y_UNIT_TEST_SUITE(TBlobStorageHullDecimal) {
        using TDecimal = ::NKikimr::NSat::TDecimal<3>;

        Y_UNIT_TEST(TestMkRatio) {
            TDecimal d1 = TDecimal::MkRatio(1, 3);
            STR << d1 << "\n";
            UNIT_ASSERT_VALUES_EQUAL(d1.GetRaw(), 333);

            TDecimal d2 = TDecimal::MkRatio(1, 4);
            STR << d2 << "\n";
            UNIT_ASSERT_VALUES_EQUAL(d2.GetRaw(), 250);

            TDecimal d3 = TDecimal::MkRatio(10, 3);
            STR << d3 << "\n";
            UNIT_ASSERT_VALUES_EQUAL(d3.GetRaw(), 3333);
        }

        Y_UNIT_TEST(TestMkDecimal) {
            TDecimal d1 = TDecimal::MkDecimal(1, 400);
            STR << d1 << "\n";
            UNIT_ASSERT_VALUES_EQUAL(d1.GetRaw(), 1400);

            TDecimal d2 = TDecimal::MkDecimal(1, 458);
            TDecimal d3 = TDecimal::MkDecimal(7, 845);
            STR << (d2 * d3) << "\n";

            UNIT_ASSERT_VALUES_EQUAL((d2 * d3).GetRaw(), 11438);
        }

        Y_UNIT_TEST(TestMult) {
            {
                TDecimal d1 = TDecimal::MkRatio(1, 3);
                TDecimal d2 = d1 * 5;
                STR << d2 << "\n";
                UNIT_ASSERT_VALUES_EQUAL(d2.GetRaw(), 1665);
            }

            {
                TDecimal d1 = TDecimal::MkRatio(1, 89);
                TDecimal d2 = d1 * 3;
                STR << d2 << "\n";
                UNIT_ASSERT_VALUES_EQUAL(d2.GetRaw(), 33);
            }

            {
                TDecimal d1 = TDecimal::MkRatio(1, 3);
                TDecimal d2 = TDecimal::MkRatio(1, 3);
                TDecimal d3 = d1 * d2;
                STR << d3 << "\n";
                UNIT_ASSERT_VALUES_EQUAL(d3.GetRaw(), 110);
            }
        }

        Y_UNIT_TEST(TestRoundToInt) {
            // round to 1
            {
                TDecimal d1 = TDecimal::MkRatio(1, 3);
                ui64 i1 = d1.RoundToInt(1, 1);
                STR << i1 << "\n";
                UNIT_ASSERT_VALUES_EQUAL(i1, 1);
            }

            {
                TDecimal d1 = TDecimal::MkRatio(2, 1);
                ui64 i1 = d1.RoundToInt(1, 1);
                STR << i1 << "\n";
                UNIT_ASSERT_VALUES_EQUAL(i1, 2);
            }

            {
                TDecimal d1 = TDecimal::MkRatio(10, 3);
                ui64 i1 = d1.RoundToInt(1, 1);
                STR << i1 << "\n";
                UNIT_ASSERT_VALUES_EQUAL(i1, 3);
            }

            // round to other values
            {
                TDecimal d1 = TDecimal::MkRatio(20, 3);
                ui64 i1 = d1.RoundToInt(5, 2);
                STR << i1 << "\n";
                UNIT_ASSERT_VALUES_EQUAL(i1, 5);
            }

            {
                TDecimal d1 = TDecimal::MkRatio(20, 3);
                ui64 i1 = d1.RoundToInt(4, 2);
                STR << i1 << "\n";
                UNIT_ASSERT_VALUES_EQUAL(i1, 6);
            }
        }

        Y_UNIT_TEST(TestToUi64) {
            // < 100%
            {
                TDecimal d1 = TDecimal::MkRatio(1, 3);
                d1 = d1 * 100u;
                ui64 i1 = d1.ToUi64();
                STR << i1 << "\n";
                UNIT_ASSERT_VALUES_EQUAL(i1, 33);
            }
            // < 100%
            {
                TDecimal d1 = TDecimal::MkRatio(138, 100);
                d1 = d1 * 100u;
                ui64 i1 = d1.ToUi64();
                STR << i1 << "\n";
                UNIT_ASSERT_VALUES_EQUAL(i1, 138);
            }
        }
    }


    Y_UNIT_TEST_SUITE(TBlobStorageLinearTrackBar) {

        template <class TTrackBar, class TDecimal>
        void Update(TTrackBar &tb, TDecimal v, typename TTrackBar::TStatus expectedRes) {
            STR << "Update: ";
            auto status = tb.Update(v);
            status.Output(STR);
            STR << "\n";
            UNIT_ASSERT_VALUES_EQUAL(status, expectedRes);
        }

        Y_UNIT_TEST(TestLinearTrackBarDouble) {
            using TDecimal = ::NKikimr::NSat::TDecimalViaDouble;
            using TTrackBar = TLinearTrackBar<TDecimal>;
            using TStatus = typename TTrackBar::TStatus;
            TTrackBar tb(2, 8, 1, 1.3, 2.5);

            Update(tb, 1.4, TStatus(false, 2));
            Update(tb, 1.8, TStatus(true, 3));
            Update(tb, 2.0, TStatus(true, 4));
            Update(tb, 2.3, TStatus(false, 4));

            Update(tb, 0.45, TStatus(true, 2));
        }

        Y_UNIT_TEST(TestLinearTrackBarWithDecimal) {
            using TDecimal = ::NKikimr::NSat::TDecimal<3>;
            using TTrackBar = TLinearTrackBar<TDecimal>;
            using TStatus = typename TTrackBar::TStatus;
            TTrackBar tb(2, 8, 1, TDecimal::MkDecimal(1, 300), TDecimal::MkDecimal(2, 500));

            Update(tb, TDecimal::MkDecimal(1, 400), TStatus(false, 2));
            Update(tb, TDecimal::MkDecimal(1, 800), TStatus(true, 3));
            Update(tb, TDecimal::MkDecimal(2, 000), TStatus(true, 4));
            Update(tb, TDecimal::MkDecimal(2, 300), TStatus(false, 4));

            Update(tb, TDecimal::MkDecimal(0, 450), TStatus(true, 2));
        }
    }

} // NKikimr
