#include "skeleton_oos_logic.h"
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hull.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>

#define STR Cnull

namespace NKikimr {


    Y_UNIT_TEST_SUITE(TOosLogicTests) {

        Y_UNIT_TEST(RenderHtml) {
            TOutOfSpaceLogic logic(nullptr, nullptr);
            TStringStream str;
            logic.RenderHtml(str);
        }

    }

} // NKikimr
