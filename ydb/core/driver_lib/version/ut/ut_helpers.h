#include <ydb/core/driver_lib/version/version.h>

class TCompatibilityInfoTest {
public:
    TCompatibilityInfoTest() = delete;

    static void Reset(NKikimrConfig::TCurrentCompatibilityInfo* newCurrent) {
        TCompatibilityInfo::Reset(newCurrent);
    }
};
