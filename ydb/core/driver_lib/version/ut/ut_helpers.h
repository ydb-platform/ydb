#include <ydb/core/driver_lib/version/version.h>

namespace NKikimr {
    
class TCompatibilityInfoTest {
public:
    TCompatibilityInfoTest() = delete;

    static void Reset(NKikimrConfig::TCurrentCompatibilityInfo* newCurrent) {
        CompatibilityInfo.Reset(newCurrent);
    }
};

}
