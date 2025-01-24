#include "feature_flags_service.h"

namespace NKikimr {

    TActorId MakeFeatureFlagsServiceID() {
        return TActorId(ui32(0), "featureflags");
    }

} // namespace NKikimr
