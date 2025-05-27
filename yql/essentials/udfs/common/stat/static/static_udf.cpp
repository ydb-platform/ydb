#include "stat_udf.h"

namespace NYql {
    namespace NUdf {
        NUdf::TUniquePtr<NUdf::IUdfModule> CreateStatModule() {
            return new TStatModule();
        }

    }
}
