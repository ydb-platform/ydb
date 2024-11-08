#include "topfreq_udf.h"

namespace NYql {
    namespace NUdf {
        NUdf::TUniquePtr<NUdf::IUdfModule> CreateTopFreqModule() {
            return new TTopFreqModule();
        }

    }
}
