#include "configure_opts.h"

namespace NYdbWorkload::NVector {

void ConfigureVectorOpts(NLastGetopt::TOpts& opts, TVectorOpts* vectorOpts) {
    opts.AddLongOption( "vector-type", "Type of vectors")
        .Required().StoreResult(&vectorOpts->VectorType);
    opts.AddLongOption( "vector-dimension", "Vector dimension")
        .Required().StoreResult(&vectorOpts->VectorDimension);
}

}
