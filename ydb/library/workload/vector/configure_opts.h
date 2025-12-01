#pragma once

#include <library/cpp/getopt/small/last_getopt_opts.h>

#include <util/generic/string.h>

namespace NYdbWorkload::NVector {

struct TVectorOpts {
    TString VectorType;
    size_t  VectorDimension;
};

void ConfigureVectorOpts(NLastGetopt::TOpts& opts, TVectorOpts* vectorOpts);

}
