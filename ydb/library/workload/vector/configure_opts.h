#pragma once

#include <library/cpp/getopt/small/last_getopt_opts.h>

#include <util/generic/string.h>

namespace NYdbWorkload::NVector {

struct TVectorOpts {
    TString VectorType = "float";
    size_t  VectorDimension = 1024;
};

void ConfigureVectorOpts(NLastGetopt::TOpts& opts, TVectorOpts* vectorOpts);

}
