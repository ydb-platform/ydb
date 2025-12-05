#pragma once

#include <library/cpp/getopt/small/last_getopt_opts.h>

#include <util/generic/string.h>

namespace NYdbWorkload::NVector {

struct TTableOpts {
    TString Name = "vector_index_workload";
};

struct TTablePartitioningOpts {
    size_t MinPartitions = 40;
    size_t PartitionSize = 2000;
    bool AutoPartitioningBySize = true;
    bool AutoPartitioningByLoad = false;
};

struct TVectorOpts {
    TString VectorType = "float";
    size_t  VectorDimension = 1024;
};

void ConfigureTableOpts(NLastGetopt::TOpts& opts, TTableOpts* tableOpts);

void ConfigureTablePartitioningOpts(NLastGetopt::TOpts& opts, TTablePartitioningOpts* partitioningOpts);

void ConfigureVectorOpts(NLastGetopt::TOpts& opts, TVectorOpts* vectorOpts);

}
