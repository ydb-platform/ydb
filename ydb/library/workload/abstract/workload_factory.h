#pragma once

#include "workload_query_generator.h"

#include <library/cpp/object_factory/object_factory.h>

namespace NYdbWorkload {

    using TWorkloadFactory = NObjectFactory::TObjectFactory<TWorkloadParams, TString>;

} // namespace NYdbWorkload
