#pragma once

#include "yql_job_calc.h"
#include "yql_job_infer_schema.h"
#include "yql_job_user.h"

#include <yt/cpp/mapreduce/interface/operation.h>

namespace NYql {

REGISTER_NAMED_RAW_JOB("TYqlCalcJob", TYqlCalcJob);
REGISTER_NAMED_RAW_JOB("TYqlInferSchemaJob", TYqlInferSchemaJob);
REGISTER_NAMED_RAW_JOB("TYqlUserJob", TYqlUserJob);

}
