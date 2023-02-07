#pragma once

#include <ydb/library/yql/dq/integration/transform/yql_dq_task_transform.h>

namespace NYql {

TTaskTransformFactory CreateYdbDqTaskTransformFactory();

}
