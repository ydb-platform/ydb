#pragma once

#include <ydb/library/yql/providers/s3/actors_factory/yql_s3_actors_factory.h>

namespace NYql::NDq {
    std::shared_ptr<IS3ActorsFactory> CreateS3ActorsFactory();
}
