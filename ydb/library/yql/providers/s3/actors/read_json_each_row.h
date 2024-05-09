#pragma once

#include <ydb/library/actors/core/actor.h>

#include <ydb/library/yql/providers/s3/common/source_context.h>

namespace NYql::NDq {

NActors::IActor* CreateS3ReadJsonEachRowActor(TSplitReadContext::TPtr context);

} // namespace NYql::NDq
