#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>

namespace NKikimr {
namespace NKqp {

void RegisterKqpReadActor(NYql::NDq::TDqAsyncIoFactory& factory);

} // namespace NKqp
} // namespace NKikimr
