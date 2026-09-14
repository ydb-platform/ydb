
#pragma once

#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/protos/kqp.pb.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>

namespace NKikimr::NKqp {

void RegisterKqpSysViewSource(NYql::NDq::TDqAsyncIoFactory& factory, TIntrusivePtr<TKqpCounters> counters);

} // namespace NKikimr::NKqp
