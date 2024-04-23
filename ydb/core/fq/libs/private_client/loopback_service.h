#pragma once

#include "events.h"

#include <ydb/core/fq/libs/signer/signer.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NFq {

NActors::IActor* CreateLoopbackServiceActor(const ::NFq::TSigner::TPtr& signer,
                                            const ::NMonitoring::TDynamicCounterPtr& counters);

} /* NFq */
