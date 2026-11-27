#pragma once

#include "public.h"

#include "grpcpp/completion_queue.h"

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

bool EnqueueCompletion(::grpc::CompletionQueue* completionQueue, void* tag);

}   // namespace NYdb::NBS
