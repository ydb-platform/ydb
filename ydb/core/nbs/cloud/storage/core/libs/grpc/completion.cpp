#include "completion.h"

#include <contrib/libs/grpc/include/grpcpp/completion_queue.h>
#include <contrib/libs/grpc/include/grpcpp/impl/completion_queue_tag.h>
#include <contrib/libs/grpc/src/core/lib/iomgr/exec_ctx.h>
#include <contrib/libs/grpc/src/core/lib/surface/completion_queue.h>

#include <util/stream/output.h>

namespace NYdb::NBS {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCompletionTag final
    : public grpc::internal::CompletionQueueTag
    , public grpc_cq_completion
{
private:
    void* Tag;

public:
    TCompletionTag(void* tag)
        : Tag(tag)
    {}

    bool FinalizeResult(void** tag, bool*) override
    {
        *tag = Tag;
        delete this;
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool EnqueueCompletion(grpc::CompletionQueue* completionQueue, void* tag)
{
    grpc_core::ExecCtx exec_ctx;

    auto completion = std::make_unique<TCompletionTag>(tag);
    if (grpc_cq_begin_op(completionQueue->cq(), completion.get())) {
        grpc_cq_end_op(
            completionQueue->cq(),
            completion.get(),
            y_absl::OkStatus(),
            [](void*, grpc_cq_completion*) {},
            nullptr,
            completion.get());

        completion.release();
        return true;   // ownership transferred to CompletionQueue
    }

    return false;
}

}   // namespace NYdb::NBS
