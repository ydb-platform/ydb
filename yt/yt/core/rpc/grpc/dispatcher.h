#pragma once

#include "helpers.h"
#include "private.h"

namespace NYT::NRpc::NGrpc {

////////////////////////////////////////////////////////////////////////////////

class TCompletionQueueTag
{
public:
    void* GetTag(int cookie = 0);
    virtual void Run(bool success, int cookie) = 0;

protected:
    virtual ~TCompletionQueueTag() = default;
};

////////////////////////////////////////////////////////////////////////////////

class TDispatcher
{
public:
    static TDispatcher* Get();

    //! Configures the dispatcher.
    /*!
     *  The call must be done prior to any GRPC client or server is created.
     *  Can only be called before initialization, the future calls will throw.
     */
    void Configure(const TDispatcherConfigPtr& config);

    [[nodiscard]] bool IsInitialized() const noexcept;

    TGrpcLibraryLockPtr GetLibraryLock();
    TGuardedGrpcCompletionQueue* PickRandomGuardedCompletionQueue();

private:
    DECLARE_LEAKY_SINGLETON_FRIEND()
    friend class TGrpcLibraryLock;

    TDispatcher();
    ~TDispatcher();

    class TImpl;
    const std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

class TGrpcLibraryLock
    : public TRefCounted
{
private:
    DECLARE_NEW_FRIEND()
    friend class TDispatcher::TImpl;

    TGrpcLibraryLock();
    ~TGrpcLibraryLock();
};

DEFINE_REFCOUNTED_TYPE(TGrpcLibraryLock)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NGrpc
