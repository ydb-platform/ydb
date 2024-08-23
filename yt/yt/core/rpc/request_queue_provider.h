#pragma once

#include "public.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

struct IRequestQueueProvider
    : public TRefCounted
{
    //! Finds or creates a queue for a particular request. May return null, in
    //! which case the default queue is supposed to be used.
    virtual TRequestQueue* GetQueue(const NRpc::NProto::TRequestHeader& header) = 0;

    //! Configures a queue.
    /*!
     *  Called once - shorty after the queue has been returned by #GetQueue for
     *  the first time.
     *  \param config The configuration to apply to the queue. May be null. The
     *                provider is free to override any and all aspects of the
     *                config provided and/or apply additional configuration.
     */
    virtual void ConfigureQueue(TRequestQueue* queue, const TMethodConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IRequestQueueProvider)

////////////////////////////////////////////////////////////////////////////////

class TRequestQueueProviderBase
    : public IRequestQueueProvider
{
public:
    void ConfigureQueue(TRequestQueue* queue, const TMethodConfigPtr& config) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
