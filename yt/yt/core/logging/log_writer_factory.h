#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

struct ILogWriterHost
{
    virtual ~ILogWriterHost() = default;

    //! Returns an invoker to be used for background compression
    //! of log data.
    /*!
     *  The threads are being spawned lazily; one should avoid unneeded
     *  invocations of this method.
     */
    virtual IInvokerPtr GetCompressionInvoker() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ILogWriterFactory
    : public virtual TRefCounted
{
    //! Checks that #configNode can be deserialized into
    //! a valid configuration for this particular log writer.
    virtual void ValidateConfig(
        const NYTree::IMapNodePtr& configNode) = 0;

    //! Creates a log writer.
    /*
     *  #configNode is checked by #ValidateConfig prior to this call.
     *
     *  This call must not throw.
     */
    virtual ILogWriterPtr CreateWriter(
        std::unique_ptr<ILogFormatter> formatter,
        TString name,
        const NYTree::IMapNodePtr& configNode,
        ILogWriterHost* host) noexcept = 0;
};

DEFINE_REFCOUNTED_TYPE(ILogWriterFactory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
