#pragma once

#include "public.h"

#include <yt/yt/core/ytree/public.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

struct TCreateBusOptions
{
    EMultiplexingBand MultiplexingBand = EMultiplexingBand::Default;
};

//! A factory for creating client IBus-es.
/*!
 *  Thread affinity: any.
 */
struct IBusClient
    : public virtual TRefCounted
{
    //! Returns a textual representation of the bus' endpoint.
    //! Typically used for logging.
    virtual const TString& GetEndpointDescription() const = 0;

    //! Returns the bus' endpoint attributes.
    //! Typically used for constructing errors.
    virtual const NYTree::IAttributeDictionary& GetEndpointAttributes() const = 0;

    //! Creates a new bus.
    /*!
     *  The bus will point to the address supplied during construction.
     *
     *  \param handler A handler that will process incoming messages.
     *  \return A new bus.
     *
     */
    virtual IBusPtr CreateBus(
        IMessageHandlerPtr handler,
        const TCreateBusOptions& options = {}) = 0;
};

DEFINE_REFCOUNTED_TYPE(IBusClient)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
