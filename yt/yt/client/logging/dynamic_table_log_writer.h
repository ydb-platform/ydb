#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/logging/log_writer.h>
#include <yt/yt/core/logging/log_writer_factory.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

struct IDynamicTableLogWriterFactory
    : public ILogWriterFactory
{
    //! Sets underlying client used by all dynamic table log writers.
    //! Singular flush iterations may continue to use the previous client until their completion.
    virtual void SetClient(NApi::IClientPtr client) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDynamicTableLogWriterFactory)

////////////////////////////////////////////////////////////////////////////////

//! Log writers created by this factory can write to both ordered and sorted dynamic tables.
//! This is a singleton, so you can use this function and the interface method declared above to
//! reconfigure the underlying client for existing dynamic table log writers.
IDynamicTableLogWriterFactoryPtr GetDynamicTableLogWriterFactory();

//! Creates and registers the dynamic table log writer factory in the default log manager.
//! NB: Make sure to call this *before* configuring any dynamic table log writers.
void RegisterDynamicTableLogWriterFactory();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
