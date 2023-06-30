#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! The purpose of this interface is to be a virtual base for TMultiChunkSequentialWriter
//! and some specific writers, e.g. IVersionedWriter, to mix them up.
struct IWriterBase
    : public virtual TRefCounted
{
    //! Returns an asynchronous flag enabling to wait until data is written.
    virtual TFuture<void> GetReadyEvent() = 0;

    //! Closes the writer. Must be the last call to the writer.
    virtual TFuture<void> Close() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
