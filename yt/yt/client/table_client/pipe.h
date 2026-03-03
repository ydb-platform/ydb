#pragma once

#include "public.h"

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/memory/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! A pipe connecting a schemaful writer to a schemaful reader.
/*!
 *  \note Thread affinity: any
 */
struct ISchemafulPipe
    : public TRefCounted
{
    //! Returns the reader side of the pipe.
    virtual ISchemafulUnversionedReaderPtr GetReader() const = 0;

    //! Returns the writer side of the pipe.
    virtual IUnversionedRowsetWriterPtr GetWriter() const = 0;

    //! When called, propagates the error to the reader.
    virtual void Fail(const TError& error) = 0;

    //! Sets the reader statistics.
    virtual void SetReaderDataStatistics(NChunkClient::NProto::TDataStatistics dataStatistics) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchemafulPipe);

////////////////////////////////////////////////////////////////////////////////

ISchemafulPipePtr CreateSchemafulPipe(IMemoryChunkProviderPtr chunkProvider);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
