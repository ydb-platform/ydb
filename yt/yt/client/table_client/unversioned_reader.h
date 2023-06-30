#pragma once

#include "public.h"
#include "config.h"

#include <yt/yt/client/chunk_client/reader_base.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IUnversionedReaderBase
    : public virtual NChunkClient::IReaderBase
{
    //! NB: The returned batch can be invalidated by the next call to `Read`.
    virtual IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ISchemafulUnversionedReader
    : public IUnversionedReaderBase
{ };

DEFINE_REFCOUNTED_TYPE(ISchemafulUnversionedReader)

////////////////////////////////////////////////////////////////////////////////

struct ISchemalessUnversionedReader
    : public IUnversionedReaderBase
{
    virtual const TNameTablePtr& GetNameTable() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchemalessUnversionedReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
