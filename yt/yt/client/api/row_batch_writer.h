#pragma once

#include "public.h"

#include <yt/yt/client/chunk_client/writer_base.h>

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct IRowBatchWriter
    : public NChunkClient::IWriterBase
{
    //! Attempts to write a bunch of rows.
    //!
    //! If false is returned, the next call to #Write must be made after
    //! invoking #GetReadyEvent and waiting for it to become ready.
    virtual bool Write(TRange<NTableClient::TUnversionedRow> rows) = 0;

    //! Returns the name table to be used for constructing rows.
    virtual const NTableClient::TNameTablePtr& GetNameTable() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IRowBatchWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
