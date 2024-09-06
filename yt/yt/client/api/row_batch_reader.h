#pragma once

#include "public.h"

#include <yt/yt/client/chunk_client/ready_event_reader_base.h>

#include <yt/yt/client/table_client/config.h>
#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct IRowBatchReader
    : public NChunkClient::IReadyEventReaderBase
{
    //! Attempts to read a batch of rows.
    //!
    //! If the returned row batch is empty, the rows are not immediately
    //! available, and the client must invoke #GetReadyEvent and wait.
    //! Returns nullptr when there are no more rows.
    virtual NTableClient::IUnversionedRowBatchPtr Read(const NTableClient::TRowBatchReadOptions& options = {}) = 0;

    //! Returns the name table used for constructing rows.
    virtual const NTableClient::TNameTablePtr& GetNameTable() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IRowBatchReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
