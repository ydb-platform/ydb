#pragma once

#include "versioned_row.h"

#include <yt/yt/client/chunk_client/writer_base.h>

#include <yt/yt/core/misc/range.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Writes a schemaful versioned rowset.
/*!
 *  Writes versioned rowset with given schema.
 *  Useful for: compactions.
 */
struct IVersionedWriter
    : public virtual NChunkClient::IWriterBase
{
    //! Enqueues more rows into the writer.
    /*!
     *  Value ids must correspond to column indexes in schema.
     *  The rows must be canonically sorted (see TVersionedRow).
     *
     *  If |false| is returned then the writer is overflowed (but the data is nevertheless accepted)
     *  The caller must wait for asynchronous flag provided by #GetReadyEvent to become set.
     *  The latter may indicate an error occurred while fetching more data.
     */
    [[nodiscard]] virtual bool Write(TRange<TVersionedRow> rows) = 0;
};

DEFINE_REFCOUNTED_TYPE(IVersionedWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
