#pragma once

#include "config.h"
#include "versioned_row.h"
#include "row_batch.h"

#include <yt/yt/client/chunk_client/reader_base.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Reads a schemaful versioned rowset.
/*!
 *  Versioned rowset implies that it is:
 *  1. Schemaful.
 *  2. Sorted.
 *  3. No two rows share the same key.
 *
 *  Useful for: merging and compactions.
 */
struct IVersionedReader
    : public virtual NChunkClient::IReaderBase
{
    virtual TFuture<void> Open() = 0;

    //! Tries to read more rows from the reader.
    /*!
     *  Depending on implementation, rows may come in two different flavors.
     *  (A) Rows containing no more than one versioned value for each cell,
     *      no more than one write timestamp (the last one), and no more than one
     *      delete timestamp (for merging).
     *  (B) Rows containing all available versions and two lists of timestamps: write and delete
     *      (for compactions).
     *
     *  Value ids correspond to column indexes in schema.
     *  The returned rows are canonically sorted (see TVersionedRow).
     *
     *  If |nullptr| is returned then the end of the rowset is reached.
     *  If non-null value is returned but the batch is empty then no more data is available
     *  at the moment. The caller must wait for the asynchronous flag provided by #GetReadyEvent
     *  to become set. The latter may indicate an error occurred while fetching more data.
     *
     *  In Case A above row timestamps have the following meaning:
     *  1. If the row is found and is known to be deleted then only the deletion
     *     timestamp is provided.
     *  2. If the row is found and is known to exist then the last write
     *     timestamp and the last deleted timestamp (if exists) are provided.
     */
    virtual IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options = {}) = 0;
};

DEFINE_REFCOUNTED_TYPE(IVersionedReader)

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateEmptyVersionedReader(int rowCount = 0);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
