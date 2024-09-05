#pragma once

#include "unversioned_row.h"

#include <yt/yt/client/chunk_client/writer_base.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/range.h>

#include <yt/yt/core/crypto/crypto.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  Writes non-versioned, fixed-width, strictly typed rowset with given schema.
 *  Useful for: query engine.
 */
struct IUnversionedRowsetWriter
    : public virtual NChunkClient::IWriterBase
{
    /*!
     *  Writes given rows.
     *
     *  The returned value is |true| if one can write next rowset immediately,
     *  otherwise one should wait for |GetReadyEvent()| future.
     *
     *  Every row must contain exactly one value for each column in schema, in the same order.
     */
    [[nodiscard]] virtual bool Write(TRange<TUnversionedRow> rows) = 0;

    /*!
     * Returns the digest of the written rows.
     *
     * Useful for checking the determinism of user jobs.
     * Returns nullopt when hash is not computed.
     *
     * Must not be called concurrently with Write method.
     */
    virtual std::optional<NCrypto::TMD5Hash> GetDigest() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IUnversionedRowsetWriter)

////////////////////////////////////////////////////////////////////////////////

//! Writes a schemaless unversioned rowset.
/*!
 *  Writes unversioned rowset with schema and variable columns.
 *  Useful for: mapreduce jobs, write command.
 */
struct IUnversionedWriter
    : public IUnversionedRowsetWriter
{
    virtual const TNameTablePtr& GetNameTable() const = 0;
    virtual const TTableSchemaPtr& GetSchema() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IUnversionedWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
