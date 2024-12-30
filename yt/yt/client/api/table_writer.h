#pragma once

#include "public.h"
#include "row_batch_writer.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct ITableWriter
    : public virtual IRowBatchWriter
{
    //! Returns the schema to be used for constructing rows.
    virtual const NTableClient::TTableSchemaPtr& GetSchema() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ITableWriter)

////////////////////////////////////////////////////////////////////////////////

struct ITableFragmentWriter
    : public ITableWriter
{
    //! Returns signed write result. Only safe to use after |Close|.
    virtual TSignedWriteFragmentResultPtr GetWriteFragmentResult() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ITableFragmentWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
