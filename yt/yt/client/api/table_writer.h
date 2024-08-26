#pragma once

#include "public.h"
#include "row_batch_writer.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct ITableWriter
    : public IRowBatchWriter
{
    //! Returns the schema to be used for constructing rows.
    virtual const NTableClient::TTableSchemaPtr& GetSchema() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ITableWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
