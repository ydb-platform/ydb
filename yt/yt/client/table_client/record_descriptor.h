#pragma once

#include "public.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IRecordDescriptor
{
    virtual ~IRecordDescriptor() = default;

    virtual const TTableSchemaPtr& GetSchema() const = 0;
    virtual const TNameTablePtr& GetNameTable() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
