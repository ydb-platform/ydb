#pragma once

#include "public.h"

#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/table_client/public.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

struct TQueueRowBatchReadOptions
{
    i64 MaxRowCount = 1000;
    i64 MaxDataWeight = 16_MB;

    //! If set, this value is used to compute the number of rows to read considering the given MaxDataWeight.
    std::optional<i64> DataWeightPerRowHint;
};

////////////////////////////////////////////////////////////////////////////////

void VerifyRowBatchReadOptions(const TQueueRowBatchReadOptions& options);

i64 ComputeRowsToRead(const TQueueRowBatchReadOptions& options);

i64 GetStartOffset(const NApi::IUnversionedRowsetPtr& rowset);

////////////////////////////////////////////////////////////////////////////////

struct IQueueRowset
    : public NApi::IUnversionedRowset
{
    virtual i64 GetStartOffset() const = 0;
    virtual i64 GetFinishOffset() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IQueueRowset)

////////////////////////////////////////////////////////////////////////////////

IQueueRowsetPtr CreateQueueRowset(NApi::IUnversionedRowsetPtr rowset, i64 startOffset);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
