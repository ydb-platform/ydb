#pragma once

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/unversioned_reader.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

extern std::pair<NTableClient::TKeyBoundRef, NTableClient::TKeyBoundRef> UniversalRange;

////////////////////////////////////////////////////////////////////////////////

NTableClient::ISchemafulUnversionedReaderPtr CreateRowsetSubrangeReader(
    TFuture<TSharedRange<NTableClient::TUnversionedRow>> asyncRows,
    std::pair<NTableClient::TKeyBoundRef, NTableClient::TKeyBoundRef> readRange = UniversalRange);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
