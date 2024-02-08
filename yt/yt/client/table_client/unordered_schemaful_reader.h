#pragma once

#include "public.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

ISchemafulUnversionedReaderPtr CreateUnorderedSchemafulReader(
    std::function<ISchemafulUnversionedReaderPtr()> getNextReader,
    int concurrency);

////////////////////////////////////////////////////////////////////////////////

ISchemafulUnversionedReaderPtr CreateOrderedSchemafulReader(
    std::function<ISchemafulUnversionedReaderPtr()> getNextReader);

ISchemafulUnversionedReaderPtr CreatePrefetchingOrderedSchemafulReader(
    std::function<ISchemafulUnversionedReaderPtr()> getNextReader);

ISchemafulUnversionedReaderPtr CreateFullPrefetchingOrderedSchemafulReader(
    std::function<ISchemafulUnversionedReaderPtr()> getNextReader);

ISchemafulUnversionedReaderPtr CreateFullPrefetchingShufflingSchemafulReader(
    std::function<ISchemafulUnversionedReaderPtr()> getNextReader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
