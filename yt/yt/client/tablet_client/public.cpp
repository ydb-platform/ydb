#include "public.h"

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

const TTabletCellId NullTabletCellId;
const TTabletId NullTabletId;
const TStoreId NullStoreId;
const TPartitionId NullPartitionId;
const THunkStorageId NullHunkStorageId;

const std::string TReplicationLogTable::ChangeTypeColumnName("change_type");
const std::string TReplicationLogTable::KeyColumnNamePrefix("key:");
const std::string TReplicationLogTable::ValueColumnNamePrefix("value:");
const std::string TReplicationLogTable::FlagsColumnNamePrefix("flags:");

const std::string TUnversionedUpdateSchema::ChangeTypeColumnName("$change_type");
const std::string TUnversionedUpdateSchema::ValueColumnNamePrefix("$value:");
const std::string TUnversionedUpdateSchema::FlagsColumnNamePrefix("$flags:");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient

