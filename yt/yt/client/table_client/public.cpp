#include "public.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

const std::string PrimaryLockName("<primary>");

const std::string SystemColumnNamePrefix("$");
const std::string NonexistentColumnName = SystemColumnNamePrefix + "__YT_NONEXISTENT_COLUMN_NAME__";
const std::string TableIndexColumnName = SystemColumnNamePrefix + "table_index";
const std::string RowIndexColumnName = SystemColumnNamePrefix + "row_index";
const std::string RangeIndexColumnName = SystemColumnNamePrefix + "range_index";
const std::string TabletIndexColumnName = SystemColumnNamePrefix + "tablet_index";
const std::string TimestampColumnName = SystemColumnNamePrefix + "timestamp";
const std::string TtlColumnName = SystemColumnNamePrefix + "ttl";
const std::string TimestampColumnPrefix = SystemColumnNamePrefix + "timestamp:";
const std::string CumulativeDataWeightColumnName = SystemColumnNamePrefix + "cumulative_data_weight";
const std::string EmptyValueColumnName = SystemColumnNamePrefix + "empty";
const std::string SequenceNumberColumnName = SystemColumnNamePrefix + "sequence_number";

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
