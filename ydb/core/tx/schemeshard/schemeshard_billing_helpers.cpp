#include "schemeshard_billing_helpers.h"

#include <library/cpp/json/json_writer.h>

#include <util/generic/size_literals.h>
#include <util/string/cast.h>

namespace NKikimr {
namespace NSchemeShard {

ui64 TRUCalculator::ReadTable(ui64 bytes) {
    return 128 * ((bytes + 1_MB - 1) / 1_MB);
}

ui64 TRUCalculator::BulkUpsert(ui64 bytes, ui64 rows) {
    return (Max(rows, (bytes + 1_KB - 1) / 1_KB) + 1) / 2;
}

} // NSchemeShard
} // NKikimr
