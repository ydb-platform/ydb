#pragma once

#include <ydb/core/metering/bill_record.h>

namespace NKikimr {
namespace NSchemeShard {

struct TRUCalculator {
    // https://a.yandex-team.ru/arc/trunk/arcadia/kikimr/docs/ru/pricing/serverless.md
    static ui64 ReadTable(ui64 bytes);
    static ui64 BulkUpsert(ui64 bytes, ui64 rows);

}; // TRUCalculator

} // NSchemeShard
} // NKikimr
