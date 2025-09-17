#pragma once

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>
#include <ydb/core/persqueue/public/constants.h>

namespace NKikimr::NPQ {

static const ui32 MAX_BLOB_PART_SIZE = 500_KB;

// Extra amount of time YDB should wait before deleting supportive partition for kafka transaction
// after transaction timeout has passed.
//
// Total time till kafka supportive partition deletion = AppData.KafkaProxyConfig.TransactionTimeoutMs + KAFKA_TRANSACTION_DELETE_DELAY_MS
static const ui32 KAFKA_TRANSACTION_DELETE_DELAY_MS = TDuration::Hours(1).MilliSeconds(); // 1 hour;

constexpr char TMP_REQUEST_MARKER[] = "__TMP__REQUEST__MARKER__";

}
