#pragma once

namespace NKikimr::NReplication {

struct TTransferReadStats {
    TDuration ReadTime = TDuration::Zero();
    TDuration DecompressCpu = TDuration::Zero();
    i64 Partition = -1;
    ui64 Offset = 0;
    ui64 Messages = 0;
    ui64 Bytes = 0;
    ui64 Errors = 0;
};

} // namespace NKikimr::NReplication
