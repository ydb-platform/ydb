#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/tx/replication/service/transfer_writer_factory.h>

#include <util/generic/string.h>

namespace NKikimr::NReplication {

struct TTransferWriteStats {
    TDuration ProcessingCpu;
    TDuration ProcessingTime;
    TDuration WriteDuration;
    ui64 WriteBytes;
    ui64 WriteRows;
    ui64 ProcessingErrors = 0;
    ui64 WriteErrors = 0;
};

namespace NTransfer {

class TTransferWriterFactory : public NKikimr::NReplication::NService::ITransferWriterFactory {
public:
    IActor* Create(const Parameters& p) const override;
};

} // namespace NTransfer
} // namespace NKikimr::NReplication
