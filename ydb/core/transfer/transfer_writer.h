#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/tx/replication/service/transfer_writer_factory.h>

#include <util/generic/string.h>

namespace NKikimr::NReplication {

struct TTransferWriteStats {
    TDuration ProcessDuration;
    ui64 ProcessCpuMs;
    TDuration WriteDuration;
    ui64 WriteCpuMs;
};

namespace NTransfer {

class TTransferWriterFactory : public NKikimr::NReplication::NService::ITransferWriterFactory {
public:
    IActor* Create(const Parameters& p) const override;
};

} // namespace NTransfer
} // namespace NKikimr::NReplication
