#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/tx/replication/common/transfer_writer_stats.h>
#include <ydb/core/tx/replication/service/transfer_writer_factory.h>

#include <util/generic/string.h>

namespace NKikimr::NReplication {

namespace NTransfer {

class TTransferWriterFactory : public NKikimr::NReplication::NService::ITransferWriterFactory {
public:
    IActor* Create(const Parameters& p) const override;
    IActor* CreateCompilationService() const override;
};

} // namespace NTransfer
} // namespace NKikimr::NReplication
