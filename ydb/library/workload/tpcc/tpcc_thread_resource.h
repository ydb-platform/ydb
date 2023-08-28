#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

namespace NYdbWorkload {
namespace NTPCC {

struct TThreadResource {
    std::shared_ptr<NYdb::NTable::TTableClient> Client;
    std::shared_ptr<NYdb::NTable::TSession> Session;
    TThreadResource(std::shared_ptr<NYdb::NTable::TTableClient>&& client)
        : Client(std::move(client))
    {
        auto status = Client->CreateSession().GetValueSync();
        NYdb::NConsoleClient::ThrowOnError(status);
        Session = std::make_shared<NYdb::NTable::TSession>(status.GetSession());
    }
};

}
}
