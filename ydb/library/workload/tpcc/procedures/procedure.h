#pragma once

#include <ydb/library/workload/tpcc/error.h>
#include <ydb/library/workload/tpcc/config.h>
#include <ydb/library/workload/tpcc/terminal.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <library/cpp/logger/log.h>

#include <util/random/fast.h>
#include <util/generic/ptr.h>


namespace NYdbWorkload {
namespace NTPCC {

using namespace NYdb;
using namespace NTable;

struct TProcedureResource {
    virtual ~TProcedureResource() = default;
};

class IProcedure {
public:
    IProcedure(EProcedureType type, TTerminal& terminal, TLog& log, 
               std::shared_ptr<TTableClient> tableClient, bool debug, ui64 seed);

    virtual ~IProcedure() = default;

    virtual NThreading::TFuture<void> Run(std::shared_ptr<TProcedureResource> resource) = 0;

    virtual bool NotStarted() = 0;

    virtual bool Finished() = 0;

    virtual void StartOver() = 0;

    EProcedureType GetType();

    virtual TString GetStageName() = 0;

    NThreading::TFuture<void> Rollback();

protected:
    virtual void NextStage() = 0;

    NThreading::TFuture<void> RunStages();

    virtual NThreading::TFuture<void> RunCurrentStage(NTable::TSession session) = 0;
    
    NThreading::TFuture<void> CommitTransaction(bool debug=false);

    NThreading::TFuture<void> StartTransaction(NTable::TSession session, bool debug=false);

    TTerminal::ERetryType GetRetryType(EStatus status);

    TFastRng32 Rng;

    EProcedureType Type;

    TMaybe<NTable::TTransaction> Transaction;

    std::shared_ptr<NTable::TTableClient> TableClient;

    TTerminal& Terminal;

    bool DebugMode;

    TLog& Log;
};

}
}
