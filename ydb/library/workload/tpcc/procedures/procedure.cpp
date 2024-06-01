#include "procedure.h"

#include <ydb/library/workload/tpcc/procedures/tables.h>


namespace NYdbWorkload {
namespace NTPCC {

IProcedure::IProcedure(EProcedureType type, TTerminal& terminal, TLog& log,
                       std::shared_ptr<TTableClient> tableClient, bool debug, ui64 seed) 
    : Rng(seed, 0)
    , Type(type)
    , TableClient(tableClient)
    , Terminal(terminal)
    , DebugMode(debug)
    , Log(log)
{
}

EProcedureType IProcedure::GetType() {
    return Type;
}

NThreading::TFuture<void> IProcedure::StartTransaction(TSession session, bool debug) {
    std::string funcName = __func__;

    TAsyncBeginTransactionResult transactionFuture = session.BeginTransaction(TTxSettings::SerializableRW());
    
    return transactionFuture.Apply(
        [this, debug, funcName](const TAsyncBeginTransactionResult& future) {
            try {
                TBeginTransactionResult result = future.GetValue();
                
                ThrowOnError(static_cast<TStatus>(result), Log, TString(funcName));
                
                Transaction = result.GetTransaction();

                if (debug) {
                    TStringStream stream;
                    stream << "Terminal ID=" << Terminal.GetTerminalId() << ": Transaction " << Transaction->GetId() << " started";
                    Log.Write(TLOG_DEBUG, stream.Str());
                }
            } catch (TErrorException& err) {
                err.AddPrefix("Start transaction failed");
                throw err;
            } catch (const std::exception& ex) {
                throw yexception() << "Start transaction failed: " << ex.what();
            }
        }
    );
}

NThreading::TFuture<void> IProcedure::CommitTransaction(bool debug) {
    if (Transaction.Empty()) {
        throw yexception() << "Attempt to commit an empty transaction";
    }
    std::string funcName = __func__;

    TString transactionId = Transaction->GetId();

    if (debug) {
        TStringStream stream;
        stream << "Terminal ID=" << Terminal.GetTerminalId() << ": ";
        stream << Type << ": Transaction commit started";
        if (!Transaction.Empty()) {
            stream << " (Transaction ID=" << Transaction->GetId() << ")";
        }
        Log.Write(TLOG_DEBUG, stream.Str());
    }

    auto settings = TCommitTxSettings();
    TAsyncCommitTransactionResult commitFuture = Transaction->Commit(settings);

    return commitFuture.Apply(
        [this, transactionId, debug, funcName](const TAsyncCommitTransactionResult& future){
            try {
                auto result = future.GetValue();

                ThrowOnError(static_cast<TStatus>(result), Log, TString(funcName));

                if (debug) {
                    TStringStream stream;
                    stream << "Terminal ID=" << Terminal.GetTerminalId() << ": ";
                    stream << Type << ": Transaction commit ended";
                    stream << " (Transaction ID=" << Transaction->GetId() << ")";
                    Log.Write(TLOG_DEBUG, stream.Str());
                }
            } catch (TErrorException& err) {
                err.AddPrefix("Commit failed (Transaction ID=" + Transaction->GetId() + ")");
                throw err;
            } catch(const std::exception& ex) {
                throw yexception() << "Commit failed (Transaction ID=" << Transaction->GetId() << "): " << ex.what();
            }
        }
    );
}

NThreading::TFuture<void> IProcedure::RunStages() {
    if (NotStarted()) {
        NextStage();
    }

    if (DebugMode) {
        TStringStream stream;
        stream << "Terminal ID=" << Terminal.GetTerminalId() << ": ";
        stream << Type << ": Stage " << GetStageName() << " started";
        if (!Transaction.Empty()) {
            stream << " (Transaction ID=" << Transaction->GetId() << ")";
        }
        
        Log.Write(TLOG_DEBUG, stream.Str());
    }

    TFuture<TSession> sessionF;
    if (Transaction.Empty()) {
        sessionF = TableClient->GetSession().Apply([this](const TAsyncCreateSessionResult& future) {
            TCreateSessionResult result = future.GetValue();
            
            ThrowOnError(result, Log);

            return result.GetSession();
        });
    } else {
        sessionF = MakeFuture(Transaction->GetSession());
    }

    TFuture<void> currentStageF = Terminal.ApplyByThreadPool(sessionF, [this](const TFuture<TSession>& future) {
        if (future.HasException()) {
            try {
                future.GetValue();
            } catch(TErrorException& ex) {
                ex.AddPrefix("Get session");
                throw ex;
            } catch(const std::exception& ex) {
                throw yexception() << "Get session: " << ex.what();
            }
        }

        TSession session = future.GetValue();

        return RunCurrentStage(session).Apply([this](const TFuture<void>& future){
            if (!future.HasException()) {
                return MakeFuture();
            }

            try {
                future.GetValue();

                return MakeFuture();
            } catch (TErrorException& err) {
                err.AddPrefix("Stage failed (" + GetStageName() + ")");
                throw err;
            } catch (const std::exception& ex) {
                throw yexception() << "Stage failed (" + GetStageName() + "): " << ex.what();
            }
        });
    });

    return Terminal.ApplyByThreadPool(currentStageF, [this, sessionF](const TFuture<void>& future) {
            if (future.HasException() || Finished()) {
                Transaction.Clear();
                return future;
            }

            if (DebugMode) {
                TStringStream stream;
                stream << "Terminal ID=" << Terminal.GetTerminalId() << ": ";
                stream << Type << ": Stage " << GetStageName() << " ended";
                stream << " (Transaction ID=" << Transaction->GetId() << ")";

                Log.Write(TLOG_DEBUG, stream.Str());
            }

            NextStage();
            return RunStages();
        }
    );
}

}
}
