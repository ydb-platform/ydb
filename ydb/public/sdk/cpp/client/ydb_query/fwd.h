#pragma once

namespace NYdb::inline V2::NQuery {

struct TClientSettings;
struct TSessionPoolSettings;
struct TCreateSessionSettings;
struct TExecuteQuerySettings;
struct TBeginTxSettings;
struct TCommitTxSettings;
struct TRollbackTxSettings;
struct TExecuteScriptSettings;
struct TFetchScriptResultsSettings;
struct TTxOnlineSettings;
struct TTxSettings;

class TQueryClient;
class TSession;

class TCreateSessionResult;
class TBeginTransactionResult;
class TExecuteQueryResult;
class TCommitTransactionResult;
class TFetchScriptResultsResult;

class TExecuteQueryPart;
class TExecuteQueryIterator;

class TTransaction;
struct TTxControl;

class TQueryContent;
class TResultSetMeta;
class TScriptExecutionOperation;
class TExecStats;

}  // namespace NYdb
