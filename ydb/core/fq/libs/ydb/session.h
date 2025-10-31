#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <library/cpp/threading/future/core/future.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/fluent_settings_helpers.h>

namespace NFq {

struct ISession : public TThrRefBase{

    using TPtr = TIntrusivePtr<ISession>;

    struct TTxControl {
        using TSelf = TTxControl;

        static TTxControl CommitTx() {return TTxControl().Commit(true);}
        static TTxControl BeginTx(bool snapshotRead = false) {return TTxControl().Begin(true).SnapshotRead(snapshotRead);}
        static TTxControl BeginAndCommitTx(bool snapshotRead = false) {return BeginTx(snapshotRead).Commit(true);}
        static TTxControl ContinueTx() {return TTxControl().Continue(true);}
        static TTxControl ContinueAndCommitTx() {return ContinueTx().Commit(true); }

        FLUENT_SETTING_DEFAULT(bool, Begin, false);
        FLUENT_SETTING_DEFAULT(bool, Commit, false);
        FLUENT_SETTING_DEFAULT(bool, Continue, false);
        FLUENT_SETTING_DEFAULT(bool, SnapshotRead, false);
    };

    virtual NThreading::TFuture<NYdb::NTable::TDataQueryResult> ExecuteDataQuery(
        const TString& sql,
        TTxControl txControl,
        std::shared_ptr<NYdb::TParamsBuilder> params,
        const NYdb::NTable::TExecDataQuerySettings& execDataQuerySettings = {}) = 0;
    
    virtual NYdb::TAsyncStatus Rollback() = 0;

    virtual NYdb::TAsyncStatus CreateTable(const std::string& db, const std::string& path, NYdb::NTable::TTableDescription&& tableDesc) = 0;
    
    virtual NYdb::TAsyncStatus DropTable(const std::string& path) = 0;

    virtual void UpdateTransaction(std::optional<NYdb::NTable::TTransaction> transaction) = 0;

    virtual bool HasActiveTransaction() = 0;
};

} // namespace NFq
