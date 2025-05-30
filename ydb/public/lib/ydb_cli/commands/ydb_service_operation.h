#pragma once

#include "ydb_command.h"
#include "ydb_common.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/lib/ydb_cli/common/format.h>

#include <util/generic/hash.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandOperation : public TClientCommandTree {
public:
    TCommandOperation();
};

class TCommandWithOperationId : public TYdbCommand {
public:
    using TYdbCommand::TYdbCommand;
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;

protected:
    NKikimr::NOperationId::TOperationId OperationId;
};

class TCommandGetOperation : public TCommandWithOperationId,
                             public TCommandWithOutput {
public:
    TCommandGetOperation();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
};

class TCommandCancelOperation : public TCommandWithOperationId {
public:
    TCommandCancelOperation();
    virtual int Run(TConfig& config) override;
};

class TCommandForgetOperation : public TCommandWithOperationId {
public:
    TCommandForgetOperation();
    virtual int Run(TConfig& config) override;
};

class TCommandListOperations : public TYdbCommand,
                               public TCommandWithOutput {

    struct THandlerWrapper {
        using THandler = std::function<void(NOperation::TOperationClient&, ui64, const TString&, EDataFormat)>;

        THandler Handler;
        bool Hidden;

        template <typename T>
        THandlerWrapper(T&& handler, bool hidden = false)
            : Handler(std::forward<T>(handler))
            , Hidden(hidden)
        {}

        template <typename... Args>
        auto operator()(Args&&... args) {
            return Handler(std::forward<Args>(args)...);
        }
    };

    void InitializeKindToHandler(TConfig& config);
    TString KindChoices();

public:
    TCommandListOperations();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString Kind;
    ui64 PageSize = 0;
    TString PageToken;
    THashMap<TString, THandlerWrapper> KindToHandler;
};

}
}
