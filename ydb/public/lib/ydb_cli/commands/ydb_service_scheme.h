#pragma once

#include "ydb_command.h"
#include "ydb_common.h"

#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/print_utils.h>
#include <ydb/public/lib/ydb_cli/common/recursive_remove.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/accessor.h>

namespace NYdb {

namespace NTopic {
    class TConsumerDescription;
}

namespace NConsoleClient {

class TCommandScheme : public TClientCommandTree {
public:
    TCommandScheme();
};

class TCommandMakeDirectory : public TYdbOperationCommand, public TCommandWithPath {
public:
    TCommandMakeDirectory();
    virtual void Config(TConfig& config) override;
    virtual void ExtractParams(TConfig& config) override;
    virtual int Run(TConfig& config) override;
};

class TCommandRemoveDirectory : public TYdbOperationCommand, public TCommandWithPath {
public:
    TCommandRemoveDirectory();
    virtual void Config(TConfig& config) override;
    virtual void ExtractParams(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    bool Recursive = false;
    TMaybe<ERecursiveRemovePrompt> Prompt;
};

// Pretty print consumer info ('scheme describe' and 'topic consumer describe' commands)
int PrintPrettyDescribeConsumerResult(const NYdb::NTopic::TConsumerDescription& description, bool withPartitionsStats, IOutputStream& out = Cout);

template <typename TCommand, typename TValue>
using TPrettyPrinter = int(TCommand::*)(const TValue&) const;

template <typename TCommand, typename TValue>
static int PrintDescription(TCommand* self, EDataFormat format, const TValue& value, TPrettyPrinter<TCommand, TValue> prettyFunc) {
    switch (format) {
        case EDataFormat::Default:
        case EDataFormat::Pretty:
            return std::invoke(prettyFunc, self, value);
        case EDataFormat::Json:
            Cerr << "Warning! Option --json is deprecated and will be removed soon. "
                 << "Use \"--format proto-json-base64\" option instead." << Endl;
            [[fallthrough]];
        case EDataFormat::ProtoJsonBase64:
            return PrintProtoJsonBase64(NDraft::TProtoAccessor::GetProto(value), Cout);
        default:
            throw TMisuseException() << "This command doesn't support " << format << " output format";
    }

    return EXIT_SUCCESS;
}

class TCommandDescribe : public TYdbOperationCommand, public TCommandWithPath, public TCommandWithOutput {
public:
    TCommandDescribe();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual void ExtractParams(TConfig& config) override;
    virtual int Run(TConfig& config) override;

    IOutputStream* OutputStream;

private:
    // Common options
    bool ShowPermissions = false;
    // Table options
    bool ShowKeyShardBoundaries = false;
    bool ShowStats = false;
    bool ShowPartitionStats = false;
    TString Database;
};

class TCommandList : public TYdbOperationCommand, public TCommandWithPath, public TCommandWithOutput {
public:
    TCommandList();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual void ExtractParams(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    bool AdvancedMode = false;
    bool Recursive = false;
    bool FromNewLine = false;
    bool Multithread = false;
};

class TCommandPermissions : public TClientCommandTree {
public:
    TCommandPermissions();
};

class TCommandPermissionGrant : public TYdbOperationCommand, public TCommandWithPath {
public:
    TCommandPermissionGrant();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual void ExtractParams(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    std::string Subject;
    std::vector<std::string> PermissionsToGrant;
};

class TCommandPermissionRevoke : public TYdbOperationCommand, public TCommandWithPath {
public:
    TCommandPermissionRevoke();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual void ExtractParams(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    std::string Subject;
    std::vector<std::string> PermissionsToRevoke;
};

class TCommandPermissionSet : public TYdbOperationCommand, public TCommandWithPath {
public:
    TCommandPermissionSet();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual void ExtractParams(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    std::string Subject;
    std::vector<std::string> PermissionsToSet;
};

class TCommandChangeOwner : public TYdbOperationCommand, public TCommandWithPath {
public:
    TCommandChangeOwner();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual void ExtractParams(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString Owner;
};

class TCommandPermissionClear : public TYdbOperationCommand, public TCommandWithPath {
public:
    TCommandPermissionClear();
    virtual void Config(TConfig& config) override;
    virtual void ExtractParams(TConfig& config) override;
    virtual int Run(TConfig& config) override;
};

class TCommandPermissionSetInheritance : public TYdbOperationCommand, public TCommandWithPath {
public:
    TCommandPermissionSetInheritance();
    virtual void Config(TConfig& config) override;
    virtual void ExtractParams(TConfig& config) override;
    virtual int Run(TConfig& config) override;
};

class TCommandPermissionClearInheritance : public TYdbOperationCommand, public TCommandWithPath {
public:
    TCommandPermissionClearInheritance();
    virtual void Config(TConfig& config) override;
    virtual void ExtractParams(TConfig& config) override;
    virtual int Run(TConfig& config) override;
};

class TCommandPermissionList : public TYdbOperationCommand, public TCommandWithPath {
public:
    TCommandPermissionList();
    virtual void Config(TConfig& config) override;
    virtual void ExtractParams(TConfig& config) override;
    virtual int Run(TConfig& config) override;
};

}
}
