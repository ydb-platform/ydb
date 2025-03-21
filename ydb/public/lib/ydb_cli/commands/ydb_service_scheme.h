#pragma once

#include "ydb_command.h"
#include "ydb_common.h"

#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/print_utils.h>
#include <ydb/public/lib/ydb_cli/common/recursive_remove.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_view.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/coordination.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

namespace NYdb {

namespace NTopic {
struct TDescribeConsumerResult;
} // namespace NTopic
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

void PrintAllPermissions(
    const std::string& owner,
    const std::vector<NScheme::TPermissions>& permissions,
    const std::vector<NScheme::TPermissions>& effectivePermissions
);

// Pretty print consumer info ('scheme describe' and 'topic consumer describe' commands)
int PrintPrettyDescribeConsumerResult(const NYdb::NTopic::TConsumerDescription& description, bool withPartitionsStats);

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
            return PrintProtoJsonBase64(TProtoAccessor::GetProto(value));
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

private:
    int PrintPathResponse(TDriver& driver, const NScheme::TDescribePathResult& result);
    int DescribeEntryDefault(NScheme::TSchemeEntry entry);
    int DescribeTable(TDriver& driver);
    int DescribeColumnTable(TDriver& driver);
    int PrintTableResponsePretty(const NTable::TTableDescription& tableDescription) const;
    void WarnAboutTableOptions();

    int DescribeTopic(TDriver& driver);
    int PrintTopicResponsePretty(const NYdb::NTopic::TTopicDescription& settings) const;

    int DescribeCoordinationNode(const TDriver& driver);
    int PrintCoordinationNodeResponsePretty(const NYdb::NCoordination::TNodeDescription& result) const;

    int DescribeReplication(const TDriver& driver);
    int PrintReplicationResponsePretty(const NYdb::NReplication::TDescribeReplicationResult& result) const;

    int DescribeView(const TDriver& driver);
    int PrintViewResponsePretty(const NYdb::NView::TDescribeViewResult& result) const;

    int DescribeExternalDataSource(const TDriver& driver);
    int PrintExternalDataSourceResponsePretty(const NYdb::NTable::TExternalDataSourceDescription& result) const;

    int DescribeExternalTable(const TDriver& driver);
    int PrintExternalTableResponsePretty(const NYdb::NTable::TExternalTableDescription& result) const;

    int TryTopicConsumerDescribeOrFail(NYdb::TDriver& driver, const NScheme::TDescribePathResult& result);
    std::pair<TString, TString> ParseTopicConsumer() const;
    int PrintConsumerResponsePretty(const NYdb::NTopic::TConsumerDescription& description) const;

    template<typename TDescriptionType>
    void PrintPermissionsIfNeeded(const TDescriptionType& description) const {
        if (ShowPermissions) {
            Cout << Endl;
            PrintAllPermissions(
                description.GetOwner(),
                description.GetPermissions(),
                description.GetEffectivePermissions()
            );
        }
    }

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
