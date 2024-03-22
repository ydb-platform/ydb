#pragma once

#include "ydb_command.h"
#include "ydb_common.h"

#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/recursive_remove.h>
#include <ydb/public/sdk/cpp/client/ydb_coordination/coordination.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandScheme : public TClientCommandTree {
public:
    TCommandScheme();
};

class TCommandMakeDirectory : public TYdbOperationCommand, public TCommandWithPath {
public:
    TCommandMakeDirectory();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
};

class TCommandRemoveDirectory : public TYdbOperationCommand, public TCommandWithPath {
public:
    TCommandRemoveDirectory();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    bool Recursive = false;
    TMaybe<ERecursiveRemovePrompt> Prompt;
};

void PrintAllPermissions(
    const TString& owner,
    const TVector<NScheme::TPermissions>& permissions,
    const TVector<NScheme::TPermissions>& effectivePermissions
);

class TCommandDescribe : public TYdbOperationCommand, public TCommandWithPath, public TCommandWithFormat {
public:
    TCommandDescribe();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    int PrintPathResponse(TDriver& driver, const NScheme::TDescribePathResult& result);
    int DescribeEntryDefault(NScheme::TSchemeEntry entry);
    int DescribeTable(TDriver& driver);
    int DescribeColumnTable(TDriver& driver);
    int PrintTableResponse(NTable::TDescribeTableResult& result);
    void PrintResponsePretty(const NTable::TTableDescription& tableDescription);
    int PrintResponseProtoJsonBase64(const NTable::TTableDescription& tableDescription);
    void WarnAboutTableOptions();

    int DescribeTopic(TDriver& driver);
    int PrintTopicResponse(const NYdb::NTopic::TDescribeTopicResult& result);
    int PrintTopicResponsePretty(const NYdb::NTopic::TTopicDescription& settings);
    int PrintTopicResponseProtoJsonBase64(const NYdb::NTopic::TDescribeTopicResult& result);

    int DescribeCoordinationNode(const TDriver& driver);
    int PrintCoordinationNodeResponse(const NYdb::NCoordination::TDescribeNodeResult& result) const;
    int PrintCoordinationNodeResponsePretty(const NYdb::NCoordination::TNodeDescription& result) const;
    int PrintCoordinationNodeResponseProtoJsonBase64(const NYdb::NCoordination::TNodeDescription& result) const;

    template<typename TDescriptionType>
    void PrintPermissionsIfNeeded(const TDescriptionType& description) {
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
};

class TCommandList : public TYdbOperationCommand, public TCommandWithPath, public TCommandWithFormat {
public:
    TCommandList();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
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
    virtual int Run(TConfig& config) override;

private:
    TString Subject;
    TVector<TString> PermissionsToGrant;
};

class TCommandPermissionRevoke : public TYdbOperationCommand, public TCommandWithPath {
public:
    TCommandPermissionRevoke();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString Subject;
    TVector<TString> PermissionsToRevoke;
};

class TCommandPermissionSet : public TYdbOperationCommand, public TCommandWithPath {
public:
    TCommandPermissionSet();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString Subject;
    TVector<TString> PermissionsToSet;
};

class TCommandChangeOwner : public TYdbOperationCommand, public TCommandWithPath {
public:
    TCommandChangeOwner();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString Owner;
};

class TCommandPermissionClear : public TYdbOperationCommand, public TCommandWithPath {
public:
    TCommandPermissionClear();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
};

class TCommandPermissionList : public TYdbOperationCommand, public TCommandWithPath {
public:
    TCommandPermissionList();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
};

}
}
