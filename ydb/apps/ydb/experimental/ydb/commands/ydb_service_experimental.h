#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_service_table.h>
#include <ydb/public/lib/ydb_cli/common/command.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/fq/fq.h>
#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_scripting.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandExperimental : public TClientCommandTree {
public:
    TCommandExperimental();
};

class TCommandStreamQuery : public TYdbCommand
{
public:
    TCommandStreamQuery();

    virtual void Config(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    void OutputResult(NTable::TScanQueryPartIterator& it, IOutputStream& out);

private:
    TString Query;
    TString FileName;
    TString StatsFileName;
    TString ProfileFileName;
};

class TCommandFederatedQueryTree: public TClientCommandTree {
public:
    TCommandFederatedQueryTree();
};

class TCommandFederatedQueryQueryTree: public TClientCommandTree {
public:
    TCommandFederatedQueryQueryTree();
};

class TCommandFederatedQueryQueryJobTree: public TClientCommandTree {
public:
    TCommandFederatedQueryQueryJobTree();
};

class TCommandFederatedQueryQueryResultTree: public TClientCommandTree {
public:
    TCommandFederatedQueryQueryResultTree();
};

class TCommandFederatedQueryJobTree: public TClientCommandTree {
public:
    TCommandFederatedQueryJobTree();
};

class TCommandFederatedQueryConnectionTree: public TClientCommandTree {
public:
    TCommandFederatedQueryConnectionTree();
};

class TCommandFederatedQueryConnectionBindingTree: public TClientCommandTree {
public:
    TCommandFederatedQueryConnectionBindingTree();
};

class TCommandFederatedQueryBindingTree: public TClientCommandTree {
public:
    TCommandFederatedQueryBindingTree();
};

class TCommandFederatedQueryCreateQuery : public TYdbCommand, public TCommandWithOutput, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryCreateQuery();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
    void PrintResult(NYdb::NFq::TClient& client, const TString& scope, const FederatedQuery::Query& query);

private:
    TString Type;
    TString Name;
    TString Content;
    TString ContentFile;
    TString Mode;
    TString Visibility;
    TString IdempotencyKey;
    TString JsonFile;
    TString Disposition;
    TString DispositionTimestamp;
    TString DispositionDuration;
    bool Automatic = false;
    bool Wait = false;
    bool SkipResult = false;
};

class TCommandFederatedQueryListQueries : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryListQueries();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString PageToken;
    int32_t Limit = 10;
    TString JsonFile;
};

class TCommandFederatedQueryDescribeQuery : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryDescribeQuery();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString QueryId;
};

class TCommandFederatedQueryGetQueryStatus : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryGetQueryStatus();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString QueryId;
};

class TCommandFederatedQueryModifyQuery : public TYdbCommand, public TCommandWithOutput, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryModifyQuery();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
    void PrintResult(NYdb::NFq::TClient& client, const TString& scope, const FederatedQuery::Query& query);

private:
    TString QueryId;
    TString Type;
    TString Visibility;
    TString Name;
    TString Content;
    TString ContentFile;
    int64_t PreviousRevision = 0;
    TString IdempotencyKey;
    TString JsonFile;
    TString Mode;
    TString Disposition;
    TString State;
    TString DispositionTimestamp;
    TString DispositionDuration;
    bool Wait = false;
    bool DispositionForce = false;
};

class TCommandFederatedQueryDeleteQuery : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryDeleteQuery();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString QueryId;
    int64_t PreviousRevision = 0;
    TString IdempotencyKey;
};

class TCommandFederatedQueryControlQuery : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryControlQuery();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString QueryId;
    TString Action;
    int64_t PreviousRevision = 0;
    TString IdempotencyKey;
};

class TCommandFederatedQueryGetResultData : public TYdbCommand, public TCommandWithOutput, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryGetResultData();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString QueryId;
    int64_t ResultSetIndex = 0;
    int64_t Offset = 0;
    int64_t Limit = 10;
    bool Wait = false;
};

class TCommandFederatedQueryQueryListJobs : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryQueryListJobs();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString QueryId;
    TString PageToken;
    int32_t Limit = 10;
    bool CreatedByMe = false;
};


struct TDataSourceClickHouse{
    static constexpr TStringBuf Name = "ClickHouse";

    static FederatedQuery::ClickHouseCluster* GetCluster(FederatedQuery::ConnectionSetting* setting) {
        return setting->mutable_clickhouse_cluster();
    }
};

struct TDataSourcePostgreSQL {
    static constexpr TStringBuf Name = "PostgreSQL";

    static FederatedQuery::PostgreSQLCluster* GetCluster(FederatedQuery::ConnectionSetting* setting) {
        return setting->mutable_postgresql_cluster();
    }
};

struct TDataSourceGreenplum {
    static constexpr TStringBuf Name = "Greenplum";

    static FederatedQuery::GreenplumCluster* GetCluster(FederatedQuery::ConnectionSetting* setting) {
        return setting->mutable_greenplum_cluster();
    }
};

class TCommandFederatedQueryCreateConnection: public TClientCommandTree {
public:
    TCommandFederatedQueryCreateConnection();
};

class TCommandFederatedQueryCreateConnectionYdb : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryCreateConnectionYdb();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
    void PrintResult(NYdb::NFq::TClient& client, const TString& scope, const FederatedQuery::Connection& connection);

private:
    TString Name;
    TString Description;
    TString Visibility;
    TString IdempotencyKey;

    TString ServiceAccount;
    bool CurrentIAM = false;
    TString DatabaseId;
    TString Endpoint;
    TString Database;
    bool Secure = false;
};

template <typename TDataSource>
class TCommandFederatedQueryCreateConnectionGeneric : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryCreateConnectionGeneric();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
    void PrintResult(NYdb::NFq::TClient& client, const TString& scope, const FederatedQuery::Connection& connection);

private:
    TString Name;
    TString Description;
    TString Visibility;
    TString IdempotencyKey;

    TString ServiceAccount;
    bool CurrentIAM = false;
    TString DatabaseId;
    TString DatabaseName;
    TString Login;
    TString Password;
    TString Host;
    int32_t Port = 0;
    bool Secure = false;
};

using TCommandFederatedQueryCreateConnectionClickHouse = TCommandFederatedQueryCreateConnectionGeneric<TDataSourceClickHouse>;
using TCommandFederatedQueryCreateConnectionPostgreSQL = TCommandFederatedQueryCreateConnectionGeneric<TDataSourcePostgreSQL>;
using TCommandFederatedQueryCreateConnectionGreenplum = TCommandFederatedQueryCreateConnectionGeneric<TDataSourceGreenplum>;

class TCommandFederatedQueryCreateConnectionDataStreams : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryCreateConnectionDataStreams();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
    void PrintResult(NYdb::NFq::TClient& client, const TString& scope, const FederatedQuery::Connection& connection);

private:
    TString Name;
    TString Description;
    TString Visibility;
    TString IdempotencyKey;

    TString ServiceAccount;
    bool CurrentIAM = false;
    TString TokenAuth;
    TString DatabaseId;
    TString Endpoint;
    TString Database;
    bool Secure = false;
    bool SharedReading = false;
};

class TCommandFederatedQueryCreateConnectionObjectStorage : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryCreateConnectionObjectStorage();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
    void PrintResult(NYdb::NFq::TClient& client, const TString& scope, const FederatedQuery::Connection& connection);

private:
    TString Name;
    TString Description;
    TString Visibility;
    TString IdempotencyKey;

    TString ServiceAccount;
    bool CurrentIAM = false;
    TString Bucket;
};

class TCommandFederatedQueryCreateConnectionIceberg : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
private:
    struct THiveMetastoreCatalogParams {
        static constexpr char URI[] = "hivemetastore-uri";
        static constexpr char DB[]  = "hivemetastore-db";

        TString Uri;
        TString Database;

        bool HasData() {
            return !Uri.empty() || !Database.empty();
        }

        void Validate() {
            if (Uri.empty() || Database.empty()) {
                throw TMisuseException()
                    << URI << " and " << DB
                    << " has to be set for HiveMetastore Catalog";
            }
        }

        void Fill(FederatedQuery::IcebergCatalog& catalog) {
            Validate();
            auto& h =  *catalog.mutable_hive_metastore();
            h.set_uri(Uri);
            h.set_database_name(Database);
        }
    };

    struct THadoopCatalogParams {
        static constexpr char DIR[] = "hadoop-dir";

        TString Directory;

        bool HasData() {
            return !Directory.empty();
        }

        void Validate() {
            if (Directory.empty()) {
                throw TMisuseException() << DIR << " has to be set for Hadoop Catalog";
            }
        }

        void Fill(FederatedQuery::IcebergCatalog& catalog) {
            Validate();
            catalog.mutable_hadoop()->set_directory(Directory);
        }
    };

    struct TS3WarehouseParams {
        static constexpr char BUCKET[] = "s3-bucket";
        static constexpr char PATH[]   = "s3-path";

        TString Bucket;
        TString Path;

        void Validate() {
            if (Bucket.empty()) {
                throw TMisuseException() << BUCKET << " has to be set for S3 Warehouse";
            }
        }

        void Fill(FederatedQuery::IcebergWarehouse& warehouse) {
            Validate();
            auto& s3 = *warehouse.mutable_s3();
            s3.set_bucket(Bucket);
            s3.set_path(Path);
        }
    };

public:
    TCommandFederatedQueryCreateConnectionIceberg();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
    void PrintResult(NYdb::NFq::TClient& client, const TString& scope, const FederatedQuery::Connection& connection);

private:
    TString Name;
    TString Description;
    TString Visibility;
    TString IdempotencyKey;

    TString ServiceAccount;
    bool CurrentIAM = false;
    THadoopCatalogParams Hadoop;
    THiveMetastoreCatalogParams HiveMetastore;
    TS3WarehouseParams S3Warehouse;
};

class TCommandFederatedQueryCreateConnectionMonitoring : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryCreateConnectionMonitoring();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
    void PrintResult(NYdb::NFq::TClient& client, const TString& scope, const FederatedQuery::Connection& connection);

private:
    TString Name;
    TString Description;
    TString Visibility;
    TString IdempotencyKey;

    TString ServiceAccount;
    bool CurrentIAM = false;
    TString Project;
    TString Cluster;
    TString TokenAuth;
};

class TCommandFederatedQueryTestConnection: public TClientCommandTree {
public:
    TCommandFederatedQueryTestConnection();
};

class TCommandFederatedQueryTestConnectionYdb : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryTestConnectionYdb();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
    void PrintResult(NYdb::NFq::TClient& client, const TString& scope, const FederatedQuery::Connection& connection);

private:
    TString ServiceAccount;
    bool CurrentIAM = false;
    TString DatabaseId;
    TString Endpoint;
    TString Database;
    bool Secure = false;
};

template <typename TDataSource>
class TCommandFederatedQueryTestConnectionGeneric : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryTestConnectionGeneric();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
    void PrintResult(NYdb::NFq::TClient& client, const TString& scope, const FederatedQuery::Connection& connection);

private:
    TString ServiceAccount;
    bool CurrentIAM = false;
    TString DatabaseId;
    TString DatabaseName;
    TString Login;
    TString Password;
    TString Host;
    int32_t Port = 0;
    bool Secure = false;
};

using TCommandFederatedQueryTestConnectionClickHouse = TCommandFederatedQueryTestConnectionGeneric<TDataSourceClickHouse>;
using TCommandFederatedQueryTestConnectionPostgreSQL = TCommandFederatedQueryTestConnectionGeneric<TDataSourcePostgreSQL>;
using TCommandFederatedQueryTestConnectionGreenplum = TCommandFederatedQueryTestConnectionGeneric<TDataSourceGreenplum>;

class TCommandFederatedQueryTestConnectionDataStreams : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryTestConnectionDataStreams();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
    void PrintResult(NYdb::NFq::TClient& client, const TString& scope, const FederatedQuery::Connection& connection);

private:
    TString ServiceAccount;
    bool CurrentIAM = false;
    TString DatabaseId;
    TString Endpoint;
    TString Database;
    bool Secure = false;
};

class TCommandFederatedQueryTestConnectionObjectStorage : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryTestConnectionObjectStorage();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
    void PrintResult(NYdb::NFq::TClient& client, const TString& scope, const FederatedQuery::Connection& connection);

private:
    TString ServiceAccount;
    bool CurrentIAM = false;
    TString Bucket;
};

class TCommandFederatedQueryTestConnectionMonitoring : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryTestConnectionMonitoring();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
    void PrintResult(NYdb::NFq::TClient& client, const TString& scope, const FederatedQuery::Connection& connection);

private:
    TString ServiceAccount;
    bool CurrentIAM = false;
    TString Project;
    TString Cluster;
};

class TCommandFederatedQueryConnectionListBindings : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryConnectionListBindings();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString PageToken;
    int32_t Limit = 10;
    TString ConnectionId;
    TString Name;
    bool CreatedByMe = false;
};

class TCommandFederatedQueryListJobs : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryListJobs();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString FolderId;
    TString PageToken;
    int32_t Limit = 10;
    bool CreatedByMe = false;
};

class TCommandFederatedQueryDescribeJob : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryDescribeJob();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString JobId;
};

class TCommandFederatedQueryListConnections : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryListConnections();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString PageToken;
    int32_t Limit = 10;
    TString Name;
    bool CreatedByMe = false;
};

class TCommandFederatedQueryDescribeConnection : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryDescribeConnection();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString ConnectionId;
};

class TCommandFederatedQueryModifyConnection: public TClientCommandTree {
public:
    TCommandFederatedQueryModifyConnection();
};

class TCommandFederatedQueryModifyConnectionYdb : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryModifyConnectionYdb();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
    void PrintResult(NYdb::NFq::TClient& client, const TString& scope, const FederatedQuery::Connection& connection);

private:
    TString ConnectionId;
    TString Name;
    TString Description;
    TString Visibility;
    int32_t PreviousRevision = 0;
    TString IdempotencyKey;

    TString ServiceAccount;
    bool CurrentIAM = false;
    TString DatabaseId;
    TString Endpoint;
    TString Database;
    bool Secure = false;
};

template <typename TDataSource>
class TCommandFederatedQueryModifyConnectionGeneric : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryModifyConnectionGeneric();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
    void PrintResult(NYdb::NFq::TClient& client, const TString& scope, const FederatedQuery::Connection& connection);

private:
    TString ConnectionId;
    TString Name;
    TString Description;
    TString Visibility;
    int32_t PreviousRevision = 0;
    TString IdempotencyKey;

    TString ServiceAccount;
    bool CurrentIAM = false;
    TString DatabaseId;
    TString DatabaseName;
    TString Login;
    TString Password;
    TString Host;
    int32_t Port = 0;
    bool Secure = false;
};

using TCommandFederatedQueryModifyConnectionClickHouse = TCommandFederatedQueryModifyConnectionGeneric<TDataSourceClickHouse>;
using TCommandFederatedQueryModifyConnectionPostgreSQL = TCommandFederatedQueryModifyConnectionGeneric<TDataSourcePostgreSQL>;
using TCommandFederatedQueryModifyConnectionGreenplum = TCommandFederatedQueryModifyConnectionGeneric<TDataSourceGreenplum>;

class TCommandFederatedQueryModifyConnectionDataStreams : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryModifyConnectionDataStreams();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
    void PrintResult(NYdb::NFq::TClient& client, const TString& scope, const FederatedQuery::Connection& connection);

private:
    TString ConnectionId;
    TString Name;
    TString Description;
    TString Visibility;
    int32_t PreviousRevision = 0;
    TString IdempotencyKey;

    TString ServiceAccount;
    bool CurrentIAM = false;
    TString DatabaseId;
    TString Endpoint;
    TString Database;
    bool Secure = false;
};

class TCommandFederatedQueryModifyConnectionObjectStorage : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryModifyConnectionObjectStorage();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
    void PrintResult(NYdb::NFq::TClient& client, const TString& scope, const FederatedQuery::Connection& connection);

private:
    TString ConnectionId;
    TString Name;
    TString Description;
    TString Visibility;
    int32_t PreviousRevision = 0;
    TString IdempotencyKey;

    TString ServiceAccount;
    bool CurrentIAM = false;
    TString Bucket;
};

class TCommandFederatedQueryModifyConnectionMonitoring : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryModifyConnectionMonitoring();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
    void PrintResult(NYdb::NFq::TClient& client, const TString& scope, const FederatedQuery::Connection& connection);

private:
    TString ConnectionId;
    TString Name;
    TString Description;
    TString Visibility;
    int32_t PreviousRevision = 0;
    TString IdempotencyKey;

    TString ServiceAccount;
    bool CurrentIAM = false;
    TString Project;
    TString Cluster;
};

class TCommandFederatedQueryDeleteConnection : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryDeleteConnection();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString ConnectionId;
    int64_t PreviousRevision = 0;
    TString IdempotencyKey;
};

class TCommandFederatedQueryCreateBinding : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryCreateBinding();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString Name;
    TString IdempotencyKey;
    TString JsonFile;
    TString Visibility;
};

class TCommandFederatedQueryListBindings : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryListBindings();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString PageToken;
    int32_t Limit = 10;
    TString Name;
    bool CreatedByMe = false;
};

class TCommandFederatedQueryDescribeBinding : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryDescribeBinding();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString BindingId;
};

class TCommandFederatedQueryModifyBinding : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryModifyBinding();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString BindingId;
    TString Name;
    int32_t PreviousRevision = 0;
    TString IdempotencyKey;
    TString JsonFile;
    TString Visibility;
};

class TCommandFederatedQueryDeleteBinding : public TYdbCommand, public TCommandWithFormat, public TCommandWithResponseHeaders {
public:
    TCommandFederatedQueryDeleteBinding();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    TString BindingId;
    int64_t PreviousRevision = 0;
    TString IdempotencyKey;
};

class TCommandGetConnection : public TClientCommand {
public:
    TCommandGetConnection();
    virtual void Config(TConfig& config) override;
    virtual int Run(TConfig& config) override;
};

// Avoid class name collision with public client
namespace NExperimentalConsoleClient {

    class TCommandExplain : public TTableCommand, TCommandQueryBase,
        public TInterruptableCommand
    {
    public:
        TCommandExplain();
        virtual void Config(TConfig& config) override;
        virtual void Parse(TConfig& config) override;
        virtual int Run(TConfig& config) override;

    private:
        void PrettyPrintPlan(IOutputStream* out, const NJson::TJsonValue& planValue, TVector<TString>& offsets);
        TString JsonToString(const NJson::TJsonValue& value);
        void DotPrintPlan(IOutputStream* out, const TString& planJson);
        void DotPrintWalk(IOutputStream* out, const NJson::TJsonValue& planValue, int parent_id);
        bool ThrowVersionError();
        void SvgPrintPlan(IOutputStream* out, const TString& planJson);

        bool PrintAst = false;
        TString QueryType;
        TString Format;
        bool Analyze = false;

        const TString Edge = "|  ";
        const TString EdgeBranch = "├──";
        const TString EdgeBranchLast = "└──";
        const TString NoEdge = "   ";
        const THashSet<TString> PPrintSkipKeys{"Plans", "Node Type", "Name", "PlanNodeType", "Parent Relationship",
                                               "Subplan Name", "PlanNodeId", "Operators", "meta", "tables"};
    };

}

class TCommandGenerate : public TClientCommandTree {
public:
    TCommandGenerate();
};

class TCommandGenerateBase : public TYdbCommand,
                                public TCommandWithPath,
                                public TCommandWithFormat {
public:
    TCommandGenerateBase(const TString& cmd, const TString& cmdDescription)
      : TYdbCommand(cmd, {}, cmdDescription)
    {}

    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;

protected:
    ui64 RowsPerRequest;
    ui64 RowsToGenerate;
    ui64 MaxInFlightRequests = 1;
    ui64 Threads = 0;
    TDuration OperationTimeout;
};

class TCommandGenerateNumbers : public TCommandGenerateBase {
public:
    TCommandGenerateNumbers(const TString& cmd = "numbers", const TString& cmdDescription = "Generate numbers")
        : TCommandGenerateBase(cmd, cmdDescription)
    {}

    void Config(TConfig& config) override;
    int Run(TConfig& config) override;
};

class TCommandJson2Svg : public TYdbCommand, public TCommandWithPath, public TCommandWithFormat {
public:
    TCommandJson2Svg()
        : TYdbCommand("json2svg", {}, "Convert plan in JSON to SVG format")
    {}

    void Config(TConfig& config) override;
    int Run(TConfig& config) override;
private:
    TString InputFileName;
    TString OutputFileName;
};

} // NConsoleClient
} // NYdb
