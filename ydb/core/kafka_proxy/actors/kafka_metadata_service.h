#pragma once

#include "actors.h"
#include <ydb/core/kafka_proxy/kafka_events.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/api/protos/ydb_scheme.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

namespace NKafka {

class TKafkaMetadataService: public NActors::TActorBootstrapped<TKafkaMetadataService> {
    using TBase = NActors::TActorBootstrapped<TKafkaMetadataService>;

public:
    enum class ETables {
        ConsumerGroupsAndMembers,
        TransactionalProducers,
    };

    TKafkaMetadataService(const TString& databasePath, ETables tables, const TString& sourceDatabasePath = {})
        : DatabasePath(databasePath)
        , SourceDatabasePath(sourceDatabasePath)
        , TablesType(tables) {}

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    struct TEvPrivate {
        enum EEv {
            EvTableCreated = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            EvTableAltered,
            EvAclModified,
            EvMigrationRead,
            EvMigrationWritten,
            EvEnd
        };

        struct TEvTableCreated: public NActors::TEventLocal<TEvTableCreated, EvTableCreated> {
            TEvTableCreated(const TString& tableName, Ydb::StatusIds::StatusCode status, const TString& error)
                : TableName(tableName)
                , Status(status)
                , Error(error) {}

            const TString TableName;
            const Ydb::StatusIds::StatusCode Status;
            const TString Error;
        };

        struct TEvTableAltered: public NActors::TEventLocal<TEvTableAltered, EvTableAltered> {
            TEvTableAltered(const TString& tableName, Ydb::StatusIds::StatusCode status, const TString& error)
                : TableName(tableName)
                , Status(status)
                , Error(error) {}

            const TString TableName;
            const Ydb::StatusIds::StatusCode Status;
            const TString Error;
        };

        struct TEvAclModified: public NActors::TEventLocal<TEvAclModified, EvAclModified> {
            TEvAclModified(const TString& tableName, Ydb::StatusIds::StatusCode status, const TString& error)
                : TableName(tableName)
                , Status(status)
                , Error(error) {}

            const TString TableName;
            const Ydb::StatusIds::StatusCode Status;
            const TString Error;
        };

        struct TEvMigrationRead: public NActors::TEventLocal<TEvMigrationRead, EvMigrationRead> {
            TEvMigrationRead(const TString& tableName, Ydb::StatusIds::StatusCode status, const TString& error, Ydb::ResultSet&& resultSet)
                : TableName(tableName)
                , Status(status)
                , Error(error)
                , ResultSet(std::move(resultSet)) {}

            const TString TableName;
            const Ydb::StatusIds::StatusCode Status;
            const TString Error;
            const Ydb::ResultSet ResultSet;
        };

        struct TEvMigrationWritten: public NActors::TEventLocal<TEvMigrationWritten, EvMigrationWritten> {
            TEvMigrationWritten(const TString& tableName, Ydb::StatusIds::StatusCode status, const TString& error, ui64 rowCount)
                : TableName(tableName)
                , Status(status)
                , Error(error)
                , RowCount(rowCount) {}

            const TString TableName;
            const Ydb::StatusIds::StatusCode Status;
            const TString Error;
            const ui64 RowCount;
        };
    };

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvTableCreated, Handle);
            HFunc(TEvPrivate::TEvTableAltered, Handle);
            HFunc(TEvPrivate::TEvAclModified, Handle);
            HFunc(TEvPrivate::TEvMigrationRead, Handle);
            HFunc(TEvPrivate::TEvMigrationWritten, Handle);
        }
    }

    static TString BuildTablePath(const TString& databasePath, const TString& tableName);
    TString GetTablePath(const TString& tableName) const;

    void InitializeConsumerMembersTable();
    void InitializeConsumerGroupsTable();
    void InitializeTransactionalProducersTable();
    void SendCreateTableRequest(Ydb::Table::CreateTableRequest&& request, const TString& tableName);
    void SendEnableAutopartitioningRequest(const TString& tableName);
    void SendAlterTableRequest(Ydb::Table::AlterTableRequest&& request, const TString& tableName);
    void SendReadOnlyAclRequest(const TString& tableName);
    void SendModifyPermissionsRequest(Ydb::Scheme::ModifyPermissionsRequest&& request, const TString& tableName);

    bool ShouldMigrate() const;
    void SendMigrationReadRequest(const TString& tableName);
    void SendMigrationUpsertRequest(const TString& tableName, const Ydb::ResultSet& resultSet);
    static TString YqlType(const Ydb::Type& type);

    void Handle(TEvPrivate::TEvTableCreated::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvTableAltered::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvAclModified::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvMigrationRead::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvMigrationWritten::TPtr& ev, const TActorContext& ctx);
    void ReplyIfRequired(const TActorContext& ctx);

    const TString DatabasePath;
    const TString SourceDatabasePath;
    const ETables TablesType;
    ui32 TablesToCreate = 0;
    ui32 ProcessedRequests = 0;
};

bool TryRequestConsumerMetadataTablesCreation(Ydb::StatusIds::StatusCode status, const TString& databasePath, const TString& sourceDatabasePath, const NActors::TActorContext& ctx);
bool TryRequestProducerMetadataTablesCreation(Ydb::StatusIds::StatusCode status, const TString& databasePath, const TString& sourceDatabasePath, const NActors::TActorContext& ctx);

} // NKafka
