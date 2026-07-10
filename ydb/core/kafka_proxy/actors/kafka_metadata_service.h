#pragma once

#include "actors.h"
#include <ydb/core/kafka_proxy/kafka_events.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/api/protos/ydb_scheme.pb.h>

namespace NKafka {

class TKafkaMetadataService: public NActors::TActorBootstrapped<TKafkaMetadataService> {
    using TBase = NActors::TActorBootstrapped<TKafkaMetadataService>;

public:
    enum class ETables {
        ConsumerGroupsAndMembers,
        TransactionalProducers,
    };

    TKafkaMetadataService(const TString& databasePath, ETables tables)
        : DatabasePath(databasePath)
        , TablesType(tables) {}

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    struct TEvPrivate {
        enum EEv {
            EvTableCreated = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            EvTableAltered,
            EvAclModified,
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
    };

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvTableCreated, Handle);
            HFunc(TEvPrivate::TEvTableAltered, Handle);
            HFunc(TEvPrivate::TEvAclModified, Handle);
        }
    }

    TString GetTablePath(const TString& tableName) const;

    void InitializeConsumerMembersTable();
    void InitializeConsumerGroupsTable();
    void InitializeTransactionalProducersTable();
    void SendCreateTableRequest(Ydb::Table::CreateTableRequest&& request, const TString& tableName);
    void SendEnableAutopartitioningRequest(const TString& tableName);
    void SendAlterTableRequest(Ydb::Table::AlterTableRequest&& request, const TString& tableName);
    void SendReadOnlyAclRequest(const TString& tableName);
    void SendModifyPermissionsRequest(Ydb::Scheme::ModifyPermissionsRequest&& request, const TString& tableName);

    void Handle(TEvPrivate::TEvTableCreated::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvTableAltered::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvAclModified::TPtr& ev, const TActorContext& ctx);
    void ReplyIfRequired(const TActorContext& ctx);

    const TString DatabasePath;
    const ETables TablesType;
    ui32 TablesToCreate = 0;
    ui32 ProcessedRequests = 0;
};

bool TryRequestConsumerMetadataTablesCreation(Ydb::StatusIds::StatusCode status, const TString& databasePath, const NActors::TActorContext& ctx);
bool TryRequestProducerMetadataTablesCreation(Ydb::StatusIds::StatusCode status, const TString& databasePath, const NActors::TActorContext& ctx);

} // NKafka
