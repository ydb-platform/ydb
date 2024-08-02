#pragma once
#include <util/generic/hash_set.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>
#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>
#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/api/protos/ydb_cms.pb.h>
#include <ydb/public/lib/deprecated/client/grpc_client.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/public/issue/yql_issue_manager.h>
#include "appdata.h"
#include "merger.h"
#include "core_ydb.h"

namespace NMVP {

struct THandlerActorYdb {
    struct TEvPrivate {
        enum EEv {
            EvDescribePathResult = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            EvDescribeTableResult,
            EvSchemeDescribeResult,
            EvListDirectoryResult,
            EvRequestResult,
            EvModifyPermissionsResult,
            EvMakeDirectoryResult,
            EvRemoveDirectoryResult,
            EvCreateSessionResult,
            EvDataQueryResult,
            EvTypedDataQueryResult,
            EvCreateTenantResult,
            EvGetDatabaseStatusResponse,
            EvListEndpointsResponse,
            EvPollEndpointsResponse,
            EvExecuteYqlResponse,
            EvPollYqlResponse,
            EvGetDatabaseResponse,
            EvListDatabaseResponse,
            EvErrorResponse,
            EvOperationResponse,
            EvDatabaseResponse,
            EvListStorageTypesResponse,
            EvListResourcePresetsResponse,
            EvExecuteYqlScriptResult,
            EvReadStarted,
            EvReadFailed,
            EvQueryBatch,
            EvGetConfigResponse,
            EvListOperationsResponse,
            EvTryAgain,
            EvSimulateResponse,
            EvCreateSessionResponse,
            EvExplainQueryResponse,
            EvAlterTableResponse,
            EvRetryRequest,
            EvListAllDatabaseResponse,
            EvGetCloudResponse,
            EvBackupResponse,
            EvListBackupsResponse,
            EvQuota,
            EvQuotaDefaultResponse,
            EvEmpty,
            EvDataStreamsListResponse,
            EvDataStreamsUpdateResponse,
            EvDataStreamsDescribeResponse,
            EvDataStreamsPutRecordsResponse,
            EvDataStreamsGetRecordsCustomResponse,
            EvDataStreamsListShardsResponse,
            EvExplainYqlResponse,
            EvDescribeTopicResult,
            EvDescribeConsumerResult,
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

        struct TEvDescribePathResult : NActors::TEventLocal<TEvDescribePathResult, EvDescribePathResult> {
            NYdb::NScheme::TDescribePathResult Result;

            TEvDescribePathResult(NYdb::NScheme::TDescribePathResult&& result)
                : Result(std::move(result))
            {}
        };

        struct TEvDescribeTableResult : NActors::TEventLocal<TEvDescribeTableResult, EvDescribeTableResult> {
            NYdb::NTable::TDescribeTableResult Result;

            TEvDescribeTableResult(NYdb::NTable::TDescribeTableResult&& result)
                : Result(std::move(result))
            {}
        };

        struct TEvDescribeTopicResult : NActors::TEventLocal<TEvDescribeTopicResult, EvDescribeTopicResult> {
            NYdb::NTopic::TDescribeTopicResult Result;
            TString Name;

            TEvDescribeTopicResult(NYdb::NTopic::TDescribeTopicResult&& result, const TString& name = {})
                : Result(std::move(result))
                , Name(name)
            {}
        };

        struct TEvDescribeConsumerResult : NActors::TEventLocal<TEvDescribeConsumerResult, EvDescribeConsumerResult> {
            NYdb::NTopic::TDescribeConsumerResult Result;

            TEvDescribeConsumerResult(NYdb::NTopic::TDescribeConsumerResult&& result)
                : Result(std::move(result))
            {}
        };

        struct TEvSchemeDescribeResult : NActors::TEventLocal<TEvSchemeDescribeResult, EvSchemeDescribeResult> {
            NKikimrClient::TResponse Result;

            TEvSchemeDescribeResult(NKikimrClient::TResponse&& result)
                : Result(std::move(result))
            {}
        };

        struct TEvListDirectoryResult : NActors::TEventLocal<TEvListDirectoryResult, EvListDirectoryResult> {
            NYdb::NScheme::TListDirectoryResult Result;

            TEvListDirectoryResult(NYdb::NScheme::TListDirectoryResult&& result)
                : Result(std::move(result))
            {}
        };

        struct TEvDataStreamsListResponse : NActors::TEventLocal<TEvDataStreamsListResponse, EvDataStreamsListResponse> {
            NYdb::NDataStreams::V1::TListStreamsResult Result;

            TEvDataStreamsListResponse(NYdb::NDataStreams::V1::TListStreamsResult&& result)
                : Result(std::move(result))
            {}
        };

        struct TEvDataStreamsUpdateResponse : NActors::TEventLocal<TEvDataStreamsUpdateResponse, EvDataStreamsUpdateResponse> {
            NYdb::NDataStreams::V1::TUpdateStreamResult Result;

            TEvDataStreamsUpdateResponse(NYdb::NDataStreams::V1::TUpdateStreamResult&& result)
                : Result(std::move(result))
            {}
        };

        struct TEvDataStreamsDescribeResponse : NActors::TEventLocal<TEvDataStreamsDescribeResponse, EvDataStreamsDescribeResponse> {
            NYdb::NDataStreams::V1::TDescribeStreamResult Result;

            TEvDataStreamsDescribeResponse(NYdb::NDataStreams::V1::TDescribeStreamResult&& result)
                : Result(std::move(result))
            {}
        };

        struct TEvDataStreamsPutRecordsResponse : NActors::TEventLocal<TEvDataStreamsPutRecordsResponse, EvDataStreamsPutRecordsResponse> {
            NYdb::NDataStreams::V1::TPutRecordsResult Result;

            TEvDataStreamsPutRecordsResponse(NYdb::NDataStreams::V1::TPutRecordsResult&& result)
                : Result(std::move(result))
            {}
        };


        struct TEvDataStreamsGetRecordsCustomResponse : NActors::TEventLocal<TEvDataStreamsGetRecordsCustomResponse, EvDataStreamsGetRecordsCustomResponse> {
            class TCookie {
            public:
                TCookie(const TString& cookie) {
                    TStringStream input(cookie);
                    try {
                        ShardId = input.ReadTo(':');
                        SeqNo = input.ReadTo(';');
                    } catch(yexception& e) {
                        ythrow yexception() << "The cookie string does not satisfy the format";
                    }
                }

                TCookie(const TString& shardId, const TString& seqNo)
                : ShardId{shardId}
                , SeqNo{seqNo}
                {}

                TString ToString() const {
                    return TStringBuilder() << ShardId << ":" << SeqNo << ";";
                }

                TString GetShardId() const {
                    return ShardId;
                }

                TString GetSeqNo() const {
                    return SeqNo;
                }

            private:
                TString ShardId;
                TString SeqNo;
            };

            struct TRecord {
                i64 Timestamp;
                TString Data;
                TString PartitionKey;
                TString SequenceNumber;
            };

            NYdb::TStatus Status;

            TString NextCookie;
            TString PreviousCookie;
            struct {
                TString Id;
                TVector<TRecord> Records;
            } Shard;

            TEvDataStreamsGetRecordsCustomResponse(const NYdb::TStatus& result, const TString& shardId)
                : Status{result}
                , Shard{.Id=shardId, .Records={}}
            {
            }

            TEvDataStreamsGetRecordsCustomResponse(const NYdb::NDataStreams::V1::TGetRecordsResult& result,
                                             const TString& shardId, ui32 limit)
                : Status{result}
                , NextCookie{""}
                , PreviousCookie{""}
                , Shard{.Id=shardId, .Records={}}
            {
                std::cerr << "Prepare to answer\n";
                if (Status.IsSuccess()) {
                    const auto N = result.GetResult().records_size();
                    Shard.Records.reserve(N);
                    for (int i = 0; i < N; ++i) {
                        const auto &protoRecord = result.GetResult().records(i);
                        TRecord record{
                            .Timestamp=protoRecord.approximate_arrival_timestamp(),
                            .Data=protoRecord.data(),
                            .PartitionKey=protoRecord.partition_key(),
                            .SequenceNumber=protoRecord.sequence_number()
                        };
                        Shard.Records.emplace_back(record);
                    }

                    ui32 seqNo{0};
                    if (N > 0) {
                        seqNo = FromString<ui32>(result.GetResult().records(0).sequence_number());
                        TCookie prev(shardId, TStringBuilder() << (seqNo > limit ? (seqNo - limit) : 0));
                        PreviousCookie = prev.ToString();
                        TCookie next(shardId, TStringBuilder() << (seqNo + limit));
                        NextCookie = next.ToString();
                    }
                }

                std::cerr << "Filled everything\n";
            }
        };

        struct TEvDataStreamsListShardsResponse : NActors::TEventLocal<TEvDataStreamsListShardsResponse, EvDataStreamsListShardsResponse> {
            NYdb::NDataStreams::V1::TListShardsResult Result;

            TEvDataStreamsListShardsResponse(NYdb::NDataStreams::V1::TListShardsResult&& result)
                : Result(std::move(result))
            {}
        };


        struct TEvRequestResult : NActors::TEventLocal<TEvRequestResult, EvRequestResult> {
            NYdb::TStatus Result;

            TEvRequestResult(NYdb::TStatus&& result)
                : Result(std::move(result))
            {}
        };

        struct TEvModifyPermissionsResult : NActors::TEventLocal<TEvModifyPermissionsResult, EvModifyPermissionsResult> {
            NYdb::TStatus Result;

            TEvModifyPermissionsResult(NYdb::TStatus&& result)
                : Result(std::move(result))
            {}
        };

        struct TEvMakeDirectoryResult : NActors::TEventLocal<TEvMakeDirectoryResult, EvMakeDirectoryResult> {
            NYdb::TStatus Result;

            TEvMakeDirectoryResult(NYdb::TStatus&& result)
                : Result(std::move(result))
            {}
        };

        struct TEvRemoveDirectoryResult : NActors::TEventLocal<TEvRemoveDirectoryResult, EvRemoveDirectoryResult> {
            NYdb::TStatus Result;

            TEvRemoveDirectoryResult(NYdb::TStatus&& result)
                : Result(std::move(result))
            {}
        };

        struct TEvCreateSessionResult : NActors::TEventLocal<TEvCreateSessionResult, EvCreateSessionResult> {
            NYdb::NTable::TCreateSessionResult Result;

            TEvCreateSessionResult(NYdb::NTable::TCreateSessionResult&& result)
                : Result(std::move(result))
            {}
        };

        struct TEvDataQueryResult : NActors::TEventLocal<TEvDataQueryResult, EvDataQueryResult> {
            NYdb::NTable::TDataQueryResult Result;
            ui32 Id;

            TEvDataQueryResult(NYdb::NTable::TDataQueryResult&& result, ui32 id = 0)
                : Result(std::move(result))
                , Id(id)
            {}
        };

        template <class TQueryType>
        struct TEvTypedDataQueryResult : NActors::TEventLocal<TEvTypedDataQueryResult<TQueryType>, EvTypedDataQueryResult> {
            NYdb::NTable::TDataQueryResult Result;
            TQueryType QueryType;

            TEvTypedDataQueryResult(NYdb::NTable::TDataQueryResult&& result, TQueryType queryType)
                : Result(std::move(result))
                , QueryType(queryType)
            {}
        };

        struct TEvExecuteYqlScriptResult : NActors::TEventLocal<TEvExecuteYqlScriptResult, EvExecuteYqlScriptResult> {
            NYdb::NScripting::TExecuteYqlResult Result;
            std::multimap<TString, TString> Meta;

            TEvExecuteYqlScriptResult(NYdb::NScripting::TExecuteYqlResult&& result)
                : Result(std::move(result))
            {}
        };

        struct TEvReadStarted : NActors::TEventLocal<TEvReadStarted, EvReadStarted> {
            NYdb::NScripting::TYqlResultPartIterator PartIterator;

            TEvReadStarted(NYdb::NScripting::TYqlResultPartIterator&& result)
                : PartIterator(std::move(result))
            {}
        };

        struct TEvReadFailed : NActors::TEventLocal<TEvReadFailed, EvReadFailed> {
            NYdb::TStatus Status;

            TEvReadFailed(NYdb::TStatus&& status)
                : Status(std::move(status))
            {}
        };

        struct TEvQueryBatch : NActors::TEventLocal<TEvQueryBatch, EvQueryBatch> {
            NYdb::NScripting::TYqlResultPart Result;

            TEvQueryBatch(NYdb::NScripting::TYqlResultPart&& result)
                : Result(std::move(result))
            {}
        };

        struct TEvGetDatabaseStatusResponse : NActors::TEventLocal<TEvGetDatabaseStatusResponse, EvGetDatabaseStatusResponse> {
            Ydb::Operations::Operation Operation;

            TEvGetDatabaseStatusResponse(Ydb::Operations::Operation&& operation)
                : Operation(std::move(operation))
            {}
        };

        struct TEvListEndpointsResponse : NActors::TEventLocal<TEvListEndpointsResponse, EvListEndpointsResponse> {
            Ydb::Operations::Operation Operation;

            TEvListEndpointsResponse(Ydb::Operations::Operation&& operation)
                : Operation(std::move(operation))
            {}
        };

        struct TEvPollEndpointsResponse : NActors::TEventLocal<TEvPollEndpointsResponse, EvPollEndpointsResponse> {
            Ydb::Operations::Operation Operation;

            TEvPollEndpointsResponse(Ydb::Operations::Operation&& operation)
                : Operation(std::move(operation))
            {}
        };

        struct TEvCreateTenantResult : NActors::TEventLocal<TEvCreateTenantResult, EvCreateTenantResult> {
            Ydb::Cms::CreateDatabaseResponse CreateTenantResult;

            TEvCreateTenantResult(Ydb::Cms::CreateDatabaseResponse&& createTenantResult)
                : CreateTenantResult(std::move(createTenantResult))
            {}
        };

        struct TEvExecuteYqlResponse : NActors::TEventLocal<TEvExecuteYqlResponse, EvExecuteYqlResponse> {
            Ydb::Operations::Operation Operation;
            std::multimap<TString, TString> Meta;

            TEvExecuteYqlResponse(Ydb::Operations::Operation&& operation)
                : Operation(std::move(operation))
            {}
        };

        struct TEvPollYqlResponse : NActors::TEventLocal<TEvPollYqlResponse, EvPollYqlResponse> {
            Ydb::Operations::Operation Operation;

            TEvPollYqlResponse(Ydb::Operations::Operation&& operation)
                : Operation(std::move(operation))
            {}
        };

        struct TEvCreateSessionResponse : NActors::TEventLocal<TEvCreateSessionResponse, EvCreateSessionResponse> {
            Ydb::Operations::Operation Operation;

            TEvCreateSessionResponse(Ydb::Operations::Operation&& operation)
                : Operation(std::move(operation))
            {}
        };

        struct TEvExplainQueryResponse : NActors::TEventLocal<TEvExplainQueryResponse, EvExplainQueryResponse> {
            Ydb::Operations::Operation Operation;

            TEvExplainQueryResponse(Ydb::Operations::Operation&& operation)
                : Operation(std::move(operation))
            {}
        };

        struct TEvAlterTableResponse : NActors::TEventLocal<TEvAlterTableResponse, EvAlterTableResponse> {
            Ydb::Operations::Operation Operation;

            TEvAlterTableResponse(Ydb::Operations::Operation&& operation)
                : Operation(std::move(operation))
            {}
        };

        struct TEvExplainYqlResponse : NActors::TEventLocal<TEvExplainYqlResponse, EvExplainYqlResponse> {
            Ydb::Operations::Operation Operation;

            TEvExplainYqlResponse(Ydb::Operations::Operation&& operation)
                : Operation(std::move(operation))
            {}
        };

        struct TEvRetryRequest : NActors::TEventLocal<TEvRetryRequest, EvRetryRequest> {
        };

        struct TEvErrorResponse : NActors::TEventLocal<TEvErrorResponse, EvErrorResponse> {
            TString Status;
            TString Message;
            TString Details;

            TEvErrorResponse(const TString& error)
                : Status("503")
                , Message(error)
            {}

            TEvErrorResponse(const TString& status, const TString& error)
                : Status(status)
                , Message(error)
            {}

            TEvErrorResponse(const NYdbGrpc::TGrpcStatus& status) {
                switch(status.GRpcStatusCode) {
                case grpc::StatusCode::NOT_FOUND:
                    Status = "404";
                    break;
                case grpc::StatusCode::INVALID_ARGUMENT:
                    Status = "400";
                    break;
                case grpc::StatusCode::DEADLINE_EXCEEDED:
                    Status = "504";
                    break;
                case grpc::StatusCode::RESOURCE_EXHAUSTED:
                    Status = "429";
                    break;
                case grpc::StatusCode::PERMISSION_DENIED:
                    Status = "403";
                    break;
                case grpc::StatusCode::UNAUTHENTICATED:
                    Status = "401";
                    break;
                case grpc::StatusCode::INTERNAL:
                    Status = "500";
                    break;
                case grpc::StatusCode::FAILED_PRECONDITION:
                    Status = "412";
                    break;
                case grpc::StatusCode::UNAVAILABLE:
                default:
                    Status = "503";
                    break;
                }
                Message = status.Msg;
                Details = status.Details;
            }
        };

        struct TEvTryAgain : NActors::TEventLocal<TEvTryAgain, EvTryAgain> {};
    };

    static NJson::TJsonReaderConfig JsonReaderConfig;
    static NJson::TJsonWriterConfig JsonWriterConfig;

    static TString GetDatabaseFromPath(const TString& path) {
        return TString(TStringBuf(path).RNextTok('/'));
    }

    static TString GetEntryType(const TString& schemeEntry) {
        // special cases exist for backward compatibility with older versions of this function
        static const THashMap<TString, TString> specialCases = {
            {"ColumnTable", "column-table"},
            {"PqGroup", "topic"},
            {"SubDomain", "database"},
            {"RtmrVolume", "processing"},
            {"BlockStoreVolume", "volume"},
            {"CoordinationNode", "coordination"},
            {"ColumnStore", "column-store"},
            {"ExternalTable", "external-table"},
            {"ExternalDataSource", "external-data-source"},
            {"ResourcePool", "resource-pool"}
        };
        if (const auto* mapping = specialCases.FindPtr(schemeEntry)) {
            return *mapping;
        }

        return to_lower(schemeEntry);
    }

    static void WriteSchemeEntryPermissions(NJson::TJsonValue& value,
                                            const TVector<NYdb::NScheme::TPermissions>& permissions,
                                            const TString& filterSubject = TString()) {
        value.SetType(NJson::JSON_ARRAY);
        THashMap<TString, THashSet<TString>> actualPermissions;
        for (const NYdb::NScheme::TPermissions& permission : permissions) {
            if (!filterSubject.empty() && filterSubject != permission.Subject) {
                continue;
            }
            auto& subjectPermissions = actualPermissions[permission.Subject];
            for (const TString& perm : permission.PermissionNames) {
                subjectPermissions.emplace(perm);
            }
        }

        for (const auto& subject : actualPermissions) {
            NJson::TJsonValue& jsonPermission = value.AppendValue(NJson::TJsonValue());
            jsonPermission["subject"] = subject.first;
            NJson::TJsonValue& jsonRights = jsonPermission["permissions"];
            jsonRights.SetType(NJson::JSON_ARRAY);
            for (const TString& permission : subject.second) {
                jsonRights.AppendValue(permission);
            }
        }
    }

    static void WriteSchemeEntry(NJson::TJsonValue& value, const NYdb::NScheme::TSchemeEntry& entry) {
        value["name"] = entry.Name;
        value["owner"] = entry.Owner;
        value["type"] = GetEntryType(ToString(entry.Type));
        WriteAccessEntry(value, entry);
    }

    static void WriteAccessEntry(NJson::TJsonValue& value,
                                 const NYdb::NScheme::TSchemeEntry& entry,
                                 const TString& filterSubject = TString()) {
        NJson::TJsonValue& access = value["access"];
        WriteSchemeEntryPermissions(access["set"], entry.Permissions, filterSubject);
        WriteSchemeEntryPermissions(access["effective"], entry.EffectivePermissions, filterSubject);
    }

    static void ColumnTypeToString(NJson::TJsonValue& root, NYdb::TType type) {
        root = NYdb::FormatType(type);
    }

    static void WriteResultSet(
        NJson::TJsonValue& root,
        const NYdb::TResultSet& resultSet,
        const std::function<void(NJson::TJsonValue&, NYdb::TType)>& columnTypeFormatter = ColumnTypeToString) {
        NJson::TJsonValue& columns = root["columns"];
        const auto& columnsMeta = resultSet.GetColumnsMeta();
        WriteColumns(columns, columnsMeta, {}, columnTypeFormatter);

        NJson::TJsonValue& data = root["data"];
        data.SetType(NJson::JSON_ARRAY);

        NYdb::TResultSetParser rsParser(resultSet);
        while (rsParser.TryNextRow()) {
            NJson::TJsonValue& row = data.AppendValue(NJson::TJsonValue());
            for (size_t columnNum = 0; columnNum < columnsMeta.size(); ++columnNum) {
                const NYdb::TColumn& columnMeta = columnsMeta[columnNum];
                row[columnMeta.Name] = ColumnValueToJsonValue(rsParser.ColumnParser(columnNum));
            }
        }
    }

    static void WriteColumns(NJson::TJsonValue& columns,
                             const TVector<NYdb::TColumn>& columnsMeta,
                             const TVector<TString>& columnsKeysMeta = TVector<TString>(),
                             const std::function<void(NJson::TJsonValue&, NYdb::TType)>& columnTypeFormatter = ColumnTypeToString) {
        for (const NYdb::TColumn& columnMeta : columnsMeta) {
            NJson::TJsonValue& column = columns.AppendValue(NJson::TJsonValue());
            column["name"] = columnMeta.Name;
            columnTypeFormatter(column["type"], columnMeta.Type);
            auto itKey = Find(columnsKeysMeta, columnMeta.Name);
            if (itKey != columnsKeysMeta.end()) {
                column["key"] = true;
                column["keyOrder"] = itKey - columnsKeysMeta.begin();
            }
        }
    }

    static void WriteIndexes(NJson::TJsonValue& indexes,
                             const TVector<NYdb::NTable::TIndexDescription>& indexesMeta) {
        indexes.SetType(NJson::JSON_ARRAY);
        for (const NYdb::NTable::TIndexDescription& indexMeta : indexesMeta) {
            NJson::TJsonValue& index = indexes.AppendValue(NJson::TJsonValue());
            index["name"] = indexMeta.GetIndexName();
            index["indexType"] = ToString(indexMeta.GetIndexType());
            index["sizeBytes"] = indexMeta.GetSizeBytes();
            NJson::TJsonValue& indexColumns = index["indexColumns"];
            indexColumns.SetType(NJson::JSON_ARRAY);
            for (const TString& column : indexMeta.GetIndexColumns()) {
                indexColumns.AppendValue(column);
            }
            NJson::TJsonValue& dataColumns = index["dataColumns"];
            dataColumns.SetType(NJson::JSON_ARRAY);
            for (const TString& column : indexMeta.GetDataColumns()) {
                dataColumns.AppendValue(column);
            }
        }
    }

    static void WriteShards(NJson::TJsonValue& shards,
                            const NYdb::NTable::TTableDescription& tableDescription) {
        shards.SetType(NJson::JSON_ARRAY);
        const TVector<NYdb::NTable::TKeyRange>& ranges = tableDescription.GetKeyRanges();
        const TVector<NYdb::NTable::TPartitionStats>& stats = tableDescription.GetPartitionStats();
        for (ui64 nPart = 0; nPart < tableDescription.GetPartitionsCount(); ++nPart) {
            NJson::TJsonValue& shard = shards.AppendValue(NJson::TJsonValue());
            shard.SetType(NJson::JSON_MAP);
            if (nPart > 0 && nPart < ranges.size()) {
                const NYdb::NTable::TKeyRange& range = ranges[nPart];
                NYdb::TValueParser parser(range.From().GetRef().GetValue());
                shard["boundary"] = ColumnValueToJsonValue(parser);
            }
            if (nPart < stats.size()) {
                const NYdb::NTable::TPartitionStats& stat = stats[nPart];
                shard["rows"] = ToString(stat.Rows); // to avoid returning ui64 as number
                shard["size"] = ToString(stat.Size);
            }
        }
    }

    static void WriteUnitResources(NJson::TJsonValue& result, const TYdbUnitResources& unitResources) {
        if (unitResources.Cpu != 0) {
            result["cpu"] = unitResources.Cpu;
        }
        if (unitResources.Memory != 0) {
            result["memory"] = unitResources.Memory;
        }
        if (unitResources.Storage != 0) {
            result["storage"] = unitResources.Storage;
        }
    }

    static void WriteTopicConsumers(NJson::TJsonValue& result, const TVector<NYdb::NTopic::TConsumer>& consumers) {
        result.SetType(NJson::JSON_ARRAY);
        for (const auto& consumer: consumers) {
            auto& item = result.AppendValue(NJson::TJsonValue());
            item.SetType(NJson::JSON_MAP);
            item["name"] = consumer.GetConsumerName();
            item["readFrom"] = consumer.GetReadFrom().ToRfc822String();
            auto& supportedCodecs = item["supportedCodecs"];
            supportedCodecs.SetType(NJson::JSON_ARRAY);
            for (const auto codec: consumer.GetSupportedCodecs()) {
                supportedCodecs.AppendValue(TStringBuilder() << codec);
            }
        }
    }

    static void WriteTopicMeteringMode(NJson::TJsonValue& result, NYdb::NTopic::EMeteringMode mode) {
        if (mode == NYdb::NTopic::EMeteringMode::RequestUnits) {
            result = "request-units";
            return;
        }
        if (mode == NYdb::NTopic::EMeteringMode::ReservedCapacity) {
            result = "reserved-capacity";
            return;
        }
        result = "unspecified";
    }

    static void WriteTopicPartitioningSettings(NJson::TJsonValue& result, const NYdb::NTopic::TPartitioningSettings& settings) {
        result.SetType(NJson::JSON_MAP);
        result["limit"] = settings.GetPartitionCountLimit();
        result["min_active"] = settings.GetMinActivePartitions();
    }

    static void WriteTopicPartitions(NJson::TJsonValue& result, const TVector<NYdb::NTopic::TPartitionInfo>& partitionsInfo) {
        result.SetType(NJson::JSON_ARRAY);
        for (const auto& info: partitionsInfo) {
            auto& item = result.AppendValue(NJson::TJsonValue());
            item["active"] = info.GetActive();
            item["id"] = info.GetPartitionId();
        }
    }

    static void CopyHeader(const NHttp::THeaders& request, NHttp::THeadersBuilder& headers, TStringBuf header) {
        if (request.Has(header)) {
            headers.Set(header, request[header]);
        }
    }

    static void CopyAuthHeaders(const NHttp::THeaders& request, NHttp::THeadersBuilder& headers) {
        CopyHeader(request, headers, "Authorization");
        CopyHeader(request, headers, "x-request-id");
        CopyHeader(request, headers, "x-yacloud-subjecttoken");
        CopyHeader(request, headers, NYdb::YDB_AUTH_TICKET_HEADER);
        CopyHeader(request, headers, "Cookie");
    }

    static TDuration GetClientTimeout() {
        return TDuration::Seconds(10);
    }

    static TDuration GetTimeout() {
        return TDuration::Seconds(20);
    }

    static TDuration GetTimeout(const TRequest& request, TDuration defaultTimeout = {}) {
        TString timeout = request.Parameters["timeout"];
        if (!defaultTimeout) {
            defaultTimeout = GetTimeout();
        }
        if (timeout) {
            return TDuration::MilliSeconds(FromStringWithDefault(timeout, defaultTimeout.MilliSeconds()));
        }
        return defaultTimeout;
    }

    static TDuration GetQueryTimeout() {
        return TDuration::Minutes(10);
    }

    static bool IsRetryableError(const NYdb::TStatus& status) {
        if (status.GetStatus() == NYdb::EStatus::CLIENT_DISCOVERY_FAILED/*402010*/) {
            return true;
        }
        return false;
    }

    static bool IsRetryableError(const Ydb::Operations::Operation& operation) {
        for (const Ydb::Issue::IssueMessage& issue : operation.issues()) {
            if (issue.message().find("database unknown") != TString::npos) {
                return true;
            }
            if (issue.message().find("#200802") != TString::npos) {
                return true;
            }
        }
        return false;
    }

    static NHttp::THttpOutgoingResponsePtr CreateStatusResponse(NHttp::THttpIncomingRequestPtr request, const NYdb::TStatus& status, const TJsonSettings& jsonSettings = TJsonSettings()) {
        Ydb::Operations::Operation operation;
        operation.set_status(static_cast<Ydb::StatusIds_StatusCode>(status.GetStatus()));
        IssuesToMessage(status.GetIssues(), operation.mutable_issues());
        return CreateStatusResponse(request, operation, jsonSettings);
    }

    static NHttp::THttpOutgoingResponsePtr CreateStatusResponse(NHttp::THttpIncomingRequestPtr request, const Ydb::Operations::Operation& operation, const TJsonSettings& jsonSettings = TJsonSettings()) {
        TStringBuf status = "503";
        TStringBuf message = "Service Unavailable";
        switch ((int)operation.status()) {
        case Ydb::StatusIds::SUCCESS:
            status = "200";
            message = "OK";
            break;
        case Ydb::StatusIds::UNAUTHORIZED:
        case (int)NYdb::EStatus::CLIENT_UNAUTHENTICATED:
            status = "401";
            message = "Unauthorized";
            break;
        case Ydb::StatusIds::BAD_REQUEST:
        case Ydb::StatusIds::SCHEME_ERROR:
        case Ydb::StatusIds::GENERIC_ERROR:
        case Ydb::StatusIds::BAD_SESSION:
        case Ydb::StatusIds::PRECONDITION_FAILED:
        case Ydb::StatusIds::ALREADY_EXISTS:
        case Ydb::StatusIds::SESSION_EXPIRED:
        case Ydb::StatusIds::UNDETERMINED:
        case Ydb::StatusIds::ABORTED:
        case Ydb::StatusIds::UNSUPPORTED:
            status = "400";
            message = "Bad Request";
            break;
        case Ydb::StatusIds::NOT_FOUND:
            status = "404";
            message = "Not Found";
            break;
        case Ydb::StatusIds::OVERLOADED:
            status = "429";
            message = "Overloaded";
            break;
        case Ydb::StatusIds::INTERNAL_ERROR:
            status = "500";
            message = "Internal Server Error";
            break;
        case Ydb::StatusIds::UNAVAILABLE:
            status = "503";
            message = "Service Unavailable";
            break;
        case Ydb::StatusIds::TIMEOUT:
            status = "504";
            message = "Gateway Time-out";
            break;
        default:
            break;
        }
        TStringStream stream;
        TProtoToJson::ProtoToJson(stream, operation, jsonSettings);
        return request->CreateResponse(status, message, "application/json", stream.Str());
    }

    static NHttp::THttpOutgoingResponsePtr CreateErrorResponse(NHttp::THttpIncomingRequestPtr request, const TEvPrivate::TEvErrorResponse* error) {
        NJson::TJsonValue json;
        json["message"] = error->Message;
        TString body = NJson::WriteJson(json, false);
        return request->CreateResponse(error->Status, error->Message, "application/json", body);
    }

    static NHttp::THttpOutgoingResponsePtr CreateErrorResponse(NHttp::THttpIncomingRequestPtr request, const TString& error) {
        NJson::TJsonValue json;
        json["message"] = error;
        TString body = NJson::WriteJson(json, false);
        return request->CreateResponseServiceUnavailable(body, "application/json");
    }

    static TString ColumnPrimitiveValueToString(NYdb::TValueParser& valueParser) {
        switch (valueParser.GetPrimitiveType()) {
            case NYdb::EPrimitiveType::Bool:
                return TStringBuilder() << valueParser.GetBool();
            case NYdb::EPrimitiveType::Int8:
                return TStringBuilder() << valueParser.GetInt8();
            case NYdb::EPrimitiveType::Uint8:
                return TStringBuilder() << valueParser.GetUint8();
            case NYdb::EPrimitiveType::Int16:
                return TStringBuilder() << valueParser.GetInt16();
            case NYdb::EPrimitiveType::Uint16:
                return TStringBuilder() << valueParser.GetUint16();
            case NYdb::EPrimitiveType::Int32:
                return TStringBuilder() << valueParser.GetInt32();
            case NYdb::EPrimitiveType::Uint32:
                return TStringBuilder() << valueParser.GetUint32();
            case NYdb::EPrimitiveType::Int64:
                return TStringBuilder() << valueParser.GetInt64();
            case NYdb::EPrimitiveType::Uint64:
                return TStringBuilder() << valueParser.GetUint64();
            case NYdb::EPrimitiveType::Float:
                return TStringBuilder() << valueParser.GetFloat();
            case NYdb::EPrimitiveType::Double:
                return TStringBuilder() << valueParser.GetDouble();
            case NYdb::EPrimitiveType::Utf8:
                return TStringBuilder() << valueParser.GetUtf8();
            case NYdb::EPrimitiveType::Date:
                return TStringBuilder() << valueParser.GetDate().ToString();
            case NYdb::EPrimitiveType::Datetime:
                return TStringBuilder() << valueParser.GetDatetime().ToString();
            case NYdb::EPrimitiveType::Timestamp:
                return TStringBuilder() << valueParser.GetTimestamp().ToString();
            case NYdb::EPrimitiveType::Interval:
                return TStringBuilder() << valueParser.GetInterval();
            case NYdb::EPrimitiveType::Date32:
                return TStringBuilder() << valueParser.GetDate32();
            case NYdb::EPrimitiveType::Datetime64:
                return TStringBuilder() << valueParser.GetDatetime64();
            case NYdb::EPrimitiveType::Timestamp64:
                return TStringBuilder() << valueParser.GetTimestamp64();
            case NYdb::EPrimitiveType::Interval64:
                return TStringBuilder() << valueParser.GetInterval64();
            case NYdb::EPrimitiveType::TzDate:
                return TStringBuilder() << valueParser.GetTzDate();
            case NYdb::EPrimitiveType::TzDatetime:
                return TStringBuilder() << valueParser.GetTzDatetime();
            case NYdb::EPrimitiveType::TzTimestamp:
                return TStringBuilder() << valueParser.GetTzTimestamp();
            case NYdb::EPrimitiveType::String:
                return TStringBuilder() << Base64Encode(valueParser.GetString());
            case NYdb::EPrimitiveType::Yson:
                return TStringBuilder() << valueParser.GetYson();
            case NYdb::EPrimitiveType::Json:
                return TStringBuilder() << valueParser.GetJson();
            case NYdb::EPrimitiveType::JsonDocument:
                return TStringBuilder() << valueParser.GetJsonDocument();
            case NYdb::EPrimitiveType::DyNumber:
                return TStringBuilder() << valueParser.GetDyNumber();
            case NYdb::EPrimitiveType::Uuid:
                return TStringBuilder() << "<uuid not implemented>";
        }
    }

    static TString ColumnValueToString(const NYdb::TValue& value) {
        NYdb::TValueParser valueParser(value);
        return ColumnValueToString(valueParser);
    }

    static TString ColumnValueToString(NYdb::TValueParser& valueParser) {
        switch (valueParser.GetKind()) {
            case NYdb::TTypeParser::ETypeKind::Primitive:
                return ColumnPrimitiveValueToString(valueParser);

            case NYdb::TTypeParser::ETypeKind::Optional:
                valueParser.OpenOptional();
                if (valueParser.GetKind() == NYdb::TTypeParser::ETypeKind::Primitive) {
                    if (valueParser.IsNull()) {
                        return "";
                    } else {
                        return ColumnPrimitiveValueToString(valueParser);
                    }
                }

                return TStringBuilder() << NYdb::TTypeParser::ETypeKind::Optional;


            default:
                return TStringBuilder() << valueParser.GetKind();
        }
    }

    static NJson::TJsonValue ColumnPrimitiveValueToJsonValue(NYdb::TValueParser& valueParser) {
        switch (valueParser.GetPrimitiveType()) {
            case NYdb::EPrimitiveType::Bool:
                return valueParser.GetBool();
            case NYdb::EPrimitiveType::Int8:
                return valueParser.GetInt8();
            case NYdb::EPrimitiveType::Uint8:
                return valueParser.GetUint8();
            case NYdb::EPrimitiveType::Int16:
                return valueParser.GetInt16();
            case NYdb::EPrimitiveType::Uint16:
                return valueParser.GetUint16();
            case NYdb::EPrimitiveType::Int32:
                return valueParser.GetInt32();
            case NYdb::EPrimitiveType::Uint32:
                return valueParser.GetUint32();
            case NYdb::EPrimitiveType::Int64:
                return TStringBuilder() << valueParser.GetInt64();
            case NYdb::EPrimitiveType::Uint64:
                return TStringBuilder() << valueParser.GetUint64();
            case NYdb::EPrimitiveType::Float:
                return valueParser.GetFloat();
            case NYdb::EPrimitiveType::Double:
                return valueParser.GetDouble();
            case NYdb::EPrimitiveType::Utf8:
                return valueParser.GetUtf8();
            case NYdb::EPrimitiveType::Date:
                return valueParser.GetDate().ToString();
            case NYdb::EPrimitiveType::Datetime:
                return valueParser.GetDatetime().ToString();
            case NYdb::EPrimitiveType::Timestamp:
                return valueParser.GetTimestamp().ToString();
            case NYdb::EPrimitiveType::Interval:
                return TStringBuilder() << valueParser.GetInterval();
            case NYdb::EPrimitiveType::Date32:
                return valueParser.GetDate32();
            case NYdb::EPrimitiveType::Datetime64:
                return valueParser.GetDatetime64();
            case NYdb::EPrimitiveType::Timestamp64:
                return valueParser.GetTimestamp64();
            case NYdb::EPrimitiveType::Interval64:
                return valueParser.GetInterval64();
            case NYdb::EPrimitiveType::TzDate:
                return valueParser.GetTzDate();
            case NYdb::EPrimitiveType::TzDatetime:
                return valueParser.GetTzDatetime();
            case NYdb::EPrimitiveType::TzTimestamp:
                return valueParser.GetTzTimestamp();
            case NYdb::EPrimitiveType::String:
                return Base64Encode(valueParser.GetString());
            case NYdb::EPrimitiveType::Yson:
                return valueParser.GetYson();
            case NYdb::EPrimitiveType::Json:
                return valueParser.GetJson();
            case NYdb::EPrimitiveType::JsonDocument:
                return valueParser.GetJsonDocument();
            case NYdb::EPrimitiveType::DyNumber:
                return valueParser.GetDyNumber();
            case NYdb::EPrimitiveType::Uuid:
                return "<uuid not implemented>";
        }
    }

    static NJson::TJsonValue ColumnValueToJsonValue(NYdb::TValueParser& valueParser) {
        switch (valueParser.GetKind()) {
        case NYdb::TTypeParser::ETypeKind::Primitive:
            return ColumnPrimitiveValueToJsonValue(valueParser);
        case NYdb::TTypeParser::ETypeKind::Optional: {
            NJson::TJsonValue jsonValue;
            valueParser.OpenOptional();
            if (valueParser.IsNull()) {
                jsonValue = NJson::JSON_NULL;
            } else {
                jsonValue = ColumnValueToJsonValue(valueParser);
            }
            valueParser.CloseOptional();
            return jsonValue;
        }
        case NYdb::TTypeParser::ETypeKind::Tuple: {
            NJson::TJsonValue jsonArray;
            jsonArray.SetType(NJson::JSON_ARRAY);
            valueParser.OpenTuple();
            while (valueParser.TryNextElement()) {
                jsonArray.AppendValue(ColumnValueToJsonValue(valueParser));
            }
            valueParser.CloseTuple();
            return jsonArray;
        }
        default:
            return NJson::JSON_UNDEFINED;
        }
    }

    static TString GetApiUrl(TString balancer, const TString& uri) {
        if (!balancer.StartsWith("http://") && !balancer.StartsWith("https://")) {
            if (balancer.find('/') == TString::npos) {
                balancer += ":8765/viewer/json";
            }
            if (balancer.find("cloud") != TString::npos) {
                balancer = "https://ydb.bastion.cloud.yandex-team.ru:443/" + balancer;
            } else if (balancer.find("ydb-") != TString::npos) {
                balancer = "https://viewer.ydb.yandex-team.ru:443/" + balancer;
            } else {
                balancer = "http://" + balancer;
            }
        }
        return balancer + uri;
    }

    static bool isalnum(const TString& str) {
        for (char c : str) {
            if (!std::isalnum(c)) {
                return false;
            }
        }
        return true;
    }

    static bool IsValidDatabaseId(const TString& databaseId) {
        return !databaseId.empty() && databaseId.size() <= 20 && isalnum(databaseId);
    }

    static bool IsValidParameterName(const TString& param) {
        for (char c : param) {
            if (c != '$' && c != '_' && !std::isalnum(c)) {
                return false;
            }
        }
        return true;
    }
};

} // namespace NMVP
