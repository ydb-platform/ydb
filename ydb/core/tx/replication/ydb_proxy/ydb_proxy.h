#pragma once

#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>

namespace NKikimrReplication {
    class TStaticCredentials;
}

namespace NKikimr::NReplication {

#pragma push_macro("RemoveDirectory")
#undef RemoveDirectory

struct TEvYdbProxy {
    #define EV_REQUEST_RESPONSE(name) \
        Ev##name##Request, \
        Ev##name##Response

    enum EEv {
        EvBegin = EventSpaceBegin(TKikimrEvents::ES_YDB_PROXY),

        EvScheme,
        EV_REQUEST_RESPONSE(MakeDirectory),
        EV_REQUEST_RESPONSE(RemoveDirectory),
        EV_REQUEST_RESPONSE(DescribePath),
        EV_REQUEST_RESPONSE(ListDirectory),
        EV_REQUEST_RESPONSE(ModifyPermissions),

        EvTable = EvBegin + 1 * 100,
        EV_REQUEST_RESPONSE(CreateSession),
        EV_REQUEST_RESPONSE(CreateTable),
        EV_REQUEST_RESPONSE(DropTable),
        EV_REQUEST_RESPONSE(AlterTable),
        EV_REQUEST_RESPONSE(CopyTable),
        EV_REQUEST_RESPONSE(CopyTables),
        EV_REQUEST_RESPONSE(RenameTables),
        EV_REQUEST_RESPONSE(DescribeTable),

        EvTopic = EvBegin + 2 * 100,
        EV_REQUEST_RESPONSE(CreateTopic),
        EV_REQUEST_RESPONSE(AlterTopic),
        EV_REQUEST_RESPONSE(DropTopic),
        EV_REQUEST_RESPONSE(DescribeTopic),
        EV_REQUEST_RESPONSE(DescribeConsumer),
        EV_REQUEST_RESPONSE(CreateTopicReader),
        EV_REQUEST_RESPONSE(ReadTopic),

        EvEnd,
    };

    #undef EV_REQUEST_RESPONSE

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_YDB_PROXY), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_YDB_PROXY)");

    template <typename TDerived, ui32 EventType, typename... Args>
    struct TGenericRequest: public std::tuple<Args...>, public TEventLocal<TDerived, EventType> {
        using TTupleArgs = std::tuple<Args...>;

        explicit TGenericRequest(Args... args)
            : std::tuple<Args...>(args...)
        {
        }

        const TTupleArgs& GetArgs() const {
            return *this;
        }

        TTupleArgs&& GetArgs() {
            return std::move(*this);
        }

        using TBase = TGenericRequest<TDerived, EventType, Args...>;
    };

    template <typename TDerived, ui32 EventType>
    struct TGenericRequest<TDerived, EventType, void>: public TEventLocal<TDerived, EventType> {
        using TBase = TGenericRequest<TDerived, EventType, void>;
    };

    template <typename TDerived, ui32 EventType, typename T>
    struct TGenericResponse: public TEventLocal<TDerived, EventType> {
        using TResult = T;
        using TAsyncResult = NThreading::TFuture<TResult>;

        TResult Result;

        template <typename... Args>
        explicit TGenericResponse(Args&&... args)
            : Result(std::forward<Args>(args)...)
        {
        }

        using TBase = TGenericResponse<TDerived, EventType, T>;
    };

    #define DEFINE_GENERIC_REQUEST(name, ...) \
        struct TEv##name##Request: public TGenericRequest<TEv##name##Request, Ev##name##Request, __VA_ARGS__> { \
            using TBase::TBase; \
        }

    #define DEFINE_GENERIC_RESPONSE(name, result_t) \
        struct TEv##name##Response: public TGenericResponse<TEv##name##Response, Ev##name##Response, result_t> { \
            using TBase::TBase; \
        }

    #define DEFINE_GENERIC_REQUEST_RESPONSE(name, result_t, ...) \
        DEFINE_GENERIC_REQUEST(name, __VA_ARGS__); \
        DEFINE_GENERIC_RESPONSE(name, result_t)

    // Scheme
    DEFINE_GENERIC_REQUEST_RESPONSE(MakeDirectory, NYdb::TStatus, TString, NYdb::NScheme::TMakeDirectorySettings);
    DEFINE_GENERIC_REQUEST_RESPONSE(RemoveDirectory, NYdb::TStatus, TString, NYdb::NScheme::TRemoveDirectorySettings);
    DEFINE_GENERIC_REQUEST_RESPONSE(DescribePath, NYdb::NScheme::TDescribePathResult, TString, NYdb::NScheme::TDescribePathSettings);
    DEFINE_GENERIC_REQUEST_RESPONSE(ListDirectory, NYdb::NScheme::TListDirectoryResult, TString, NYdb::NScheme::TListDirectorySettings);
    DEFINE_GENERIC_REQUEST_RESPONSE(ModifyPermissions, NYdb::TStatus, TString, NYdb::NScheme::TModifyPermissionsSettings);
    // Table
    DEFINE_GENERIC_RESPONSE(CreateSession, NYdb::NTable::TCreateSessionResult);
    DEFINE_GENERIC_REQUEST_RESPONSE(CreateTable, NYdb::TStatus, TString, NYdb::NTable::TTableDescription, NYdb::NTable::TCreateTableSettings);
    DEFINE_GENERIC_REQUEST_RESPONSE(DropTable, NYdb::TStatus, TString, NYdb::NTable::TDropTableSettings);
    DEFINE_GENERIC_REQUEST_RESPONSE(AlterTable, NYdb::TStatus, TString, NYdb::NTable::TAlterTableSettings);
    DEFINE_GENERIC_REQUEST_RESPONSE(CopyTable, NYdb::TStatus, TString, TString, NYdb::NTable::TCopyTableSettings);
    DEFINE_GENERIC_REQUEST_RESPONSE(CopyTables, NYdb::TStatus, TVector<NYdb::NTable::TCopyItem>, NYdb::NTable::TCopyTablesSettings);
    DEFINE_GENERIC_REQUEST_RESPONSE(RenameTables, NYdb::TStatus, TVector<NYdb::NTable::TRenameItem>, NYdb::NTable::TRenameTablesSettings);
    DEFINE_GENERIC_REQUEST_RESPONSE(DescribeTable, NYdb::NTable::TDescribeTableResult, TString, NYdb::NTable::TDescribeTableSettings);
    // Topic
    DEFINE_GENERIC_REQUEST_RESPONSE(CreateTopic, NYdb::TStatus, TString, NYdb::NTopic::TCreateTopicSettings);
    DEFINE_GENERIC_REQUEST_RESPONSE(AlterTopic, NYdb::TStatus, TString, NYdb::NTopic::TAlterTopicSettings);
    DEFINE_GENERIC_REQUEST_RESPONSE(DropTopic, NYdb::TStatus, TString, NYdb::NTopic::TDropTopicSettings);
    DEFINE_GENERIC_REQUEST_RESPONSE(DescribeTopic, NYdb::NTopic::TDescribeTopicResult, TString, NYdb::NTopic::TDescribeTopicSettings);
    DEFINE_GENERIC_REQUEST_RESPONSE(DescribeConsumer, NYdb::NTopic::TDescribeConsumerResult, TString, TString, NYdb::NTopic::TDescribeConsumerSettings);
    DEFINE_GENERIC_REQUEST_RESPONSE(CreateTopicReader, TActorId, NYdb::NTopic::TReadSessionSettings);
    DEFINE_GENERIC_REQUEST_RESPONSE(ReadTopic, NYdb::NTopic::TReadSessionEvent::TEvent, void);

    #undef DEFINE_GENERIC_REQUEST_RESPONSE
    #undef DEFINE_GENERIC_RESPONSE
    #undef DEFINE_GENERIC_REQUEST

}; // TEvYdbProxy

#pragma pop_macro("RemoveDirectory")

IActor* CreateYdbProxy(const TString& endpoint, const TString& database);
IActor* CreateYdbProxy(const TString& endpoint, const TString& database, const TString& token);
IActor* CreateYdbProxy(const TString& endpoint, const TString& database,
    const NKikimrReplication::TStaticCredentials& credentials);

}
