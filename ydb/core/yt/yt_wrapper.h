#pragma once

#include <ydb/core/base/events.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/ypath/rich.h>

#include <util/generic/vector.h>

namespace NKikimr {
namespace NWrappers {

struct TEvYtWrapper {
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
        using TResult = NYT::TErrorOr<T>;
        using TAsyncResult = NYT::TFuture<T>;

        TResult Result;

        explicit TGenericResponse(const TResult& result)
            : Result(result)
        {
        }

        using TBase = TGenericResponse<TDerived, EventType, T>;
    };

    #define EV_REQUEST_RESPONSE(name) \
        Ev##name##Request, \
        Ev##name##Response

    enum EEv {
        EvBegin = EventSpaceBegin(TKikimrEvents::ES_YT_WRAPPER),

        // Transactions
        EvTransactions,
        EV_REQUEST_RESPONSE(StartTransaction),

        // Tables
        EvTables = EvBegin + 1 * 100,
        EV_REQUEST_RESPONSE(LookupRows),
        EV_REQUEST_RESPONSE(VersionedLookupRows),
        EV_REQUEST_RESPONSE(SelectRows),
        EV_REQUEST_RESPONSE(ExplainQuery),

        EvTableReader = EvBegin + 2 * 100,
        EV_REQUEST_RESPONSE(CreateTableReader),

        EvTableWriter = EvBegin + 3 * 100,
        EV_REQUEST_RESPONSE(CreateTableWriter),
        EV_REQUEST_RESPONSE(WriteTable),
        EV_REQUEST_RESPONSE(GetNameTable),

        // Cypress
        EvCypress = EvBegin + 4 * 100,
        EV_REQUEST_RESPONSE(GetNode),
        EV_REQUEST_RESPONSE(SetNode),
        EV_REQUEST_RESPONSE(MultisetAttributesNode),
        EV_REQUEST_RESPONSE(RemoveNode),
        EV_REQUEST_RESPONSE(ListNode),
        EV_REQUEST_RESPONSE(CreateNode),
        EV_REQUEST_RESPONSE(LockNode),
        EV_REQUEST_RESPONSE(UnlockNode),
        EV_REQUEST_RESPONSE(CopyNode),
        EV_REQUEST_RESPONSE(MoveNode),
        EV_REQUEST_RESPONSE(LinkNode),
        EV_REQUEST_RESPONSE(ConcatenateNodes),
        EV_REQUEST_RESPONSE(NodeExists),
        EV_REQUEST_RESPONSE(ExternalizeNode),
        EV_REQUEST_RESPONSE(InternalizeNode),

        // Objects
        EvObjects = EvBegin + 5 * 100,
        EV_REQUEST_RESPONSE(CreateObject),

        // Files
        EvFileReader = EvBegin + 6 * 100,
        EV_REQUEST_RESPONSE(CreateFileReader),

        EvFileWriter = EvBegin + 7 * 100,
        EV_REQUEST_RESPONSE(CreateFileWriter),

        // Journals
        EvJournalReader = EvBegin + 8 * 100,
        EV_REQUEST_RESPONSE(CreateJournalReader),

        EvJournalWriter = EvBegin + 9 * 100,
        EV_REQUEST_RESPONSE(CreateJournalWriter),

        EvEnd,
    };

    #undef EV_REQUEST_RESPONSE

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_YT_WRAPPER), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_YT_WRAPPER)");

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

    DEFINE_GENERIC_REQUEST_RESPONSE(CreateTableWriter, TActorId, NYT::NYPath::TRichYPath, NYT::NApi::TTableWriterOptions);
    DEFINE_GENERIC_REQUEST_RESPONSE(WriteTable, void, TVector<NYT::NTableClient::TUnversionedOwningRow>, bool);
    DEFINE_GENERIC_REQUEST_RESPONSE(GetNameTable, NYT::NTableClient::TNameTablePtr, void);
    DEFINE_GENERIC_REQUEST_RESPONSE(GetNode, NYT::NYson::TYsonString, NYT::NYPath::TYPath, NYT::NApi::TGetNodeOptions);
    DEFINE_GENERIC_REQUEST_RESPONSE(CreateNode, NYT::NCypressClient::TNodeId, NYT::NYPath::TYPath, NYT::NObjectClient::EObjectType, NYT::NApi::TCreateNodeOptions);
    DEFINE_GENERIC_REQUEST_RESPONSE(NodeExists, bool, NYT::NYPath::TYPath, NYT::NApi::TNodeExistsOptions);

    #undef DEFINE_GENERIC_REQUEST_RESPONSE
    #undef DEFINE_GENERIC_RESPONSE
    #undef DEFINE_GENERIC_REQUEST

}; // TEvYtWrapper

IActor* CreateYtWrapper(NYT::NApi::NRpcProxy::TConnectionConfigPtr config, const NYT::NApi::TClientOptions& options = {});
IActor* CreateYtWrapper(const TString& serverName, const TString& token);

} // NWrappers
} // NKikimr
