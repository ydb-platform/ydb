#pragma once

#include <util/random/random.h>
#include <util/memory/blob.h>

#include <ydb/library/actors/core/actor.h>
#include <library/cpp/yson/node/node.h>

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/api/client.h>
#include <yt/yt/client/ypath/rich.h>

#include <ydb/library/yql/dq/common/dq_common.h>

#include "yt_events.h"

namespace NYql {

    template <ui32 EventType, typename... Args>
    struct TGenericYtCommand
       : public std::tuple<Args...>,
          NActors::TEventLocal<TGenericYtCommand<EventType, Args...>, EventType>
    {
        TGenericYtCommand(ui64 requestId, Args... args)
            : std::tuple<Args...>(args...)
            , RequestId(requestId)
        { }

        TGenericYtCommand(Args... args)
            : std::tuple<Args...>(args...)
            , RequestId(RandomNumber<ui64>())
        { }

        TGenericYtCommand() = default;

        const ui64 RequestId;
    };

    template <ui32 EventType, typename... Args>
    struct TGenericYtResponse
        : public TGenericYtCommand<EventType, Args ...>
    {
        TGenericYtResponse(ui64 requestId, Args... args)
            : TGenericYtCommand<EventType, Args ...>(requestId, args...)
        { }
    };

    using TEvStartOperation = TGenericYtCommand<TYtEvents::ES_START_OPERATION, NYT::NScheduler::EOperationType, TString, NYT::NApi::TStartOperationOptions>;
    using TEvStartOperationResponse = TGenericYtResponse<TYtEvents::ES_START_OPERATION_RESPONSE, NYT::TErrorOr<NYT::NScheduler::TOperationId>>;

    using TEvGetOperation = TGenericYtCommand<TYtEvents::ES_GET_OPERATION, NYT::NScheduler::TOperationIdOrAlias, NYT::NApi::TGetOperationOptions>;
    using TEvGetOperationResponse = TGenericYtResponse<TYtEvents::ES_GET_OPERATION_RESPONSE, NYT::TErrorOr<TString>>;

    using TEvListOperations = TGenericYtCommand<TYtEvents::ES_LIST_OPERATIONS, NYT::NApi::TListOperationsOptions>;
    using TEvListOperationsResponse = TGenericYtResponse<TYtEvents::ES_LIST_OPERATIONS_RESPONSE, NYT::TErrorOr<NYT::NApi::TListOperationsResult>>;

    using TEvGetJob = TGenericYtCommand<TYtEvents::ES_GET_JOB, NYT::NScheduler::TOperationId, NYT::NJobTrackerClient::TJobId, NYT::NApi::TGetJobOptions>;
    using TEvGetJobResponse = TGenericYtResponse<TYtEvents::ES_GET_JOB_RESPONSE, NYT::TErrorOr<TString>>;

    using TEvWriteFile = TGenericYtCommand<TYtEvents::ES_WRITE_FILE, TFile, NYT::NYPath::TRichYPath, THashMap<TString, NYT::TNode>, NYT::NApi::TFileWriterOptions>;
    using TEvWriteFileResponse = TGenericYtResponse<TYtEvents::ES_WRITE_FILE_RESPONSE, NYT::TErrorOr<void>>;

    using TEvReadFile = TGenericYtCommand<TYtEvents::ES_READ_FILE, NYT::NYPath::TRichYPath, TString, NYT::NApi::TFileReaderOptions>;
    using TEvReadFileResponse = TGenericYtResponse<TYtEvents::ES_READ_FILE_RESPONSE, NYT::TErrorOr<void>>;

    using TEvListNode = TGenericYtCommand<TYtEvents::ES_LIST_NODE, TString, NYT::NApi::TListNodeOptions>;
    using TEvListNodeResponse = TGenericYtResponse<TYtEvents::ES_LIST_NODE_RESPONSE, NYT::TErrorOr<TString>>;

    using TEvSetNode = TGenericYtCommand<TYtEvents::ES_SET_NODE, TString, NYT::NYson::TYsonString, NYT::NApi::TSetNodeOptions>;
    using TEvSetNodeResponse = TGenericYtResponse<TYtEvents::ES_SET_NODE_RESPONSE, NYT::TErrorOr<void>>;

    using TEvGetNode = TGenericYtCommand<TYtEvents::ES_GET_NODE, TString, NYT::NApi::TGetNodeOptions>;
    using TEvGetNodeResponse = TGenericYtResponse<TYtEvents::ES_GET_NODE_RESPONSE, NYT::TErrorOr<NYT::NYson::TYsonString>>;

    using TEvRemoveNode = TGenericYtCommand<TYtEvents::ES_REMOVE_NODE, TString, NYT::NApi::TRemoveNodeOptions>;
    using TEvRemoveNodeResponse = TGenericYtResponse<TYtEvents::ES_REMOVE_NODE_RESPONSE, NYT::TErrorOr<void>>;

    using TEvCreateNode = TGenericYtCommand<TYtEvents::ES_CREATE_NODE, TString, NYT::NObjectClient::EObjectType, NYT::NApi::TCreateNodeOptions>;
    using TEvCreateNodeResponse = TGenericYtResponse<TYtEvents::ES_CREATE_NODE_RESPONSE, NYT::TErrorOr<NYT::NCypressClient::TNodeId>>;

    using TEvStartTransaction = TGenericYtCommand<TYtEvents::ES_START_TRANSACTION, NYT::NTransactionClient::ETransactionType, NYT::NApi::TTransactionStartOptions>;
    using TEvStartTransactionResponse = TGenericYtResponse<TYtEvents::ES_START_TRANSACTION_RESPONSE, NYT::TErrorOr<NYT::NApi::ITransactionPtr>>;

    using TEvPrintJobStderr = TGenericYtCommand<TYtEvents::ES_PRINT_JOB_STDERR, NYT::NScheduler::TOperationId>;

    NActors::IActor* CreateYtWrapper(const NYT::NApi::IClientPtr& client);

} // namespace NYql
