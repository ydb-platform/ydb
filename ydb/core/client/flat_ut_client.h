#pragma once

#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

#include <library/cpp/testing/unittest/registar.h>
#include <google/protobuf/text_format.h>

namespace NKikimr {
namespace NFlatTests {

class TFlatMsgBusClient : public Tests::TClient {
public:
    TFlatMsgBusClient(const Tests::TServerSettings& settings)
        : TClient(settings)
    {}

    TFlatMsgBusClient(ui16 port)
        : TFlatMsgBusClient(Tests::TServerSettings(port))
    {}

    void InitRoot() {
        InitRootScheme();
    }

    using TClient::FlatQuery;

    NKikimrMiniKQL::TResult FlatQuery(const TString& mkql) {
        NKikimrMiniKQL::TResult res;
        TClient::TFlatQueryOptions opts;
        bool success = TClient::FlatQuery(mkql, opts, res, NMsgBusProxy::MSTATUS_OK);
        UNIT_ASSERT(success);
        return res;
    }

    NKikimrMiniKQL::TResult FlatQuery(const TString& mkql, ui32 expectedStatus, ui32 expectedProxyErrorCode = TEvTxUserProxy::TResultStatus::Unknown) {
        NKikimrMiniKQL::TResult res;
        TClient::TFlatQueryOptions opts;
        NKikimrClient::TResponse expectedResponse;
        expectedResponse.SetStatus(expectedStatus);
        if (expectedProxyErrorCode != TEvTxUserProxy::TResultStatus::Unknown) {
            expectedResponse.SetProxyErrorCode(expectedProxyErrorCode);
        }
        bool success = TClient::FlatQuery(mkql, opts, res, expectedResponse);
        UNIT_ASSERT(success == (expectedStatus == NMsgBusProxy::MSTATUS_OK));
        return res;
    }

    TAutoPtr<NMsgBusProxy::TBusResponse> LsPathId(ui64 schemeshardId, ui64 pathId) {
        TAutoPtr<NMsgBusProxy::TBusSchemeDescribe> request(new NMsgBusProxy::TBusSchemeDescribe());
        request->Record.SetPathId(pathId);
        request->Record.SetSchemeshardId(schemeshardId);
        TAutoPtr<NBus::TBusMessage> reply;
        NBus::EMessageStatus msgStatus = SendWhenReady(request, reply);
        UNIT_ASSERT_VALUES_EQUAL(msgStatus, NBus::MESSAGE_OK);
        Cout << PrintToString<NMsgBusProxy::TBusResponse>(reply.Get()) << Endl;
        return dynamic_cast<NMsgBusProxy::TBusResponse*>(reply.Release());
    }

    void ResetSchemeCache(Tests::TServer &server, TTableId tableId) {
        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId txProxy = MakeTxProxyID();
        TActorId sender = runtime->AllocateEdgeActor();
        TAutoPtr<TEvTxUserProxy::TEvInvalidateTable> ev(new TEvTxUserProxy::TEvInvalidateTable(tableId));
        runtime->Send(new IEventHandle(txProxy, sender, ev.Release()));
        TAutoPtr<IEventHandle> handle;
        auto readSchemeStringResult = runtime->GrabEdgeEventRethrow<TEvTxUserProxy::TEvInvalidateTableResult>(handle);
        Y_UNUSED(readSchemeStringResult);
    }

    void KillTablet(Tests::TServer &server, ui64 tabletId) {
        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();

        runtime->Send(new IEventHandle(MakeTabletResolverID(), sender, new TEvTabletResolver::TEvTabletProblem(tabletId, TActorId())));
        runtime->Send(new IEventHandle(MakeTabletResolverID(), sender, new TEvTabletResolver::TEvForward(tabletId, nullptr)));

        TAutoPtr<IEventHandle> handle;
        auto forwardResult = runtime->GrabEdgeEventRethrow<TEvTabletResolver::TEvForwardResult>(handle);
        UNIT_ASSERT(forwardResult && forwardResult->Tablet);
        runtime->Send(new IEventHandle(forwardResult->Tablet, sender, new TEvents::TEvPoisonPill()));
        runtime->Send(new IEventHandle(MakeTabletResolverID(), sender, new TEvTabletResolver::TEvTabletProblem(tabletId, TActorId())));
    }

    TVector<ui64> GetTablePartitions(const TString& tablePath) {
        TAutoPtr<NMsgBusProxy::TBusResponse> msg = Ls(tablePath);
        const NKikimrClient::TResponse &response = msg->Record;
        UNIT_ASSERT_VALUES_EQUAL(response.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        const auto& descr = response.GetPathDescription();
        TVector<ui64> partitions;
        for (ui32 i = 0; i < descr.TablePartitionsSize(); ++i) {
            partitions.push_back(descr.GetTablePartitions(i).GetDatashardId());
            // Cerr << partitions.back() << Endl;
        }
        return partitions;
    }

    void TrySplitTablePartition(const TString& tablePath, const TString& splitDescription, NKikimrClient::TResponse& response) {
        TAutoPtr<NMsgBusProxy::TBusSchemeOperation> request(new NMsgBusProxy::TBusSchemeOperation());
        auto *op = request->Record.MutableTransaction()->MutableModifyScheme();
        op->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpSplitMergeTablePartitions);
        UNIT_ASSERT(::google::protobuf::TextFormat::ParseFromString(splitDescription, op->MutableSplitMergeTablePartitions()));
        op->MutableSplitMergeTablePartitions()->SetTablePath(tablePath);

        TAutoPtr<NBus::TBusMessage> reply;
        NBus::EMessageStatus status = SendAndWaitCompletion(request.Release(), reply);
        UNIT_ASSERT_VALUES_EQUAL(status, NBus::MESSAGE_OK);
        UNIT_ASSERT_VALUES_EQUAL(reply->GetHeader()->Type, (int)NMsgBusProxy::MTYPE_CLIENT_RESPONSE);
        response = static_cast<NMsgBusProxy::TBusResponse *>(reply.Get())->Record;
    }

    NMsgBusProxy::EResponseStatus SplitTablePartition(const TString& tablePath, const TString& splitDescription) {
        NKikimrClient::TResponse response;
        TrySplitTablePartition(tablePath, splitDescription, response);
        UNIT_ASSERT_VALUES_EQUAL(response.GetStatus(), (int)NMsgBusProxy::MSTATUS_OK);
        return (NMsgBusProxy::EResponseStatus)response.GetStatus();
    }
};

}}
