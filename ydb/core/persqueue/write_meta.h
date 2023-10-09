#pragma once

#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/persqueue.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

#include <util/string/vector.h>

namespace NKikimr {

template <class TWriteRequestInit>
void FillExtraFieldsForDataChunk(
    const TWriteRequestInit& init, NKikimrPQClient::TDataChunk& data, TString& server, TString& ident, TString& logType, TString& file);


template <class TWriteRequestInit>
NKikimrPQClient::TDataChunk GetInitialDataChunk(const TWriteRequestInit& init, const TString& topic, const TString& peerName) {
    NKikimrPQClient::TDataChunk data;
    TString server, ident, logType, file;

    FillExtraFieldsForDataChunk(init, data, server, ident, logType, file);

    if (server.empty()) {
        server = peerName;
    }
    if (ident.empty()) {
        auto p = SplitString(topic, "--");
        if (p.size() == 3)
            ident = p[1];
        else
            ident = "unknown";
    }
    if (logType.empty()) {
        auto p = SplitString(topic, "--");
        if (p.size() == 3)
            logType = p.back();
        else
            logType = "unknown";
    }

    data.MutableMeta()->SetServer(server);
    data.MutableMeta()->SetIdent(ident);
    data.MutableMeta()->SetLogType(logType);
    if (!file.empty())
        data.MutableMeta()->SetFile(file);

    data.SetIp(peerName);
    return data;
}

template <class TData>
void FillChunkDataFromReq(NKikimrPQClient::TDataChunk& proto, const TData& data, int);

template <class TData>
void FillChunkDataFromReq(NKikimrPQClient::TDataChunk& proto, const TData& data);

template <typename... TArgs>
TString GetSerializedData(const NKikimrPQClient::TDataChunk& init, TArgs&...args) {
    NKikimrPQClient::TDataChunk proto;
    proto.CopyFrom(init);
    FillChunkDataFromReq(proto, std::forward<TArgs>(args)...);

    TString str;
    bool res = proto.SerializeToString(&str);
    Y_ABORT_UNLESS(res);
    return str;
}

TString GetSerializedData(const NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent::TCompressedMessage& message);
TString GetSerializedData(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TCompressedMessage& message);

NKikimrPQClient::TDataChunk GetDeserializedData(const TString& string);

} // NKikimr

