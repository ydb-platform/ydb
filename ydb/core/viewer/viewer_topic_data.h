#pragma once
#include "json_pipe_req.h"
#include "viewer.h"
#include <ydb/core/viewer/yaml/yaml.h>
#include <ydb/core/persqueue/user_info.h>
#include <ydb/core/persqueue/write_meta.h>

namespace NKikimr::NViewer {

struct TEvViewerTopicData {
    enum EEv {
        EvTopicDataUnpacked = EventSpaceBegin(TKikimrEvents::ES_VIEWER),
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_VIEWER), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_VIEWER)");

    struct TEvTopicDataUnpacked : TEventLocal<TEvTopicDataUnpacked, EEv::EvTopicDataUnpacked> {
        explicit TEvTopicDataUnpacked() = delete;
        explicit TEvTopicDataUnpacked(bool status, NJson::TJsonValue&& data)
            : Status(status)
            , Data(std::move(data))
        {
        }

        bool Status = true;
        NJson::TJsonValue Data;
    };
}; // TEvViewerTopicData


class TTopicData : public TViewerPipeClient {
    using TBase = TViewerPipeClient;
    using TThis = TTopicData;
    using TBase::ReplyAndPassAway;
    using TBase::GetHTTPBADREQUEST;

private:
    void HandleDescribe(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
    void SendPQReadRequest();
    void HandlePQResponse(TEvPersQueue::TEvResponse::TPtr& ev);
    void FillProtoResponse(ui64 maxSingleMessageSize = 1_MB, ui64 maxTotalSize = 10_MB);
    NYdb::NTopic::ICodec* GetCodec(NPersQueueCommon::ECodec codec);
    bool GetIntegerParam(const TString& name, i64& value);

    STATEFN(StateRequestedDescribe);


public:
    TTopicData(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {}

    void Bootstrap() override;
    void ReplyAndPassAway() override;

private:
    ui64 TabletId;
    TString TopicPath;
    i64 PartitionId;
    i64 Offset;
    i64 Limit;
    TMap<ui32, THolder<NYdb::NTopic::ICodec>> Codecs;
    std::optional<TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult>> NavigateResponse;

    TAutoPtr<TEvPersQueue::TEvResponse> ReadResponse;
    NKikimrViewer::TTopicDataResponse ProtoResponse;

    static constexpr ui32 READ_TIMEOUT_MS = 1000;
    static constexpr ui32 MAX_MESSAGES_LIMIT = 1000;

public:
    static YAML::Node GetSwagger() {
        YAML::Node node = YAML::Load(R"___(
        get:
            tags:
              - viewer
            summary: Read topic data
            description: Reads and returns data from topic (if any)
            parameters:
              - name: database
                in: query
                description: database name
                type: string
                required: false
              - name: path
                in: query
                description: path of topic
                required: true
                type: string
              - name: partition
                in: query
                description: partition to read from
                required: true
                type: integer
              - name: offset
                in: query
                description: start offset to read from
                required: true
                type: integer
              - name: limit
                in: query
                description: max number of messages to read (default = 10)
                required: false
                type: integer
              - name: timeout
                in: query
                description: timeout in ms
                required: false
                type: integer
            responses:
                200:
                    description: OK
                    content:
                        application/json:
                            schema:
                                {}
                400:
                    description: Bad Request
                403:
                    description: Forbidden
                500:
                    description: Internal Server Error
                504:
                    description: Gateway Timeout
        )___");

        node["get"]["responses"]["200"]["content"]["application/json"]["schema"] = TProtoToYaml::ProtoToYamlSchema<NKikimrViewer::TTopicDataResponse>();

        return node;
    }
};

} // namespace NKikimr::NViewer

