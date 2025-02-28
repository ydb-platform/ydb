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


class TGetTopicData : public TViewerPipeClient {
    using TBase = TViewerPipeClient;
    using TThis = TGetTopicData;
    using TBase::ReplyAndPassAway;
    using TBase::GetHTTPBADREQUEST;

private:
    void HandleDescribe(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
    void SendPQReadRequest();
    void HandlePQResponse(TEvPersQueue::TEvResponse::TPtr& ev);
    void HandleDataUnpacked(TEvViewerTopicData::TEvTopicDataUnpacked::TPtr& ev);

    bool GetIntegerParam(const TString& name, i64& value);

    STATEFN(StateRequestedDescribe);


public:
    TGetTopicData(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
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
    ui32 Timeout = 0;

    TAutoPtr<TEvPersQueue::TEvResponse> ReadResponse;
    NJson::TJsonValue Response;

    static constexpr ui32 READ_TIMEOUT_MS = 1000;
    static constexpr ui32 MAX_MESSAGES_LIMIT = 1000;

public:
    static YAML::Node GetSwagger() {
        YAML::Node node = YAML::Load(R"___(
        get:
            tags:
              - viewer
            summary: ACL information
            description: Returns information about ACL of an object
            parameters:
              - name: database
                in: query
                description: database name
                type: string
                required: false
              - name: topic_path
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
                description: max number of messages to read
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
                                type: object
                                properties:
                                    StartOffset:
                                        type: integer
                                    EndOffset:
                                        type: integer
                                    Messages:
                                        type: array
                                        items:
                                            type: object
                                            properties:
                                                Offset:
                                                    type: integer
                                                CreateTimestamp:
                                                    type: integer
                                                WriteTimestamp:
                                                    type: integer
                                                Timestamp Diff:
                                                    type: integer
                                                Message:
                                                    type: string
                                                Size:
                                                    type: integer
                                                OriginalSize:
                                                    type: integer
                                                Codec:
                                                    type: integer
                                                ProducerId:
                                                    type: string
                                                SeqNo:
                                                    type: integer
                                                Metadata:
                                                    type: array
                                                    items:
                                                        type: object
                                                        properties:
                                                            Key:
                                                                type: string
                                                            Value:
                                                                type: string
                400:
                    description: Bad Request
                403:
                    description: Forbidden
                500:
                    description: Internal Server Error
                504:
                    description: Gateway Timeout
                )___");

        return node;
    }
};

} // namespace NKikimr::NViewer

