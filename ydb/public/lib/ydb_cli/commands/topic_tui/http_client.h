#pragma once

#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYdb::NConsoleClient {

// Message from topic
struct TTopicMessage {
    ui64 Offset = 0;
    ui64 SeqNo = 0;
    TInstant WriteTime;
    TInstant CreateTime;
    TString MessageGroupId;
    TString Data;
    ui64 UncompressedSize = 0;
};

// HTTP client for Viewer API
class TViewerHttpClient {
public:
    explicit TViewerHttpClient(const TString& endpoint);
    
    // Read messages from topic partition
    // GET /viewer/topic_data?path=...&partition=...&offset=...&limit=...
    TVector<TTopicMessage> ReadMessages(
        const TString& topicPath,
        ui32 partition,
        ui64 offset,
        ui32 limit = 10,
        TDuration timeout = TDuration::Seconds(5));
    
    // Write message to topic
    // POST /viewer/put_record
    bool WriteMessage(
        const TString& topicPath,
        const TString& data,
        const TString& messageGroupId = "",
        TMaybe<ui32> partition = Nothing());
    
    bool IsAvailable() const { return !Endpoint_.empty(); }
    
private:
    NJson::TJsonValue DoGet(const TString& path);
    NJson::TJsonValue DoPost(const TString& path, const TString& body);
    
private:
    TString Endpoint_;
    THolder<TKeepAliveHttpClient> HttpClient_;
};

} // namespace NYdb::NConsoleClient
