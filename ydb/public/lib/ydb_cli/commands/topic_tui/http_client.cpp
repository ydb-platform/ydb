#include "http_client.h"

#include <library/cpp/json/json_writer.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb::NConsoleClient {

TViewerHttpClient::TViewerHttpClient(const TString& endpoint)
    : Endpoint_(endpoint)
{
    if (!Endpoint_.empty()) {
        // Parse endpoint to create HTTP client
        TStringBuf scheme, host;
        TStringBuf(Endpoint_).TrySplit("://", scheme, host);
        if (host.empty()) {
            host = Endpoint_;
        }
        
        TStringBuf hostPart, portPart;
        if (host.TrySplit(':', hostPart, portPart)) {
            ui16 port = FromString<ui16>(portPart);
            HttpClient_ = MakeHolder<TKeepAliveHttpClient>(TString(hostPart), port);
        } else {
            HttpClient_ = MakeHolder<TKeepAliveHttpClient>(TString(host), 8765);
        }
    }
}

TVector<TTopicMessage> TViewerHttpClient::ReadMessages(
    const TString& topicPath,
    ui32 partition,
    ui64 offset,
    ui32 limit,
    TDuration /* timeout */)
{
    TVector<TTopicMessage> result;
    
    if (!HttpClient_) {
        return result;
    }
    
    TStringBuilder path;
    path << "/viewer/topic_data?"
         << "path=" << topicPath
         << "&partition=" << partition
         << "&offset=" << offset
         << "&limit=" << limit;
    
    try {
        auto json = DoGet(path);
        
        if (json.Has("Messages") && json["Messages"].IsArray()) {
            for (const auto& msgJson : json["Messages"].GetArray()) {
                TTopicMessage msg;
                
                if (msgJson.Has("Offset")) {
                    msg.Offset = msgJson["Offset"].GetUInteger();
                }
                if (msgJson.Has("SeqNo")) {
                    msg.SeqNo = msgJson["SeqNo"].GetUInteger();
                }
                if (msgJson.Has("WriteTimestamp")) {
                    msg.WriteTime = TInstant::MicroSeconds(msgJson["WriteTimestamp"].GetUInteger());
                }
                if (msgJson.Has("CreateTimestamp")) {
                    msg.CreateTime = TInstant::MicroSeconds(msgJson["CreateTimestamp"].GetUInteger());
                }
                if (msgJson.Has("MessageGroupId")) {
                    msg.MessageGroupId = msgJson["MessageGroupId"].GetString();
                }
                if (msgJson.Has("Data")) {
                    msg.Data = msgJson["Data"].GetString();
                }
                if (msgJson.Has("UncompressedSize")) {
                    msg.UncompressedSize = msgJson["UncompressedSize"].GetUInteger();
                }
                
                result.push_back(std::move(msg));
            }
        }
    } catch (const std::exception&) {
        // Silently ignore HTTP errors for now
    }
    
    return result;
}

bool TViewerHttpClient::WriteMessage(
    const TString& topicPath,
    const TString& data,
    const TString& messageGroupId,
    TMaybe<ui32> partition)
{
    if (!HttpClient_) {
        return false;
    }
    
    NJson::TJsonValue body;
    body["path"] = topicPath;
    body["data"] = data;
    if (!messageGroupId.empty()) {
        body["message_group_id"] = messageGroupId;
    }
    if (partition.Defined()) {
        body["partition"] = *partition;
    }
    
    try {
        TString bodyStr = NJson::WriteJson(body, false);
        
        auto response = DoPost("/viewer/put_record", bodyStr);
        return response.Has("Status") && response["Status"].GetString() == "OK";
    } catch (const std::exception&) {
        return false;
    }
}

NJson::TJsonValue TViewerHttpClient::DoGet(const TString& path) {
    TStringStream response;
    TKeepAliveHttpClient::THeaders headers;
    
    HttpClient_->DoGet(path, &response, headers);
    
    NJson::TJsonValue result;
    NJson::ReadJsonTree(response.Str(), &result, true);
    return result;
}

NJson::TJsonValue TViewerHttpClient::DoPost(const TString& path, const TString& body) {
    TStringStream response;
    TKeepAliveHttpClient::THeaders headers;
    headers["Content-Type"] = "application/json";
    
    HttpClient_->DoPost(path, body, &response, headers);
    
    NJson::TJsonValue result;
    NJson::ReadJsonTree(response.Str(), &result, true);
    return result;
}

} // namespace NYdb::NConsoleClient
