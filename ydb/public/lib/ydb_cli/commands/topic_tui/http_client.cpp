#include "http_client.h"

#include <library/cpp/json/json_writer.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb::NConsoleClient {

TViewerHttpClient::TViewerHttpClient(const TString& endpoint)
    : Endpoint_(endpoint)
{
    if (!Endpoint_.empty()) {
        // Parse endpoint: scheme://host:port[/path]
        TStringBuf remaining = Endpoint_;
        TStringBuf scheme;
        
        // Remove scheme if present
        if (remaining.Contains("://")) {
            remaining.TrySplit("://", scheme, remaining);
        }
        
        // Split host:port from path
        TStringBuf hostPort;
        size_t slashPos = remaining.find('/');
        if (slashPos != TStringBuf::npos) {
            hostPort = remaining.SubStr(0, slashPos);
            PathPrefix_ = TString(remaining.SubStr(slashPos));
        } else {
            hostPort = remaining;
        }
        
        // Parse host and port
        TStringBuf hostPart, portPart;
        if (hostPort.TrySplit(':', hostPart, portPart)) {
            ui16 port = FromString<ui16>(portPart);
            HttpClient_ = MakeHolder<TKeepAliveHttpClient>(TString(hostPart), port);
        } else {
            HttpClient_ = MakeHolder<TKeepAliveHttpClient>(TString(hostPort), 8765);
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
    
    // Extract database from topic path (format: /Root/db/topicname -> /Root/db)
    TString database;
    size_t lastSlash = topicPath.rfind('/');
    if (lastSlash != TString::npos && lastSlash > 0) {
        database = topicPath.substr(0, lastSlash);
    }
    
    TStringBuilder path;
    // Use PathPrefix_ (e.g., /node/50004) if specified in endpoint URL
    path << PathPrefix_
         << "/viewer/json/topic_data?"
         << "path=" << topicPath
         << "&database=" << database
         << "&partition=" << partition
         << "&offset=" << offset
         << "&limit=" << limit
         << "&message_size_limit=1000";
    
    auto json = DoGet(path);
    
    // Parse the actual Viewer response format
    if (json.Has("Messages") && json["Messages"].IsArray()) {
        for (const auto& msgJson : json["Messages"].GetArray()) {
            TTopicMessage msg;
            
            if (msgJson.Has("Offset")) {
                msg.Offset = msgJson["Offset"].GetUInteger();
            }
            if (msgJson.Has("SeqNo")) {
                msg.SeqNo = msgJson["SeqNo"].GetUInteger();
            }
            // Timestamps are in MILLISECONDS in actual API
            if (msgJson.Has("WriteTimestamp")) {
                msg.WriteTime = TInstant::MilliSeconds(msgJson["WriteTimestamp"].GetUInteger());
            }
            if (msgJson.Has("CreateTimestamp")) {
                msg.CreateTime = TInstant::MilliSeconds(msgJson["CreateTimestamp"].GetUInteger());
            }
            if (msgJson.Has("TimestampDiff")) {
                msg.TimestampDiff = msgJson["TimestampDiff"].GetInteger();
            }
            if (msgJson.Has("ProducerId")) {
                msg.ProducerId = msgJson["ProducerId"].GetString();
            }
            if (msgJson.Has("Codec")) {
                msg.Codec = msgJson["Codec"].GetUInteger();
            }
            if (msgJson.Has("StorageSize")) {
                msg.StorageSize = msgJson["StorageSize"].GetUInteger();
            }
            if (msgJson.Has("OriginalSize")) {
                msg.OriginalSize = msgJson["OriginalSize"].GetUInteger();
            }
            // Message body is in "Message" field (base64 encoded)
            if (msgJson.Has("Message")) {
                TString encoded = msgJson["Message"].GetString();
                try {
                    msg.Data = Base64Decode(encoded);
                } catch (...) {
                    msg.Data = encoded;  // If decode fails, show raw
                }
            }
            
            result.push_back(std::move(msg));
        }
    }
    // Let exceptions propagate - they'll be shown in the UI
    
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

TTopicDescribeResult TViewerHttpClient::GetTopicDescribe(
    const TString& topicPath,
    bool includeTablets,
    TDuration /* timeout */)
{
    TTopicDescribeResult result;
    
    if (!HttpClient_) {
        return result;
    }
    
    // Extract database from topic path (format: /Root/db/topicname -> /Root/db)
    TString database;
    size_t lastSlash = topicPath.rfind('/');
    if (lastSlash != TString::npos && lastSlash > 0) {
        database = topicPath.substr(0, lastSlash);
    }
    
    TStringBuilder path;
    path << PathPrefix_
         << "/viewer/json/describe?"
         << "database=" << database
         << "&path=" << topicPath;
    if (includeTablets) {
        path << "&tablets=true";
    }
    
    auto json = DoGet(path);
    
    // Store raw JSON for debug
    result.RawJson = NJson::WriteJson(json, true);
    result.Path = topicPath;
    
    // Parse PathDescription.Self for basic info
    if (json.Has("PathDescription")) {
        const auto& pathDesc = json["PathDescription"];
        
        if (pathDesc.Has("Self")) {
            const auto& self = pathDesc["Self"];
            if (self.Has("Owner")) {
                result.Owner = self["Owner"].GetString();
            }
            if (self.Has("PathType")) {
                result.PathType = self["PathType"].GetString();
            }
            if (self.Has("PathId")) {
                result.PathId = self["PathId"].GetUInteger();
            }
            if (self.Has("SchemeshardId")) {
                result.SchemeshardId = self["SchemeshardId"].GetUInteger();
            }
            if (self.Has("CreateStep")) {
                // CreateStep is in microseconds
                result.CreateTime = TInstant::MicroSeconds(self["CreateStep"].GetUInteger());
            }
        }
        
        // Parse PersQueueGroup for config
        if (pathDesc.Has("PersQueueGroup")) {
            const auto& pqGroup = pathDesc["PersQueueGroup"];
            if (pqGroup.Has("Partitions") && pqGroup["Partitions"].IsArray()) {
                result.PartitionsCount = pqGroup["Partitions"].GetArray().size();
            }
            
            // Parse PQTabletConfig for detailed settings
            if (pqGroup.Has("PQTabletConfig")) {
                const auto& config = pqGroup["PQTabletConfig"];
                
                if (config.Has("PartitionConfig")) {
                    const auto& partConfig = config["PartitionConfig"];
                    if (partConfig.Has("LifetimeSeconds")) {
                        result.RetentionSeconds = partConfig["LifetimeSeconds"].GetUInteger();
                    }
                    if (partConfig.Has("StorageLimitBytes")) {
                        result.RetentionBytes = partConfig["StorageLimitBytes"].GetUInteger();
                    }
                    if (partConfig.Has("WriteSpeedInBytesPerSecond")) {
                        result.WriteSpeedBytesPerSec = partConfig["WriteSpeedInBytesPerSecond"].GetUInteger();
                    }
                    if (partConfig.Has("BurstSize")) {
                        result.BurstBytes = partConfig["BurstSize"].GetUInteger();
                    }
                }
                
                if (config.Has("Codecs") && config["Codecs"].Has("Ids") && config["Codecs"]["Ids"].IsArray()) {
                    for (const auto& codecId : config["Codecs"]["Ids"].GetArray()) {
                        ui32 id = codecId.GetUInteger();
                        switch (id) {
                            case 0: result.SupportedCodecs.push_back("raw"); break;
                            case 1: result.SupportedCodecs.push_back("gzip"); break;
                            case 2: result.SupportedCodecs.push_back("lzop"); break;
                            case 3: result.SupportedCodecs.push_back("zstd"); break;
                            default: result.SupportedCodecs.push_back(ToString(id)); break;
                        }
                    }
                }
                
                if (config.Has("MeteringMode")) {
                    result.MeteringMode = config["MeteringMode"].GetString();
                }
            }
        }
        
        // Parse TabletStateInfo for tablets
        if (pathDesc.Has("TabletStateInfo") && pathDesc["TabletStateInfo"].IsArray()) {
            for (const auto& tabletJson : pathDesc["TabletStateInfo"].GetArray()) {
                TTabletInfo tablet;
                
                if (tabletJson.Has("TabletId")) {
                    tablet.TabletId = tabletJson["TabletId"].GetUInteger();
                }
                if (tabletJson.Has("Type")) {
                    tablet.Type = tabletJson["Type"].GetString();
                }
                if (tabletJson.Has("State")) {
                    tablet.State = tabletJson["State"].GetString();
                }
                if (tabletJson.Has("NodeId")) {
                    tablet.NodeId = tabletJson["NodeId"].GetUInteger();
                }
                if (tabletJson.Has("FQDN")) {
                    tablet.NodeFQDN = tabletJson["FQDN"].GetString();
                }
                if (tabletJson.Has("Generation")) {
                    tablet.Generation = tabletJson["Generation"].GetUInteger();
                }
                if (tabletJson.Has("ChangeTime")) {
                    // ChangeTime is in milliseconds
                    tablet.ChangeTime = TInstant::MilliSeconds(tabletJson["ChangeTime"].GetUInteger());
                }
                
                result.Tablets.push_back(std::move(tablet));
            }
        }
    }
    
    return result;
}

TVector<TTabletInfo> TViewerHttpClient::GetTabletInfo(
    const TString& topicPath,
    TDuration /* timeout */)
{
    TVector<TTabletInfo> result;
    
    if (!HttpClient_) {
        return result;
    }
    
    // Extract database from topic path
    TString database;
    size_t lastSlash = topicPath.rfind('/');
    if (lastSlash != TString::npos && lastSlash > 0) {
        database = topicPath.substr(0, lastSlash);
    }
    
    TStringBuilder path;
    path << PathPrefix_
         << "/viewer/json/tabletinfo?"
         << "database=" << database
         << "&path=" << topicPath
         << "&enums=true";
    
    auto json = DoGet(path);
    
    // Parse TabletStateInfo array
    if (json.Has("TabletStateInfo") && json["TabletStateInfo"].IsArray()) {
        for (const auto& tabletJson : json["TabletStateInfo"].GetArray()) {
            TTabletInfo tablet;
            
            if (tabletJson.Has("TabletId")) {
                // TabletId can be string or number in JSON
                if (tabletJson["TabletId"].IsString()) {
                    tablet.TabletId = FromString<ui64>(tabletJson["TabletId"].GetString());
                } else {
                    tablet.TabletId = tabletJson["TabletId"].GetUInteger();
                }
            }
            if (tabletJson.Has("Type")) {
                tablet.Type = tabletJson["Type"].GetString();
            }
            if (tabletJson.Has("State")) {
                tablet.State = tabletJson["State"].GetString();
            }
            if (tabletJson.Has("NodeId")) {
                tablet.NodeId = tabletJson["NodeId"].GetUInteger();
            }
            if (tabletJson.Has("Generation")) {
                tablet.Generation = tabletJson["Generation"].GetUInteger();
            }
            if (tabletJson.Has("ChangeTime")) {
                // ChangeTime can be string or number
                ui64 changeTimeMs = 0;
                if (tabletJson["ChangeTime"].IsString()) {
                    changeTimeMs = FromString<ui64>(tabletJson["ChangeTime"].GetString());
                } else {
                    changeTimeMs = tabletJson["ChangeTime"].GetUInteger();
                }
                tablet.ChangeTime = TInstant::MilliSeconds(changeTimeMs);
            }
            if (tabletJson.Has("Leader")) {
                tablet.Leader = tabletJson["Leader"].GetBoolean();
            }
            if (tabletJson.Has("Overall")) {
                tablet.Overall = tabletJson["Overall"].GetString();
            }
            if (tabletJson.Has("HiveId")) {
                // HiveId can be string or number
                if (tabletJson["HiveId"].IsString()) {
                    tablet.HiveId = FromString<ui64>(tabletJson["HiveId"].GetString());
                } else {
                    tablet.HiveId = tabletJson["HiveId"].GetUInteger();
                }
            }
            
            result.push_back(std::move(tablet));
        }
    }
    
    return result;
}

NJson::TJsonValue TViewerHttpClient::DoGet(const TString& path) {
    TString fullUrl = Endpoint_ + path;
    
    // Use cached redirect prefix if we learned it from a previous redirect
    TString currentPath = CachedRedirectPrefix_ + path;
    
    // Follow up to 3 redirects
    for (int redirects = 0; redirects < 3; ++redirects) {
        TStringStream response;
        TKeepAliveHttpClient::THeaders headers;
        THttpHeaders responseHeaders;
        
        auto httpCode = HttpClient_->DoGet(currentPath, &response, headers, &responseHeaders);
        
        // Check for redirects (301, 302, 307, 308)
        if (httpCode >= 300 && httpCode < 400) {
            // Find Location header
            for (const auto& header : responseHeaders) {
                if (header.Name() == "Location") {
                    TString location = header.Value();
                    // Extract /node/XXXX prefix from redirect and cache it
                    // Redirect location: /node/50004/viewer/json/topic_data?...
                    // We want to cache: /node/50004
                    size_t viewerPos = location.find("/viewer/");
                    if (viewerPos != TString::npos && viewerPos > 0) {
                        CachedRedirectPrefix_ = location.substr(0, viewerPos);
                    }
                    currentPath = location;
                    break;
                }
            }
            continue;  // Follow redirect
        }
        
        TString responseStr = response.Str();
        if (responseStr.empty()) {
            throw std::runtime_error(TStringBuilder() << "Empty response (HTTP " << httpCode << ") from: " << fullUrl);
        }
        
        NJson::TJsonValue result;
        if (!NJson::ReadJsonTree(responseStr, &result, true)) {
            throw std::runtime_error(TStringBuilder() << "Invalid JSON from: " << fullUrl);
        }
        return result;
    }
    
    throw std::runtime_error(TStringBuilder() << "Too many redirects from: " << fullUrl);
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
