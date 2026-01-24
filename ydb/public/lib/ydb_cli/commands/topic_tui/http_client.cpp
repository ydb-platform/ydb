#include "http_client.h"

#include <library/cpp/json/json_reader.h>
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
    ui64 messageSizeLimit,
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
         << "&message_size_limit=" << messageSizeLimit;
    
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
    bool includeEnums,
    bool includePartitionStats,
    bool includeSubs,
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
    if (includeEnums) {
        path << "&enums=true";
    }
    if (includePartitionStats) {
        path << "&partition_stats=true";
    }
    if (!includeSubs) {
        path << "&subs=0";
    }
    
    auto json = DoGet(path);
    
    // Store raw JSON for debug
    result.RawJson = NJson::WriteJson(json, true);
    result.Path = topicPath;

    auto getUint64 = [](const NJson::TJsonValue& value, ui64& out) -> bool {
        try {
            if (value.IsUInteger()) {
                out = value.GetUInteger();
                return true;
            }
            if (value.IsInteger()) {
                out = static_cast<ui64>(value.GetInteger());
                return true;
            }
            if (value.IsString()) {
                out = FromString<ui64>(value.GetString());
                return true;
            }
        } catch (const std::exception&) {
        }
        return false;
    };
    
    auto getUint32 = [&getUint64](const NJson::TJsonValue& value, ui32& out) -> bool {
        ui64 tmp = 0;
        if (!getUint64(value, tmp)) {
            return false;
        }
        out = static_cast<ui32>(tmp);
        return true;
    };
    
    auto getString = [](const NJson::TJsonValue& value) -> TString {
        if (value.IsString()) {
            return value.GetString();
        }
        if (value.IsInteger()) {
            return ToString(value.GetInteger());
        }
        if (value.IsUInteger()) {
            return ToString(value.GetUInteger());
        }
        return {};
    };
    
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
            if (self.Has("PathState")) {
                result.PathState = self["PathState"].GetString();
            }
            if (self.Has("PathId")) {
                getUint64(self["PathId"], result.PathId);
            }
            if (self.Has("SchemeshardId")) {
                getUint64(self["SchemeshardId"], result.SchemeshardId);
            }
            if (self.Has("CreateTxId")) {
                getUint64(self["CreateTxId"], result.CreateTxId);
            }
            if (self.Has("ParentPathId")) {
                getUint64(self["ParentPathId"], result.ParentPathId);
            }
            if (self.Has("Name")) {
                result.Name = self["Name"].GetString();
            }
            if (self.Has("ChildrenExist")) {
                result.ChildrenExist = self["ChildrenExist"].GetBoolean();
            }
            if (self.Has("CreateStep")) {
                // CreateStep is in microseconds
                ui64 createStep = 0;
                if (getUint64(self["CreateStep"], createStep)) {
                    result.CreateTime = TInstant::MicroSeconds(createStep);
                }
            }
        }
        
        // Parse PersQueueGroup for config
        if (pathDesc.Has("PersQueueGroup")) {
            const auto& pqGroup = pathDesc["PersQueueGroup"];
            if (pqGroup.Has("Partitions") && pqGroup["Partitions"].IsArray()) {
                result.PartitionsCount = pqGroup["Partitions"].GetArray().size();
                result.Partitions.clear();
                for (const auto& partitionJson : pqGroup["Partitions"].GetArray()) {
                    TTopicDescribeResult::TPQPartitionInfo partition;
                    if (partitionJson.Has("PartitionId")) {
                        getUint32(partitionJson["PartitionId"], partition.PartitionId);
                    }
                    if (partitionJson.Has("TabletId")) {
                        getUint64(partitionJson["TabletId"], partition.TabletId);
                    }
                    if (partitionJson.Has("Status")) {
                        partition.Status = partitionJson["Status"].GetString();
                    }
                    result.Partitions.push_back(std::move(partition));
                }
            }
            if (pqGroup.Has("BalancerTabletID")) {
                getUint64(pqGroup["BalancerTabletID"], result.BalancerTabletId);
            }
            if (pqGroup.Has("AlterVersion")) {
                getUint64(pqGroup["AlterVersion"], result.AlterVersion);
            }
            if (pqGroup.Has("NextPartitionId")) {
                getUint32(pqGroup["NextPartitionId"], result.NextPartitionId);
            }
            if (pqGroup.Has("PartitionPerTablet")) {
                getUint32(pqGroup["PartitionPerTablet"], result.PartitionPerTablet);
            }
            if (pqGroup.Has("TotalGroupCount")) {
                getUint32(pqGroup["TotalGroupCount"], result.TotalGroupCount);
            }
            
            // Parse PQTabletConfig for detailed settings
            if (pqGroup.Has("PQTabletConfig")) {
                const auto& config = pqGroup["PQTabletConfig"];
                
                if (config.Has("RequireAuthRead")) {
                    result.RequireAuthRead = config["RequireAuthRead"].GetBoolean();
                    result.HasRequireAuthRead = true;
                }
                if (config.Has("RequireAuthWrite")) {
                    result.RequireAuthWrite = config["RequireAuthWrite"].GetBoolean();
                    result.HasRequireAuthWrite = true;
                }
                if (config.Has("FormatVersion")) {
                    result.FormatVersion = getString(config["FormatVersion"]);
                }
                if (config.Has("YdbDatabasePath")) {
                    result.YdbDatabasePath = config["YdbDatabasePath"].GetString();
                }
                
                if (config.Has("PartitionConfig")) {
                    const auto& partConfig = config["PartitionConfig"];
                    if (partConfig.Has("LifetimeSeconds")) {
                        getUint64(partConfig["LifetimeSeconds"], result.RetentionSeconds);
                    }
                    if (partConfig.Has("StorageLimitBytes")) {
                        getUint64(partConfig["StorageLimitBytes"], result.RetentionBytes);
                    }
                    if (partConfig.Has("WriteSpeedInBytesPerSecond")) {
                        getUint64(partConfig["WriteSpeedInBytesPerSecond"], result.WriteSpeedBytesPerSec);
                    }
                    if (partConfig.Has("BurstSize")) {
                        getUint64(partConfig["BurstSize"], result.BurstBytes);
                    }
                    if (partConfig.Has("MaxCountInPartition")) {
                        getUint64(partConfig["MaxCountInPartition"], result.MaxCountInPartition);
                    }
                    if (partConfig.Has("SourceIdLifetimeSeconds")) {
                        getUint64(partConfig["SourceIdLifetimeSeconds"], result.SourceIdLifetimeSeconds);
                    }
                    if (partConfig.Has("SourceIdMaxCounts")) {
                        getUint64(partConfig["SourceIdMaxCounts"], result.SourceIdMaxCounts);
                    }
                }
                
                if (config.Has("Codecs")) {
                    const auto& codecs = config["Codecs"];
                    if (codecs.Has("Codecs") && codecs["Codecs"].IsArray()) {
                        for (const auto& codecName : codecs["Codecs"].GetArray()) {
                            result.SupportedCodecs.push_back(codecName.GetString());
                        }
                    } else if (codecs.Has("Ids") && codecs["Ids"].IsArray()) {
                        for (const auto& codecId : codecs["Ids"].GetArray()) {
                            ui32 id = 0;
                            if (!getUint32(codecId, id)) {
                                result.SupportedCodecs.push_back("unknown");
                                continue;
                            }
                            switch (id) {
                                case 0: result.SupportedCodecs.push_back("raw"); break;
                                case 1: result.SupportedCodecs.push_back("gzip"); break;
                                case 2: result.SupportedCodecs.push_back("lzop"); break;
                                case 3: result.SupportedCodecs.push_back("zstd"); break;
                                default: result.SupportedCodecs.push_back(ToString(id)); break;
                            }
                        }
                    }
                }
                
                if (config.Has("MeteringMode")) {
                    result.MeteringMode = config["MeteringMode"].GetString();
                }
                
                if (config.Has("Consumers") && config["Consumers"].IsArray()) {
                    result.Consumers.clear();
                    for (const auto& consumerJson : config["Consumers"].GetArray()) {
                        TTopicDescribeResult::TConsumerConfigInfo consumer;
                        if (consumerJson.Has("Name")) {
                            consumer.Name = consumerJson["Name"].GetString();
                        }
                        if (consumerJson.Has("ServiceType")) {
                            consumer.ServiceType = consumerJson["ServiceType"].GetString();
                        }
                        if (consumerJson.Has("Type")) {
                            consumer.Type = consumerJson["Type"].GetString();
                        }
                        if (consumerJson.Has("ReadFromTimestampsMs")) {
                            getUint64(consumerJson["ReadFromTimestampsMs"], consumer.ReadFromTimestampMs);
                        }
                        if (consumerJson.Has("Version")) {
                            getUint32(consumerJson["Version"], consumer.Version);
                        }
                        if (consumerJson.Has("FormatVersion")) {
                            getUint32(consumerJson["FormatVersion"], consumer.FormatVersion);
                        }
                        result.Consumers.push_back(std::move(consumer));
                    }
                }
            }
        }
        
        // Parse TabletStateInfo for tablets
        if (pathDesc.Has("TabletStateInfo") && pathDesc["TabletStateInfo"].IsArray()) {
            for (const auto& tabletJson : pathDesc["TabletStateInfo"].GetArray()) {
                TTabletInfo tablet;
                
                if (tabletJson.Has("TabletId")) {
                    getUint64(tabletJson["TabletId"], tablet.TabletId);
                }
                if (tabletJson.Has("Type")) {
                    tablet.Type = tabletJson["Type"].GetString();
                }
                if (tabletJson.Has("State")) {
                    tablet.State = tabletJson["State"].GetString();
                }
                if (tabletJson.Has("NodeId")) {
                    getUint32(tabletJson["NodeId"], tablet.NodeId);
                }
                if (tabletJson.Has("FQDN")) {
                    tablet.NodeFQDN = tabletJson["FQDN"].GetString();
                }
                if (tabletJson.Has("Generation")) {
                    getUint64(tabletJson["Generation"], tablet.Generation);
                }
                if (tabletJson.Has("ChangeTime")) {
                    // ChangeTime is in milliseconds
                    ui64 changeTime = 0;
                    if (getUint64(tabletJson["ChangeTime"], changeTime)) {
                        tablet.ChangeTime = TInstant::MilliSeconds(changeTime);
                    }
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
        
        // Handle empty JSON object/array or whitespace-only responses
        TString trimmed = responseStr;
        while (!trimmed.empty() && (trimmed.front() == ' ' || trimmed.front() == '\n' || trimmed.front() == '\t')) {
            trimmed = trimmed.substr(1);
        }
        if (trimmed.empty() || trimmed == "{}" || trimmed == "[]") {
            // Return empty JSON object for empty responses
            return NJson::TJsonValue(NJson::JSON_MAP);
        }
        
        NJson::TJsonValue result;
        NJson::TJsonReaderConfig config;
        config.DontValidateUtf8 = true;  // Handle invalid UTF-8 from Viewer API
        if (!NJson::ReadJsonTree(responseStr, &config, &result, false)) {
            // If parsing fails, return empty object instead of throwing
            // This handles edge cases like empty partition responses
            return NJson::TJsonValue(NJson::JSON_MAP);
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
