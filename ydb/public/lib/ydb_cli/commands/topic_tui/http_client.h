#pragma once

#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYdb::NConsoleClient {

// Message from topic - all metadata from Viewer API
struct TTopicMessage {
    ui64 Offset = 0;
    ui64 SeqNo = 0;
    TInstant WriteTime;
    TInstant CreateTime;
    i64 TimestampDiff = 0;  // WriteTimestamp - CreateTimestamp (ms)
    TString ProducerId;      // Producer ID (same as MessageGroupId)
    TString Data;
    ui64 StorageSize = 0;
    ui64 OriginalSize = 0;
    ui32 Codec = 0;          // Compression codec
};

// Tablet info from Viewer API /viewer/json/tabletinfo
struct TTabletInfo {
    ui64 TabletId = 0;
    TString Type;           // "PersQueue", "PersQueueReadBalancer"
    TString State;          // "Active", "Dead", etc.
    ui32 NodeId = 0;
    TString NodeFQDN;       // Node host name (if available)
    ui64 Generation = 0;
    TInstant ChangeTime;    // For uptime calculation
    bool Leader = false;
    TString Overall;        // "Green", "Yellow", "Red"
    ui64 HiveId = 0;
};

// Topic describe result from Viewer API
struct TTopicDescribeResult {
    // Basic info from PathDescription.Self
    TString Path;
    TString Owner;
    TString PathType;
    TString PathState;
    ui64 PathId = 0;
    ui64 SchemeshardId = 0;
    ui64 CreateTxId = 0;
    ui64 ParentPathId = 0;
    TString Name;
    bool ChildrenExist = false;
    TInstant CreateTime;
    
    // Tablets from TabletStateInfo
    TVector<TTabletInfo> Tablets;
    
    // PersQueue-specific from PersQueueGroup.PQTabletConfig
    ui32 PartitionsCount = 0;
    ui64 BalancerTabletId = 0;
    ui64 AlterVersion = 0;
    ui32 NextPartitionId = 0;
    ui32 PartitionPerTablet = 0;
    ui32 TotalGroupCount = 0;
    ui64 RetentionSeconds = 0;
    ui64 RetentionBytes = 0;
    ui64 WriteSpeedBytesPerSec = 0;
    ui64 BurstBytes = 0;
    ui64 MaxCountInPartition = 0;
    ui64 SourceIdLifetimeSeconds = 0;
    ui64 SourceIdMaxCounts = 0;
    TVector<TString> SupportedCodecs;
    TString MeteringMode;  // "REQUEST_UNITS" or "RESERVED_CAPACITY"
    TString FormatVersion;
    TString YdbDatabasePath;
    bool RequireAuthRead = false;
    bool RequireAuthWrite = false;
    bool HasRequireAuthRead = false;
    bool HasRequireAuthWrite = false;
    
    struct TPQPartitionInfo {
        ui32 PartitionId = 0;
        ui64 TabletId = 0;
        TString Status;
    };
    TVector<TPQPartitionInfo> Partitions;
    
    struct TConsumerConfigInfo {
        TString Name;
        TString ServiceType;
        TString Type;
        ui64 ReadFromTimestampMs = 0;
        ui32 Version = 0;
        ui32 FormatVersion = 0;
    };
    TVector<TConsumerConfigInfo> Consumers;
    
    // Error if any
    TString Error;
    
    // Raw JSON for debug
    TString RawJson;
};

// HTTP client for Viewer API
class TViewerHttpClient {
public:
    explicit TViewerHttpClient(const TString& endpoint);
    
    // Read messages from topic partition
    // GET /viewer/json/topic_data?path=...&partition=...&offset=...&limit=...
    TVector<TTopicMessage> ReadMessages(
        const TString& topicPath,
        ui32 partition,
        ui64 offset,
        ui32 limit = 10,
        ui64 messageSizeLimit = 4096,
        TDuration timeout = TDuration::Seconds(5));
    
    // Get topic description with tablets
    // GET /viewer/json/describe?path=...&tablets=true
    TTopicDescribeResult GetTopicDescribe(
        const TString& topicPath,
        bool includeTablets = true,
        bool includeEnums = false,
        bool includePartitionStats = false,
        bool includeSubs = false,
        TDuration timeout = TDuration::Seconds(5));
    
    // Get tablet info for a topic
    // GET /viewer/json/tabletinfo?path=...&enums=true
    TVector<TTabletInfo> GetTabletInfo(
        const TString& topicPath,
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
    TString PathPrefix_;  // e.g., "/node/50004" if specified in endpoint URL
    mutable TString CachedRedirectPrefix_;  // Learned from first 307 redirect
    THolder<TKeepAliveHttpClient> HttpClient_;
};

} // namespace NYdb::NConsoleClient
