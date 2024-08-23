#pragma once
#include "common_helper.h"
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::Tests::NCS {

class THelperSchemaless : public NCommon::THelper {
private:
    using TBase = NCommon::THelper;
public:
    static constexpr const char * ROOT_PATH = "/Root";

    using TBase::TBase;
    void CreateTestOlapStore(TActorId sender, TString scheme);
    void CreateTestOlapTable(TActorId sender, TString storeOrDirName, TString scheme);
    void SendDataViaActorSystem(TString testTable, ui64 pathIdBegin, ui64 tsBegin, size_t rowCount, const ui32 tsStepUs = 1) const;
    void SendDataViaActorSystem(TString testTable, std::shared_ptr<arrow::RecordBatch> batch, const Ydb::StatusIds_StatusCode& expectedStatus =  Ydb::StatusIds::SUCCESS) const;

    virtual std::shared_ptr<arrow::RecordBatch> TestArrowBatch(ui64 pathIdBegin, ui64 tsBegin, size_t rowCount, const ui32 tsStepUs = 1) const = 0;
};

class THelper: public THelperSchemaless {
private:
    using TBase = THelperSchemaless;

    std::shared_ptr<arrow::Schema> GetArrowSchema() const;
    YDB_FLAG_ACCESSOR(WithJsonDocument, false);
    YDB_ACCESSOR(TString, OptionalStorageId, "__MEMORY");
protected:
    TString ShardingMethod = "HASH_FUNCTION_CONSISTENCY_64";
private:
    bool WithSomeNulls_ = false;
protected:
    void CreateOlapTableWithStore(TString tableName = "olapTable", TString storeName = "olapStore",
        ui32 storeShardsCount = 4, ui32 tableShardsCount = 3);
public:
    using TBase::TBase;

    THelper& SetShardingMethod(const TString& value) {
        Y_ABORT_UNLESS(value == "HASH_FUNCTION_CLOUD_LOGS" || value == "HASH_FUNCTION_MODULO_N" || value == "HASH_FUNCTION_CONSISTENCY_64");
        ShardingMethod = value;
        return *this;
    }

    static constexpr const char * PROTO_SCHEMA = R"(
        Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
        Columns { Name: "resource_id" Type: "Utf8" DataAccessorConstructor{ ClassName: "SPARSED" }}
        Columns { Name: "uid" Type: "Utf8" }
        Columns { Name: "level" Type: "Int32" DataAccessorConstructor{ ClassName: "SPARSED" }}
        Columns { Name: "message" Type: "Utf8" }
        KeyColumnNames: "timestamp"
        Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
    )";

    void WithSomeNulls() {
        WithSomeNulls_ = true;
    };

    virtual std::vector<TString> GetShardingColumns() const {
        return {"timestamp", "uid"};
    }
    virtual TString GetTestTableSchema() const;

    virtual std::shared_ptr<arrow::RecordBatch> TestArrowBatch(ui64 pathIdBegin, ui64 tsBegin, size_t rowCount, const ui32 tsStepUs = 1) const override;
};

class TCickBenchHelper: public THelperSchemaless {
private:
    using TBase = THelperSchemaless;

    std::shared_ptr<arrow::Schema> GetArrowSchema() const;

public:
    using TBase::TBase;

    static constexpr const char * PROTO_SCHEMA = R"(
        Columns { Name: "WatchID" Type: "Int64" NotNull: true }
        Columns { Name: "JavaEnable" Type: "Int16" NotNull: true }
        Columns { Name: "Title" Type: "Utf8" NotNull: true }
        Columns { Name: "GoodEvent" Type: "Int16" NotNull: true }
        Columns { Name: "EventTime" Type: "Timestamp" NotNull: true }
        Columns { Name: "EventDate" Type: "Timestamp" NotNull: true }
        Columns { Name: "CounterID" Type: "Int32" NotNull: true }
        Columns { Name: "ClientIP" Type: "Int32" NotNull: true }
        Columns { Name: "RegionID" Type: "Int32" NotNull: true }
        Columns { Name: "UserID" Type: "Int64" NotNull: true }
        Columns { Name: "CounterClass" Type: "Int16" NotNull: true }
        Columns { Name: "OS" Type: "Int16" NotNull: true }
        Columns { Name: "UserAgent" Type: "Int16" NotNull: true }
        Columns { Name: "URL" Type: "Utf8" NotNull: true }
        Columns { Name: "Referer" Type: "Utf8" NotNull: true }
        Columns { Name: "IsRefresh" Type: "Int16" NotNull: true }
        Columns { Name: "RefererCategoryID" Type: "Int16" NotNull: true }
        Columns { Name: "RefererRegionID" Type: "Int32" NotNull: true }
        Columns { Name: "URLCategoryID" Type: "Int16" NotNull: true }
        Columns { Name: "URLRegionID" Type: "Int32" NotNull: true }
        Columns { Name: "ResolutionWidth" Type: "Int16" NotNull: true }
        Columns { Name: "ResolutionHeight" Type: "Int16" NotNull: true }
        Columns { Name: "ResolutionDepth" Type: "Int16" NotNull: true }
        Columns { Name: "FlashMajor" Type: "Int16" NotNull: true }
        Columns { Name: "FlashMinor" Type: "Int16" NotNull: true }
        Columns { Name: "FlashMinor2" Type: "Utf8" NotNull: true }
        Columns { Name: "NetMajor" Type: "Int16" NotNull: true }
        Columns { Name: "NetMinor" Type: "Int16" NotNull: true }
        Columns { Name: "UserAgentMajor" Type: "Int16" NotNull: true }
        Columns { Name: "UserAgentMinor" Type: "String" NotNull: true }
        Columns { Name: "CookieEnable" Type: "Int16" NotNull: true }
        Columns { Name: "JavascriptEnable" Type: "Int16" NotNull: true }
        Columns { Name: "IsMobile" Type: "Int16" NotNull: true }
        Columns { Name: "MobilePhone" Type: "Int16" NotNull: true }
        Columns { Name: "MobilePhoneModel" Type: "Utf8" NotNull: true }
        Columns { Name: "Params" Type: "Utf8" NotNull: true }
        Columns { Name: "IPNetworkID" Type: "Int32" NotNull: true }
        Columns { Name: "TraficSourceID" Type: "Int16" NotNull: true }
        Columns { Name: "SearchEngineID" Type: "Int16" NotNull: true }
        Columns { Name: "SearchPhrase" Type: "Utf8" NotNull: true }
        Columns { Name: "AdvEngineID" Type: "Int16" NotNull: true }
        Columns { Name: "IsArtifical" Type: "Int16" NotNull: true }
        Columns { Name: "WindowClientWidth" Type: "Int16" NotNull: true }
        Columns { Name: "WindowClientHeight" Type: "Int16" NotNull: true }
        Columns { Name: "ClientTimeZone" Type: "Int16" NotNull: true }
        Columns { Name: "ClientEventTime" Type: "Timestamp" NotNull: true }
        Columns { Name: "SilverlightVersion1" Type: "Int16" NotNull: true }
        Columns { Name: "SilverlightVersion2" Type: "Int16" NotNull: true }
        Columns { Name: "SilverlightVersion3" Type: "Int32" NotNull: true }
        Columns { Name: "SilverlightVersion4" Type: "Int16" NotNull: true }
        Columns { Name: "PageCharset" Type: "Utf8" NotNull: true }
        Columns { Name: "CodeVersion" Type: "Int32" NotNull: true }
        Columns { Name: "IsLink" Type: "Int16" NotNull: true }
        Columns { Name: "IsDownload" Type: "Int16" NotNull: true }
        Columns { Name: "IsNotBounce" Type: "Int16" NotNull: true }
        Columns { Name: "FUniqID" Type: "Int64" NotNull: true }
        Columns { Name: "OriginalURL" Type: "Utf8" NotNull: true }
        Columns { Name: "HID" Type: "Int32" NotNull: true }
        Columns { Name: "IsOldCounter" Type: "Int16" NotNull: true }
        Columns { Name: "IsEvent" Type: "Int16" NotNull: true }
        Columns { Name: "IsParameter" Type: "Int16" NotNull: true }
        Columns { Name: "DontCountHits" Type: "Int16" NotNull: true }
        Columns { Name: "WithHash" Type: "Int16" NotNull: true }
        Columns { Name: "HitColor" Type: "String" NotNull: true }
        Columns { Name: "LocalEventTime" Type: "Timestamp" NotNull: true }
        Columns { Name: "Age" Type: "Int16" NotNull: true }
        Columns { Name: "Sex" Type: "Int16" NotNull: true }
        Columns { Name: "Income" Type: "Int16" NotNull: true }
        Columns { Name: "Interests" Type: "Int16" NotNull: true }
        Columns { Name: "Robotness" Type: "Int16" NotNull: true }
        Columns { Name: "RemoteIP" Type: "Int32" NotNull: true }
        Columns { Name: "WindowName" Type: "Int32" NotNull: true }
        Columns { Name: "OpenerName" Type: "Int32" NotNull: true }
        Columns { Name: "HistoryLength" Type: "Int16" NotNull: true }
        Columns { Name: "BrowserLanguage" Type: "Utf8" NotNull: true }
        Columns { Name: "BrowserCountry" Type: "Utf8" NotNull: true }
        Columns { Name: "SocialNetwork" Type: "Utf8" NotNull: true }
        Columns { Name: "SocialAction" Type: "Utf8" NotNull: true }
        Columns { Name: "HTTPError" Type: "Int16" NotNull: true }
        Columns { Name: "SendTiming" Type: "Int32" NotNull: true }
        Columns { Name: "DNSTiming" Type: "Int32" NotNull: true }
        Columns { Name: "ConnectTiming" Type: "Int32" NotNull: true }
        Columns { Name: "ResponseStartTiming" Type: "Int32" NotNull: true }
        Columns { Name: "ResponseEndTiming" Type: "Int32" NotNull: true }
        Columns { Name: "FetchTiming" Type: "Int32" NotNull: true }
        Columns { Name: "SocialSourceNetworkID" Type: "Int16" NotNull: true }
        Columns { Name: "SocialSourcePage" Type: "Utf8" NotNull: true }
        Columns { Name: "ParamPrice" Type: "Int64" NotNull: true }
        Columns { Name: "ParamOrderID" Type: "Utf8" NotNull: true }
        Columns { Name: "ParamCurrency" Type: "Utf8" NotNull: true }
        Columns { Name: "ParamCurrencyID" Type: "Int16" NotNull: true }
        Columns { Name: "OpenstatServiceName" Type: "Utf8" NotNull: true }
        Columns { Name: "OpenstatCampaignID" Type: "Utf8" NotNull: true }
        Columns { Name: "OpenstatAdID" Type: "Utf8" NotNull: true }
        Columns { Name: "OpenstatSourceID" Type: "Utf8" NotNull: true }
        Columns { Name: "UTMSource" Type: "Utf8" NotNull: true }
        Columns { Name: "UTMMedium" Type: "Utf8" NotNull: true }
        Columns { Name: "UTMCampaign" Type: "Utf8" NotNull: true }
        Columns { Name: "UTMContent" Type: "Utf8" NotNull: true }
        Columns { Name: "UTMTerm" Type: "Utf8" NotNull: true }
        Columns { Name: "FromTag" Type: "Utf8" NotNull: true }
        Columns { Name: "HasGCLID" Type: "Int16" NotNull: true }
        Columns { Name: "RefererHash" Type: "Int64" NotNull: true }
        Columns { Name: "URLHash" Type: "Int64" NotNull: true }
        Columns { Name: "CLID" Type: "Int32" NotNull: true }
        KeyColumnNames: ["EventTime", "EventDate", "CounterID", "UserID", "WatchID"]
    )";

    std::shared_ptr<arrow::RecordBatch> TestArrowBatch(ui64, ui64 begin, size_t rowCount, const ui32 tsStepUs = 1) const override;
};

class TTableWithNullsHelper: public THelperSchemaless {
private:
    using TBase = THelperSchemaless;

    std::shared_ptr<arrow::Schema> GetArrowSchema() const;
public:
    using TBase::TBase;

    static constexpr const char * PROTO_SCHEMA = R"(
        Columns { Name: "id" Type: "Int32" NotNull: true }
        Columns { Name: "resource_id" Type: "Utf8" }
        Columns { Name: "level" Type: "Int32" }
        Columns { Name: "binary_str" Type: "String" }
        Columns { Name: "jsonval" Type: "Json" }
        Columns { Name: "jsondoc" Type: "JsonDocument" }
        KeyColumnNames: "id"
    )";

    std::shared_ptr<arrow::RecordBatch> TestArrowBatch(ui64, ui64, size_t rowCount = 10, const ui32 tsStepUs = 1) const override;
    std::shared_ptr<arrow::RecordBatch> TestArrowBatch() const;
};

}
