#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/operation/operation.h>

#include <optional>
#include <string>
#include <vector>

namespace Ydb::Udf {
    class ModuleInfo;
    class PlatformCompileStatus;
    class UploadModuleResult;
    class ListModulesResult;
    class DescribeModuleResult;
} // namespace Ydb::Udf

namespace NYdb::inline Dev::NUdf {

enum class EModuleType {
    Unspecified = 0,
    Module = 1,
    Library = 2,
};

enum class EModuleKind {
    Unspecified = 0,
    Wasm = 1,
    Native = 2,
};

enum class ECompileStatus {
    Unspecified = 0,
    Pending = 1,
    Compiling = 2,
    Ready = 3,
    Failed = 4,
};

enum class EWriteMode {
    Unspecified = 0,
    CreateOrReplace = 1,
    CreateOnly = 2,
    ReplaceOnly = 3,
};

struct TModuleInfo {
    std::string Name;
    EModuleType Type = EModuleType::Unspecified;
    EModuleKind Kind = EModuleKind::Unspecified;
    std::string Uid;
    std::string Md5;
    uint64_t Size = 0;
    uint64_t Version = 0;
    TInstant CreatedAt;
};

struct TPlatformCompileStatus {
    std::string CpuSpec;
    ECompileStatus Status = ECompileStatus::Unspecified;
    std::string CompileError;
};

struct TUploadModuleSettings: public TOperationRequestSettings<TUploadModuleSettings> {
    using TSelf = TUploadModuleSettings;

    FLUENT_SETTING(std::string, ManifestJson);
    FLUENT_SETTING_DEFAULT(EWriteMode, WriteMode, EWriteMode::Unspecified);
    FLUENT_SETTING(std::string, ExpectedUid);
    FLUENT_SETTING_OPTIONAL(uint64_t, Version);
    FLUENT_SETTING(std::string, ExpectedMd5);
    //! Client→server data chunk size. Does not have to match the store's
    //! on-disk chunk size; the server reassembles the body before splitting.
    FLUENT_SETTING_DEFAULT(size_t, ChunkSize, size_t(1) << 20);
};

struct TDeleteModuleSettings: public TOperationRequestSettings<TDeleteModuleSettings> {
    using TSelf = TDeleteModuleSettings;

    FLUENT_SETTING_DEFAULT(EModuleType, Type, EModuleType::Unspecified);
    FLUENT_SETTING_DEFAULT(EModuleKind, Kind, EModuleKind::Unspecified);
    FLUENT_SETTING(std::string, ExpectedUid);
};

struct TListModulesSettings: public TOperationRequestSettings<TListModulesSettings> {
    using TSelf = TListModulesSettings;

    FLUENT_SETTING_DEFAULT(EModuleType, TypeFilter, EModuleType::Unspecified);
    FLUENT_SETTING_DEFAULT(EModuleKind, KindFilter, EModuleKind::Unspecified);
    FLUENT_SETTING_OPTIONAL(uint32_t, PageSize);
    FLUENT_SETTING(std::string, PageToken);
};

struct TDescribeModuleSettings: public TOperationRequestSettings<TDescribeModuleSettings> {};

struct TUploadModuleResult: public TStatus {
    TUploadModuleResult(TStatus&& status, Ydb::Udf::UploadModuleResult&& proto);

    const std::string& GetName() const;
    const std::string& GetUid() const;
    const std::string& GetMd5() const;
    uint64_t GetSize() const;
    bool GetReplacedExisting() const;

private:
    std::string Name_;
    std::string Uid_;
    std::string Md5_;
    uint64_t Size_ = 0;
    bool ReplacedExisting_ = false;
};

struct TListModulesResult: public TStatus {
    TListModulesResult(TStatus&& status, Ydb::Udf::ListModulesResult&& proto);

    const std::vector<TModuleInfo>& GetModules() const;
    const std::string& GetNextPageToken() const;

private:
    std::vector<TModuleInfo> Modules_;
    std::string NextPageToken_;
};

struct TDescribeModuleResult: public TStatus {
    TDescribeModuleResult(TStatus&& status, Ydb::Udf::DescribeModuleResult&& proto);

    const TModuleInfo& GetModule() const;
    const std::string& GetManifestJson() const;
    const std::vector<TPlatformCompileStatus>& GetPlatforms() const;

private:
    TModuleInfo ModuleInfo_;
    std::string ManifestJson_;
    std::vector<TPlatformCompileStatus> Platforms_;
};

using TAsyncUploadModuleResult = NThreading::TFuture<TUploadModuleResult>;
using TAsyncListModulesResult = NThreading::TFuture<TListModulesResult>;
using TAsyncDescribeModuleResult = NThreading::TFuture<TDescribeModuleResult>;

class TUdfClient {
    class TImpl;

public:
    TUdfClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    //! Streams `body` as UploadModule chunks (metadata first, then data).
    TAsyncUploadModuleResult UploadModule(std::string body, const TUploadModuleSettings& settings = {});

    //! Opens a regular file and reads it incrementally. The file must remain unchanged
    //! until the future completes. Open/read errors are reported through the future.
    TAsyncUploadModuleResult UploadModuleFromFile(const std::string& path, const TUploadModuleSettings& settings = {});

    TAsyncStatus DeleteModule(const std::string& name, const TDeleteModuleSettings& settings = {});

    TAsyncListModulesResult ListModules(const TListModulesSettings& settings = {});

    TAsyncDescribeModuleResult DescribeModule(const std::string& name, const TDescribeModuleSettings& settings = {});

private:
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::inline Dev::NUdf
