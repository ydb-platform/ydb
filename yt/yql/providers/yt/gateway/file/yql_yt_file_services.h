#pragma once

#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/core/file_storage/file_storage.h>
#include <yql/essentials/core/file_storage/proto/file_storage.pb.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/ptr.h>
#include <util/system/spinlock.h>
#include <util/folder/dirut.h>

#include <vector>
#include <unordered_map>

namespace NYql::NFile {

class TYtFileServices: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TYtFileServices>;
    ~TYtFileServices();

    static TPtr Make(const NKikimr::NMiniKQL::IFunctionRegistry* registry, const THashMap<TString, TString>& mapping = {},
        TFileStoragePtr fileStorage = {}, const TString& tmpDir = {}, bool keepTempTables = false, const THashMap<TString, TString>& dirMapping = {})
    {
        return new TYtFileServices(registry, mapping, fileStorage, tmpDir.empty() ? GetSystemTempDir() : tmpDir, keepTempTables, dirMapping);
    }

    const NKikimr::NMiniKQL::IFunctionRegistry* GetFunctionRegistry() const {
        return FunctionRegistry_;
    }

    const TString& GetTmpDir() const {
        return TmpDir_;
    }

    THashMap<TString, TString>& GetTablesMapping() {
        return TablesMapping_;
    }

    bool GetKeepTempTables() const {
        return KeepTempTables_;
    }

    TString GetTmpTablePath(TStringBuf table);
    TString GetTablePath(TStringBuf cluster, TStringBuf table, bool isAnonimous);
    TString GetTableSnapshotPath(TStringBuf cluster, TStringBuf table, bool isAnonimous, ui32 epoch = 0);

    TString SnapshotTable(const TString& tablePath, TStringBuf cluster, TStringBuf table, ui32 epoch);

    void PushTableContent(const TString& path, const TString& content);
    std::vector<TString> GetTableContent(const TString& path);

    TFileStoragePtr GetFileStorage() const {
        return FileStorage_;
    }

private:
    TYtFileServices(
        const NKikimr::NMiniKQL::IFunctionRegistry* registry,
        const THashMap<TString, TString>& mapping,
        TFileStoragePtr fileStorage,
        const TString& tmpDir,
        bool keepTempTables,
        const THashMap<TString, TString>& dirMapping
    );

    TFileStoragePtr FileStorage_;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry_;
    THashMap<TString, TString> TablesMapping_; // [cluster].[name] -> [file path]
    THashMap<TString, TString> TablesDirMapping_; // [cluster] -> [dir path]
    TString TmpDir_;
    bool KeepTempTables_;

    std::unordered_multimap<TString, TString> Contents_; // path -> pickled content
    THashMap<std::pair<TString, ui32>, TString> Snapshots_;
    TAdaptiveLock Mutex_;
};

} // NYql::NFile
