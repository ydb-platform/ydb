#pragma once

#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/core/file_storage/file_storage.h>
#include <ydb/library/yql/core/file_storage/proto/file_storage.pb.h>

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
        return FunctionRegistry;
    }

    const TString& GetTmpDir() const {
        return TmpDir;
    }

    THashMap<TString, TString>& GetTablesMapping() {
        return TablesMapping;
    }

    bool GetKeepTempTables() const {
        return KeepTempTables;
    }

    TString GetTablePath(TStringBuf cluster, TStringBuf table, bool isTemp, bool noLocks = false);

    void LockPath(const TString& path, const TString& fullTableName);

    void PushTableContent(const TString& path, const TString& content);
    std::vector<TString> GetTableContent(const TString& path);

    TFileStoragePtr GetFileStorage() const {
        return FileStorage;
    }

private:
    TYtFileServices(
        const NKikimr::NMiniKQL::IFunctionRegistry* registry,
        const THashMap<TString, TString>& mapping,
        TFileStoragePtr fileStorage,
        const TString& tmpDir,
        bool keepTempTables,
        const THashMap<TString, TString>& dirMapping
    )
        : FunctionRegistry(registry)
        , TablesMapping(mapping)
        , TablesDirMapping(dirMapping)
        , TmpDir(tmpDir)
        , KeepTempTables(keepTempTables)
    {
        FileStorage = fileStorage;
        if (!FileStorage) {
            TFileStorageConfig params;
            FileStorage = CreateFileStorage(params);
        }
    }

    TFileStoragePtr FileStorage;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry;
    THashMap<TString, TString> TablesMapping; // [cluster].[name] -> [file path]
    THashMap<TString, TString> TablesDirMapping; // [cluster] -> [dir path]
    TString TmpDir;
    bool KeepTempTables;

    std::unordered_multimap<TString, TString> Contents; // path -> pickled content
    THashMap<TString, TString> Locks;
    TAdaptiveLock Mutex;
};

} // NYql::NFile
