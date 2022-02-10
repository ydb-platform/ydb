#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

namespace NYdb {
namespace NScheme {

////////////////////////////////////////////////////////////////////////////////

struct TPermissions {
    TPermissions(const TString& subject)
        : Subject(subject)
    {}
    TPermissions(const TString& subject, const TVector<TString>& names)
        : Subject(subject)
        , PermissionNames(names)
    {}
    TString Subject;
    TVector<TString> PermissionNames;
};

enum class ESchemeEntryType : i32 {
    Unknown = -1,
    Directory = 1,
    Table = 2,
    PqGroup = 3,
    SubDomain = 4,
    RtmrVolume = 5,
    BlockStoreVolume = 6,
    CoordinationNode = 7,
    Sequence = 15,
    Replication = 16, 
};

struct TSchemeEntry {
    TString Name;
    TString Owner;
    ESchemeEntryType Type;
    TVector<TPermissions> EffectivePermissions;
    TVector<TPermissions> Permissions;
    ui64 SizeBytes = 0; 
};

////////////////////////////////////////////////////////////////////////////////

class TDescribePathResult;
class TListDirectoryResult;

using TAsyncDescribePathResult = NThreading::TFuture<TDescribePathResult>;
using TAsyncListDirectoryResult = NThreading::TFuture<TListDirectoryResult>;

////////////////////////////////////////////////////////////////////////////////

struct TMakeDirectorySettings : public TOperationRequestSettings<TMakeDirectorySettings> {};

struct TRemoveDirectorySettings : public TOperationRequestSettings<TRemoveDirectorySettings> {};

struct TDescribePathSettings : public TOperationRequestSettings<TDescribePathSettings> {};

struct TListDirectorySettings : public TOperationRequestSettings<TListDirectorySettings> {};

enum class EModifyPermissionsAction {
    Grant,
    Revoke,
    Set,
    Chown
};

struct TModifyPermissionsSettings : public TOperationRequestSettings<TModifyPermissionsSettings> {
    TModifyPermissionsSettings& AddGrantPermissions(const TPermissions& permissions) {
        AddAction(EModifyPermissionsAction::Grant, permissions);
        return *this;
    }
    TModifyPermissionsSettings& AddRevokePermissions(const TPermissions& permissions) {
        AddAction(EModifyPermissionsAction::Revoke, permissions);
        return *this;
    }
    TModifyPermissionsSettings& AddSetPermissions(const TPermissions& permissions) {
        AddAction(EModifyPermissionsAction::Set, permissions);
        return *this;
    }
    TModifyPermissionsSettings& AddChangeOwner(const TString& owner) {
        AddAction(EModifyPermissionsAction::Chown, TPermissions(owner));
        return *this;
    }
    TModifyPermissionsSettings& AddClearAcl() {
        ClearAcl_ = true;
        return *this;
    }

    TVector<std::pair<EModifyPermissionsAction, TPermissions>> Actions_;
    bool ClearAcl_ = false;
    void AddAction(EModifyPermissionsAction action, const TPermissions& permissions) {
        Actions_.emplace_back(std::pair<EModifyPermissionsAction, TPermissions>{action, permissions});
    }
};

class TSchemeClient {
    class TImpl;

public:
    TSchemeClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    TAsyncStatus MakeDirectory(const TString& path,
        const TMakeDirectorySettings& settings = TMakeDirectorySettings());

    TAsyncStatus RemoveDirectory(const TString& path,
        const TRemoveDirectorySettings& settings = TRemoveDirectorySettings());

    TAsyncDescribePathResult DescribePath(const TString& path,
        const TDescribePathSettings& settings = TDescribePathSettings());

    TAsyncListDirectoryResult ListDirectory(const TString& path,
        const TListDirectorySettings& settings = TListDirectorySettings());

    TAsyncStatus ModifyPermissions(const TString& path,
        const TModifyPermissionsSettings& data);

private:
    std::shared_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

class TDescribePathResult : public TStatus {
public:
    TDescribePathResult(TSchemeEntry&& entry, TStatus&& status);
    TSchemeEntry GetEntry() const;

private:
    TSchemeEntry Entry_;
};

class TListDirectoryResult : public TDescribePathResult {
public:
    TListDirectoryResult(TVector<TSchemeEntry>&& children, TSchemeEntry&& self, TStatus&& status);
    TVector<TSchemeEntry> GetChildren() const;

private:
    TVector<TSchemeEntry> Children_;
};

} // namespace NScheme
} // namespace NYdb
