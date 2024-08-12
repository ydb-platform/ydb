#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

namespace Ydb {
    class VirtualTimestamp;
    namespace Scheme {
        class Entry;
        class ModifyPermissionsRequest;
        class Permissions;
    }
}

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

    void SerializeTo(::Ydb::Scheme::Permissions& proto) const;
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
    ColumnStore = 12,
    ColumnTable = 13,
    Sequence = 15,
    Replication = 16,
    Topic = 17,
    ExternalTable = 18,
    ExternalDataSource = 19,
    View = 20,
    ResourcePool = 21
};

struct TVirtualTimestamp {
    ui64 PlanStep = 0;
    ui64 TxId = 0;

    TVirtualTimestamp() = default;
    TVirtualTimestamp(ui64 planStep, ui64 txId);
    TVirtualTimestamp(const ::Ydb::VirtualTimestamp& proto);

    TString ToString() const;
    void Out(IOutputStream& out) const;

    bool operator<(const TVirtualTimestamp& rhs) const;
    bool operator<=(const TVirtualTimestamp& rhs) const;
    bool operator>(const TVirtualTimestamp& rhs) const;
    bool operator>=(const TVirtualTimestamp& rhs) const;
    bool operator==(const TVirtualTimestamp& rhs) const;
    bool operator!=(const TVirtualTimestamp& rhs) const;
};

struct TSchemeEntry {
    TString Name;
    TString Owner;
    ESchemeEntryType Type;
    TVector<TPermissions> EffectivePermissions;
    TVector<TPermissions> Permissions;
    ui64 SizeBytes = 0;
    TVirtualTimestamp CreatedAt;

    TSchemeEntry() = default;
    TSchemeEntry(const ::Ydb::Scheme::Entry& proto);

    void Out(IOutputStream& out) const;

    // Fills ModifyPermissionsRequest proto from this entry
    void SerializeTo(::Ydb::Scheme::ModifyPermissionsRequest& request) const;
};

////////////////////////////////////////////////////////////////////////////////

class TDescribePathResult;
class TListDirectoryResult;

using TAsyncDescribePathResult = NThreading::TFuture<TDescribePathResult>;
using TAsyncListDirectoryResult = NThreading::TFuture<TListDirectoryResult>;

////////////////////////////////////////////////////////////////////////////////

struct TMakeDirectorySettings : public TOperationRequestSettings<TMakeDirectorySettings> {};

struct TRemoveDirectorySettings : public TOperationRequestSettings<TRemoveDirectorySettings> {
    FLUENT_SETTING_DEFAULT(bool, NotExistsIsOk, false);
};

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
    TDescribePathResult(TStatus&& status, const TSchemeEntry& entry);
    const TSchemeEntry& GetEntry() const;

    void Out(IOutputStream& out) const;

private:
    TSchemeEntry Entry_;
};

class TListDirectoryResult : public TDescribePathResult {
public:
    TListDirectoryResult(TStatus&& status, const TSchemeEntry& self, TVector<TSchemeEntry>&& children);
    const TVector<TSchemeEntry>& GetChildren() const;

    void Out(IOutputStream& out) const;

private:
    TVector<TSchemeEntry> Children_;
};

} // namespace NScheme
} // namespace NYdb
