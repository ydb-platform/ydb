#pragma once

#include <ydb-cpp-sdk/client/driver/driver.h>

namespace Ydb {
    class VirtualTimestamp;
    namespace Scheme {
        class Entry;
        class ModifyPermissionsRequest;
        class Permissions;
    }
}

namespace NYdb::inline Dev {
namespace NScheme {

////////////////////////////////////////////////////////////////////////////////

struct TPermissions {
    TPermissions(const std::string& subject)
        : Subject(subject)
    {}
    TPermissions(const std::string& subject, const std::vector<std::string>& names)
        : Subject(subject)
        , PermissionNames(names)
    {}
    TPermissions(const ::Ydb::Scheme::Permissions& proto);

    std::string Subject;
    std::vector<std::string> PermissionNames;

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
    ResourcePool = 21,
};

struct TVirtualTimestamp {
    uint64_t PlanStep = 0;
    uint64_t TxId = 0;

    TVirtualTimestamp() = default;
    TVirtualTimestamp(uint64_t planStep, uint64_t txId);
    TVirtualTimestamp(const ::Ydb::VirtualTimestamp& proto);

    std::string ToString() const;
    void Out(IOutputStream& out) const;

    bool operator<(const TVirtualTimestamp& rhs) const;
    bool operator<=(const TVirtualTimestamp& rhs) const;
    bool operator>(const TVirtualTimestamp& rhs) const;
    bool operator>=(const TVirtualTimestamp& rhs) const;
    bool operator==(const TVirtualTimestamp& rhs) const;
    bool operator!=(const TVirtualTimestamp& rhs) const;
};

struct TSchemeEntry {
    std::string Name;
    std::string Owner;
    ESchemeEntryType Type;
    std::vector<TPermissions> EffectivePermissions;
    std::vector<TPermissions> Permissions;
    uint64_t SizeBytes = 0;
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

    TModifyPermissionsSettings& AddChangeOwner(const std::string& owner) {
        AddAction(EModifyPermissionsAction::Chown, TPermissions(owner));
        return *this;
    }

    TModifyPermissionsSettings& AddClearAcl() {
        ClearAcl_ = true;
        return *this;
    }

    TModifyPermissionsSettings& AddInterruptInheritance(bool value) {
        SetInterruptInheritance_ = true;
        InterruptInheritanceValue_ = value;
        return *this;
    }

    std::vector<std::pair<EModifyPermissionsAction, TPermissions>> Actions_;
    bool ClearAcl_ = false;
    bool SetInterruptInheritance_ = false;
    bool InterruptInheritanceValue_ = false;
    void AddAction(EModifyPermissionsAction action, const TPermissions& permissions) {
        Actions_.emplace_back(std::pair<EModifyPermissionsAction, TPermissions>{action, permissions});
    }

    TModifyPermissionsSettings() = default;
    explicit TModifyPermissionsSettings(const ::Ydb::Scheme::ModifyPermissionsRequest& request);
};

class TSchemeClient {
    class TImpl;

public:
    TSchemeClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    TAsyncStatus MakeDirectory(const std::string& path,
        const TMakeDirectorySettings& settings = TMakeDirectorySettings());

    TAsyncStatus RemoveDirectory(const std::string& path,
        const TRemoveDirectorySettings& settings = TRemoveDirectorySettings());

    TAsyncDescribePathResult DescribePath(const std::string& path,
        const TDescribePathSettings& settings = TDescribePathSettings());

    TAsyncListDirectoryResult ListDirectory(const std::string& path,
        const TListDirectorySettings& settings = TListDirectorySettings());

    TAsyncStatus ModifyPermissions(const std::string& path,
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
    TListDirectoryResult(TStatus&& status, const TSchemeEntry& self, std::vector<TSchemeEntry>&& children);
    const std::vector<TSchemeEntry>& GetChildren() const;

    void Out(IOutputStream& out) const;

private:
    std::vector<TSchemeEntry> Children_;
};

} // namespace NScheme
} // namespace NYdb
