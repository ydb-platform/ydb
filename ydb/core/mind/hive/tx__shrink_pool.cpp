#include "hive_impl.h"
#include "hive_log.h"

#include <ranges>

namespace NKikimr::NHive {

class TTxShrinkPool : public TTransactionBase<THive> {
    const TActorId Source;
    const TString StoragePool;
    const ui64 NewSize;
    const ui64 Cookie;
    const ui64 Version;
    TSideEffects SideEffects;

public:
    TTxShrinkPool(const TActorId& source, const TString& storagePool, ui64 newSize, ui64 cookie, ui64 version, THive* hive)
        : TBase(hive)
        , Source(source)
        , StoragePool(storagePool)
        , NewSize(newSize)
        , Cookie(cookie)
	, Version(version)
    {
    }

    TTxType GetTxType() const override { return NHive::TXTYPE_SHRINK_POOL; }

    struct TGroupCmp {
        static auto GetKey(const TStorageGroupInfo* group) {
            return group->AcquiredResources.Size;
        }

        bool operator()(const TStorageGroupInfo* lhs, const TStorageGroupInfo* rhs) {
            return GetKey(lhs) < GetKey(rhs);
        }
    };

    void ReplyWithError(const TString& error) {
        auto reply = std::make_unique<TEvHive::TEvShrinkStoragePoolReply>();
        reply->Record.SetStatus(NKikimrProto::ERROR);
        reply->Record.SetError(error);
        reply->Record.SetStoragePool(StoragePool);
        reply->Record.SetVersion(Version);
        SideEffects.Send(Source, reply.release(), 0, Cookie);
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        BLOG_D("THive::TTxShrinkPool::Execute");
        SideEffects.Reset(Self->SelfId());
        NIceDb::TNiceDb db(txc.DB);
        auto* storagePool = Self->FindStoragePool(StoragePool);
        if (storagePool == nullptr) {
            ReplyWithError("unknown storage pool");
            return true;
        }
        if (NewSize > storagePool->Groups.size()) {
            ReplyWithError("cannot increase number of groups with ShrinkPool operation");
            return true;
        }
        if (NewSize == 0) {
            ReplyWithError("cannot remove all groups");
            return true;
        }

        i64 groupsToRemove = std::ssize(storagePool->Groups) - static_cast<i64>(NewSize);
        // groupsToRemove < InactiveGroups - we are cancelling some group removals
        std::ranges::sort(storagePool->InactiveGroups, TGroupCmp(), [storagePool](auto groupId) { return &storagePool->GetStorageGroup(groupId); });
        while (groupsToRemove < std::ssize(storagePool->InactiveGroups)) {
            auto groupId = storagePool->InactiveGroups.back();
            BLOG_D("THive::TTxShrinkPool::Execute marking group " << groupId << "as active");
            auto& groupInfo = storagePool->GetStorageGroup(groupId);
            groupInfo.Status = EGroupState::Active;
            db.Table<Schema::Group>().Key(groupId).Delete();
            storagePool->InactiveGroups.pop_back();
        }
        // groupsToRemove > InactiveGroups - we need to remove some more groups
        if (groupsToRemove > std::ssize(storagePool->InactiveGroups)) {
            std::vector<TStorageGroupInfo*> groups;
            groups.reserve(storagePool->Groups.size());
            for (auto& [_, groupInfo] : storagePool->Groups) {
                groups.push_back(&groupInfo);
            }
            std::ranges::sort(groups, TGroupCmp());
            auto newGroupsToRemove = groups
                | std::views::filter([&] (auto* group) { return group->IsActive(); })
                | std::views::take(groupsToRemove - std::ssize(storagePool->InactiveGroups));
            storagePool->InactiveGroups.reserve(static_cast<size_t>(groupsToRemove));
            for (auto* group : newGroupsToRemove) {
                BLOG_D("THive::TTxShrinkPool::Execute marking group " << group->Id << "as inactive");
                group->Status = EGroupState::Inactive;
                db.Table<Schema::Group>().Key(group->Id).Update(
                    NIceDb::TUpdate<Schema::Group::StoragePool>(StoragePool),
                    NIceDb::TUpdate<Schema::Group::Status>(EGroupState::Inactive)
                );
                storagePool->InactiveGroups.push_back(group->Id);
           }
        }
        auto reply = std::make_unique<TEvHive::TEvShrinkStoragePoolReply>();
        reply->Record.SetStatus(NKikimrProto::OK);
        reply->Record.MutableGroupsToRemove()->Assign(storagePool->InactiveGroups.begin(), storagePool->InactiveGroups.end());
        reply->Record.SetStoragePool(StoragePool);
        reply->Record.SetVersion(Version);
        SideEffects.Send(Source, reply.release(), 0, Cookie);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SideEffects.Complete(ctx);
    }
};

ITransaction* THive::CreateShrinkPool(TEvHive::TEvShrinkStoragePool::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    return new TTxShrinkPool(ev->Sender, record.GetStoragePool(), record.GetNewSize(), ev->Cookie, record.GetVersion(), this);
}

class TTxShrinkPoolReply : public TTransactionBase<THive> {
    TEvHive::TEvShrinkStoragePoolReply::TPtr Event;

    TTxType GetTxType() const override { return NHive::TXTYPE_SHRINK_POOL; }

public:
    TTxShrinkPoolReply(TEvHive::TEvShrinkStoragePoolReply::TPtr ev, THive* hive)
        : TBase(hive)
        , Event(ev)
    {
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        if (Event->Get()->Record.GetStatus() != NKikimrProto::OK) {
            return true;
        }
        NIceDb::TNiceDb db(txc.DB);
        const auto& poolName = Event->Get()->Record.GetStoragePool();
        auto& storagePool = Self->GetStoragePool(poolName);
        if (storagePool.ConsoleVersion > Event->Get()->Record.GetVersion()) {
            return true;
        }
        const auto& groupsToRemove = Event->Get()->Record.GetGroupsToRemove();
        std::unordered_set<TStorageGroupId> inactiveGroups(groupsToRemove.begin(), groupsToRemove.end());
        for (auto groupId : inactiveGroups) {
            auto& groupInfo = storagePool.GetStorageGroup(groupId);
            groupInfo.Status = EGroupState::Inactive;
            db.Table<Schema::Group>().Key(groupId).Update(
                NIceDb::TUpdate<Schema::Group::StoragePool>(poolName),
                NIceDb::TUpdate<Schema::Group::Status>(EGroupState::Inactive)
            );
        }
        for (auto groupId : storagePool.InactiveGroups) {
            if (!inactiveGroups.contains(groupId)) {
                auto& groupInfo = storagePool.GetStorageGroup(groupId);
                groupInfo.Status = EGroupState::Active;
                db.Table<Schema::Group>().Key(groupId).Delete();
            }
        }
        storagePool.InactiveGroups.assign(inactiveGroups.begin(), inactiveGroups.end());
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(Self->ShrinkPoolInitiator, Event->Release(), 0, Event->Cookie);
    }
};

ITransaction* THive::CreateShrinkPoolReply(TEvHive::TEvShrinkStoragePoolReply::TPtr ev) {
    return new TTxShrinkPoolReply(std::move(ev), this);
}

} // NKikimr::NHive
