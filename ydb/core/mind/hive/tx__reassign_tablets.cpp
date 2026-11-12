#include "tx__reassign_tablets.h"

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <ranges>

namespace NKikimr::NHive {

TTxReassignTablets::TTxReassignTablets(const TActorId& source, THive* hive, TString user)
    : TBase(hive)
    , TLoggedTransaction(std::move(user), hive)
    , Source(source)
{}

TTxReassignTablets::TTxReassignTablets(TEvReassignTablets::TEvCreateRequest::TPtr& ev, THive* hive)
    : TTxReassignTablets(ev->Sender, hive, ev->Get()->Record.GetUserSID())
{
    const auto& record = ev->Get()->Record;
    Cookie = ev->Cookie;
    GroupId = record.GetGroupId();
    StoragePool = record.GetStoragePool();
    Async = record.GetAsync();
    MaxInFlight = record.GetMaxInFlight();
    TabletPercent = record.GetPercent();
    if ((StoragePool.empty() && !GroupId) || MaxInFlight == 0 || TabletPercent > 100) {
        Error = "invalid reassign parameters";
    }
}

bool TTxReassignTablets::TTabletFilter::operator()(const TLeaderTabletInfo* tablet) const {
    if (auto* tabletId = std::get_if<TTabletId>(&TabletId); tabletId && *tabletId != tablet->Id) {
        return false;
    }
    return TabletType == TTabletTypes::TypeInvalid || TabletType == tablet->Type;
}

bool TTxReassignTablets::TTabletFilter::IsValidFilter(TString& error) const {
    if (std::holds_alternative<std::monostate>(TabletId)) {
        error = "must specify tablet";
        return false;
    }
    return true;
}

TInstant TTxReassignTablets::GetMaxTimestamp(const TReassignOperation& operation) const {
    TInstant max;
    const auto* tablet = Self->FindTablet(operation.TabletId);
    for (const auto& channel : tablet->TabletStorageInfo->Channels) {
        if (operation.TabletChannels.empty()
                || std::find(
                    operation.TabletChannels.begin(),
                    operation.TabletChannels.end(),
                    channel.Channel) != operation.TabletChannels.end()) {
            if (tablet->ChannelProfileNewGroup.test(channel.Channel)) {
                return TInstant::Max();
            }
            const auto* latest = channel.LatestEntry();
            if (latest != nullptr && latest->Timestamp > max) {
                max = latest->Timestamp;
            }
        }
    }
    return max;
}

bool TTxReassignTablets::Execute(TTransactionContext& txc, const TActorContext&) {
    if (Error) {
        return true;
    }
    if (!ForcedGroupIds.empty() && ForcedGroupIds.size() != TabletChannels.size()) {
        Error = "forcedGroup size should be equal to channel size";
        return true;
    }
    if (!ForcedGroupIds.empty() && Async) {
        Error = "cannot provide force groups in async mode";
        return true;
    }
    if (!TabletFilter.IsValidFilter(Error)) {
        return true;
    }
    auto tablets = Self->Tablets
        | std::views::transform([](auto&& p) -> TLeaderTabletInfo* { return &p.second; })
        | std::views::filter(TabletFilter);

    std::vector<TReassignOperation> operations;
    for (TLeaderTabletInfo* tablet : tablets) {
        TVector<ui32> channels;
        TVector<ui32> forcedGroupIds;
        bool skip = false;
        if (GroupId != 0 || StoragePool != "") {
            skip = true;
            for (const auto& channel : tablet->TabletStorageInfo->Channels) {
                if (StoragePool && channel.StoragePool != StoragePool) {
                    continue;
                }
                if (TabletChannels.empty() || Find(TabletChannels, channel.Channel) != TabletChannels.end()) {
                    const auto* latest = channel.LatestEntry();
                    if (latest != nullptr && (GroupId == 0 || latest->GroupID == GroupId)) {
                        skip = false;
                        channels.push_back(channel.Channel);
                        if (!ForcedGroupIds.empty()) {
                            auto itTabletChannel = Find(TabletChannels, channel.Channel);
                            forcedGroupIds.push_back(ForcedGroupIds[std::distance(TabletChannels.begin(), itTabletChannel)]);
                        }
                    }
                }
            }
        } else {
            channels = TabletChannels;
            forcedGroupIds = ForcedGroupIds;
        }
        if (skip) {
            continue;
        }
        operations.emplace_back(tablet->Id, channels, forcedGroupIds, Async);
    }

    TStringBuilder description;
    if (TabletPercent != 100) {
        description << TabletPercent << "% of ";
    }
    TabletFilter.ToString(description);
    if (StoragePool) {
        description << StoragePool << ", ";
    }
    description << (GroupId ? ToString(GroupId) : "all groups");
    if (TabletPercent != 100) {
        std::sort(operations.begin(), operations.end(), [this](const auto& lhs, const auto& rhs) -> bool {
            return GetMaxTimestamp(lhs) < GetMaxTimestamp(rhs);
        });
        auto it = operations.begin() + operations.size() * TabletPercent / 100;
        operations.erase(it, operations.end());
    }

    if (!CheckTabletsCount(operations.size())) {
        return true;
    }

    NIceDb::TNiceDb db(txc.DB);
    NJson::TJsonValue jsonOperation;
    jsonOperation["Reassign"] = description;
    jsonOperation["TabletsCount"] = operations.size();
    jsonOperation["MaxInFlight"] = MaxInFlight;
    jsonOperation["Async"] = Async;
    WriteOperation(db, jsonOperation, LogDescription);

    SelectedTablets = operations.size();
    const TActorId completionSource = Callback ? Source : TActorId();
    ReassignId = Self->StartReassignActor(std::move(operations), completionSource, MaxInFlight, description, std::move(Callback));
    return true;
}

void TTxReassignTablets::Complete(const TActorContext& ctx) {
    auto reply = std::make_unique<TEvReassignTablets::TEvCreateResponse>();
    reply->Record.SetStatus(Error ? Ydb::StatusIds::PRECONDITION_FAILED : Ydb::StatusIds::SUCCESS);
    reply->Record.SetSelectedTablets(SelectedTablets);
    if (Error) {
        NYql::IssueToMessage(NYql::TIssue(Error), reply->Record.AddIssues());
    } else {
        reply->Record.SetReassignId(ReassignId);
        reply->Record.SetHiveGeneration(Self->HiveGeneration);
    }
    ctx.Send(Source, reply.release(), 0, Cookie);
}

void THive::Handle(TEvReassignTablets::TEvCreateRequest::TPtr& ev) {
    Execute(new TTxReassignTablets(ev, this));
}

} // namespace NKikimr::NHive
