#include "change_exchange.h"
#include "change_exchange_impl.h"
#include "datashard_impl.h"

#include <ydb/core/protos/services.pb.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>

namespace NKikimr {
namespace NDataShard {

class TChangeSender: public TActor<TChangeSender> {
    using ESenderType = TEvChangeExchange::ESenderType;
    using TEnqueuedRecord = TEvChangeExchange::TEvEnqueueRecords::TRecordInfo;

    struct TSender {
        TTableId UserTableId;
        ESenderType Type;
        TActorId ActorId;
    };

    bool IsActive() const {
        return CurrentStateFunc() == static_cast<TReceiveFunc>(&TThis::StateActive);
    }

    TStringBuf GetLogPrefix() const {
        if (!LogPrefix) {
            LogPrefix = TStringBuilder()
                << "[ChangeSender]"
                << "[" << DataShard.TabletId << ":" << DataShard.Generation << "]"
                << SelfId() /* contains brackets */
                << (IsActive() ? "" : "[Inactive]") << " ";
        }

        return LogPrefix.GetRef();
    }

    TSender& AddChangeSender(const TPathId& pathId, const TTableId& userTableId, ESenderType type) {
        Y_VERIFY_DEBUG(!Senders.contains(pathId));

        auto& sender = Senders[pathId];
        sender.UserTableId = userTableId;
        sender.Type = type;

        return sender;
    }

    TActorId RegisterChangeSender(const TPathId& pathId, const TTableId& userTableId, ESenderType type) const {
        switch (type) {
        case ESenderType::AsyncIndex:
            return Register(CreateAsyncIndexChangeSender(DataShard, userTableId, pathId));
        case ESenderType::CdcStream:
            return Register(CreateCdcStreamChangeSender(DataShard, pathId));
        }
    }

    void RegisterChangeSender(const TPathId& pathId, TSender& sender) const {
        Y_VERIFY_DEBUG(!sender.ActorId);
        sender.ActorId = RegisterChangeSender(pathId, sender.UserTableId, sender.Type);
    }

    void Handle(TEvChangeExchange::TEvEnqueueRecords::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        auto& records = ev->Get()->Records;

        if (!IsActive()) {
            std::move(records.begin(), records.end(), std::back_inserter(Enqueued));
        } else {
            Handle(std::move(records));
        }
    }

    void Handle(TVector<TEnqueuedRecord>&& enqueued) {
        THashMap<TActorId, TVector<TEnqueuedRecord>> forward;
        TVector<ui64> remove;

        for (auto& record : enqueued) {
            auto it = Senders.find(record.PathId);
            if (it != Senders.end()) {
                forward[it->second.ActorId].push_back(std::move(record));
            } else {
                remove.push_back(record.Order);
            }
        }

        for (auto& [to, records] : forward) {
            Send(to, new TEvChangeExchange::TEvEnqueueRecords(std::move(records)));
        }

        if (remove) {
            Send(DataShard.ActorId, new TEvChangeExchange::TEvRemoveRecords(std::move(remove)));
        }
    }

    void Handle(TEvChangeExchange::TEvAddSender::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        const auto& msg = *ev->Get();

        auto it = Senders.find(msg.PathId);
        if (it != Senders.end()) {
            Y_VERIFY(it->second.UserTableId == msg.UserTableId);
            Y_VERIFY(it->second.Type == msg.Type);
            LOG_W("Trying to add duplicate sender"
                << ": userTableId# " << msg.UserTableId
                << ", type# " << msg.Type
                << ", pathId# " << msg.PathId);
            return;
        }

        LOG_N("Add sender"
            << ": userTableId# " << msg.UserTableId
            << ", type# " << msg.Type
            << ", pathId# " << msg.PathId);

        auto& sender = AddChangeSender(msg.PathId, msg.UserTableId, msg.Type);
        if (IsActive()) {
            RegisterChangeSender(msg.PathId, sender);
        }
    }

    void Handle(TEvChangeExchange::TEvRemoveSender::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        const auto& pathId = ev->Get()->PathId;

        auto it = Senders.find(pathId);
        if (it == Senders.end()) {
            LOG_W("Trying to remove unknown sender"
                << ": pathId# " << pathId);
            return;
        }

        LOG_N("Remove sender"
            << ": type# " << it->second.Type
            << ", pathId# " << it->first);

        if (const auto& actorId = it->second.ActorId) {
            Send(actorId, new TEvChangeExchange::TEvRemoveSender(pathId));
        }

        Senders.erase(it);
    }

    void Handle(TEvChangeExchange::TEvActivateSender::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        Become(&TThis::StateActive);
        LogPrefix.Clear();

        for (auto& [pathId, sender] : Senders) {
            RegisterChangeSender(pathId, sender);
        }

        if (Enqueued) {
            Handle(std::move(Enqueued));
        }
    }

    void PassAway() override {
        for (const auto& [_, sender] : Senders) {
            if (!sender.ActorId) {
                continue;
            }

            Send(sender.ActorId, new TEvents::TEvPoisonPill());
        }

        TActor::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CHANGE_SENDER_ACTOR;
    }

    explicit TChangeSender(const TDataShard* self)
        : TActor(&TThis::StateInactive)
        , DataShard{self->TabletID(), self->Generation(), self->SelfId()}
    {
        for (const auto& [tableId, tableInfo] : self->GetUserTables()) {
            const auto fullTableId = TTableId(self->GetPathOwnerId(), tableId);

            for (const auto& [indexPathId, indexInfo] : tableInfo->Indexes) {
                if (indexInfo.Type != TUserTable::TTableIndex::EIndexType::EIndexTypeGlobalAsync) {
                    continue;
                }

                AddChangeSender(indexPathId, fullTableId, ESenderType::AsyncIndex);
            }

            for (const auto& [streamPathId, _] : tableInfo->CdcStreams) {
                AddChangeSender(streamPathId, fullTableId, ESenderType::CdcStream);
            }
        }
    }

    STATEFN(StateBase) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvChangeExchange::TEvEnqueueRecords, Handle);
            hFunc(TEvChangeExchange::TEvAddSender, Handle);
            hFunc(TEvChangeExchange::TEvRemoveSender, Handle);

            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }

    STATEFN(StateInactive) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvChangeExchange::TEvActivateSender, Handle);
        default:
            return StateBase(ev, TlsActivationContext->AsActorContext());
        }
    }

    STATEFN(StateActive) {
        return StateBase(ev, TlsActivationContext->AsActorContext());
    }

private:
    const TDataShardId DataShard;
    mutable TMaybe<TString> LogPrefix;

    THashMap<TPathId, TSender> Senders;
    TVector<TEnqueuedRecord> Enqueued; // Enqueued while inactive

}; // TChangeSender

IActor* CreateChangeSender(const TDataShard* self) {
    return new TChangeSender(self);
}

} // NDataShard
} // NKikimr
