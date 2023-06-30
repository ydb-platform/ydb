#include "load_actor_impl.h"

namespace NKikimr::NTestShard {

    std::optional<TString> TLoadActor::FindKeyToDelete() {
        std::vector<std::tuple<ui64, TString>> options; // (accumLen, key)
        options.reserve(Keys.size());
        ui64 accumLen = 0;
        for (const auto& [key, info] : Keys) {
            if (info.ConfirmedState == info.PendingState && info.ConfirmedState == ::NTestShard::TStateServer::CONFIRMED &&
                    !KeysBeingRead.contains(key)) {
                accumLen += info.Len;
                options.emplace_back(accumLen, key);
            }
        }
        if (options.empty()) {
            return std::nullopt;
        }

        const size_t num = std::upper_bound(options.begin(), options.end(), std::make_tuple(
            TAppData::RandomProvider->Uniform(accumLen), TString())) - options.begin();
        Y_VERIFY(num < options.size());
        return std::get<1>(options[num]);
    }

    void TLoadActor::IssueDelete() {
        const ui64 barrier = TAppData::RandomProvider->Uniform(Settings.GetMinDataBytes(), Settings.GetMaxDataBytes());
        while (BytesOfData > barrier) {
            const auto& key = FindKeyToDelete();
            if (!key) {
                break;
            }

            auto ev = CreateRequest();
            auto& record = ev->Record;
            auto *del = record.AddCmdDeleteRange();
            auto *r = del->MutableRange();
            r->SetFrom(*key);
            r->SetIncludeFrom(true);
            r->SetTo(*key);
            r->SetIncludeTo(true);

            STLOG(PRI_INFO, TEST_SHARD, TS09, "deleting data", (TabletId, TabletId), (Key, key));

            const auto [difIt, difInserted] = DeletesInFlight.try_emplace(record.GetCookie(), *key);
            Y_VERIFY(difInserted);
            Y_VERIFY(difIt->second.KeysInQuery.size() == 1);

            const auto it = Keys.find(*key);
            Y_VERIFY(it != Keys.end());
            RegisterTransition(*it, ::NTestShard::TStateServer::CONFIRMED, ::NTestShard::TStateServer::DELETE_PENDING, std::move(ev));

            BytesOfData -= it->second.Len;
            BytesProcessed += it->second.Len;
            ++KeysDeleted;
        }
    }

    void TLoadActor::ProcessDeleteResult(ui64 cookie,
            const google::protobuf::RepeatedPtrField<NKikimrClient::TKeyValueResponse::TDeleteRangeResult>& results) {
        if (const auto difIt = DeletesInFlight.find(cookie); difIt != DeletesInFlight.end()) {
            TDeleteInfo& info = difIt->second;
            Y_VERIFY(info.KeysInQuery.size() == (size_t)results.size(), "%zu/%d", info.KeysInQuery.size(), results.size());
            for (size_t i = 0; i < info.KeysInQuery.size(); ++i) {
                // validate that delete was successful
                const auto& res = results[i];
                Y_VERIFY_S(res.GetStatus() == NKikimrProto::OK, "TabletId# " << TabletId << " CmdDeleteRange failed Status# "
                    << NKikimrProto::EReplyStatus_Name(NKikimrProto::EReplyStatus(res.GetStatus())));

                const auto it = Keys.find(info.KeysInQuery[i]);
                Y_VERIFY(it != Keys.end());
                RegisterTransition(*it, ::NTestShard::TStateServer::DELETE_PENDING, ::NTestShard::TStateServer::DELETED);
            }
            DeletesInFlight.erase(difIt);
        }
    }

} // NKikimr::NTestShard
