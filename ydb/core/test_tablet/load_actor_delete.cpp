#include "load_actor_impl.h"

namespace NKikimr::NTestShard {

    void TLoadActor::IssueDelete() {
        std::vector<TString> options;
        options.reserve(ConfirmedKeys.size());
        for (const TString& key : ConfirmedKeys) {
            if (!KeysBeingRead.contains(key)) {
                options.emplace_back(key);
            }
        }

        const ui64 barrier = Settings.GetMinDataBytes() + RandomNumber<ui64>(Settings.GetMaxDataBytes() - Settings.GetMinDataBytes() + 1);
        while (!options.empty() && BytesOfData > barrier) {
            const size_t index = RandomNumber(options.size());
            std::swap(options[index], options.back());
            TString key = std::move(options.back());
            options.pop_back();

            auto ev = CreateRequest();
            auto& record = ev->Record;
            const ui64 cookie = record.GetCookie();
            auto *del = record.AddCmdDeleteRange();
            auto *r = del->MutableRange();
            r->SetFrom(key);
            r->SetIncludeFrom(true);
            r->SetTo(key);
            r->SetIncludeTo(true);

            STLOG(PRI_INFO, TEST_SHARD, TS09, "deleting data", (TabletId, TabletId), (Key, key));

            const auto it = Keys.find(key);
            Y_ABORT_UNLESS(it != Keys.end());
            RegisterTransition(*it, ::NTestShard::TStateServer::CONFIRMED, ::NTestShard::TStateServer::DELETE_PENDING, std::move(ev));

            const auto [difIt, difInserted] = DeletesInFlight.try_emplace(cookie, std::move(key));
            Y_ABORT_UNLESS(difInserted);
            Y_ABORT_UNLESS(difIt->second.KeysInQuery.size() == 1);

            BytesOfData -= it->second.Len;
            BytesProcessed += it->second.Len;
            ++KeysDeleted;
        }
    }

    void TLoadActor::ProcessDeleteResult(ui64 cookie,
            const google::protobuf::RepeatedPtrField<NKikimrClient::TKeyValueResponse::TDeleteRangeResult>& results) {
        if (const auto difIt = DeletesInFlight.find(cookie); difIt != DeletesInFlight.end()) {
            TDeleteInfo& info = difIt->second;
            Y_ABORT_UNLESS(info.KeysInQuery.size() == (size_t)results.size(), "%zu/%d", info.KeysInQuery.size(), results.size());
            for (size_t i = 0; i < info.KeysInQuery.size(); ++i) {
                // validate that delete was successful
                const auto& res = results[i];
                Y_VERIFY_S(res.GetStatus() == NKikimrProto::OK, "TabletId# " << TabletId << " CmdDeleteRange failed Status# "
                    << NKikimrProto::EReplyStatus_Name(NKikimrProto::EReplyStatus(res.GetStatus())));

                const auto it = Keys.find(info.KeysInQuery[i]);
                Y_ABORT_UNLESS(it != Keys.end());
                RegisterTransition(*it, ::NTestShard::TStateServer::DELETE_PENDING, ::NTestShard::TStateServer::DELETED);
            }
            DeletesInFlight.erase(difIt);
        }
    }

} // NKikimr::NTestShard
