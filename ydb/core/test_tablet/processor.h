#pragma once

namespace NKikimr::NTestShard {

    class TProcessor {
        struct TTabletInfo {
            ui32 LastSeenGeneration = 0;
            std::map<TString, ::NTestShard::TStateServer::EEntityState> State;
        };

        std::unordered_map<ui64, TTabletInfo> Tablets;

    public:
        ::NTestShard::TStateServer::TWriteResult Execute(const ::NTestShard::TStateServer::TWrite& cmd) {
            //printf("W %" PRIu64 " %s %s -> %s\n", cmd.GetTabletId(), cmd.GetKey().data(),
            //    ::NTestShard::TStateServer::EEntityState_Name(cmd.GetOriginState()).data(),
            //    ::NTestShard::TStateServer::EEntityState_Name(cmd.GetTargetState()).data());
            ::NTestShard::TStateServer::TWriteResult res;
            auto& info = Tablets[cmd.GetTabletId()];
            if (cmd.GetGeneration() < info.LastSeenGeneration) {
                res.SetStatus(::NTestShard::TStateServer::RACE);
            } else {
                info.LastSeenGeneration = cmd.GetGeneration();
                const auto it = info.State.lower_bound(cmd.GetKey());
                const auto state = (it != info.State.end() && it->first == cmd.GetKey())
                    ? it->second
                    : ::NTestShard::TStateServer::ABSENT;
                if (state != cmd.GetOriginState()) {
                    Cerr << "TabletId# " << cmd.GetTabletId() << " State# " << ::NTestShard::TStateServer::EEntityState_Name(state)
                        << " != OriginState# " << ::NTestShard::TStateServer::EEntityState_Name(cmd.GetOriginState())
                        << " Key# " << cmd.GetKey() << Endl;
                    res.SetStatus(::NTestShard::TStateServer::ERROR);
                } else {
                    Apply(cmd);
                    res.SetStatus(::NTestShard::TStateServer::OK);
                }
            }
            return res;
        }

        void Apply(const ::NTestShard::TStateServer::TWrite& cmd) {
            //printf("WW %" PRIu64 " %s %s\n", cmd.GetTabletId(), cmd.GetKey().data(),
            //    ::NTestShard::TStateServer::EEntityState_Name(cmd.GetTargetState()).data());
            auto& info = Tablets[cmd.GetTabletId()];
            if (cmd.GetTargetState() == ::NTestShard::TStateServer::DELETED) {
                info.State.erase(cmd.GetKey());
            } else {
                info.State[cmd.GetKey()] = cmd.GetTargetState();
            }
        }

        ::NTestShard::TStateServer::TReadResult Execute(const ::NTestShard::TStateServer::TRead& cmd) {
            ::NTestShard::TStateServer::TReadResult res;
            auto& info = Tablets[cmd.GetTabletId()];
            if (cmd.GetGeneration() < info.LastSeenGeneration) {
                res.SetStatus(::NTestShard::TStateServer::RACE);
            } else {
                info.LastSeenGeneration = cmd.GetGeneration();
                //printf("R %" PRIu64 "\n", cmd.GetTabletId());
                for (auto it = info.State.upper_bound(cmd.GetCookie()); it != info.State.end(); ++it) {
                    auto *item = res.AddItems();
                    item->SetKey(it->first);
                    item->SetState(it->second);
                    res.SetCookie(it->first);
                    //printf("RR %s @ %s\n", it->first.data(), ::NTestShard::TStateServer::EEntityState_Name(it->second).data());
                }
                res.SetStatus(::NTestShard::TStateServer::OK);
            }
            return res;
        }
    };

} // NKikimr::NTestShard
