#pragma once
#include "defs.h"

namespace NKikimr {

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // TDelayedResponses -- this class holds delayed VDisk responses for incoming messages.
    // Some messages must be responded with BLOCK or ALREADY status, but this status
    // is based on LoggedRec that is in flight. So we pospone response until depending
    // message is committed to recovery log.
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    class TDelayedResponses {
    public:
        using TAction = std::function<void (const TActorId &id, ui64 cookie, IEventBase *msg)>;

        void Put(IEventBase *msg, const TActorId &recipient, ui64 recipientCookie, ui64 lsn) {
            Map.emplace(lsn, TValue {recipient, recipientCookie, std::unique_ptr<IEventBase>(msg)}); 
        }

        void ConfirmLsn(ui64 lsn, const TAction &action) {
            TMap::iterator it = Map.begin();
            while (it != Map.end() && it->first <= lsn) {
                TValue &v = it->second;
                action(v.Recipient, v.RecipientCookie, v.Msg.release()); 
                ++it;
            }
            // remove all traversed elements
            Map.erase(Map.begin(), it);
        }

    private:
        struct TValue {
            TActorId Recipient;
            ui64 RecipientCookie;
            std::unique_ptr<IEventBase> Msg; 
        };

        using TMap = std::multimap<ui64, TValue>;
        TMap Map;
    };

} // NKikimr

