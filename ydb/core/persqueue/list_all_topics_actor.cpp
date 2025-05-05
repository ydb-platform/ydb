#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>


using namespace NActors;

namespace NKikimr::NPersQueue {

class TListAllTopicsActor : public NActors::TActorBootstrapped<TListAllTopicsActor> {

    public:
        TListAllTopicsActor(const TActorId& respondTo, const TString& databasePath, const TString& token,
                            bool recursive, const TString& startFrom, const TMaybe<ui64>& limit);
        //~TListStreamsActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx);

        void StateWork(TAutoPtr<IEventHandle>& ev);
        void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);

    protected:
        void SendNavigateRequest(const TString& path);
        void SendPendingRequests();
        void SendResponse();


    private:
        static constexpr ui32 MAX_IN_FLIGHT = 100;

        TActorId RespondTo;
        TString DatabasePath;
        TString Token;
        bool Recursive;
        TString StartFrom;
        TMaybe<ui64> Limit;


        ui32 RequestsInFlight = 0;
        std::vector<std::unique_ptr<TEvTxProxySchemeCache::TEvNavigateKeySet>> WaitingList;
        std::vector<TString> Topics;
    };

    TListAllTopicsActor::TListAllTopicsActor(
            const TActorId& respondTo, const TString& databasePath, const TString& token,
            bool recursive, const TString& startFrom, const TMaybe<ui64>& limit
    )
        : RespondTo(respondTo)
        , DatabasePath(databasePath)
        , Token(token)
        , Recursive(recursive)
        , StartFrom(startFrom)
        , Limit(limit)
    {
    }

    void TListAllTopicsActor::Bootstrap(const NActors::TActorContext&) {
        SendNavigateRequest(DatabasePath);
        Become(&TListAllTopicsActor::StateWork);
    }

    void TListAllTopicsActor::SendPendingRequests() {
        if (RequestsInFlight < MAX_IN_FLIGHT && WaitingList.size() > 0) {
            Send(MakeSchemeCacheID(), WaitingList.back().release());
            WaitingList.pop_back();
            RequestsInFlight++;
        }
    }

    void TListAllTopicsActor::SendResponse() {
        Y_ENSURE(WaitingList.size() == 0 && RequestsInFlight == 0);

        for (TString& topic : Topics) {
            topic = TFsPath(topic).RelativePath(DatabasePath).GetPath();
        }
        Sort(Topics.begin(), Topics.end());
        auto response = new TEvPQ::TEvListAllTopicsResponse();
        for (auto& topicName : Topics) {
            if (!StartFrom.empty() && StartFrom >= topicName) {
                continue;
            }
            if (Limit.Defined() && response->Topics.size() >= *Limit) {
                response->HaveMoreTopics = true;
                break;
            }
            response->Topics.emplace_back(std::move(topicName));
        }

        Send(RespondTo, response);
        Die(ActorContext());
    }

    void TListAllTopicsActor::SendNavigateRequest(const TString& path) {
        auto schemeCacheRequest = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
        schemeCacheRequest->DatabaseName = DatabasePath;
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = NKikimr::SplitPath(path);

        if (!Token.empty()) {
            schemeCacheRequest->UserToken = new NACLib::TUserToken(Token);
        }

        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
        schemeCacheRequest->ResultSet.emplace_back(entry);
        WaitingList.push_back(std::make_unique<TEvTxProxySchemeCache::TEvNavigateKeySet>(schemeCacheRequest.release()));
        SendPendingRequests();
    }

    void TListAllTopicsActor::StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            default:
                Y_DEBUG_ABORT_UNLESS(false);
        }
    }

    void TListAllTopicsActor::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const NSchemeCache::TSchemeCacheNavigate* navigate = ev->Get()->Request.Get();
        for (const auto& entry : navigate->ResultSet) {

            if (entry.Kind == NSchemeCache::TSchemeCacheNavigate::EKind::KindTable) {
                for (const auto& stream : entry.CdcStreams) {
                    TString childFullPath = JoinPath({JoinPath(entry.Path), stream.GetName()});
                    Topics.push_back(childFullPath);
                }
            } else if (entry.Kind == NSchemeCache::TSchemeCacheNavigate::EKind::KindPath
                || entry.Kind == NSchemeCache::TSchemeCacheNavigate::EKind::KindSubdomain)
            {
                Y_ENSURE(entry.ListNodeEntry, "ListNodeEntry is zero");
                for (const auto& child : entry.ListNodeEntry->Children) {
                    TString childFullPath = JoinPath({JoinPath(entry.Path), child.Name});
                    switch (child.Kind) {
                        case NSchemeCache::TSchemeCacheNavigate::EKind::KindPath:
                        case NSchemeCache::TSchemeCacheNavigate::EKind::KindTable:
                            if (Recursive) {
                                SendNavigateRequest(childFullPath);
                            }
                            break;
                        case NSchemeCache::TSchemeCacheNavigate::EKind::KindTopic:
                            Topics.push_back(childFullPath);
                            break;

                        default:
                            break;
                            // ignore all other types
                    }
                }
            }
        }
        RequestsInFlight--;
        SendPendingRequests();
        if (RequestsInFlight == 0) {
            SendResponse();
        }
    }

NActors::IActor* MakeListAllTopicsActor(
        const TActorId& respondTo, const TString& databasePath, const TString& token, bool recursive, const TString& startFrom,
        const TMaybe<ui64>& limit
) {
    return new TListAllTopicsActor(respondTo, databasePath, token, recursive, startFrom, limit);
}

} // namespace