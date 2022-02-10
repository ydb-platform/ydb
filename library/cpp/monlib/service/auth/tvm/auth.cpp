#include "auth.h"

#include <util/generic/hash_set.h>


using namespace NTvmAuth;


namespace NMonitoring {
namespace {
    template <class TTvmClientPtr = THolder<TTvmClient>>
    class TTvmManager final: public ITvmManager {
    public:
        TTvmManager(NTvmApi::TClientSettings settings, TVector<TTvmId> clients, TLoggerPtr logger)
            : AllowedClients_{clients.begin(), clients.end()}
            , Tvm_(new TTvmClient{std::move(settings), std::move(logger)})
        {
        }

        TTvmManager(NTvmTool::TClientSettings settings, TVector<TTvmId> clients, TLoggerPtr logger)
            : AllowedClients_{clients.begin(), clients.end()}
            , Tvm_(new TTvmClient{std::move(settings), std::move(logger)})
        {
        }

        TTvmManager(TTvmClientPtr tvm, TVector<TTvmId> clients)
            : AllowedClients_{clients.begin(), clients.end()}
            , Tvm_(std::move(tvm))
        {
        }

        bool IsAllowedClient(TTvmId clientId) override {
            return AllowedClients_.contains(clientId);
        }

        TCheckedServiceTicket CheckServiceTicket(TStringBuf ticket) override {
            return Tvm_->CheckServiceTicket(ticket);
        }

    private:
        THashSet<TTvmId> AllowedClients_;
        TTvmClientPtr Tvm_;
    };

    class TTvmAuthProvider final: public IAuthProvider {
    public:
        TTvmAuthProvider(THolder<ITvmManager> manager)
            : TvmManager_{std::move(manager)}
        {
        }

        TAuthResult Check(const IHttpRequest& req) override {
            auto ticketHeader = req.GetHeaders().FindHeader("X-Ya-Service-Ticket");
            if (!ticketHeader) {
                return TAuthResult::NoCredentials();
            }

            const auto ticket = TvmManager_->CheckServiceTicket(ticketHeader->Value());
            if (!ticket) {
                return TAuthResult::Denied();
            }

            return TvmManager_->IsAllowedClient(ticket.GetSrc())
                ? TAuthResult::Ok()
                : TAuthResult::Denied();
        }

    private:
        THolder<ITvmManager> TvmManager_;
    };
} // namespace

THolder<ITvmManager> CreateDefaultTvmManager(NTvmApi::TClientSettings settings, TVector<TTvmId> allowedClients, TLoggerPtr logger) {
    return MakeHolder<TTvmManager<>>(std::move(settings), std::move(allowedClients), std::move(logger));
}

THolder<ITvmManager> CreateDefaultTvmManager(NTvmTool::TClientSettings settings, TVector<TTvmId> allowedClients, TLoggerPtr logger) {
    return MakeHolder<TTvmManager<>>(std::move(settings), std::move(allowedClients), std::move(logger));
}

THolder<ITvmManager> CreateDefaultTvmManager(TAtomicSharedPtr<NTvmAuth::TTvmClient> client, TVector<TTvmId> allowedClients) {
    return MakeHolder<TTvmManager<TAtomicSharedPtr<NTvmAuth::TTvmClient>>>(std::move(client), std::move(allowedClients));
}

THolder<ITvmManager> CreateDefaultTvmManager(std::shared_ptr<NTvmAuth::TTvmClient> client, TVector<TTvmId> allowedClients) {
    return MakeHolder<TTvmManager<std::shared_ptr<NTvmAuth::TTvmClient>>>(std::move(client), std::move(allowedClients));
}

THolder<IAuthProvider> CreateTvmAuth(THolder<ITvmManager> manager) {
    return MakeHolder<TTvmAuthProvider>(std::move(manager));
}

} // namespace NMonitoring
