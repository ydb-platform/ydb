#include "name_service.h"

namespace NSQLComplete {

    class TDeadlinedNameService: public INameService {
    public:
        TDeadlinedNameService(INameService::TPtr origin, TDuration timeout)
            : Origin_(std::move(origin))
            , Timeout_(std::move(timeout))
        {
        }

        TFuture<TNameResponse> Lookup(TNameRequest request) override {
            auto future = Origin_->Lookup(std::move(request));
            if (!future.Wait(Timeout_)) {
                auto e = NThreading::TFutureException() << "Timeout " << Timeout_;
                return NThreading::MakeErrorFuture<TNameResponse>(std::make_exception_ptr(e));
            }
            return future;
        }

    private:
        INameService::TPtr Origin_;
        TDuration Timeout_;
    };

    class TFallbackNameService: public INameService {
    public:
        TFallbackNameService(INameService::TPtr primary, INameService::TPtr standby)
            : Primary_(std::move(primary))
            , Standby_(std::move(standby))
        {
        }

        TFuture<TNameResponse> Lookup(TNameRequest request) override {
            auto future = Primary_->Lookup(request);
            future.Wait();
            if (future.HasException()) {
                return Standby_->Lookup(request);
            }
            return future;
        }

    private:
        INameService::TPtr Primary_;
        INameService::TPtr Standby_;
    };

    INameService::TPtr MakeDeadlinedNameService(INameService::TPtr origin, TDuration timeout) {
        return INameService::TPtr(new TDeadlinedNameService(std::move(origin), std::move(timeout)));
    }

    INameService::TPtr MakeFallbackNameService(INameService::TPtr primary, INameService::TPtr standby) {
        return INameService::TPtr(new TFallbackNameService(std::move(primary), std::move(standby)));
    }

} // namespace NSQLComplete
