#include "tablet_impl.h"

namespace NKikimr::NKesus {

struct TKesusTablet::TTxSemaphoresDescribe : public TTxBase {
    const TActorId Sender;
    const ui64 Cookie;
    THolder<TEvKesus::TEvDescribeSemaphoresResult> Reply;

    TTxSemaphoresDescribe(TSelf* self, const TActorId& sender, ui64 cookie)
        : TTxBase(self)
        , Sender(sender)
        , Cookie(cookie)
    {}

    TTxType GetTxType() const override { return TXTYPE_SEMAPHORES_DESCRIBE; }

    bool Execute(TTransactionContext&, const TActorContext&) override {
        Reply = MakeHolder<TEvKesus::TEvDescribeSemaphoresResult>();
        auto& result = Reply->Record;
        result.MutableError()->SetStatus(Ydb::StatusIds::SUCCESS);

        // Leave room for the tablet-pipe envelope in the serialized event.
        constexpr size_t maxBytes = NActors::EventMaxByteSize - 1024;
        size_t bytes = 0;
        for (const auto& [id, info] : Self->Semaphores) {
            if (info.Ephemeral) {
                continue;
            }
            Ydb::Coordination::SessionRequest::CreateSemaphore semaphore;
            semaphore.set_name(info.Name);
            semaphore.set_data(info.Data);
            semaphore.set_limit(info.Limit);
            bytes += semaphore.ByteSizeLong() + 16;
            if (bytes > maxBytes) {
                result.ClearSemaphores();
                TEvKesus::FillError(result.MutableError(), Ydb::StatusIds::PRECONDITION_FAILED,
                    "Semaphore definitions exceed the export response size limit");
                return true;
            }
            *result.AddSemaphores() = std::move(semaphore);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(Sender, Reply.Release(), 0, Cookie);
    }
};

void TKesusTablet::Handle(TEvKesus::TEvDescribeSemaphores::TPtr& ev) {
    Execute(new TTxSemaphoresDescribe(this, ev->Sender, ev->Cookie),
        TActivationContext::AsActorContext());
}

} // namespace NKikimr::NKesus
