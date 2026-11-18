#include "tablet_impl.h"

#include <google/protobuf/wire_format_lite.h>

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

        using google::protobuf::internal::WireFormatLite;
        const size_t semaphoreTagSize = WireFormatLite::TagSize(
            NKikimrKesus::TEvDescribeSemaphoresResult::kSemaphoresFieldNumber,
            WireFormatLite::TYPE_MESSAGE);
        // Include Error, then account for each repeated message without rescanning the list.
        size_t bytes = Reply->CalculateSerializedSize();
        for (const auto& [id, info] : Self->Semaphores) {
            if (info.Ephemeral) {
                continue;
            }
            NKikimrKesus::TSemaphoreDescription semaphore;
            semaphore.SetName(info.Name);
            semaphore.SetData(info.Data);
            semaphore.SetLimit(info.Limit);
            semaphore.SetCount(info.Count);
            semaphore.SetEphemeral(info.Ephemeral);
            bytes += semaphoreTagSize + WireFormatLite::MessageSize(semaphore);
            if (bytes > NActors::EventMaxByteSize) {
                result.ClearSemaphores();
                TEvKesus::FillError(result.MutableError(), Ydb::StatusIds::PRECONDITION_FAILED,
                    "Semaphore descriptions exceed the response size limit");
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
