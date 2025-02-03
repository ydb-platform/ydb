#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr::NHive {

class TTxConfigureScaleRecommender : public TTransactionBase<THive> {
    const TEvHive::TEvConfigureScaleRecommender::TPtr Request;
    TSideEffects SideEffects;

public:
    TTxConfigureScaleRecommender(TEvHive::TEvConfigureScaleRecommender::TPtr request, THive* hive)
        : TBase(hive)
        , Request(std::move(request))
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_CONFIGURE_SCALE_RECOMMENDER; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        BLOG_D("THive::TTxConfigureScaleRecommender::Execute");
        SideEffects.Reset(Self->SelfId());

        auto response = MakeHolder<TEvHive::TEvConfigureScaleRecommenderReply>();

        const auto& record = Request->Get()->Record;
        if (!record.HasDomainKey()) {
            response->Record.SetStatus(NKikimrProto::ERROR);
            SideEffects.Send(Request->Sender, response.Release(), 0, Request->Cookie);
            return true;
        }

        TSubDomainKey domainKey(record.GetDomainKey());
        TDomainInfo* domain = Self->FindDomain(domainKey);
        if (domain == nullptr) {
            response->Record.SetStatus(NKikimrProto::ERROR);
            SideEffects.Send(Request->Sender, response.Release(), 0, Request->Cookie);
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::SubDomain>()
                .Key(domainKey.first, domainKey.second)
                .Update<Schema::SubDomain::ScaleRecommenderPolicies>(record.policies());
        domain->SetScaleRecommenderPolicies(record.policies());

        response->Record.SetStatus(NKikimrProto::OK);
        SideEffects.Send(Request->Sender, response.Release(), 0, Request->Cookie);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_D("THive::TTxConfigureScaleRecommender::Complete");
        SideEffects.Complete(ctx);
    }
};

ITransaction* THive::CreateConfigureScaleRecommender(TEvHive::TEvConfigureScaleRecommender::TPtr event) {
    return new TTxConfigureScaleRecommender(std::move(event), this);
};

} // namespace NKikimr::NHive
