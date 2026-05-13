#include "schemeshard_impl.h"

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TTxUpgradeAccessDatabaseRights : public TTransactionBase<TSchemeShard> {
    bool IsDryRun;
    TActorId AnswerTo;
    std::function< NActors::IEventBase* (const TMap<TPathId, TSet<TString>>&) > AnswerFunc;
    TSideEffects SideEffects;

    TTxUpgradeAccessDatabaseRights(TSelf* self,
                                   const bool isDryRun,
                                   const TActorId& answerTo,
                                   std::function< NActors::IEventBase* (const TMap<TPathId, TSet<TString>>&) > answerFunc)
        : TTransactionBase<TSchemeShard>(self)
        , IsDryRun(isDryRun)
        , AnswerTo(answerTo)
        , AnswerFunc(answerFunc)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_UPGRADE_SCHEME;
    }

    TVector<TString> GetSids(const TString& owner, const TString& aclData) {
        TVector<TString> result;

        NACLib::TACL acl(aclData);

        result.push_back(owner);

        for (const NACLibProto::TACE& ace : acl.GetACE()) {
            if (ace.GetAccessType() == static_cast<ui32>(NACLib::EAccessType::Allow)) {
                result.push_back(ace.GetSID());
            }
        }

        return result;
    }

    TVector<TString> GetSidsWithConnect(const TString& owner, const TString& aclData) {
        TVector<TString> result;

        NACLib::TACL acl(aclData);

        result.push_back(owner);

        for (const NACLibProto::TACE& ace : acl.GetACE()) {
            if (ace.GetAccessType() == static_cast<ui32>(NACLib::EAccessType::Allow)) {
                if (ace.GetAccessRight() & NACLib::EAccessRights::ConnectDatabase) {
                    result.push_back(ace.GetSID());
                }
            }
        }

        return result;
    }

    TString UpgradeAccess(const TString& aclData, const TSet<TString>& sids) {
        NACLib::TACL acl(aclData);

        for (const auto& x: sids) {
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::EAccessRights::ConnectDatabase, x, NACLib::InheritNone);
        }

        TString proto;
        Y_PROTOBUF_SUPPRESS_NODISCARD acl.SerializeToString(&proto);

        return proto;
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTxUpgradeSchema.Execute");

        if (!Self->IsSchemeShardConfigured()) {
            return true;
        }

        TMap<TPathId, TSet<TString>> sidsByDomain;

        for (const auto& item: Self->PathsById) {
            const TPathElement::TPtr pathElem = item.second;

            if (pathElem->Dropped() || pathElem->IsMigrated()) {
                continue;
            }

            TPathId domainId = pathElem->IsDomainRoot()
                ? pathElem->PathId
                : pathElem->DomainPathId;

            sidsByDomain[domainId].insert(pathElem->Owner);

            if (!pathElem->ACL) {
                continue;
            }

            for (const auto& x: GetSids(pathElem->Owner, pathElem->ACL)) {
                sidsByDomain[domainId].insert(x);
            }
        }

        for (auto& item: sidsByDomain) {
            TPathId domainId = item.first;
            const TPathElement::TPtr domainElem = Self->PathsById.at(domainId);
            Y_ABORT_UNLESS(domainElem->IsDomainRoot());

            TVector<TString> alreadyUpgraded = GetSidsWithConnect(domainElem->Owner, domainElem->ACL);

            for (const auto& sid: alreadyUpgraded) {
                item.second.erase(sid);
            }
        }

        if (!IsDryRun) {
            NIceDb::TNiceDb db(txc.DB);

            for (const auto& item: sidsByDomain) {
                TPathId domainId = item.first;
                const TPathElement::TPtr domainElem = Self->PathsById.at(domainId);
                Y_ABORT_UNLESS(domainElem->IsDomainRoot());

                domainElem->ACL = UpgradeAccess(domainElem->ACL, item.second);
                domainElem->ACLVersion += 1;

                Self->PersistACL(db, domainElem);

                SideEffects.PublishToSchemeBoard(InvalidOperationId, domainId);
                SideEffects.UpdateTenant(domainId);
            }
        }

        SideEffects.Send(AnswerTo, AnswerFunc(sidsByDomain));

        SideEffects.ApplyOnExecute(Self, txc, ctx);
        return true;
    }

    void Complete(const TActorContext &ctx) override {
        SideEffects.ApplyOnComplete(Self, ctx);
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxUpgradeAccessDatabaseRights(
    const TActorId& answerTo, bool isDryRun, std::function< NActors::IEventBase*(const TMap<TPathId, TSet<TString>>&) > func) {
    return new TTxUpgradeAccessDatabaseRights(this, isDryRun, answerTo, func);
}

} // NSchemeShard
} // NKikimr
