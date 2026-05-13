#include "schemeshard_impl.h"

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TTxMakeAccessDatabaseNoInheritable : public TTransactionBase<TSchemeShard> {
    bool IsDryRun;
    TActorId AnswerTo;
    std::function< NActors::IEventBase* (const TMap<TPathId, TSet<TString>>&) > AnswerFunc;
    TSideEffects SideEffects;

    TTxMakeAccessDatabaseNoInheritable(TSelf* self,
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

    TSet<TString> GetSidsWithInheritedConnect(const TString& aclData) {
        TSet<TString> result;

        NACLib::TACL acl(aclData);

        for (const NACLibProto::TACE& ace : acl.GetACE()) {
            if (ace.GetAccessType() == static_cast<ui32>(NACLib::EAccessType::Allow)
                && ace.GetAccessRight() == NACLib::EAccessRights::ConnectDatabase
                && ace.GetInheritanceType() == (NACLib::EInheritanceType::InheritObject | NACLib::EInheritanceType::InheritContainer))
            {
                result.insert(ace.GetSID());
            }
        }
        return result;
    }

    TString FixAccess(const TString& aclData, const TSet<TString>& sids) {
        NACLib::TACL acl(aclData);

        for (const auto& x: sids) {
            acl.RemoveAccess(NACLib::EAccessType::Allow, NACLib::EAccessRights::ConnectDatabase, x, NACLib::EInheritanceType::InheritObject | NACLib::EInheritanceType::InheritContainer);
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

            if (!pathElem->IsDomainRoot()) {
                continue;
            }

            if (!pathElem->ACL) {
                continue;
            }

            TPathId domainId = pathElem->PathId;

            auto affectedSids = GetSidsWithInheritedConnect(pathElem->ACL);
            if (affectedSids) {
                sidsByDomain[domainId] = std::move(affectedSids);
            }
        }

        if (!IsDryRun) {
            NIceDb::TNiceDb db(txc.DB);

            for (const auto& item: sidsByDomain) {
                TPathId domainId = item.first;
                const TPathElement::TPtr domainElem = Self->PathsById.at(domainId);
                Y_ABORT_UNLESS(domainElem->IsDomainRoot());

                domainElem->ACL = FixAccess(domainElem->ACL, item.second);
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

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxMakeAccessDatabaseNoInheritable(
    const TActorId& answerTo, bool isDryRun, std::function< NActors::IEventBase*(const TMap<TPathId, TSet<TString>>&) > func) {
    return new TTxMakeAccessDatabaseNoInheritable(this, isDryRun, answerTo, func);
}

} // NSchemeShard
} // NKikimr
