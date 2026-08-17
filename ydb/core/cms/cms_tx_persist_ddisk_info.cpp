#include "cms_impl.h"
#include "scheme.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CMS

namespace NKikimr::NCms {

class TCms::TTxPersistDDiskInfo : public TTransactionBase<TCms> {
    NKikimrBlobStorage::TEvControllerDDiskInfoGetTabletResult Record;
    TString SerializedState;
    TInstant ChangedAt;

public:
    TTxPersistDDiskInfo(TCms* self, TEvPrivate::TEvPersistDDiskInfo::TPtr& ev)
        : TBase(self)
        , Record(ev->Get()->Record)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_PERSIST_DDISK_INFO;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        YDB_LOG_DEBUG_CTX(ctx, "TTxPersistDDiskInfo Execute",
            {"tabletId", Record.GetTabletId()},
            {"revision", Record.GetRevision()});

        if (Record.GetStatus() != NKikimrProto::OK) {
            return true;
        }

        if (!Record.SerializeToString(&SerializedState)) {
            return true;
        }

        ChangedAt = ctx.Now();
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::DDiskInfo>().Key(Record.GetTabletId()).Update<
            Schema::DDiskInfo::Revision,
            Schema::DDiskInfo::LastChangedAt,
            Schema::DDiskInfo::State>(
                Record.GetRevision(),
                ChangedAt.MicroSeconds(),
                SerializedState);
        return true;
    }

    void Complete(const TActorContext& /*ctx*/) override {
        if (Record.GetStatus() == NKikimrProto::OK && SerializedState) {
            Self->State->DDiskInfo[Record.GetTabletId()] = TCmsDDiskInfo{
                .Revision = Record.GetRevision(),
                .LastChangedAt = ChangedAt,
                .State = std::move(SerializedState)
            };
        }
    }
};

ITransaction* TCms::CreateTxPersistDDiskInfo(TEvPrivate::TEvPersistDDiskInfo::TPtr& ev) {
    return new TTxPersistDDiskInfo(this, ev);
}

} // namespace NKikimr::NCms
