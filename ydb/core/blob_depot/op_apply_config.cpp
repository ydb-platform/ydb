#include "blob_depot_tablet.h"
#include "schema.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDBLOG_THIS_FILE_COMPONENT BLOB_DEPOT

namespace NKikimr::NBlobDepot {

    void TBlobDepot::Handle(TEvBlobDepot::TEvApplyConfig::TPtr ev) {
        YDBLOG_DEBUG("TEvApplyConfig",
            {"Marker", "BDT15"},
            {"Id", GetLogId()},
            {"Msg", ev->Get()->Record});

        class TTxApplyConfig : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            std::unique_ptr<IEventHandle> Response;
            TString ConfigProtobuf;

        public:
            TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_APPLY_CONFIG; }

            TTxApplyConfig(TBlobDepot *self, TEvBlobDepot::TEvApplyConfig& ev, std::unique_ptr<IEventHandle> response,
                    TActorId interconnectSession)
                : TTransactionBase(self)
                , Response(std::move(response))
            {
                if (interconnectSession) {
                    Response->Rewrite(TEvInterconnect::EvForward, interconnectSession);
                }
                const bool success = ev.Record.GetConfig().SerializeToString(&ConfigProtobuf);
                Y_ABORT_UNLESS(success);
            }

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                YDBLOG_DEBUG("TTxApplyConfig::Execute",
                    {"Marker", "BDT16"},
                    {"Id", Self->GetLogId()});

                NIceDb::TNiceDb db(txc.DB);

                auto table = db.Table<Schema::Config>().Key(Schema::Config::Key::Value).Select();
                if (!table.IsReady()) {
                    return false;
                }

                db.Table<Schema::Config>().Key(Schema::Config::Key::Value).Update(
                    NIceDb::TUpdate<Schema::Config::ConfigProtobuf>(ConfigProtobuf)
                );

                const bool success = Self->Config.ParseFromString(ConfigProtobuf);
                Y_ABORT_UNLESS(success);

                return true;
            }

            void Complete(const TActorContext&) override {
                YDBLOG_DEBUG("TTxApplyConfig::Complete",
                    {"Marker", "BDT17"},
                    {"Id", Self->GetLogId()});

                if (!std::exchange(Self->Configured, true)) {
                    Self->StartOperation();
                } else {
                    // TODO(alexvru): handle it in a better way, without tablet restart
                    Self->Send(Self->Tablet(), new TEvents::TEvPoison);
                }

                TActivationContext::Send(Response.release());
            }
        };

        const auto& record = ev->Get()->Record;
        if (record.HasGroupInfo()) {
            TStringStream err;
            GroupInfo = TBlobStorageGroupInfo::Parse(record.GetGroupInfo(), nullptr, &err);
            Y_DEBUG_ABORT_UNLESS(GroupInfo);
        }

        auto responseEvent = std::make_unique<TEvBlobDepot::TEvApplyConfigResult>(TabletID(), record.GetTxId());
        auto response = std::make_unique<IEventHandle>(ev->Sender, SelfId(), responseEvent.release(), 0, ev->Cookie);
        Execute(std::make_unique<TTxApplyConfig>(this, *ev->Get(), std::move(response), ev->InterconnectSession));
    }

} // NKikimr::NBlobDepot
