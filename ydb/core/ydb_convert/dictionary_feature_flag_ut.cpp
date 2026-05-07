#include "table_description.h"

#include <ydb/core/testlib/test_client.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_local.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(ConvertColumnTableDictionaryEncodingFeatureFlag) {
    struct TResult {
        bool Ok = false;
        Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
        TString Error;
    };

    struct TEvConvertResult: NActors::TEventLocal<TEvConvertResult, NActors::TEvents::ES_PRIVATE + 207> {
        TResult Result;
        explicit TEvConvertResult(TResult result)
            : Result(std::move(result))
        {}
    };

    class TConvertActor: public NActors::TActorBootstrapped<TConvertActor> {
    private:
        const NActors::TActorId ReplyTo;
        const bool IsAlter;

    public:
        TConvertActor(const NActors::TActorId& replyTo, const bool isAlter)
            : ReplyTo(replyTo)
            , IsAlter(isAlter)
        {}

        void Bootstrap(const NActors::TActorContext& ctx) {
            Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS;
            TString error;
            bool ok = false;

            if (!IsAlter) {
                google::protobuf::RepeatedPtrField<Ydb::Table::ColumnMeta> cols;
                {
                    auto* pk = cols.Add();
                    pk->set_name("pk");
                    pk->mutable_type()->set_type_id(Ydb::Type::UINT64);
                    pk->set_not_null(true);
                }
                {
                    auto* msg = cols.Add();
                    msg->set_name("message");
                    msg->mutable_type()->set_type_id(Ydb::Type::UTF8);
                    msg->set_not_null(false);
                    msg->add_encoding()->mutable_dictionary();
                }
                NKikimrSchemeOp::TColumnTableDescription out;
                ok = FillColumnDescription(out, cols, status, error);
            } else {
                Ydb::Table::AlterTableRequest req;
                {
                    auto* alter = req.add_alter_columns();
                    alter->set_name("message");
                    alter->add_encoding()->mutable_dictionary();
                }
                NKikimrSchemeOp::TModifyScheme modifyScheme;
                ok = BuildAlterColumnTableModifyScheme("/Root/ColumnTable", &req, &modifyScheme, status, error);
            }

            ctx.Send(ReplyTo, new TEvConvertResult(TResult{ok, status, error}));
            this->Die(ctx);
        }
    };

    static TResult Run(bool enableDictFlag, bool isAlter) {
        using namespace Tests;
        TPortManager pm;

        auto settings = MakeIntrusive<TServerSettings>(pm.GetPort(2134));
        settings->SetNodeCount(1);
        settings->SetUseRealThreads(false);
        settings->SetEnableOlapSink(true);
        settings->FeatureFlags.SetEnableCsDictionaryEncoding(enableDictFlag);

        TServer server(settings);
        auto& runtime = *server.GetRuntime();

        const auto edge = runtime.AllocateEdgeActor();
        runtime.Register(new TConvertActor(edge, isAlter), 0, runtime.GetAppData().SystemPoolId);
        auto ev = runtime.GrabEdgeEvent<TEvConvertResult>(edge);
        return ev->Get()->Result;
    }

    Y_UNIT_TEST(CreatePathBlockedWhenFlagOff) {
        const auto r = Run(false, false);
        UNIT_ASSERT_C(!r.Ok, "FillColumnDescription must fail when EnableCsDictionaryEncoding is off");
        UNIT_ASSERT_VALUES_EQUAL(r.Status, Ydb::StatusIds::UNSUPPORTED);
        UNIT_ASSERT_STRING_CONTAINS(r.Error, "Dictionary encoding is not enabled");
    }

    Y_UNIT_TEST(CreatePathAllowedWhenFlagOn) {
        const auto r = Run(true, false);
        UNIT_ASSERT_C(r.Ok, r.Error);
        UNIT_ASSERT_VALUES_EQUAL(r.Status, Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(AlterPathBlockedWhenFlagOff) {
        const auto r = Run(false, true);
        UNIT_ASSERT_C(!r.Ok, "BuildAlterColumnTableModifyScheme must fail when EnableCsDictionaryEncoding is off");
        UNIT_ASSERT_VALUES_EQUAL(r.Status, Ydb::StatusIds::UNSUPPORTED);
        UNIT_ASSERT_STRING_CONTAINS(r.Error, "Dictionary encoding is not enabled");
    }

    Y_UNIT_TEST(AlterPathAllowedWhenFlagOn) {
        const auto r = Run(true, true);
        UNIT_ASSERT_C(r.Ok, r.Error);
        UNIT_ASSERT_VALUES_EQUAL(r.Status, Ydb::StatusIds::SUCCESS);
    }
}

} // namespace NKikimr

