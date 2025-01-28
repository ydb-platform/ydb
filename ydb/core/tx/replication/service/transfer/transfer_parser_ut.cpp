#include "service.h"
#include "table_writer.h"
#include "worker.h"

#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/string/printf.h>
#include <util/string/strip.h>

#include <yql/essentials/public/purecalc/purecalc.h>
#include <yql/essentials/public/purecalc/io_specs/protobuf/spec.h>
#include <yql/essentials/public/purecalc/helpers/stream/stream_from_vector.h>

namespace NKikimr::NReplication::NService {

Y_UNIT_TEST_SUITE(LocalTableWriter) {
    using namespace NTestHelpers;
    using TRecord = TEvWorker::TEvData::TRecord;

    using namespace NYql::NPureCalc;

    Y_UNIT_TEST(Run) {

        TString lambda = R"(
            __ydb_transfer_lambda = ($meta) -> {
                return $x;
            };
        )";

        auto ischeme = []() -> NYT::TNode {
            NYT::TNode members {NYT::TNode::CreateList()};

            {
                auto typeNode = NYT::TNode::CreateList()
                        .Add("DataType")
                        .Add("String");
                members.Add(NYT::TNode::CreateList()
                        .Add("message")
                        .Add(typeNode)
                    );
            }
            {
                auto typeNode = NYT::TNode::CreateList()
                        .Add("DataType")
                        .Add("UInt64");
                members.Add(NYT::TNode::CreateList()
                        .Add("offset")
                        .Add(typeNode)
                    );
            }

            NYT::TNode schema = NYT::TNode::CreateList()
                    .Add("StructType")
                    .Add(members);

            return schema;
        };

        class TISchema : public TInputSpecBase {
        public:
            TISchema(TVector<NYT::TNode> spec)
                : Spec(spec) {}
            ~TISchema() = default;

            const TVector<NYT::TNode>& GetSchemas() const override {
                return Spec;
            }

            const TVector<NYT::TNode> Spec;
        };

        auto oscheme = []() -> NYT::TNode {
            NYT::TNode members {NYT::TNode::CreateList()};

            {
                auto typeNode = NYT::TNode::CreateList()
                        .Add("DataType")
                        .Add("String");
                members.Add(NYT::TNode::CreateList()
                        .Add("omg")
                        .Add(typeNode)
                    );
            }
            {
                auto typeNode = NYT::TNode::CreateList()
                        .Add("DataType")
                        .Add("UInt64");
                members.Add(NYT::TNode::CreateList()
                        .Add("offset")
                        .Add(typeNode)
                    );
            }

            NYT::TNode schema = NYT::TNode::CreateList()
                    .Add("StructType")
                    .Add(members);

            return schema;
        };

        class TOScheme : public TOutputSpecBase {
        public:
            TOScheme(NYT::TNode spec)
                : Spec(spec) {}

            ~TOScheme() = default;

            const NYT::TNode& GetSchema() const {
                return Spec;
            }

            const NYT::TNode Spec;
        };

        auto factory = MakeProgramFactory(
            TProgramFactoryOptions()
        );

        auto program = factory->MakePullStreamProgram(
            TISchema({ischeme()}),
            TOScheme(oscheme()),
            lambda,
            ETranslationMode::SQL);

        TVector<NYT::TNode> input;
        {
            auto m = NYT::TNode::CreateMap();
            m["message"] = NYT::TNode("value");
            m["offset"] = NYT::TNode(123);
            input.push_back(m);
        }


        auto result = program->Apply(StreamFromVector(input));

        while (auto* message = result->Fetch()) {
            Cout << "path = " << message->GetPath() << Endl;
            Cout << "host = " << message->GetHost() << Endl;
        }
    }
}

}
