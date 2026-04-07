
#include <ydb/core/kqp/compile_service/helpers/kqp_compile_service_helpers.h>
#include <ydb/core/protos/table_service_config.pb.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/kqp_compile_settings.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <google/protobuf/descriptor.h>
#include "google/protobuf/descriptor.pb.h"

namespace NKikimr {
namespace NKqp {

Y_UNIT_TEST_SUITE(KqpCompileServiceHelpers) {
    Y_UNIT_TEST(CheckInvalidator) {

        NKikimrConfig::TTableServiceConfig prev;
        NKikimrConfig::TTableServiceConfig defaultEmpty;
        Cerr << ShouldInvalidateCompileCache(prev, defaultEmpty).value_or("empty") << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ShouldInvalidateCompileCache(prev, defaultEmpty).value_or("empty"), "empty");

        prev.MutableResourceManager()->SetMkqlHeavyProgramMemoryLimit(31457280);

        Cerr << prev.ShortUtf8DebugString() << Endl;
        Cerr << defaultEmpty.ShortUtf8DebugString() << Endl;
        Cerr << ShouldInvalidateCompileCache(prev, defaultEmpty).value_or("empty") << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ShouldInvalidateCompileCache(prev, defaultEmpty).value_or("empty"), "empty");

        prev.MutableResourceManager()->SetMkqlHeavyProgramMemoryLimit(31457281);

        Cerr << prev.ShortUtf8DebugString() << Endl;
        Cerr << defaultEmpty.ShortUtf8DebugString() << Endl;
        Cerr << ShouldInvalidateCompileCache(prev, defaultEmpty).value_or("empty") << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ShouldInvalidateCompileCache(prev, defaultEmpty).value_or("empty"), "modified: MkqlHeavyProgramMemoryLimit: 31457281 -> 31457280\n");


        prev.SetEnableKqpScanQuerySourceRead(true);
        Cerr << prev.ShortUtf8DebugString() << Endl;
        Cerr << defaultEmpty.ShortUtf8DebugString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ShouldInvalidateCompileCache(prev, defaultEmpty).value_or("empty"), "modified: EnableKqpScanQuerySourceRead: true -> false\nmodified: MkqlHeavyProgramMemoryLimit: 31457281 -> 31457280\n");

        prev.SetEnableKqpScanQuerySourceRead(true);
        prev.SetEnableKqpScanQueryStreamIdxLookupJoin(true);
        Cerr << prev.ShortUtf8DebugString() << Endl;
        Cerr << defaultEmpty.ShortUtf8DebugString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ShouldInvalidateCompileCache(prev, defaultEmpty).value_or("empty"), "modified: EnableKqpScanQuerySourceRead: true -> false\nmodified: EnableKqpScanQueryStreamIdxLookupJoin: true -> false\nmodified: MkqlHeavyProgramMemoryLimit: 31457281 -> 31457280\n");

        defaultEmpty.SetEnableKqpScanQuerySourceRead(true);
        defaultEmpty.SetEnableKqpScanQueryStreamIdxLookupJoin(true);
        Cerr << prev.ShortUtf8DebugString() << Endl;
        Cerr << defaultEmpty.ShortUtf8DebugString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ShouldInvalidateCompileCache(prev, defaultEmpty).value_or("empty"), "modified: MkqlHeavyProgramMemoryLimit: 31457281 -> 31457280\n");

        defaultEmpty.SetEnableKqpScanQuerySourceRead(true);
        defaultEmpty.MutableResourceManager()->SetMkqlHeavyProgramMemoryLimit(31457281);
        Cerr << prev.ShortUtf8DebugString() << Endl;
        Cerr << defaultEmpty.ShortUtf8DebugString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ShouldInvalidateCompileCache(prev, defaultEmpty).value_or("empty"), "empty");
    }

    Y_UNIT_TEST(OnlyRmAndTopLevelOptionsAreSupportedToInvalidate)
    {
        auto topLevel = NKikimrConfig::TTableServiceConfig::descriptor();
        auto rm = NKikimrConfig::TTableServiceConfig::TResourceManager::descriptor();

        std::deque<const ::google::protobuf::Descriptor *> traversal;
        traversal.push_back(NKikimrConfig::TAppConfig::descriptor());
        std::unordered_set<const ::google::protobuf::Descriptor *> visited;

        while(!traversal.empty()) {
            const auto* d = traversal.front();
            traversal.pop_front();

            if (visited.contains(d)) {
                continue;
            }

            visited.emplace(d);

            for (int fieldIndex = 0; fieldIndex < d->field_count(); ++fieldIndex) {
                const auto* field = d->field(fieldIndex);
                if (field->options().GetExtension(NKikimrConfig::InvalidateCompileCache)) {
                    UNIT_ASSERT_C(d->name() == topLevel->name() || d->name() == rm->name(),
                        "Only TTableServiceConfig or ResourceManager fields can have InvalidateCompileCache extention, "
                        "but " << d->name() << " found." <<
                        "If you want more, update ShouldInvalidateCompileCache accordingly.");
                }

                if (field->cpp_type() == ::google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
                    traversal.push_back(field->message_type());
                }
            }
        }
        UNIT_ASSERT(visited.contains(rm));
        UNIT_ASSERT(visited.contains(topLevel));
    }
}
}
}