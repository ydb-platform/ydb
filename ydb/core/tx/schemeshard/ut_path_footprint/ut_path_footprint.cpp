#include <ydb/core/tx/schemeshard/schemeshard_audit_log_fragment.h>
#include <ydb/core/tx/schemeshard/schemeshard_path_footprint.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <google/protobuf/descriptor.h>

#include <library/cpp/logger/backend.h>
#include <library/cpp/logger/record.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/split.h>

using namespace NKikimr;
using namespace NKikimr::NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

////////////////////////////////////////////////////////////////////////////////
// Layer-1 helpers




////////////////////////////////////////////////////////////////////////////////
}  // namespace

Y_UNIT_TEST_SUITE(TSchemeShardPathFootprintExtract) {

    Y_UNIT_TEST(EveryPathFieldRendersAndIsListedOnce) {
        const size_t count = static_cast<size_t>(EPathField::Count);
        UNIT_ASSERT_C(count > 100, "the field table has only " << count << " rows");

        THashSet<TString> templates;
        THashSet<TString> protoNames;
        size_t synthetic = 0;
        for (size_t i = 0; i < count; ++i) {
            const auto field = static_cast<EPathField>(i);
            const TString tmpl(PathFieldName(field));
            UNIT_ASSERT_C(!tmpl.empty(), "field " << i << " has no field-path template");
            // A template is the identity of a field path: two rows rendering
            // the same string would be indistinguishable in a log line.
            UNIT_ASSERT_C(templates.insert(tmpl).second,
                "two path fields share the field-path template " << tmpl);

            // Rendering substitutes every placeholder and leaves no brace.
            TPathRef ref;
            ref.Field = field;
            ref.Index = 3;
            ref.SubIndex = 7;
            ref.MapKey = "someKey";
            const TString rendered = FieldPath(ref);
            UNIT_ASSERT_C(rendered.find('{') == TString::npos
                    && rendered.find('}') == TString::npos,
                "unexpanded placeholder in " << rendered);
            if (tmpl.Contains("{i}")) {
                UNIT_ASSERT_C(rendered.Contains("[3]"), rendered);
            }
            if (tmpl.Contains("{j}")) {
                UNIT_ASSERT_C(rendered.Contains("[7]"), rendered);
            }
            if (tmpl.Contains("{key}")) {
                UNIT_ASSERT_C(rendered.Contains("[someKey]"), rendered);
            }
            if (tmpl.find('{') == TString::npos) {
                UNIT_ASSERT_VALUES_EQUAL(rendered, tmpl);
            }

            const TString proto(PathFieldProtoName(field));
            if (proto.empty()) {
                ++synthetic;
            } else {
                protoNames.insert(proto);
            }
        }
        UNIT_ASSERT_C(synthetic > 0, "no synthetic (marker or id) field rows");

        // KnownPathFieldNames() is exactly the non-empty proto column,
        // deduplicated and sorted: the descriptor walk uses it as a set, and a
        // duplicate would hide a second field behind the first.
        const auto& known = KnownPathFieldNames();
        THashSet<TString> knownSet;
        for (const TStringBuf name : known) {
            UNIT_ASSERT_C(!name.empty(), "KnownPathFieldNames() has an empty entry");
            UNIT_ASSERT_C(knownSet.insert(TString(name)).second,
                "KnownPathFieldNames() lists " << name << " twice");
        }
        UNIT_ASSERT_VALUES_EQUAL(known.size(), protoNames.size());
        for (const auto& name : protoNames) {
            UNIT_ASSERT_C(knownSet.contains(name),
                name << " is in the field table but not in KnownPathFieldNames()");
        }
        UNIT_ASSERT_C(IsSorted(known.begin(), known.end()),
            "KnownPathFieldNames() is not sorted");
    }

    // Extraction reads the request, it does not copy it: every value is a view
    // into the TModifyScheme that was passed in. Only the resolve step, which
    // has to outlive the request, materializes strings.

}
