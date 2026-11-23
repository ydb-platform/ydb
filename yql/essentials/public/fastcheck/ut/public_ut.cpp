#include <yql/essentials/public/fastcheck/fastcheck.h>
#include <yql/essentials/public/fastcheck/linter.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NFastCheck {
namespace {

constexpr TStringBuf Metadata = R"json({
    "Example": [
        {"name":"Plain", "argCount":0, "callableType":"[\"CallableType\";[];[[\"DataType\";\"Int32\"]];[]]"},
        {"name":"Value", "argCount":0,
         "callableType":"[\"CallableType\";[];[[\"DataType\";\"Int32\"]];[]]",
         "runConfigType":"[\"DataType\";\"String\"]", "isStrict":true,
         "supportsBlocks":true, "minLangVer":"2025.01", "maxLangVer":"2025.04"},
        {"name":"Optional", "argCount":1, "optionalArgCount":1, "callableType":"type\u0000tail"},
        {"name":"Poly", "isTypeAwareness":true, "polyArgs":"arguments", "resolvedCallableTypes":"types"}
    ]
})json";

} // namespace

Y_UNIT_TEST_SUITE(TFastCheckPublic) {
Y_UNIT_TEST(MovedMetadataOwnsCookieAndPreservesViews) {
    TMaybe<TUdfMeta::TMeta> view;
    const auto metadata = [&] {
        auto original = LoadUdfMeta(Metadata);
        view = original.GetMetadata("example", "plain");
        return TUdfMeta(std::move(original));
    }();
    UNIT_ASSERT(metadata.HasFunction("example", "plain"));
    UNIT_ASSERT(view);
    UNIT_ASSERT_VALUES_EQUAL(view->CallableType, metadata.GetMetadata("example", "plain")->CallableType);
    TChecksRequest request;
    request.Program = "SELECT Example::Plain() + 1";
    request.Filters = ParseChecks("typecheck");
    request.UdfMeta = &metadata;
    const auto result = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(result.Checks.size(), 1);
    UNIT_ASSERT_C(result.Checks.front().Success, result.Checks.front().Issues.ToString());
}

Y_UNIT_TEST(MetadataIsBorrowedByRequests) {
    const auto metadata = LoadUdfMeta(Metadata);
    TChecksRequest request;
    request.Program = "SELECT Example::Plain() + 1";
    request.Filters = ParseChecks("typecheck");
    request.UdfMeta = &metadata;
    for (size_t call = 0; call < 2; ++call) {
        const auto result = RunChecks(request);
        UNIT_ASSERT_VALUES_EQUAL(result.Checks.size(), 1);
        UNIT_ASSERT_C(result.Checks.front().Success, result.Checks.front().Issues.ToString());
        UNIT_ASSERT(metadata.HasFunction("example", "plain"));
    }
}

Y_UNIT_TEST(MetadataViewsRemainValidAcrossLookups) {
    const auto metadata = LoadUdfMeta(Metadata);
    UNIT_ASSERT(metadata.HasModule("example"));
    UNIT_ASSERT(metadata.HasFunction("example", "value"));
    const auto first = metadata.GetMetadata("example", "value");
    UNIT_ASSERT(first);
    const auto optional = metadata.GetMetadata("example", "optional");
    UNIT_ASSERT(optional);
    UNIT_ASSERT_VALUES_EQUAL(optional->ArgCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(optional->OptionalArgCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(optional->CallableType, TStringBuf("type\0tail", 9));
    UNIT_ASSERT(!metadata.GetMetadata("example", "missing"));
    const auto again = metadata.GetMetadata("example", "value");
    UNIT_ASSERT(again);
    UNIT_ASSERT(first->CallableType.data() == again->CallableType.data());
    UNIT_ASSERT_VALUES_EQUAL(first->RunConfigType, "[\"DataType\";\"String\"]");
    UNIT_ASSERT_VALUES_EQUAL(first->ArgCount, 0);
    UNIT_ASSERT(first->IsStrict);
    UNIT_ASSERT(first->SupportsBlocks);
    UNIT_ASSERT_VALUES_EQUAL(first->MinLangVer, MakeLangVersion(2025, 1));
    UNIT_ASSERT_VALUES_EQUAL(first->MaxLangVer, MakeLangVersion(2025, 4));
}

Y_UNIT_TEST(PolymorphicMetadataViews) {
    const auto metadata = LoadUdfMeta(Metadata);
    const auto value = metadata.GetMetadata("example", "poly");
    UNIT_ASSERT(value);
    UNIT_ASSERT(value->IsTypeAwareness);
    UNIT_ASSERT_VALUES_EQUAL(value->PolyArgs, "arguments");
    UNIT_ASSERT_VALUES_EQUAL(value->ResolvedCallableTypes, "types");
    UNIT_ASSERT(value->CallableType.empty());
}

Y_UNIT_TEST(EmptyMetadataAndNames) {
    const auto metadata = LoadUdfMeta("{}");
    UNIT_ASSERT(!metadata.HasModule(""));
    UNIT_ASSERT(!metadata.HasFunction("", ""));
    UNIT_ASSERT(!metadata.GetMetadata("", ""));
    UNIT_ASSERT(!metadata.HasModule("missing"));
}

Y_UNIT_TEST(InvalidMetadataThrowsAndNextLoadWorks) {
    UNIT_ASSERT_EXCEPTION(LoadUdfMeta(""), yexception);
    UNIT_ASSERT_EXCEPTION(LoadUdfMeta("!"), yexception);
    UNIT_ASSERT_EXCEPTION(LoadUdfMeta("[]"), yexception);
    UNIT_ASSERT(LoadUdfMeta(Metadata).HasModule("example"));
}

Y_UNIT_TEST(PreservesPermissiveJsonParsing) {
    UNIT_ASSERT(!LoadUdfMeta("{").HasModule("example"));
}

Y_UNIT_TEST(MetadataCookieIsReusableAcrossChecks) {
    const auto metadata = LoadUdfMeta(Metadata);
    const auto view = metadata.GetMetadata("example", "value");
    TChecksRequest request;
    request.UdfMeta = &metadata;
    request.Filters = ParseChecks("translator");
    for (const TString program : {"SELECT Example::Value()", "SELECT Example::Missing()", "SELECT Example::Value()"}) {
        request.Program = program;
        const auto response = RunChecks(request);
        UNIT_ASSERT_VALUES_EQUAL(response.Checks.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response.Checks.front().Success, program != "SELECT Example::Missing()");
    }
    UNIT_ASSERT_VALUES_EQUAL(view->CallableType, metadata.GetMetadata("example", "value")->CallableType);
}

Y_UNIT_TEST(ExplicitEmptyFiltersRemainEmpty) {
    TChecksRequest request;
    request.Program = "select broken syntax";
    request.Filters.ConstructInPlace();
    UNIT_ASSERT(RunChecks(request).Checks.empty());
    UNIT_ASSERT(ListChecks(request.Filters).empty());
    UNIT_ASSERT(!ListChecks().empty());
}

Y_UNIT_TEST(TypeAnnotationReadsBorrowedMetadata) {
    const auto metadata = LoadUdfMeta(Metadata);
    TChecksRequest request;
    request.UdfMeta = &metadata;
    request.Filters = ParseChecks("typecheck");
    request.Program = "SELECT Example::Plain() + 1";
    const auto good = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(good.Checks.size(), 1);
    UNIT_ASSERT_C(good.Checks.front().Success, good.Checks.front().Issues.ToString());
    request.Program = "SELECT Example::Plain() || 'text'";
    const auto bad = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(bad.Checks.size(), 1);
    UNIT_ASSERT(!bad.Checks.front().Success);
    UNIT_ASSERT(!bad.Checks.front().Issues.Empty());
}

Y_UNIT_TEST(CheckProgramAppendsIssues) {
    TIssues issues;
    issues.AddIssue(TIssue(TPosition(4, 3, "before.sql"), "existing diagnostic"));
    UNIT_ASSERT(!CheckProgram("select1;", {}, issues));
    UNIT_ASSERT(issues.Size() > 1);
    UNIT_ASSERT_VALUES_EQUAL(issues.begin()->GetMessage(), "existing diagnostic");
    UNIT_ASSERT_VALUES_EQUAL(issues.begin()->Position.Row, 3);
    UNIT_ASSERT_VALUES_EQUAL(issues.begin()->Position.Column, 4);
}

} // Y_UNIT_TEST_SUITE(TFastCheckPublic)

} // namespace NYql::NFastCheck
