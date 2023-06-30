#include "yql_udf_index.h"
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;
namespace {
class TResourceBuilder {
    TIntrusivePtr<TResourceInfo> Resource_ = new TResourceInfo();

public:
    explicit TResourceBuilder(const TDownloadLink& link) {
        Resource_->Link = link;
    }

    TResourceBuilder& AddFunction(const TFunctionInfo& f) {
        auto module = TString(NKikimr::NMiniKQL::ModuleName(TStringBuf(f.Name)));
        Resource_->Modules.insert(module);
        Resource_->SetFunctions({ f });
        return *this;
    }

    TResourceInfo::TPtr Build() const {
        return Resource_;
    }
};

TFunctionInfo BuildFunctionInfo(const TString& name, int argCount) {
    TFunctionInfo result;
    result.Name = name;
    result.ArgCount = argCount;
    return result;
}

void EnsureFunctionsEqual(const TFunctionInfo& f1, const TFunctionInfo& f2) {
    UNIT_ASSERT_VALUES_EQUAL(f1.Name, f2.Name);
    UNIT_ASSERT_VALUES_EQUAL(f1.ArgCount, f2.ArgCount);
}

void EnsureLinksEqual(const TDownloadLink& link1, const TDownloadLink& link2) {
    UNIT_ASSERT_VALUES_EQUAL(link1.IsUrl, link2.IsUrl);
    UNIT_ASSERT_VALUES_EQUAL(link1.Path, link2.Path);
}

void EnsureContainsFunction(TUdfIndex::TPtr index, TString module, const TFunctionInfo& f) {
    TFunctionInfo existingFunc;
    UNIT_ASSERT(index->FindFunction(module, f.Name, existingFunc));
    EnsureFunctionsEqual(f, existingFunc);
}
}

Y_UNIT_TEST_SUITE(TUdfIndexTests) {
    Y_UNIT_TEST(Empty) {
        auto index1 = MakeIntrusive<TUdfIndex>();

        UNIT_ASSERT(!index1->ContainsModule("M1"));
        UNIT_ASSERT(index1->FindResourceByModule("M1") == nullptr);
        TFunctionInfo f1;
        UNIT_ASSERT(!index1->FindFunction("M1", "M1.F1", f1));

        auto index2 = index1->Clone();
        UNIT_ASSERT(!index2->ContainsModule("M1"));
        UNIT_ASSERT(index2->FindResourceByModule("M1") == nullptr);
        UNIT_ASSERT(!index2->FindFunction("M1", "M1.F1", f1));
    }

    Y_UNIT_TEST(SingleModuleAndFunction) {
        auto index1 = MakeIntrusive<TUdfIndex>();
        auto func1 = BuildFunctionInfo("M1.F1", 1);
        auto link1 = TDownloadLink::File("file1");

        TResourceBuilder b(link1);
        b.AddFunction(func1);

        index1->RegisterResource(b.Build(), TUdfIndex::EOverrideMode::RaiseError);
        UNIT_ASSERT(index1->ContainsModule("M1"));
        UNIT_ASSERT(!index1->ContainsModule("M2"));

        UNIT_ASSERT(index1->FindResourceByModule("M2") == nullptr);
        auto resource1 = index1->FindResourceByModule("M1");
        UNIT_ASSERT(resource1 != nullptr);
        EnsureLinksEqual(resource1->Link, link1);

        TFunctionInfo f1;
        UNIT_ASSERT(!index1->FindFunction("M2", "M2.F1", f1));

        UNIT_ASSERT(index1->FindFunction("M1", "M1.F1", f1));
        EnsureFunctionsEqual(f1, func1);

        // ensure both indexes contain the same info
        auto index2 = index1->Clone();

        UNIT_ASSERT(index1->ContainsModule("M1"));
        UNIT_ASSERT(index2->ContainsModule("M1"));

        TFunctionInfo f2;
        UNIT_ASSERT(index2->FindFunction("M1", "M1.F1", f2));
        EnsureFunctionsEqual(f1, f2);

        auto resource2 = index2->FindResourceByModule("M1");
        UNIT_ASSERT(resource2 != nullptr);
    }

    Y_UNIT_TEST(SeveralModulesAndFunctions) {
        auto index1 = MakeIntrusive<TUdfIndex>();
        auto func11 = BuildFunctionInfo("M1.F1", 1);
        auto func12 = BuildFunctionInfo("M1.F2", 2);
        auto func13 = BuildFunctionInfo("M2.F1", 3);

        auto link1 = TDownloadLink::File("file1");
        auto resource1 = TResourceBuilder(link1)
            .AddFunction(func11)
            .AddFunction(func12)
            .AddFunction(func13)
            .Build();

        auto func21 = BuildFunctionInfo("M3.F1", 4);
        auto func22 = BuildFunctionInfo("M4.F2", 5);

        auto link2 = TDownloadLink::Url("url1");
        auto resource2 = TResourceBuilder(link2)
            .AddFunction(func21)
            .AddFunction(func22)
            .Build();

        index1->RegisterResource(resource1, TUdfIndex::EOverrideMode::RaiseError);
        index1->RegisterResource(resource2, TUdfIndex::EOverrideMode::RaiseError);

        // check resources by module
        UNIT_ASSERT(index1->FindResourceByModule("M5") == nullptr);
        auto r11 = index1->FindResourceByModule("M1");
        auto r12 = index1->FindResourceByModule("M2");
        UNIT_ASSERT(r11 != nullptr && r12 != nullptr);
        EnsureLinksEqual(r11->Link, link1);
        EnsureLinksEqual(r12->Link, link1);

        auto r21 = index1->FindResourceByModule("M3");
        auto r22 = index1->FindResourceByModule("M4");
        UNIT_ASSERT(r21 != nullptr && r22 != nullptr);
        EnsureLinksEqual(r21->Link, link2);
        EnsureLinksEqual(r22->Link, link2);

        // check modules
        UNIT_ASSERT(index1->ContainsModule("M1"));
        UNIT_ASSERT(index1->ContainsModule("M2"));
        UNIT_ASSERT(index1->ContainsModule("M3"));
        UNIT_ASSERT(index1->ContainsModule("M4"));
        UNIT_ASSERT(!index1->ContainsModule("M5"));

        EnsureContainsFunction(index1, "M1", func11);
        EnsureContainsFunction(index1, "M1", func12);
        EnsureContainsFunction(index1, "M2", func13);
        EnsureContainsFunction(index1, "M3", func21);
        EnsureContainsFunction(index1, "M4", func22);

        // works because M2 refers to the same resource
        EnsureContainsFunction(index1, "M2", func11);

        TFunctionInfo f;
        // known func, but non-existent module
        UNIT_ASSERT(!index1->FindFunction("M5", "M1.F1", f));
        UNIT_ASSERT(!index1->FindFunction("M2", "M3.F1", f));
    }

    Y_UNIT_TEST(ConflictRaiseError) {
        auto index1 = MakeIntrusive<TUdfIndex>();
        auto func11 = BuildFunctionInfo("M1.F1", 1);
        auto func12 = BuildFunctionInfo("M1.F2", 2);
        auto func13 = BuildFunctionInfo("M2.F1", 3);

        auto link1 = TDownloadLink::File("file1");
        auto resource1 = TResourceBuilder(link1)
            .AddFunction(func11)
            .AddFunction(func12)
            .AddFunction(func13)
            .Build();

        auto func21 = BuildFunctionInfo("M3.F1", 4);
        auto func22 = BuildFunctionInfo("M2.F1", 5);

        auto link2 = TDownloadLink::Url("url1");
        auto resource2 = TResourceBuilder(link2)
            .AddFunction(func21)
            .AddFunction(func22)
            .Build();

        index1->RegisterResource(resource1, TUdfIndex::EOverrideMode::RaiseError);
        UNIT_ASSERT_EXCEPTION_CONTAINS(index1->RegisterResource(resource2, TUdfIndex::EOverrideMode::RaiseError), std::exception, "Conflict during resource url1 registration");

        // ensure state untouched
        UNIT_ASSERT(index1->FindResourceByModule("M3") == nullptr);
        auto r1 = index1->FindResourceByModule("M1");
        auto r2 = index1->FindResourceByModule("M2");
        UNIT_ASSERT(r1 != nullptr && r2 != nullptr);
        EnsureLinksEqual(r1->Link, link1);
        EnsureLinksEqual(r2->Link, link1);

        EnsureContainsFunction(index1, "M1", func11);
        EnsureContainsFunction(index1, "M1", func12);
        EnsureContainsFunction(index1, "M2", func13);

        TFunctionInfo f;
        UNIT_ASSERT(!index1->FindFunction("M3", "M3.F1", f));
    }

    Y_UNIT_TEST(ConflictPreserveExisting) {
        auto index1 = MakeIntrusive<TUdfIndex>();
        auto func11 = BuildFunctionInfo("M1.F1", 1);
        auto func12 = BuildFunctionInfo("M1.F2", 2);
        auto func13 = BuildFunctionInfo("M2.F1", 3);

        auto link1 = TDownloadLink::File("file1");
        auto resource1 = TResourceBuilder(link1)
            .AddFunction(func11)
            .AddFunction(func12)
            .AddFunction(func13)
            .Build();

        auto func21 = BuildFunctionInfo("M3.F1", 4);
        auto func22 = BuildFunctionInfo("M2.F1", 5);

        auto link2 = TDownloadLink::Url("url1");
        auto resource2 = TResourceBuilder(link2)
            .AddFunction(func21)
            .AddFunction(func22)
            .Build();

        index1->RegisterResource(resource1, TUdfIndex::EOverrideMode::RaiseError);
        index1->RegisterResource(resource2, TUdfIndex::EOverrideMode::PreserveExisting);

        // ensure state untouched
        UNIT_ASSERT(index1->FindResourceByModule("M3") == nullptr);
        auto r1 = index1->FindResourceByModule("M1");
        auto r2 = index1->FindResourceByModule("M2");
        UNIT_ASSERT(r1 != nullptr && r2 != nullptr);
        EnsureLinksEqual(r1->Link, link1);
        EnsureLinksEqual(r2->Link, link1);

        EnsureContainsFunction(index1, "M1", func11);
        EnsureContainsFunction(index1, "M1", func12);
        EnsureContainsFunction(index1, "M2", func13);

        TFunctionInfo f;
        UNIT_ASSERT(!index1->FindFunction("M3", "M3.F1", f));
    }

    Y_UNIT_TEST(ConflictReplace1WithNew) {
        // single resource will be replaced
        auto index1 = MakeIntrusive<TUdfIndex>();
        auto func11 = BuildFunctionInfo("M1.F1", 1);
        auto func12 = BuildFunctionInfo("M1.F2", 2);
        auto func13 = BuildFunctionInfo("M2.F1", 3);

        auto link1 = TDownloadLink::File("file1");
        auto resource1 = TResourceBuilder(link1)
            .AddFunction(func11)
            .AddFunction(func12)
            .AddFunction(func13)
            .Build();

        auto func21 = BuildFunctionInfo("M3.F3", 4);
        auto func22 = BuildFunctionInfo("M4.F4", 5);

        auto link2 = TDownloadLink::Url("url1");
        auto resource2 = TResourceBuilder(link2)
            .AddFunction(func21)
            .AddFunction(func22)
            .Build();


        auto func31 = BuildFunctionInfo("M5.F5", 6);
        // conflict by module name
        auto func32 = BuildFunctionInfo("M1.F7", 7);

        auto link3 = TDownloadLink::Url("url3");
        auto resource3 = TResourceBuilder(link3)
            .AddFunction(func31)
            .AddFunction(func32)
            .Build();

        index1->RegisterResource(resource1, TUdfIndex::EOverrideMode::RaiseError);
        index1->RegisterResource(resource2, TUdfIndex::EOverrideMode::RaiseError);
        index1->RegisterResource(resource3, TUdfIndex::EOverrideMode::ReplaceWithNew);

        UNIT_ASSERT(index1->FindResourceByModule("M2") == nullptr);
        auto r1 = index1->FindResourceByModule("M3");
        UNIT_ASSERT(r1 != nullptr);
        EnsureLinksEqual(r1->Link, link2);

        auto r2 = index1->FindResourceByModule("M1");
        UNIT_ASSERT(r2 != nullptr);
        EnsureLinksEqual(r2->Link, link3);

        // ensure untouched
        EnsureContainsFunction(index1, "M3", func21);
        EnsureContainsFunction(index1, "M4", func22);

        EnsureContainsFunction(index1, "M5", func31);
        EnsureContainsFunction(index1, "M1", func32);

        // not here anymore
        TFunctionInfo f;
        UNIT_ASSERT(!index1->FindFunction("M1", "M1.F1", f));
        UNIT_ASSERT(!index1->FindFunction("M1", "M1.F2", f));
        UNIT_ASSERT(!index1->FindFunction("M2", "M2.F1", f));
    }

    Y_UNIT_TEST(ConflictReplace2WithNew) {
        // both resources will be replaced
        auto index1 = MakeIntrusive<TUdfIndex>();
        auto func11 = BuildFunctionInfo("M1.F1", 1);
        auto func12 = BuildFunctionInfo("M1.F2", 2);
        auto func13 = BuildFunctionInfo("M2.F1", 3);

        auto link1 = TDownloadLink::File("file1");
        auto resource1 = TResourceBuilder(link1)
            .AddFunction(func11)
            .AddFunction(func12)
            .AddFunction(func13)
            .Build();

        auto func21 = BuildFunctionInfo("M3.F3", 4);
        auto func22 = BuildFunctionInfo("M4.F4", 5);

        auto link2 = TDownloadLink::Url("url1");
        auto resource2 = TResourceBuilder(link2)
            .AddFunction(func21)
            .AddFunction(func22)
            .Build();


        // conflict by module name
        auto func31 = BuildFunctionInfo("M3.F7", 6);
        // conflict by func name
        auto func32 = BuildFunctionInfo("M1.F1", 7);

        auto link3 = TDownloadLink::Url("url3");
        auto resource3 = TResourceBuilder(link3)
            .AddFunction(func31)
            .AddFunction(func32)
            .Build();

        index1->RegisterResource(resource1, TUdfIndex::EOverrideMode::RaiseError);
        index1->RegisterResource(resource2, TUdfIndex::EOverrideMode::RaiseError);
        index1->RegisterResource(resource3, TUdfIndex::EOverrideMode::ReplaceWithNew);

        UNIT_ASSERT(index1->FindResourceByModule("M2") == nullptr);
        UNIT_ASSERT(index1->FindResourceByModule("M4") == nullptr);
        auto r1 = index1->FindResourceByModule("M3");
        UNIT_ASSERT(r1 != nullptr);
        EnsureLinksEqual(r1->Link, link3);

        auto r2 = index1->FindResourceByModule("M1");
        UNIT_ASSERT(r2 != nullptr);
        EnsureLinksEqual(r2->Link, link3);

        // ensure untouched
        EnsureContainsFunction(index1, "M3", func31);
        EnsureContainsFunction(index1, "M1", func32);

        // not here anymore
        TFunctionInfo f;
        UNIT_ASSERT(!index1->FindFunction("M1", "M1.F2", f));
        UNIT_ASSERT(!index1->FindFunction("M2", "M2.F1", f));

        UNIT_ASSERT(!index1->FindFunction("M3", "M3.F3", f));
        UNIT_ASSERT(!index1->FindFunction("M4", "M4.F4", f));
    }
}
