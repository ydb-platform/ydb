#include "registry.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NConsole {

namespace {

class TTestValidator : public IConfigValidator {
public:
    TTestValidator(const TString &name,
                   THashSet<ui32> kinds)
        : IConfigValidator(name, kinds)
        , Counter(0)
        , Limit(Max<ui64>())
    {
    }

    TTestValidator(const TString &name,
                   ui32 kind)
        : IConfigValidator(name, kind)
        , Counter(0)
        , Limit(Max<ui64>())
    {
    }

    TTestValidator(const TString &name)
        : IConfigValidator(name, THashSet<ui32>())
        , Counter(0)
        , Limit(Max<ui64>())
    {
    }

    ui64 GetCounter() const
    {
        return Counter;
    }

    bool CheckConfig(const NKikimrConfig::TAppConfig &newConfig,
                     const NKikimrConfig::TAppConfig &oldConfig,
                     TVector<Ydb::Issue::IssueMessage> &issues) const override
    {
        Y_UNUSED(oldConfig);
        Y_UNUSED(newConfig);
        Y_UNUSED(issues);
        ++Counter;
        return Counter <= Limit;
    }

    void SetLimit(ui64 limit)
    {
        Limit = limit;
    }

private:
    mutable volatile ui64 Counter;
    ui64 Limit;
};

void CollectClasses(THashSet<TDynBitMap> &classes)
{
    Y_UNUSED(classes);
}

template<typename ...Ts>
void CollectClasses(THashSet<TDynBitMap> &classes,
                    ui32 kind,
                    Ts... args);
template<typename ...Ts>
void CollectClasses(THashSet<TDynBitMap> &classes,
                    THashSet<ui32> kinds,
                    Ts... args);

template<typename ...Ts>
void CollectClasses(THashSet<TDynBitMap> &classes,
                    ui32 kind,
                    Ts... args)
{
    TDynBitMap kinds;
    kinds.Set(kind);
    classes.insert(kinds);
    CollectClasses(classes, args...);
}

template<typename ...Ts>
void CollectClasses(THashSet<TDynBitMap> &classes,
                    THashSet<ui32> kinds,
                    Ts... args)
{
    TDynBitMap map;
    for (auto kind : kinds)
        map.Set(kind);
    classes.insert(map);
    CollectClasses(classes, args...);
}

template<typename ...Ts>
void CheckValidatorClasses(Ts... args)
{
    THashSet<TDynBitMap> classes;
    CollectClasses(classes, args...);

    UNIT_ASSERT_VALUES_EQUAL(TValidatorsRegistry::Instance()->GetValidatorClasses().size(),
                             classes.size());
    for (auto kinds : TValidatorsRegistry::Instance()->GetValidatorClasses()) {
        UNIT_ASSERT(classes.contains(kinds));
    }
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TRegistryTests) {
    Y_UNIT_TEST(TestAddGet) {
        TValidatorsRegistry::DropInstance();
        auto registry = TValidatorsRegistry::Instance();
        UNIT_ASSERT(registry->AddValidator(new TTestValidator("test1", 1)));
        UNIT_ASSERT(registry->AddValidator(new TTestValidator("test2", 1)));
        UNIT_ASSERT(registry->AddValidator(new TTestValidator("test3", 1)));
        UNIT_ASSERT(!registry->AddValidator(new TTestValidator("test1", 1)));

        registry->LockValidators();

        UNIT_ASSERT(registry->GetValidator("test1"));
        UNIT_ASSERT(registry->GetValidator("test2"));
        UNIT_ASSERT(registry->GetValidator("test3"));
        UNIT_ASSERT(!registry->GetValidator("test4"));
        UNIT_ASSERT_VALUES_EQUAL(registry->GetValidators().size(), 3);
    }

    Y_UNIT_TEST(TestLock) {
        TValidatorsRegistry::DropInstance();
        auto registry = TValidatorsRegistry::Instance();
        UNIT_ASSERT(registry->AddValidator(new TTestValidator("test1", 1)));
        UNIT_ASSERT(!registry->IsLocked());
        registry->LockValidators();
        UNIT_ASSERT(!registry->AddValidator(new TTestValidator("test2", 1)));
        UNIT_ASSERT(registry->IsLocked());
        registry->LockValidators();
        UNIT_ASSERT(registry->IsLocked());
    }

    Y_UNIT_TEST(TestClasses) {
        TValidatorsRegistry::DropInstance();
        auto registry = TValidatorsRegistry::Instance();
        UNIT_ASSERT(registry->AddValidator(new TTestValidator("test1", 1)));
        UNIT_ASSERT(registry->AddValidator(new TTestValidator("test2", 1)));
        UNIT_ASSERT(registry->AddValidator(new TTestValidator("test3", 2)));
        UNIT_ASSERT(registry->AddValidator(new TTestValidator("test4")));
        UNIT_ASSERT(registry->AddValidator(new TTestValidator("test5", THashSet<ui32>({1, 2, 3}))));
        UNIT_ASSERT(registry->AddValidator(new TTestValidator("test6", THashSet<ui32>({1, 2, 3}))));
        registry->LockValidators();

        CheckValidatorClasses(1, 2, THashSet<ui32>(), THashSet<ui32>({1, 2, 3}));

        UNIT_ASSERT(registry->DisableValidator("test1"));
        UNIT_ASSERT(registry->DisableValidator("test5"));

        CheckValidatorClasses(1, 2, THashSet<ui32>(), THashSet<ui32>({1, 2, 3}));

        UNIT_ASSERT(registry->DisableValidator("test2"));
        UNIT_ASSERT(registry->DisableValidator("test6"));

        CheckValidatorClasses(2, THashSet<ui32>());

        UNIT_ASSERT(registry->EnableValidator("test6"));

        CheckValidatorClasses(2, THashSet<ui32>(), THashSet<ui32>({1, 2, 3}));
    }

    Y_UNIT_TEST(TestCheckConfig) {
        TValidatorsRegistry::DropInstance();
        auto registry = TValidatorsRegistry::Instance();
        TIntrusivePtr<TTestValidator> test1 = new TTestValidator("test1", 1);
        TIntrusivePtr<TTestValidator> test2 = new TTestValidator("test2", 1);
        TIntrusivePtr<TTestValidator> test3 = new TTestValidator("test3");

        UNIT_ASSERT(registry->AddValidator(test1));
        UNIT_ASSERT(registry->AddValidator(test2));
        UNIT_ASSERT(registry->AddValidator(test3));

        registry->LockValidators();

        TVector<Ydb::Issue::IssueMessage> issues;
        UNIT_ASSERT(registry->CheckConfig({}, {}, issues));
        UNIT_ASSERT_VALUES_EQUAL(test1->GetCounter(), 1);
        UNIT_ASSERT_VALUES_EQUAL(test2->GetCounter(), 1);
        UNIT_ASSERT_VALUES_EQUAL(test3->GetCounter(), 1);

        {
            TDynBitMap kinds;
            kinds.Set(1);
            UNIT_ASSERT(registry->CheckConfig({}, {}, kinds, issues));
            UNIT_ASSERT_VALUES_EQUAL(test1->GetCounter(), 2);
            UNIT_ASSERT_VALUES_EQUAL(test2->GetCounter(), 2);
            UNIT_ASSERT_VALUES_EQUAL(test3->GetCounter(), 1);
        }

        {
            TDynBitMap kinds;
            kinds.Set(2);
            UNIT_ASSERT(registry->CheckConfig({}, {}, kinds, issues));
            UNIT_ASSERT_VALUES_EQUAL(test1->GetCounter(), 2);
            UNIT_ASSERT_VALUES_EQUAL(test2->GetCounter(), 2);
            UNIT_ASSERT_VALUES_EQUAL(test3->GetCounter(), 1);
        }

        {
            TDynBitMap kinds;
            UNIT_ASSERT(registry->CheckConfig({}, {}, kinds, issues));
            UNIT_ASSERT_VALUES_EQUAL(test1->GetCounter(), 2);
            UNIT_ASSERT_VALUES_EQUAL(test2->GetCounter(), 2);
            UNIT_ASSERT_VALUES_EQUAL(test3->GetCounter(), 2);
        }

        test1->SetLimit(2);
        UNIT_ASSERT(!registry->CheckConfig({}, {}, issues));
        UNIT_ASSERT_VALUES_EQUAL(test1->GetCounter(), 3);
        {
            TDynBitMap kinds;
            kinds.Set(1);
            UNIT_ASSERT(!registry->CheckConfig({}, {}, kinds, issues));
            UNIT_ASSERT_VALUES_EQUAL(test1->GetCounter(), 4);
        }
    }

    Y_UNIT_TEST(TestDisableEnable) {
        TValidatorsRegistry::DropInstance();
        auto registry = TValidatorsRegistry::Instance();
        TIntrusivePtr<TTestValidator> test1 = new TTestValidator("test1", 1);
        TIntrusivePtr<TTestValidator> test2 = new TTestValidator("test2", 1);

        test1->SetLimit(0);

        UNIT_ASSERT(registry->AddValidator(test1));
        UNIT_ASSERT(registry->AddValidator(test2));

        registry->LockValidators();

        TVector<Ydb::Issue::IssueMessage> issues;
        UNIT_ASSERT(!registry->CheckConfig({}, {}, issues));
        UNIT_ASSERT_VALUES_EQUAL(test1->GetCounter(), 1);

        UNIT_ASSERT(registry->DisableValidator("test1"));
        UNIT_ASSERT(registry->DisableValidator("test1"));
        UNIT_ASSERT(!registry->DisableValidator("unknown"));

        UNIT_ASSERT(registry->CheckConfig({}, {}, issues));
        UNIT_ASSERT_VALUES_EQUAL(test1->GetCounter(), 1);

        UNIT_ASSERT(registry->EnableValidator("test1"));
        UNIT_ASSERT(registry->EnableValidator("test1"));
        UNIT_ASSERT(!registry->EnableValidator("unknown"));

        UNIT_ASSERT(!registry->CheckConfig({}, {}, issues));
        UNIT_ASSERT_VALUES_EQUAL(test1->GetCounter(), 2);

        registry->EnableValidators();
        UNIT_ASSERT(registry->IsValidatorEnabled("test1"));
        UNIT_ASSERT(registry->IsValidatorEnabled("test2"));

        registry->DisableValidators();
        UNIT_ASSERT(!registry->IsValidatorEnabled("test1"));
        UNIT_ASSERT(!registry->IsValidatorEnabled("test2"));
    }
}

} // namespace NKikimr::NConsole
