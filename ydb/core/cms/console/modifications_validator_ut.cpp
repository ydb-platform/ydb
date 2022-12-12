#include "modifications_validator.h"
#include "ut_helpers.h"

#include <ydb/core/cms/console/validators/registry.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NConsole {

using namespace NUT;

namespace {

TConfigsConfig MakeConfigsConfig(NKikimrConsole::EValidationLevel level,
                                 ui64 limit = 0,
                                 bool failOnLimit = false,
                                 bool treatWarningAsError = false)
{
    TConfigsConfig result;
    result.ValidationLevel = level;
    result.MaxConfigChecksPerModification = limit;
    result.FailOnExceededConfigChecksLimit = failOnLimit;
    result.TreatWarningAsError = treatWarningAsError;
    return result;
}

void AssignIds(ui64)
{
}

template <typename ...Ts>
void AssignIds(ui64 id,
               NKikimrConsole::TConfigItem &item,
               Ts&... args)
{
    item.MutableId()->SetId(id);
    item.MutableId()->SetGeneration(1);
    item.SetOrder(static_cast<ui32>(id));
    AssignIds(id + 1, args...);
}

template <typename ...Ts>
void AssignIdsAndOrder(NKikimrConsole::TConfigItem &item,
                       Ts&... args)
{
    AssignIds(1, item, args...);
}

NKikimrConsole::TConfigItem ITEM_DOMAIN_LOG_1;
NKikimrConsole::TConfigItem ITEM_DOMAIN_LOG_2;
NKikimrConsole::TConfigItem ITEM_DOMAIN_LOG_3;
NKikimrConsole::TConfigItem ITEM_DOMAIN_LOG_4;
NKikimrConsole::TConfigItem ITEM_NODE1_LOG_1;
NKikimrConsole::TConfigItem ITEM_NODE2_LOG_1;
NKikimrConsole::TConfigItem ITEM_NODE12_LOG_1;
NKikimrConsole::TConfigItem ITEM_HOST1_LOG_1;
NKikimrConsole::TConfigItem ITEM_HOST2_LOG_1;
NKikimrConsole::TConfigItem ITEM_HOST12_LOG_1;
NKikimrConsole::TConfigItem ITEM_TENANT1_LOG_1;
NKikimrConsole::TConfigItem ITEM_TENANT1_LOG_2;
NKikimrConsole::TConfigItem ITEM_TENANT2_LOG_1;
NKikimrConsole::TConfigItem ITEM_TENANT2_LOG_2;
NKikimrConsole::TConfigItem ITEM_TENANT3_LOG_1;
NKikimrConsole::TConfigItem ITEM_TENANT3_LOG_2;
NKikimrConsole::TConfigItem ITEM_TYPE1_LOG_1;
NKikimrConsole::TConfigItem ITEM_TYPE1_LOG_2;
NKikimrConsole::TConfigItem ITEM_TYPE2_LOG_1;
NKikimrConsole::TConfigItem ITEM_TYPE2_LOG_2;
NKikimrConsole::TConfigItem ITEM_TYPE3_LOG_1;
NKikimrConsole::TConfigItem ITEM_TYPE3_LOG_2;
NKikimrConsole::TConfigItem ITEM_TENANT1_TYPE1_LOG_1;
NKikimrConsole::TConfigItem ITEM_TENANT1_TYPE1_LOG_2;
NKikimrConsole::TConfigItem ITEM_TENANT1_TYPE2_LOG_1;
NKikimrConsole::TConfigItem ITEM_TENANT2_TYPE2_LOG_1;
NKikimrConsole::TConfigItem ITEM_TENANT2_TYPE2_LOG_2;
NKikimrConsole::TConfigItem ITEM_TENANT3_TYPE3_LOG_1;
NKikimrConsole::TConfigItem ITEM_TENANT3_TYPE3_LOG_2;

NKikimrConsole::TConfigItem ITEM_DOMAIN_POOL_1;
NKikimrConsole::TConfigItem ITEM_TENANT3_POOL_1;
NKikimrConsole::TConfigItem ITEM_TYPE3_POOL_1;

void InitializeTestConfigItems()
{
    NKikimrConfig::TAppConfig logConfig;
    logConfig.MutableLogConfig()->AddEntry();

    ITEM_DOMAIN_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {}, {}, "", "", 0,
                         NKikimrConsole::TConfigItem::MERGE);
    ITEM_DOMAIN_LOG_2
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {}, {}, "", "", 0,
                         NKikimrConsole::TConfigItem::MERGE);
    ITEM_DOMAIN_LOG_3
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {}, {}, "", "", 0,
                         NKikimrConsole::TConfigItem::MERGE);
    ITEM_DOMAIN_LOG_4
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {}, {}, "", "", 0,
                         NKikimrConsole::TConfigItem::MERGE);
    ITEM_NODE1_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {1}, {}, "", "", 0,
                         NKikimrConsole::TConfigItem::MERGE);
    ITEM_NODE2_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {2}, {}, "", "", 0,
                         NKikimrConsole::TConfigItem::MERGE);
    ITEM_NODE12_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {1, 2}, {}, "", "", 0,
                         NKikimrConsole::TConfigItem::MERGE);
    ITEM_HOST1_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {}, {"host1"}, "", "", 0,
                         NKikimrConsole::TConfigItem::MERGE);
    ITEM_HOST2_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {}, {"host2"}, "", "", 0,
                         NKikimrConsole::TConfigItem::MERGE);
    ITEM_HOST12_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {}, {"host1", "host2"}, "", "", 0,
                         NKikimrConsole::TConfigItem::MERGE);
    ITEM_TENANT1_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {}, {}, "tenant1", "", 0,
                         NKikimrConsole::TConfigItem::MERGE);
    ITEM_TENANT1_LOG_2
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {}, {}, "tenant1", "", 0,
                         NKikimrConsole::TConfigItem::MERGE);
    ITEM_TENANT2_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {}, {}, "tenant2", "", 0,
                         NKikimrConsole::TConfigItem::MERGE);
    ITEM_TENANT2_LOG_2
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {}, {}, "tenant2", "", 0,
                         NKikimrConsole::TConfigItem::MERGE);
    ITEM_TENANT3_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {}, {}, "tenant3", "", 0,
                         NKikimrConsole::TConfigItem::MERGE);
    ITEM_TENANT3_LOG_2
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {}, {}, "tenant3", "", 0,
                         NKikimrConsole::TConfigItem::MERGE);
    ITEM_TYPE1_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {}, {}, "", "type1", 0,
                         NKikimrConsole::TConfigItem::MERGE, "cookie1");
    ITEM_TYPE1_LOG_2
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {}, {}, "", "type1", 0,
                         NKikimrConsole::TConfigItem::MERGE, "cookie1");
    ITEM_TYPE2_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {}, {}, "", "type2", 0,
                         NKikimrConsole::TConfigItem::MERGE, "cookie1");
    ITEM_TYPE2_LOG_2
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {}, {}, "", "type2", 0,
                         NKikimrConsole::TConfigItem::MERGE, "cookie1");
    ITEM_TYPE3_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {}, {}, "", "type3", 0,
                         NKikimrConsole::TConfigItem::MERGE, "cookie1");
    ITEM_TYPE3_LOG_2
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {}, {}, "", "type3", 0,
                         NKikimrConsole::TConfigItem::MERGE, "cookie1");
    ITEM_TENANT1_TYPE1_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {}, {}, "tenant1", "type1", 0,
                         NKikimrConsole::TConfigItem::MERGE);
    ITEM_TENANT1_TYPE1_LOG_2
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {}, {}, "tenant1", "type1", 0,
                         NKikimrConsole::TConfigItem::MERGE);
    ITEM_TENANT1_TYPE2_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {}, {}, "tenant1", "type2", 0,
                         NKikimrConsole::TConfigItem::MERGE);
    ITEM_TENANT2_TYPE2_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {}, {}, "tenant2", "type2", 0,
                         NKikimrConsole::TConfigItem::MERGE);
    ITEM_TENANT2_TYPE2_LOG_2
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {}, {}, "tenant2", "type2", 0,
                         NKikimrConsole::TConfigItem::MERGE);
    ITEM_TENANT3_TYPE3_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {}, {}, "tenant3", "type3", 0,
                         NKikimrConsole::TConfigItem::MERGE);
    ITEM_TENANT3_TYPE3_LOG_2
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         logConfig, {}, {}, "tenant3", "type3", 0,
                         NKikimrConsole::TConfigItem::MERGE);

    ITEM_DOMAIN_POOL_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::TenantPoolConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "", "", 0,
                         NKikimrConsole::TConfigItem::MERGE);
    ITEM_TENANT3_POOL_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::TenantPoolConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "tenant3", "", 0,
                         NKikimrConsole::TConfigItem::MERGE);
    ITEM_TYPE3_POOL_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::TenantPoolConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "", "type3", 0,
                         NKikimrConsole::TConfigItem::MERGE);

    AssignIdsAndOrder(ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2,
                      ITEM_DOMAIN_LOG_3, ITEM_DOMAIN_LOG_4,
                      ITEM_NODE1_LOG_1, ITEM_NODE2_LOG_1, ITEM_NODE12_LOG_1,
                      ITEM_HOST1_LOG_1, ITEM_HOST2_LOG_1, ITEM_HOST12_LOG_1,
                      ITEM_TENANT1_LOG_1, ITEM_TENANT1_LOG_2,
                      ITEM_TENANT2_LOG_1, ITEM_TENANT2_LOG_2,
                      ITEM_TENANT3_LOG_1, ITEM_TENANT3_LOG_2,
                      ITEM_TYPE1_LOG_1, ITEM_TYPE1_LOG_2,
                      ITEM_TYPE2_LOG_1, ITEM_TYPE2_LOG_2,
                      ITEM_TYPE3_LOG_1, ITEM_TYPE3_LOG_2,
                      ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE1_LOG_2,
                      ITEM_TENANT1_TYPE2_LOG_1,
                      ITEM_TENANT2_TYPE2_LOG_1, ITEM_TENANT2_TYPE2_LOG_2,
                      ITEM_TENANT3_TYPE3_LOG_1, ITEM_TENANT3_TYPE3_LOG_2,
                      ITEM_DOMAIN_POOL_1, ITEM_TENANT3_POOL_1, ITEM_TYPE3_POOL_1);
}

TConfigIndex MakeDefaultIndex()
{
    TConfigIndex result;
    result.AddItem(new TConfigItem(ITEM_DOMAIN_LOG_1));
    result.AddItem(new TConfigItem(ITEM_DOMAIN_LOG_2));
    result.AddItem(new TConfigItem(ITEM_DOMAIN_LOG_3));
    result.AddItem(new TConfigItem(ITEM_NODE1_LOG_1));
    result.AddItem(new TConfigItem(ITEM_NODE2_LOG_1));
    result.AddItem(new TConfigItem(ITEM_HOST1_LOG_1));
    result.AddItem(new TConfigItem(ITEM_HOST2_LOG_1));
    result.AddItem(new TConfigItem(ITEM_TENANT1_LOG_1));
    result.AddItem(new TConfigItem(ITEM_TENANT2_LOG_1));
    result.AddItem(new TConfigItem(ITEM_TENANT3_LOG_1));
    result.AddItem(new TConfigItem(ITEM_TYPE1_LOG_1));
    result.AddItem(new TConfigItem(ITEM_TYPE2_LOG_1));
    result.AddItem(new TConfigItem(ITEM_TYPE3_LOG_1));
    result.AddItem(new TConfigItem(ITEM_TENANT1_TYPE1_LOG_1));
    result.AddItem(new TConfigItem(ITEM_TENANT2_TYPE2_LOG_1));
    result.AddItem(new TConfigItem(ITEM_TENANT3_TYPE3_LOG_1));
    return result;
}

TConfigIndex MakeSmallIndex()
{
    TConfigIndex result;
    result.AddItem(new TConfigItem(ITEM_DOMAIN_LOG_1));
    result.AddItem(new TConfigItem(ITEM_TENANT1_LOG_1));
    result.AddItem(new TConfigItem(ITEM_TYPE2_LOG_1));
    result.AddItem(new TConfigItem(ITEM_TENANT3_TYPE3_LOG_1));
    return result;
}

TConfigModifications MakeDiffAddItems()
{
    TConfigModifications result;
    result.AddedItems.push_back(new TConfigItem(ITEM_DOMAIN_LOG_4));
    result.AddedItems.push_back(new TConfigItem(ITEM_NODE12_LOG_1));
    result.AddedItems.push_back(new TConfigItem(ITEM_HOST12_LOG_1));
    result.AddedItems.push_back(new TConfigItem(ITEM_TENANT1_LOG_2));
    result.AddedItems.push_back(new TConfigItem(ITEM_TYPE1_LOG_2));
    result.AddedItems.push_back(new TConfigItem(ITEM_TENANT1_TYPE1_LOG_2));
    return result;
}


TConfigModifications MakeDiffRemoveItems()
{
    TConfigModifications result;
    result.RemovedItems[ITEM_DOMAIN_LOG_1.GetId().GetId()]
        = new TConfigItem(ITEM_DOMAIN_LOG_1);
    result.RemovedItems[ITEM_NODE1_LOG_1.GetId().GetId()]
        = new TConfigItem(ITEM_NODE1_LOG_1);
    result.RemovedItems[ITEM_HOST1_LOG_1.GetId().GetId()]
        = new TConfigItem(ITEM_HOST1_LOG_1);
    result.RemovedItems[ITEM_TENANT1_LOG_1.GetId().GetId()]
        = new TConfigItem(ITEM_TENANT1_LOG_1);
    result.RemovedItems[ITEM_TYPE1_LOG_1.GetId().GetId()]
        = new TConfigItem(ITEM_TYPE1_LOG_1);
    result.RemovedItems[ITEM_TENANT1_TYPE1_LOG_1.GetId().GetId()]
        = new TConfigItem(ITEM_TENANT1_TYPE1_LOG_1);
    return result;
}

TConfigModifications MakeDiffModifyItemsSameScope()
{
    TConfigModifications result;
    result.ModifiedItems[ITEM_DOMAIN_LOG_2.GetId().GetId()]
        = new TConfigItem(ITEM_DOMAIN_LOG_2);
    result.ModifiedItems[ITEM_NODE2_LOG_1.GetId().GetId()]
        = new TConfigItem(ITEM_NODE2_LOG_1);
    result.ModifiedItems[ITEM_HOST2_LOG_1.GetId().GetId()]
        = new TConfigItem(ITEM_HOST2_LOG_1);
    result.ModifiedItems[ITEM_TENANT2_LOG_1.GetId().GetId()]
        = new TConfigItem(ITEM_TENANT2_LOG_1);
    result.ModifiedItems[ITEM_TYPE2_LOG_1.GetId().GetId()]
        = new TConfigItem(ITEM_TYPE2_LOG_1);
    result.ModifiedItems[ITEM_TENANT2_TYPE2_LOG_1.GetId().GetId()]
        = new TConfigItem(ITEM_TENANT2_TYPE2_LOG_1);
    return result;
}

void AddScopeModification(TConfigModifications &diff,
                          const NKikimrConsole::TConfigItem &item,
                          const NKikimrConsole::TConfigItem &scopeSource)
{
    diff.ModifiedItems[item.GetId().GetId()]
        = new TConfigItem(item);
    diff.ModifiedItems[item.GetId().GetId()]->UsageScope
        = TUsageScope(scopeSource.GetUsageScope(),
                      item.GetOrder());
}

TConfigModifications MakeDiffModifyItemsExpandScope()
{
    TConfigModifications result;
    AddScopeModification(result, ITEM_DOMAIN_LOG_2, ITEM_DOMAIN_LOG_2);
    AddScopeModification(result, ITEM_NODE2_LOG_1, ITEM_DOMAIN_LOG_1);
    AddScopeModification(result, ITEM_HOST2_LOG_1, ITEM_TENANT1_TYPE1_LOG_1);
    AddScopeModification(result, ITEM_TENANT2_LOG_1, ITEM_DOMAIN_LOG_1);
    AddScopeModification(result, ITEM_TYPE2_LOG_1, ITEM_DOMAIN_LOG_1);
    AddScopeModification(result, ITEM_TENANT2_TYPE2_LOG_1, ITEM_TENANT2_LOG_1);
    return result;
}

TConfigModifications MakeDiffModifyItemsNarrowScope()
{
    TConfigModifications result;
    AddScopeModification(result, ITEM_DOMAIN_LOG_2, ITEM_TENANT2_LOG_2);
    AddScopeModification(result, ITEM_NODE2_LOG_1, ITEM_NODE2_LOG_1);
    AddScopeModification(result, ITEM_HOST2_LOG_1, ITEM_HOST2_LOG_1);
    AddScopeModification(result, ITEM_TENANT2_LOG_1, ITEM_HOST2_LOG_1);
    AddScopeModification(result, ITEM_TYPE2_LOG_1, ITEM_TENANT2_TYPE2_LOG_1);
    AddScopeModification(result, ITEM_TENANT2_TYPE2_LOG_1, ITEM_NODE2_LOG_1);
    return result;
}

TConfigModifications MakeDiffForRequiredChecks(bool domainAffected)
{
    TConfigModifications result;
    if (domainAffected)
        AddScopeModification(result, ITEM_DOMAIN_LOG_2, ITEM_TENANT1_LOG_1);
    else
        AddScopeModification(result, ITEM_TENANT1_LOG_2, ITEM_TENANT1_LOG_1);
    AddScopeModification(result, ITEM_TENANT2_LOG_1, ITEM_TYPE1_LOG_1);
    AddScopeModification(result, ITEM_TENANT3_LOG_1, ITEM_TENANT3_TYPE3_LOG_1);
    AddScopeModification(result, ITEM_TENANT2_TYPE2_LOG_1, ITEM_TENANT1_TYPE1_LOG_1);
    AddScopeModification(result, ITEM_TYPE2_LOG_1, ITEM_TENANT1_LOG_1);
    AddScopeModification(result, ITEM_TYPE3_LOG_1, ITEM_TENANT3_TYPE3_LOG_1);
    if (domainAffected)
        result.AddedItems.push_back(new TConfigItem(ITEM_DOMAIN_POOL_1));
    result.AddedItems.push_back(new TConfigItem(ITEM_TENANT3_POOL_1));
    result.AddedItems.push_back(new TConfigItem(ITEM_TYPE3_POOL_1));
    return result;
}

TConfigModifications MakeDiffForAllAffected(bool domainAffected)
{
    TConfigModifications result;
    if (domainAffected) {
        result.AddedItems.push_back(new TConfigItem(ITEM_DOMAIN_POOL_1));
        result.RemovedItems[ITEM_DOMAIN_LOG_1.GetId().GetId()]
            = new TConfigItem(ITEM_DOMAIN_LOG_1);
    }
    AddScopeModification(result, ITEM_TENANT2_LOG_1, ITEM_TYPE2_LOG_1);
    AddScopeModification(result, ITEM_TENANT3_TYPE3_LOG_1, ITEM_TYPE2_LOG_1);
    result.AddedItems.push_back(new TConfigItem(ITEM_TENANT3_POOL_1));
    result.AddedItems.push_back(new TConfigItem(ITEM_TYPE3_POOL_1));
    result.RemovedItems[ITEM_TYPE1_LOG_1.GetId().GetId()]
        = new TConfigItem(ITEM_TYPE1_LOG_1);
    result.RemovedItems[ITEM_TENANT1_LOG_1.GetId().GetId()]
        = new TConfigItem(ITEM_TENANT1_LOG_1);
    result.RemovedItems[ITEM_TENANT2_TYPE2_LOG_1.GetId().GetId()]
        = new TConfigItem(ITEM_TENANT2_TYPE2_LOG_1);
    return result;
}

class TTestValidator : public IConfigValidator {
public:
    TTestValidator(ui32 maxCount, bool countOld = false, bool warning = false)
        : IConfigValidator("test", NKikimrConsole::TConfigItem::LogConfigItem)
        , MaxCount(maxCount)
        , CountOld(countOld)
        , Warning(warning)
    {
    }

    bool CheckConfig(const NKikimrConfig::TAppConfig &oldConfig,
                     const NKikimrConfig::TAppConfig &newConfig,
                     TVector<Ydb::Issue::IssueMessage> &issues) const override
    {
        size_t count = newConfig.GetLogConfig().EntrySize();
        if (CountOld)
            count += oldConfig.GetLogConfig().EntrySize();
        if (count > MaxCount) {
            AddIssue(issues, "too many entries",
                     Warning ? NYql::TSeverityIds::S_WARNING : NYql::TSeverityIds::S_ERROR);
            return false;
        }
        return true;
    }

private:
    ui32 MaxCount;
    bool CountOld;
    bool Warning;
};

} // anonymous namespace

class TModificationsValidatorTests : public NUnitTest::TTestBase {
public:
    UNIT_TEST_SUITE(TModificationsValidatorTests)
    UNIT_TEST(TestIsValidationRequired_NONE)
    UNIT_TEST(TestIsValidationRequired_DOMAIN)
    UNIT_TEST(TestIsValidationRequired_TENANTS)
    UNIT_TEST(TestIsValidationRequired_TENANTS_AND_NODE_TYPES)
    UNIT_TEST(TestIndexAndModificationsShrink_AddItems_NONE)
    UNIT_TEST(TestIndexAndModificationsShrink_AddItems_DOMAIN)
    UNIT_TEST(TestIndexAndModificationsShrink_AddItems_TENANTS)
    UNIT_TEST(TestIndexAndModificationsShrink_AddItems_TENANTS_AND_NODE_TYPES)
    UNIT_TEST(TestIndexAndModificationsShrink_RemoveItems_NONE)
    UNIT_TEST(TestIndexAndModificationsShrink_RemoveItems_DOMAIN)
    UNIT_TEST(TestIndexAndModificationsShrink_RemoveItems_TENANTS)
    UNIT_TEST(TestIndexAndModificationsShrink_RemoveItems_TENANTS_AND_NODE_TYPES)
    UNIT_TEST(TestIndexAndModificationsShrink_ModifyItemsSameScope_NONE)
    UNIT_TEST(TestIndexAndModificationsShrink_ModifyItemsSameScope_DOMAIN)
    UNIT_TEST(TestIndexAndModificationsShrink_ModifyItemsSameScope_TENANTS)
    UNIT_TEST(TestIndexAndModificationsShrink_ModifyItemsSameScope_TENANTS_AND_NODE_TYPES)
    UNIT_TEST(TestIndexAndModificationsShrink_ModifyItemsExpandScope_NONE)
    UNIT_TEST(TestIndexAndModificationsShrink_ModifyItemsExpandScope_DOMAIN)
    UNIT_TEST(TestIndexAndModificationsShrink_ModifyItemsExpandScope_TENANTS)
    UNIT_TEST(TestIndexAndModificationsShrink_ModifyItemsExpandScope_TENANTS_AND_NODE_TYPES)
    UNIT_TEST(TestIndexAndModificationsShrink_ModifyItemsNarrowScope_NONE)
    UNIT_TEST(TestIndexAndModificationsShrink_ModifyItemsNarrowScope_DOMAIN)
    UNIT_TEST(TestIndexAndModificationsShrink_ModifyItemsNarrowScope_TENANTS)
    UNIT_TEST(TestIndexAndModificationsShrink_ModifyItemsNarrowScope_TENANTS_AND_NODE_TYPES)
    UNIT_TEST(TestComputeAffectedConfigs_DomainAffected_DOMAIN)
    UNIT_TEST(TestComputeAffectedConfigs_DomainAffected_TENANTS)
    UNIT_TEST(TestComputeAffectedConfigs_DomainAffected_TENANTS_AND_NODE_TYPES)
    UNIT_TEST(TestComputeAffectedConfigs_DomainUnaffected_TENANTS)
    UNIT_TEST(TestComputeAffectedConfigs_DomainUnaffected_TENANTS_AND_NODE_TYPES)
    UNIT_TEST(TestComputeAffectedConfigs_All_DomainAffected_DOMAIN)
    UNIT_TEST(TestComputeAffectedConfigs_All_DomainAffected_TENANTS)
    UNIT_TEST(TestComputeAffectedConfigs_All_DomainAffected_TENANTS_AND_NODE_TYPES)
    UNIT_TEST(TestComputeAffectedConfigs_All_DomainUnaffected_TENANTS)
    UNIT_TEST(TestComputeAffectedConfigs_All_DomainUnaffected_TENANTS_AND_NODE_TYPES)
    UNIT_TEST(TestApplyValidators_TENANTS);
    UNIT_TEST(TestApplyValidators_TENANTS_AND_NODE_TYPES);
    UNIT_TEST(TestApplyValidatorsWithOldConfig);
    UNIT_TEST(TestChecksLimitError);
    UNIT_TEST(TestChecksLimitWarning);
    UNIT_TEST_SUITE_END();

    void TestIsValidationRequired_NONE()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_NONE);
        TModificationsValidator validator(TConfigIndex(),
                                          TConfigModifications(),
                                          config);
        UNIT_ASSERT(!validator.IsValidationRequired(new TConfigItem(ITEM_DOMAIN_LOG_1)));
        UNIT_ASSERT(!validator.IsValidationRequired(new TConfigItem(ITEM_NODE12_LOG_1)));
        UNIT_ASSERT(!validator.IsValidationRequired(new TConfigItem(ITEM_HOST12_LOG_1)));
        UNIT_ASSERT(!validator.IsValidationRequired(new TConfigItem(ITEM_TENANT1_LOG_1)));
        UNIT_ASSERT(!validator.IsValidationRequired(new TConfigItem(ITEM_TYPE1_LOG_1)));
        UNIT_ASSERT(!validator.IsValidationRequired(new TConfigItem(ITEM_TENANT1_TYPE1_LOG_1)));
    }

    void TestIsValidationRequired_DOMAIN()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_DOMAIN);
        TModificationsValidator validator(TConfigIndex(),
                                          TConfigModifications(),
                                          config);
        UNIT_ASSERT(validator.IsValidationRequired(new TConfigItem(ITEM_DOMAIN_LOG_1)));
        UNIT_ASSERT(!validator.IsValidationRequired(new TConfigItem(ITEM_NODE12_LOG_1)));
        UNIT_ASSERT(!validator.IsValidationRequired(new TConfigItem(ITEM_HOST12_LOG_1)));
        UNIT_ASSERT(!validator.IsValidationRequired(new TConfigItem(ITEM_TENANT1_LOG_1)));
        UNIT_ASSERT(!validator.IsValidationRequired(new TConfigItem(ITEM_TYPE1_LOG_1)));
        UNIT_ASSERT(!validator.IsValidationRequired(new TConfigItem(ITEM_TENANT1_TYPE1_LOG_1)));
    }

    void TestIsValidationRequired_TENANTS()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_TENANTS);
        TModificationsValidator validator(TConfigIndex(),
                                          TConfigModifications(),
                                          config);
        UNIT_ASSERT(validator.IsValidationRequired(new TConfigItem(ITEM_DOMAIN_LOG_1)));
        UNIT_ASSERT(!validator.IsValidationRequired(new TConfigItem(ITEM_NODE12_LOG_1)));
        UNIT_ASSERT(!validator.IsValidationRequired(new TConfigItem(ITEM_HOST12_LOG_1)));
        UNIT_ASSERT(validator.IsValidationRequired(new TConfigItem(ITEM_TENANT1_LOG_1)));
        UNIT_ASSERT(!validator.IsValidationRequired(new TConfigItem(ITEM_TYPE1_LOG_1)));
        UNIT_ASSERT(!validator.IsValidationRequired(new TConfigItem(ITEM_TENANT1_TYPE1_LOG_1)));
    }

    void TestIsValidationRequired_TENANTS_AND_NODE_TYPES()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_TENANTS_AND_NODE_TYPES);
        TModificationsValidator validator(TConfigIndex(),
                                          TConfigModifications(),
                                          config);
        UNIT_ASSERT(validator.IsValidationRequired(new TConfigItem(ITEM_DOMAIN_LOG_1)));
        UNIT_ASSERT(!validator.IsValidationRequired(new TConfigItem(ITEM_NODE12_LOG_1)));
        UNIT_ASSERT(!validator.IsValidationRequired(new TConfigItem(ITEM_HOST12_LOG_1)));
        UNIT_ASSERT(validator.IsValidationRequired(new TConfigItem(ITEM_TENANT1_LOG_1)));
        UNIT_ASSERT(validator.IsValidationRequired(new TConfigItem(ITEM_TYPE1_LOG_1)));
        UNIT_ASSERT(validator.IsValidationRequired(new TConfigItem(ITEM_TENANT1_TYPE1_LOG_1)));
    }

    void TestIndexAndModificationsShrink_AddItems_NONE()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_NONE);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffAddItems();
        TModificationsValidator validator(index, diff, config);

        UNIT_ASSERT_VALUES_EQUAL(validator.Index.GetConfigItems().size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(validator.ModifiedItems.size(), 0);
    }

    void TestIndexAndModificationsShrink_AddItems_DOMAIN()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_DOMAIN);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffAddItems();
        TModificationsValidator validator(index, diff, config);

        // Expect original items and new items with required scope.
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_3.GetId().GetId()));
        UNIT_ASSERT_VALUES_EQUAL(validator.Index.GetConfigItems().size(), 4);

        // Added items have tmp ids but also have order equal to old ids.
        THashSet<ui64> orders;
        orders.insert(ITEM_DOMAIN_LOG_4.GetId().GetId());
        for (auto &item : validator.ModifiedItems) {
            UNIT_ASSERT(orders.contains(item->UsageScope.Order));
            orders.erase(item->UsageScope.Order);
        }
        UNIT_ASSERT_VALUES_EQUAL(validator.ModifiedItems.size(), 1);
    }

    void TestIndexAndModificationsShrink_AddItems_TENANTS()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_TENANTS);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffAddItems();
        TModificationsValidator validator(index, diff, config);

        // Expect original items and new items with required scope.
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_3.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT1_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT2_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT3_LOG_1.GetId().GetId()));
        UNIT_ASSERT_VALUES_EQUAL(validator.Index.GetConfigItems().size(), 8);

        // Added items have tmp ids but also have order equal to old ids.
        THashSet<ui64> orders;
        orders.insert(ITEM_DOMAIN_LOG_4.GetId().GetId());
        orders.insert(ITEM_TENANT1_LOG_2.GetId().GetId());
        for (auto &item : validator.ModifiedItems) {
            UNIT_ASSERT(orders.contains(item->UsageScope.Order));
            orders.erase(item->UsageScope.Order);
        }
        UNIT_ASSERT_VALUES_EQUAL(validator.ModifiedItems.size(), 2);
    }

    void TestIndexAndModificationsShrink_AddItems_TENANTS_AND_NODE_TYPES()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_TENANTS_AND_NODE_TYPES);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffAddItems();
        TModificationsValidator validator(index, diff, config);

        // Expect original items and new items with required scope.
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_3.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT1_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT2_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT3_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TYPE1_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TYPE2_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TYPE3_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT1_TYPE1_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT2_TYPE2_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT3_TYPE3_LOG_1.GetId().GetId()));
        UNIT_ASSERT_VALUES_EQUAL(validator.Index.GetConfigItems().size(), 16);

        // Added items have tmp ids but also have order equal to old ids.
        THashSet<ui64> orders;
        orders.insert(ITEM_DOMAIN_LOG_4.GetId().GetId());
        orders.insert(ITEM_TENANT1_LOG_2.GetId().GetId());
        orders.insert(ITEM_TYPE1_LOG_2.GetId().GetId());
        orders.insert(ITEM_TENANT1_TYPE1_LOG_2.GetId().GetId());
        for (auto &item : validator.ModifiedItems) {
            UNIT_ASSERT(orders.contains(item->UsageScope.Order));
            orders.erase(item->UsageScope.Order);
        }
        UNIT_ASSERT_VALUES_EQUAL(validator.ModifiedItems.size(), 4);
    }

    void TestIndexAndModificationsShrink_RemoveItems_NONE()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_NONE);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffRemoveItems();
        TModificationsValidator validator(index, diff, config);

        UNIT_ASSERT_VALUES_EQUAL(validator.Index.GetConfigItems().size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(validator.ModifiedItems.size(), 0);
    }

    void TestIndexAndModificationsShrink_RemoveItems_DOMAIN()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_DOMAIN);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffRemoveItems();
        TModificationsValidator validator(index, diff, config);

        // Expect original items except removed with required scope.
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_3.GetId().GetId()));
        UNIT_ASSERT_VALUES_EQUAL(validator.Index.GetConfigItems().size(), 2);

        // Expect all removed items with required scope.
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_DOMAIN_LOG_1.GetId().GetId())));
        UNIT_ASSERT_VALUES_EQUAL(validator.ModifiedItems.size(), 1);
    }

    void TestIndexAndModificationsShrink_RemoveItems_TENANTS()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_TENANTS);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffRemoveItems();
        TModificationsValidator validator(index, diff, config);

        // Expect original items except removed with required scope.
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_3.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT2_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT3_LOG_1.GetId().GetId()));
        UNIT_ASSERT_VALUES_EQUAL(validator.Index.GetConfigItems().size(), 4);

        // Expect all removed items with required scope.
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_DOMAIN_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_TENANT1_LOG_1.GetId().GetId())));
        UNIT_ASSERT_VALUES_EQUAL(validator.ModifiedItems.size(), 2);
    }

    void TestIndexAndModificationsShrink_RemoveItems_TENANTS_AND_NODE_TYPES()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_TENANTS_AND_NODE_TYPES);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffRemoveItems();
        TModificationsValidator validator(index, diff, config);

        // Expect original items except removed with required scope.
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_3.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT2_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT3_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TYPE2_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TYPE3_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT2_TYPE2_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT3_TYPE3_LOG_1.GetId().GetId()));
        UNIT_ASSERT_VALUES_EQUAL(validator.Index.GetConfigItems().size(), 8);

        // Expect all removed items with required scope.
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_DOMAIN_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_TENANT1_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_TYPE1_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_TENANT1_TYPE1_LOG_1.GetId().GetId())));
        UNIT_ASSERT_VALUES_EQUAL(validator.ModifiedItems.size(), 4);
    }

    void TestIndexAndModificationsShrink_ModifyItemsSameScope_NONE()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_NONE);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffModifyItemsSameScope();
        TModificationsValidator validator(index, diff, config);

        UNIT_ASSERT_VALUES_EQUAL(validator.Index.GetConfigItems().size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(validator.ModifiedItems.size(), 0);
    }

    void TestIndexAndModificationsShrink_ModifyItemsSameScope_DOMAIN()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_DOMAIN);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffModifyItemsSameScope();
        TModificationsValidator validator(index, diff, config);

        // Expect all original items with required scope.
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_3.GetId().GetId()));
        UNIT_ASSERT_VALUES_EQUAL(validator.Index.GetConfigItems().size(), 3);

        // Expect all modified items (both original and resulting ones) with required scope.
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(validator.Index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId())));
        UNIT_ASSERT_VALUES_EQUAL(validator.ModifiedItems.size(), 2);
    }

    void TestIndexAndModificationsShrink_ModifyItemsSameScope_TENANTS()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_TENANTS);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffModifyItemsSameScope();
        TModificationsValidator validator(index, diff, config);

        // Expect all original items with required scope.
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_3.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT1_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT2_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT3_LOG_1.GetId().GetId()));
        UNIT_ASSERT_VALUES_EQUAL(validator.Index.GetConfigItems().size(), 6);

        // Expect all modified items (both original and resulting ones) with required scope.
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_TENANT2_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(validator.Index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(validator.Index.GetItem(ITEM_TENANT2_LOG_1.GetId().GetId())));
        UNIT_ASSERT_VALUES_EQUAL(validator.ModifiedItems.size(), 4);
    }

    void TestIndexAndModificationsShrink_ModifyItemsSameScope_TENANTS_AND_NODE_TYPES()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_TENANTS_AND_NODE_TYPES);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffModifyItemsSameScope();
        TModificationsValidator validator(index, diff, config);

        // Expect all original items with required scope.
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_3.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT1_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT2_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT3_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TYPE1_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TYPE2_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TYPE3_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT1_TYPE1_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT2_TYPE2_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT3_TYPE3_LOG_1.GetId().GetId()));
        UNIT_ASSERT_VALUES_EQUAL(validator.Index.GetConfigItems().size(), 12);

        // Expect all modified items (both original and resulting ones) with required scope.
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_TENANT2_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_TYPE2_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_TENANT2_TYPE2_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(validator.Index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(validator.Index.GetItem(ITEM_TENANT2_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(validator.Index.GetItem(ITEM_TYPE2_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(validator.Index.GetItem(ITEM_TENANT2_TYPE2_LOG_1.GetId().GetId())));
        UNIT_ASSERT_VALUES_EQUAL(validator.ModifiedItems.size(), 8);
    }

    void TestIndexAndModificationsShrink_ModifyItemsExpandScope_NONE()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_NONE);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffModifyItemsExpandScope();
        TModificationsValidator validator(index, diff, config);

        UNIT_ASSERT_VALUES_EQUAL(validator.Index.GetConfigItems().size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(validator.ModifiedItems.size(), 0);
    }

    void TestIndexAndModificationsShrink_ModifyItemsExpandScope_DOMAIN()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_DOMAIN);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffModifyItemsExpandScope();
        TModificationsValidator validator(index, diff, config);

        // Expect all original items with required scope and modified items with
        // matching resulting scope.
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_3.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_NODE2_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT2_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TYPE2_LOG_1.GetId().GetId()));
        UNIT_ASSERT_VALUES_EQUAL(validator.Index.GetConfigItems().size(), 6);

        // Expect all modified items (both original and resulting ones) with required scope.
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(validator.Index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(validator.Index.GetItem(ITEM_NODE2_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(validator.Index.GetItem(ITEM_TENANT2_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(validator.Index.GetItem(ITEM_TYPE2_LOG_1.GetId().GetId())));
        UNIT_ASSERT_VALUES_EQUAL(validator.ModifiedItems.size(), 5);
    }

    void TestIndexAndModificationsShrink_ModifyItemsExpandScope_TENANTS()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_TENANTS);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffModifyItemsExpandScope();
        TModificationsValidator validator(index, diff, config);

        // Expect all original items with required scope and modified items with
        // matching resulting scope.
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_3.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_NODE2_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT1_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT2_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT3_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TYPE2_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT2_TYPE2_LOG_1.GetId().GetId()));
        UNIT_ASSERT_VALUES_EQUAL(validator.Index.GetConfigItems().size(), 9);

        // Expect all modified items (both original and resulting ones) with required scope.
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_TENANT2_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(validator.Index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(validator.Index.GetItem(ITEM_NODE2_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(validator.Index.GetItem(ITEM_TENANT2_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(validator.Index.GetItem(ITEM_TYPE2_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(validator.Index.GetItem(ITEM_TENANT2_TYPE2_LOG_1.GetId().GetId())));
        UNIT_ASSERT_VALUES_EQUAL(validator.ModifiedItems.size(), 7);
    }

    void TestIndexAndModificationsShrink_ModifyItemsExpandScope_TENANTS_AND_NODE_TYPES()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_TENANTS_AND_NODE_TYPES);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffModifyItemsExpandScope();
        TModificationsValidator validator(index, diff, config);

        // Expect all original items with required scope and modified items with
        // matching resulting scope.
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_3.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_NODE2_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_HOST2_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT1_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT2_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT3_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TYPE1_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TYPE2_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TYPE3_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT1_TYPE1_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT2_TYPE2_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT3_TYPE3_LOG_1.GetId().GetId()));
        UNIT_ASSERT_VALUES_EQUAL(validator.Index.GetConfigItems().size(), 14);

        // Expect all modified items (both original and resulting ones) with required scope.
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_TENANT2_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_TYPE2_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_TENANT2_TYPE2_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(validator.Index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(validator.Index.GetItem(ITEM_NODE2_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(validator.Index.GetItem(ITEM_HOST2_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(validator.Index.GetItem(ITEM_TENANT2_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(validator.Index.GetItem(ITEM_TYPE2_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(validator.Index.GetItem(ITEM_TENANT2_TYPE2_LOG_1.GetId().GetId())));
        UNIT_ASSERT_VALUES_EQUAL(validator.ModifiedItems.size(), 10);
    }

    void TestIndexAndModificationsShrink_ModifyItemsNarrowScope_NONE()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_NONE);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffModifyItemsNarrowScope();
        TModificationsValidator validator(index, diff, config);

        UNIT_ASSERT_VALUES_EQUAL(validator.Index.GetConfigItems().size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(validator.ModifiedItems.size(), 0);
    }

    void TestIndexAndModificationsShrink_ModifyItemsNarrowScope_DOMAIN()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_DOMAIN);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffModifyItemsNarrowScope();
        TModificationsValidator validator(index, diff, config);

        // Expect all original items with required scope and modified items with
        // matching resulting scope.
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_3.GetId().GetId()));
        UNIT_ASSERT_VALUES_EQUAL(validator.Index.GetConfigItems().size(), 2);

        // Expect all modified items (both original and resulting ones) with required scope.
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId())));
        UNIT_ASSERT_VALUES_EQUAL(validator.ModifiedItems.size(), 1);
    }

    void TestIndexAndModificationsShrink_ModifyItemsNarrowScope_TENANTS()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_TENANTS);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffModifyItemsNarrowScope();
        TModificationsValidator validator(index, diff, config);

        // Expect all original items with required scope and modified items with
        // matching resulting scope.
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_3.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT1_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT3_LOG_1.GetId().GetId()));
        UNIT_ASSERT_VALUES_EQUAL(validator.Index.GetConfigItems().size(), 5);

        // Expect all modified items (both original and resulting ones) with required scope.
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_TENANT2_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(validator.Index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId())));
        UNIT_ASSERT_VALUES_EQUAL(validator.ModifiedItems.size(), 3);
    }

    void TestIndexAndModificationsShrink_ModifyItemsNarrowScope_TENANTS_AND_NODE_TYPES()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_TENANTS_AND_NODE_TYPES);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffModifyItemsNarrowScope();
        TModificationsValidator validator(index, diff, config);

        // Expect all original items with required scope and modified items with
        // matching resulting scope.
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_DOMAIN_LOG_3.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT1_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT3_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TYPE1_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TYPE2_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TYPE3_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT1_TYPE1_LOG_1.GetId().GetId()));
        UNIT_ASSERT(validator.Index.GetItem(ITEM_TENANT3_TYPE3_LOG_1.GetId().GetId()));
        UNIT_ASSERT_VALUES_EQUAL(validator.Index.GetConfigItems().size(), 10);

        // Expect all modified items (both original and resulting ones) with required scope.
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_TENANT2_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_TYPE2_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(index.GetItem(ITEM_TENANT2_TYPE2_LOG_1.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(validator.Index.GetItem(ITEM_DOMAIN_LOG_2.GetId().GetId())));
        UNIT_ASSERT(validator.ModifiedItems.contains(validator.Index.GetItem(ITEM_TYPE2_LOG_1.GetId().GetId())));
        UNIT_ASSERT_VALUES_EQUAL(validator.ModifiedItems.size(), 6);
    }

    void TestComputeAffectedConfigs_DomainAffected_DOMAIN()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_DOMAIN);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffForRequiredChecks(true);
        TModificationsValidator validator(index, diff, config);

        {
            TDynBitMap kinds;
            kinds.Set(NKikimrConsole::TConfigItem::LogConfigItem);
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, true);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString(""))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 1);
        }

        {
            TDynBitMap kinds;
            kinds.Set(NKikimrConsole::TConfigItem::TenantPoolConfigItem);
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, true);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString(""))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 1);
        }

        {
            TDynBitMap kinds;
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, true);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString(""))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 1);
        }
    }

    void TestComputeAffectedConfigs_DomainAffected_TENANTS()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_TENANTS);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffForRequiredChecks(true);
        TModificationsValidator validator(index, diff, config);

        {
            TDynBitMap kinds;
            kinds.Set(NKikimrConsole::TConfigItem::LogConfigItem);
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, true);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString(""))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 2);
        }

        {
            TDynBitMap kinds;
            kinds.Set(NKikimrConsole::TConfigItem::TenantPoolConfigItem);
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, true);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString(""))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 2);
        }

        {
            TDynBitMap kinds;
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, true);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString(""))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 3);
        }
    }

    void TestComputeAffectedConfigs_DomainAffected_TENANTS_AND_NODE_TYPES()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_TENANTS_AND_NODE_TYPES);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffForRequiredChecks(true);
        TModificationsValidator validator(index, diff, config);

        {
            TDynBitMap kinds;
            kinds.Set(NKikimrConsole::TConfigItem::LogConfigItem);
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, true);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString("type1"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString("type1"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString("type3"))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 5);
        }

        {
            TDynBitMap kinds;
            kinds.Set(NKikimrConsole::TConfigItem::TenantPoolConfigItem);
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, true);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString("type3"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString("type3"))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 4);
        }

        {
            TDynBitMap kinds;
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, true);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString("type1"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString("type3"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString("type1"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString("type3"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString("type1"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString("type3"))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 9);
        }
    }

    void TestComputeAffectedConfigs_DomainUnaffected_TENANTS()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_TENANTS);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffForRequiredChecks(false);
        TModificationsValidator validator(index, diff, config);

        {
            TDynBitMap kinds;
            kinds.Set(NKikimrConsole::TConfigItem::LogConfigItem);
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, true);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString(""))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 1);
        }

        {
            TDynBitMap kinds;
            kinds.Set(NKikimrConsole::TConfigItem::TenantPoolConfigItem);
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, true);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString(""))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 1);
        }

        {
            TDynBitMap kinds;
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, true);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString(""))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 2);
        }
    }

    void TestComputeAffectedConfigs_DomainUnaffected_TENANTS_AND_NODE_TYPES()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_TENANTS_AND_NODE_TYPES);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffForRequiredChecks(false);
        TModificationsValidator validator(index, diff, config);

        {
            TDynBitMap kinds;
            kinds.Set(NKikimrConsole::TConfigItem::LogConfigItem);
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, true);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString("type1"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString("type1"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString("type3"))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 4);
        }

        {
            TDynBitMap kinds;
            kinds.Set(NKikimrConsole::TConfigItem::TenantPoolConfigItem);
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, true);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString("type3"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString("type3"))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 3);
        }

        {
            TDynBitMap kinds;
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, true);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString("type1"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString("type3"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString("type1"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString("type3"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString("type1"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString("type3"))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 8);
        }
    }

    void TestComputeAffectedConfigs_All_DomainAffected_DOMAIN()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_DOMAIN);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffForAllAffected(true);
        TModificationsValidator validator(index, diff, config);

        {
            TDynBitMap kinds;
            kinds.Set(NKikimrConsole::TConfigItem::LogConfigItem);
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, false);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString(""))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 1);
        }

        {
            TDynBitMap kinds;
            kinds.Set(NKikimrConsole::TConfigItem::TenantPoolConfigItem);
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, false);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString(""))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 1);
        }

        {
            TDynBitMap kinds;
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, false);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString(""))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 1);
        }
    }

    void TestComputeAffectedConfigs_All_DomainAffected_TENANTS()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_TENANTS);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffForAllAffected(true);
        TModificationsValidator validator(index, diff, config);

        {
            TDynBitMap kinds;
            kinds.Set(NKikimrConsole::TConfigItem::LogConfigItem);
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, false);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant2"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString(""))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 4);
        }

        {
            TDynBitMap kinds;
            kinds.Set(NKikimrConsole::TConfigItem::TenantPoolConfigItem);
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, false);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString(""))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 2);
        }

        {
            TDynBitMap kinds;
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, false);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant2"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString(""))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 4);
        }
    }

    void TestComputeAffectedConfigs_All_DomainAffected_TENANTS_AND_NODE_TYPES()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_TENANTS_AND_NODE_TYPES);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffForAllAffected(true);
        TModificationsValidator validator(index, diff, config);

        {
            TDynBitMap kinds;
            kinds.Set(NKikimrConsole::TConfigItem::LogConfigItem);
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, false);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString("type1"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString("type2"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString("type3"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant2"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant2"), TString("type1"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant2"), TString("type2"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant2"), TString("type3"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString("type1"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString("type2"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString("type3"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString("type1"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString("type2"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString("type3"))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 16);
        }

        {
            TDynBitMap kinds;
            kinds.Set(NKikimrConsole::TConfigItem::TenantPoolConfigItem);
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, false);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString("type3"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString("type3"))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 4);
        }

        {
            TDynBitMap kinds;
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, false);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString("type1"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString("type2"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString("type3"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant2"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant2"), TString("type1"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant2"), TString("type2"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant2"), TString("type3"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString("type1"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString("type2"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString("type3"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString("type1"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString("type2"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString("type3"))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 16);
        }
    }

    void TestComputeAffectedConfigs_All_DomainUnaffected_TENANTS()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_TENANTS);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffForAllAffected(false);
        TModificationsValidator validator(index, diff, config);

        {
            TDynBitMap kinds;
            kinds.Set(NKikimrConsole::TConfigItem::LogConfigItem);
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, false);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant2"), TString(""))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 2);
        }

        {
            TDynBitMap kinds;
            kinds.Set(NKikimrConsole::TConfigItem::TenantPoolConfigItem);
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, false);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString(""))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 1);
        }

        {
            TDynBitMap kinds;
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, false);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant2"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString(""))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 3);
        }
    }

    void TestComputeAffectedConfigs_All_DomainUnaffected_TENANTS_AND_NODE_TYPES()
    {
        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_TENANTS_AND_NODE_TYPES);
        TConfigIndex index = MakeDefaultIndex();
        TConfigModifications diff = MakeDiffForAllAffected(false);
        TModificationsValidator validator(index, diff, config);

        {
            TDynBitMap kinds;
            kinds.Set(NKikimrConsole::TConfigItem::LogConfigItem);
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, false);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString("type1"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString("type2"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString("type3"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant2"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant2"), TString("type1"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant2"), TString("type2"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant2"), TString("type3"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString("type1"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString("type2"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString("type3"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString("type1"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString("type2"))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 13);
        }

        {
            TDynBitMap kinds;
            kinds.Set(NKikimrConsole::TConfigItem::TenantPoolConfigItem);
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, false);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString("type3"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString("type3"))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 3);
        }

        {
            TDynBitMap kinds;
            auto requiredChecks = validator.ComputeAffectedConfigs(kinds, false);
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString("type1"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString("type2"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant1"), TString("type3"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant2"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant2"), TString("type1"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant2"), TString("type2"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant2"), TString("type3"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString(""))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString("type1"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString("type2"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString("tenant3"), TString("type3"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString("type1"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString("type2"))));
            UNIT_ASSERT(requiredChecks.contains(TTenantAndNodeType(TString(""), TString("type3"))));
            UNIT_ASSERT_VALUES_EQUAL(requiredChecks.size(), 15);
        }
    }

    void TestApplyValidators_TENANTS()
    {
        TValidatorsRegistry::DropInstance();
        RegisterValidator(new TTestValidator(3));
        auto registry = TValidatorsRegistry::Instance();
        registry->LockValidators();

        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_TENANTS);
        TConfigIndex index = MakeSmallIndex();

        {
            TConfigModifications diff;
            diff.AddedItems.push_back(new TConfigItem(ITEM_TENANT3_LOG_1));
            TModificationsValidator validator(index, diff, config);
            UNIT_ASSERT(validator.ApplyValidators());
            UNIT_ASSERT_VALUES_EQUAL(validator.GetChecksDone(), 1);
        }

        {
            TConfigModifications diff;
            diff.AddedItems.push_back(new TConfigItem(ITEM_DOMAIN_LOG_2));
            diff.AddedItems.push_back(new TConfigItem(ITEM_TENANT3_LOG_1));
            TModificationsValidator validator(index, diff, config);
            UNIT_ASSERT(validator.ApplyValidators());
            UNIT_ASSERT_VALUES_EQUAL(validator.GetChecksDone(), 3);
        }

        {
            TConfigModifications diff;
            diff.AddedItems.push_back(new TConfigItem(ITEM_DOMAIN_LOG_2));
            diff.AddedItems.push_back(new TConfigItem(ITEM_TENANT1_LOG_2));
            diff.AddedItems.push_back(new TConfigItem(ITEM_TENANT3_LOG_1));
            TModificationsValidator validator(index, diff, config);
            UNIT_ASSERT(!validator.ApplyValidators());
            UNIT_ASSERT(validator.GetErrorMessage());
        }
    }

    void TestApplyValidators_TENANTS_AND_NODE_TYPES()
    {
        TValidatorsRegistry::DropInstance();
        RegisterValidator(new TTestValidator(3));
        auto registry = TValidatorsRegistry::Instance();
        registry->LockValidators();

        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_TENANTS_AND_NODE_TYPES);
        TConfigIndex index = MakeSmallIndex();

        {
            TConfigModifications diff;
            diff.AddedItems.push_back(new TConfigItem(ITEM_TENANT3_LOG_1));
            TModificationsValidator validator(index, diff, config);
            UNIT_ASSERT(validator.ApplyValidators());
            UNIT_ASSERT_VALUES_EQUAL(validator.GetChecksDone(), 2);
        }

        {
            TConfigModifications diff;
            diff.AddedItems.push_back(new TConfigItem(ITEM_TENANT3_LOG_1));
            AddScopeModification(diff, ITEM_TENANT3_TYPE3_LOG_1, ITEM_TENANT1_TYPE1_LOG_1);
            TModificationsValidator validator(index, diff, config);
            UNIT_ASSERT(validator.ApplyValidators());
            UNIT_ASSERT_VALUES_EQUAL(validator.GetChecksDone(), 3);
        }


        {
            TConfigModifications diff;
            AddScopeModification(diff, ITEM_TENANT3_TYPE3_LOG_1, ITEM_TENANT1_TYPE2_LOG_1);
            TModificationsValidator validator(index, diff, config);
            UNIT_ASSERT(!validator.ApplyValidators());
            UNIT_ASSERT(validator.GetErrorMessage());
        }


        {
            TConfigModifications diff;
            AddScopeModification(diff, ITEM_TYPE2_LOG_1, ITEM_TYPE1_LOG_1);
            AddScopeModification(diff, ITEM_TENANT3_TYPE3_LOG_1, ITEM_TENANT1_TYPE1_LOG_1);
            TModificationsValidator validator(index, diff, config);
            UNIT_ASSERT(!validator.ApplyValidators());
            UNIT_ASSERT(validator.GetErrorMessage());
        }

        {
            TConfigModifications diff;
            diff.AddedItems.push_back(new TConfigItem(ITEM_TENANT3_LOG_1));
            diff.AddedItems.push_back(new TConfigItem(ITEM_TYPE3_LOG_1));
            TModificationsValidator validator(index, diff, config);
            UNIT_ASSERT(!validator.ApplyValidators());
            UNIT_ASSERT(validator.GetErrorMessage());
        }
    }

    void TestApplyValidatorsWithOldConfig()
    {
        TValidatorsRegistry::DropInstance();
        RegisterValidator(new TTestValidator(5, true));
        auto registry = TValidatorsRegistry::Instance();
        registry->LockValidators();

        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_TENANTS);
        TConfigIndex index = MakeSmallIndex();

        {
            TConfigModifications diff;
            diff.AddedItems.push_back(new TConfigItem(ITEM_TENANT1_LOG_2));
            TModificationsValidator validator(index, diff, config);
            UNIT_ASSERT(validator.ApplyValidators());
            UNIT_ASSERT_VALUES_EQUAL(validator.GetChecksDone(), 1);
        }

        {
            TConfigModifications diff;
            diff.AddedItems.push_back(new TConfigItem(ITEM_DOMAIN_LOG_2));
            diff.AddedItems.push_back(new TConfigItem(ITEM_TENANT1_LOG_2));
            TModificationsValidator validator(index, diff, config);
            UNIT_ASSERT(!validator.ApplyValidators());
            UNIT_ASSERT(validator.GetErrorMessage());
        }
    }

    void TestChecksLimitError()
    {
        TValidatorsRegistry::DropInstance();
        RegisterValidator(new TTestValidator(10));
        auto registry = TValidatorsRegistry::Instance();
        registry->LockValidators();

        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_TENANTS, 2, true);
        TConfigIndex index = MakeSmallIndex();

        {
            TConfigModifications diff;
            diff.AddedItems.push_back(new TConfigItem(ITEM_TENANT3_LOG_1));
            TModificationsValidator validator(index, diff, config);
            UNIT_ASSERT(validator.ApplyValidators());
            UNIT_ASSERT_VALUES_EQUAL(validator.GetChecksDone(), 1);
        }

        {
            TConfigModifications diff;
            diff.AddedItems.push_back(new TConfigItem(ITEM_DOMAIN_LOG_2));
            TModificationsValidator validator(index, diff, config);
            UNIT_ASSERT(validator.ApplyValidators());
            UNIT_ASSERT_VALUES_EQUAL(validator.GetChecksDone(), 2);
        }

        {
            TConfigModifications diff;
            diff.AddedItems.push_back(new TConfigItem(ITEM_DOMAIN_LOG_2));
            diff.AddedItems.push_back(new TConfigItem(ITEM_TENANT2_LOG_1));
            TModificationsValidator validator(index, diff, config);
            UNIT_ASSERT(!validator.ApplyValidators());
            UNIT_ASSERT(validator.GetErrorMessage());
            UNIT_ASSERT_VALUES_EQUAL(validator.GetChecksDone(), 2);
        }
    }

    void TestChecksLimitWarning()
    {
        TValidatorsRegistry::DropInstance();
        RegisterValidator(new TTestValidator(10));
        auto registry = TValidatorsRegistry::Instance();
        registry->LockValidators();

        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_TENANTS, 2, false);
        TConfigIndex index = MakeSmallIndex();

        {
            TConfigModifications diff;
            diff.AddedItems.push_back(new TConfigItem(ITEM_TENANT3_LOG_1));
            TModificationsValidator validator(index, diff, config);
            UNIT_ASSERT(validator.ApplyValidators());
            UNIT_ASSERT_VALUES_EQUAL(validator.GetChecksDone(), 1);
        }

        {
            TConfigModifications diff;
            diff.AddedItems.push_back(new TConfigItem(ITEM_DOMAIN_LOG_2));
            TModificationsValidator validator(index, diff, config);
            UNIT_ASSERT(validator.ApplyValidators());
            UNIT_ASSERT_VALUES_EQUAL(validator.GetChecksDone(), 2);
        }

        {
            TConfigModifications diff;
            diff.AddedItems.push_back(new TConfigItem(ITEM_DOMAIN_LOG_2));
            diff.AddedItems.push_back(new TConfigItem(ITEM_TENANT2_LOG_1));
            TModificationsValidator validator(index, diff, config);
            UNIT_ASSERT(validator.ApplyValidators());
            UNIT_ASSERT_VALUES_EQUAL(validator.GetIssues().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(validator.GetChecksDone(), 2);
        }
    }

    void TestTreatWarningAsError()
    {
        TValidatorsRegistry::DropInstance();
        RegisterValidator(new TTestValidator(2, false, true));
        auto registry = TValidatorsRegistry::Instance();
        registry->LockValidators();

        InitializeTestConfigItems();
        auto config = MakeConfigsConfig(NKikimrConsole::VALIDATE_TENANTS, 0, false);
        TConfigIndex index = MakeSmallIndex();

        {
            TConfigModifications diff;
            diff.AddedItems.push_back(new TConfigItem(ITEM_TENANT1_LOG_2));
            TModificationsValidator validator(index, diff, config);
            UNIT_ASSERT(validator.ApplyValidators());
        }

        config.TreatWarningAsError = true;
        {
            TConfigModifications diff;
            diff.AddedItems.push_back(new TConfigItem(ITEM_TENANT1_LOG_2));
            TModificationsValidator validator(index, diff, config);
            UNIT_ASSERT(!validator.ApplyValidators());
        }
    }
};

UNIT_TEST_SUITE_REGISTRATION(TModificationsValidatorTests);

} // namespace NKikimr::NConsole
