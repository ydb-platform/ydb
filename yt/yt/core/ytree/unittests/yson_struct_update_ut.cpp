#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/yson_struct.h>
#include <yt/yt/core/ytree/yson_struct_update.h>

namespace NYT::NYTree {
namespace {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

template <class TSpecPtr>
auto CreateSpec(const TString& specString)
{
    return ConvertTo<TSpecPtr>(TYsonString(specString));
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TMapperSpec)

struct TMapperSpec
    : public TYsonStruct
{
    std::string Command;

    REGISTER_YSON_STRUCT(TMapperSpec);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("command", &TThis::Command)
            .Default("cat");
    }
};

DEFINE_REFCOUNTED_TYPE(TMapperSpec)

DECLARE_REFCOUNTED_STRUCT(TSpecWithPool)

struct TSpecWithPool
    : public TYsonStruct
{
public:
    std::string Pool;
    int NonUpdatable;
    IMapNodePtr MapNode;

    REGISTER_YSON_STRUCT(TSpecWithPool);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("pool", &TThis::Pool);
        registrar.Parameter("non_updatable", &TThis::NonUpdatable)
            .Default(10);
        registrar.Parameter("map_node", &TThis::MapNode)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TSpecWithPool)

DECLARE_REFCOUNTED_STRUCT(TSpecBase)

struct TSpecBase
    : public TSpecWithPool
{
public:
    int MaxFailedJobCount;
    TMapperSpecPtr Mapper;

    REGISTER_YSON_STRUCT(TSpecBase);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("max_failed_job_count", &TThis::MaxFailedJobCount)
            .Default(10)
            .GreaterThan(0)
            .LessThan(1000);

        registrar.Parameter("mapper", &TThis::Mapper)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TSpecBase)

DECLARE_REFCOUNTED_STRUCT(TVanillaTaskSpec)

struct TVanillaTaskSpec
    : public TYsonStruct
{
public:
    std::string Command;
    bool Creatable;
    bool Removable;

    REGISTER_YSON_STRUCT(TVanillaTaskSpec);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("command", &TThis::Command)
            .Default();
        registrar.Parameter("creatable", &TThis::Creatable)
            .Default(true);
        registrar.Parameter("removable", &TThis::Removable)
            .Default(true);
    }
};

DEFINE_REFCOUNTED_TYPE(TVanillaTaskSpec)

DECLARE_REFCOUNTED_STRUCT(TVanillaSpec)

struct TVanillaSpec
    : public TYsonStruct
{
public:
    THashMap<TString, TVanillaTaskSpecPtr> Tasks;

    REGISTER_YSON_STRUCT(TVanillaSpec);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("tasks", &TThis::Tasks)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TVanillaSpec)

////////////////////////////////////////////////////////////////////////////////


TEST(TUpdateYsonStructTest, Simple)
{
    auto oldSpec = ConvertTo<TSpecWithPoolPtr>(TYsonString(TString("{pool=pool;}")));
    auto newSpec = ConvertTo<TSpecWithPoolPtr>(TYsonString(TString("{pool=new_pool;}")));

    std::string updatedPool;

    auto configurator = TConfigurator<TSpecWithPool>();
    {
        configurator.Field("pool", &TSpecBase::Pool)
            .Updater(BIND([&] (const std::string& newPool) {
                updatedPool = newPool;
            }));
    }

    std::move(configurator).Seal().Update(oldSpec, newSpec);

    EXPECT_EQ(updatedPool, "new_pool");
}

TEST(TUpdateYsonStructTest, NonUpdatable)
{
    auto oldSpec = ConvertTo<TSpecWithPoolPtr>(TYsonString(TString("{non_updatable=42;pool=pool}")));
    auto newSpec = ConvertTo<TSpecWithPoolPtr>(TYsonString(TString("{non_updatable=43;pool=pool}")));

    std::string updatedPool;

    auto configurator = TConfigurator<TSpecWithPool>();
    {
        configurator.Field("pool", &TSpecBase::Pool)
            .Updater(BIND([&] (const std::string& newPool) {
                updatedPool = newPool;
            }));
    }

    EXPECT_ANY_THROW({
        std::move(configurator).Seal().Update(oldSpec, newSpec);
    });
}


TEST(TUpdateYsonStructTest, Inherited)
{
    auto oldSpec = ConvertTo<TSpecBasePtr>(TYsonString(TString("{pool=pool;}")));
    auto newSpec = ConvertTo<TSpecBasePtr>(TYsonString(TString("{pool=new_pool;}")));

    std::string updatedPool;

    auto configurator = TConfigurator<TSpecBase>();
    {
        TConfigurator<TSpecWithPool> parentConfigurator = configurator;
        parentConfigurator.Field("pool", &TSpecBase::Pool)
            .Updater(BIND([&] (const std::string& newPool) {
                updatedPool = newPool;
            }));
    }

    std::move(configurator).Seal().Update(oldSpec, newSpec);

    EXPECT_EQ(updatedPool, "new_pool");
}

TEST(TUpdateYsonStructTest, Nested)
{
    auto oldSpec = ConvertTo<TSpecBasePtr>(TYsonString(TString("{pool=pool;    mapper={command=cat};}")));
    auto newSpec = ConvertTo<TSpecBasePtr>(TYsonString(TString("{pool=new_pool;mapper={command=sort};}")));

    std::string updatedPool;
    std::string updatedCommand;

    auto configurator = TConfigurator<TSpecBase>();
    {
        TConfigurator<TSpecWithPool> parentConfigurator = configurator;
        parentConfigurator.Field("pool", &TSpecBase::Pool)
            .Updater(BIND([&] (const std::string& newPool) {
                updatedPool = newPool;
            }));
    }
    configurator.Field("mapper", &TSpecBase::Mapper)
        .NestedUpdater(BIND([&] () {
            TConfigurator<TMapperSpec> configurator;
            configurator.Field("command", &TMapperSpec::Command)
                .Updater(BIND([&] (const std::string& newCommand) {
                    updatedCommand = newCommand;
                }));
            return TSealedConfigurator(std::move(configurator));
        }));

    std::move(configurator).Seal().Update(oldSpec, newSpec);

    EXPECT_EQ(updatedPool, "new_pool");
    EXPECT_EQ(updatedCommand, "sort");
}

TEST(TUpdateYsonStructTest, Validate)
{
    auto oldSpec = ConvertTo<TSpecWithPoolPtr>(TYsonString(TString("{pool=pool;}")));
    auto longPoolSpec = ConvertTo<TSpecWithPoolPtr>(TYsonString(TString("{pool=new_pool;}")));
    auto shortPoolSpec = ConvertTo<TSpecWithPoolPtr>(TYsonString(TString("{pool=p;}")));

    std::string updatedPool;

    auto configurator = TConfigurator<TSpecWithPool>();
    configurator.Field("pool", &TSpecWithPool::Pool)
        .Validator(BIND([&] (const std::string& newPool) {
            THROW_ERROR_EXCEPTION_IF(
                newPool.size() > 4,
                "Pool name too long");
        }))
        .Updater(BIND([&] (const std::string& newPool) {
            updatedPool = newPool;
        }));

    auto sealed = std::move(configurator).Seal();

    EXPECT_THROW_WITH_SUBSTRING(
        sealed.Validate(oldSpec, longPoolSpec),
        "Pool name too long");

    sealed.Validate(oldSpec, shortPoolSpec);
    sealed.Update(oldSpec, shortPoolSpec);

    EXPECT_EQ(updatedPool, "p");
}


TEST(TUpdateYsonStructTest, MapField)
{
    std::string firstCommand;
    std::string secondCommand;

    auto configurator = TConfigurator<TVanillaSpec>();
    {
        TConfigurator<TVanillaSpec> parentConfigurator = configurator;
        auto& mapConfigurator = parentConfigurator.MapField("tasks", &TVanillaSpec::Tasks)
            .ValidateOnAdded(BIND([] (const TString&, const TVanillaTaskSpecPtr&) {
                THROW_ERROR_EXCEPTION("Non-fatal create exception");
            }))
            .ValidateOnRemoved(BIND([] (const TString&, const TVanillaTaskSpecPtr&) {
                THROW_ERROR_EXCEPTION("Non-fatal remove exception");
            }))
            .OnAdded(BIND([] (const TString&, const TVanillaTaskSpecPtr&) -> TConfigurator<TVanillaTaskSpec> {
                THROW_ERROR_EXCEPTION("Fatal create exception");
            }))
            .OnRemoved(BIND([] (const TString&, const TVanillaTaskSpecPtr&) {
                THROW_ERROR_EXCEPTION("Fatal remove exception");
            }));

        auto configureChild = [&] (std::string& ref) {
            TConfigurator<TVanillaTaskSpec> configurator;
            configurator.Field("command", &TVanillaTaskSpec::Command)
                .Updater(BIND([&] (const std::string& newCommand) {
                    ref = newCommand;
                }));
            return configurator;
        };

        mapConfigurator.ConfigureChild("one", configureChild(firstCommand));
        mapConfigurator.ConfigureChild("two", configureChild(secondCommand));
    }

    auto sealed = std::move(configurator).Seal();

    auto oldSpec = CreateSpec<TVanillaSpecPtr>("{tasks={one={command=one};two={command=two};}}");
    auto withCreatedTask = CreateSpec<TVanillaSpecPtr>("{tasks={one={command=one};two={command=two};three={command=three};}}");
    auto withRemovedTask = CreateSpec<TVanillaSpecPtr>("{tasks={one={command=one};}}");
    auto changedCommandSpec = CreateSpec<TVanillaSpecPtr>("{tasks={one={command=one};two={command=\"two --new\"};}}");

    EXPECT_THROW_WITH_SUBSTRING(
        sealed.Validate(oldSpec, withCreatedTask),
        "Non-fatal create exception");
    EXPECT_THROW_WITH_SUBSTRING(
        sealed.Update(oldSpec, withCreatedTask),
        "Fatal create exception");

    EXPECT_THROW_WITH_SUBSTRING(
        sealed.Validate(oldSpec, withRemovedTask),
        "Non-fatal remove exception");
    EXPECT_THROW_WITH_SUBSTRING(
        sealed.Update(oldSpec, withRemovedTask),
        "Fatal remove exception");

    sealed.Update(oldSpec, changedCommandSpec);
    // Did not update.
    EXPECT_EQ(firstCommand, "");
    // Updated.
    EXPECT_EQ(secondCommand, "two --new");
}

TEST(TUpdateYsonStructTest, MapFieldCustomCreate)
{
    std::string firstCommand;
    std::string secondCommand;
    std::string thirdCommand;
    bool thirdConfigured = false;

    auto configure = [&] () {
        auto configurator = TConfigurator<TVanillaSpec>();
        auto configureChild = [&] (std::string& ref) {
            TConfigurator<TVanillaTaskSpec> taskConfigurator;
            taskConfigurator.Field("command", &TVanillaTaskSpec::Command)
                .Updater(BIND([&] (const std::string& newCommand) {
                    ref = newCommand;
                }));
            return taskConfigurator;
        };

        TConfigurator<TVanillaSpec> parentConfigurator = configurator;
        auto& mapConfigurator = parentConfigurator.MapField("tasks", &TVanillaSpec::Tasks)
            .ValidateOnAdded(BIND([] (const TString&, const TVanillaTaskSpecPtr& taskSpec) {
                THROW_ERROR_EXCEPTION_IF(
                    !taskSpec->Creatable,
                    "Non-fatal create exception");
            }))
            .ValidateOnRemoved(BIND([] (const TString&, const TVanillaTaskSpecPtr& taskSpec) {
                THROW_ERROR_EXCEPTION_IF(
                    !taskSpec->Removable,
                    "Non-fatal remove exception");
            }))
            .OnAdded(BIND([&] (const TString&, const TVanillaTaskSpecPtr& taskSpec) -> TConfigurator<TVanillaTaskSpec> {
                THROW_ERROR_EXCEPTION_IF(
                    !taskSpec->Creatable,
                    "Fatal create exception");
                EXPECT_FALSE(thirdConfigured);
                thirdConfigured = true;
                return configureChild(thirdCommand);
            }))
            .OnRemoved(BIND([] (const TString&, const TVanillaTaskSpecPtr& taskSpec) {
                THROW_ERROR_EXCEPTION_IF(
                    !taskSpec->Removable,
                    "Fatal remove exception");
            }));

        mapConfigurator.ConfigureChild("one", configureChild(firstCommand));
        mapConfigurator.ConfigureChild("two", configureChild(secondCommand));

        return configurator;
    };

    auto sealed = configure().Seal();

    auto oldSpec = CreateSpec<TVanillaSpecPtr>("{tasks={one={command=one;removable=%false};two={command=two};}}");
    auto removedOne = CreateSpec<TVanillaSpecPtr>("{tasks={two={command=two};}}");
    auto createdCreatableThree = CreateSpec<TVanillaSpecPtr>("{tasks={one={command=one;removable=%false};two={command=two};three={command=three}}}");
    auto createdNonCreatableThree = CreateSpec<TVanillaSpecPtr>("{tasks={one={command=one;removable=%false};two={command=two};three={command=three;creatable=%false}}}");
    auto removedTwo = CreateSpec<TVanillaSpecPtr>("{tasks={one={command=one;removable=%false};}}");
    auto updatedThree = CreateSpec<TVanillaSpecPtr>("{tasks={one={command=one;removable=%false};two={command=two};three={command=three_updated}}}");

    // removedOne.
    EXPECT_THROW_WITH_SUBSTRING(
        sealed.Validate(oldSpec, removedOne),
        "Non-fatal remove exception");
    EXPECT_THROW_WITH_SUBSTRING(
        sealed.Update(oldSpec, removedOne),
        "Fatal remove exception");

    // createdNonCreatableThree.
    EXPECT_THROW_WITH_SUBSTRING(
        sealed.Validate(oldSpec, createdNonCreatableThree),
        "Non-fatal create exception");
    EXPECT_THROW_WITH_SUBSTRING(
        sealed.Update(oldSpec, createdNonCreatableThree),
        "Fatal create exception");

    // removedTwo.
    sealed.Validate(oldSpec, removedTwo);
    sealed.Update(oldSpec, removedTwo);

    // Reconfigure |sealed|. See also todo for TMapFieldConfigurator.
    sealed = configure().Seal();

    // createdCreatableThree.
    sealed.Validate(oldSpec, createdCreatableThree);
    sealed.Update(oldSpec, createdCreatableThree);

    // |thirdCommand| should not be updated yet.
    // It is only created and configured.
    EXPECT_EQ(thirdCommand, "");

    // Check that we can now update the third task.
    sealed.Validate(createdCreatableThree, updatedThree);
    sealed.Update(createdCreatableThree, updatedThree);

    EXPECT_EQ(thirdCommand, "three_updated");
}

} // namespace
} // namespace NYT::NYTree
