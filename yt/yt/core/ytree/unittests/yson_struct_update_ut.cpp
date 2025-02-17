#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/yson_struct.h>
#include <yt/yt/core/ytree/yson_struct_update.h>

namespace NYT::NYTree {
namespace {

using namespace NYson;

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

// TODO(coteeq): Validate
// static constexpr auto BadMaxFailedJobCount = 42;

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

TEST(TUpdateYsonStructTest, Simple)
{
    auto oldSpec = ConvertTo<TSpecWithPoolPtr>(TYsonString(TString("{pool=pool;}")));
    auto newSpec = ConvertTo<TSpecWithPoolPtr>(TYsonString(TString("{pool=new_pool;}")));

    std::string updatedPool;

    auto configurator = NYsonStructUpdate::TConfigurator<TSpecWithPool>();
    {
        configurator.Field("pool", &TSpecBase::Pool)
            .Updater(BIND([&] (const std::string& newPool) {
                updatedPool = newPool;
            }));
    }

    NYsonStructUpdate::Update(configurator, oldSpec, newSpec);

    EXPECT_EQ(updatedPool, "new_pool");
}

TEST(TUpdateYsonStructTest, NonUpdatable)
{
    auto oldSpec = ConvertTo<TSpecWithPoolPtr>(TYsonString(TString("{non_updatable=42;pool=pool}")));
    auto newSpec = ConvertTo<TSpecWithPoolPtr>(TYsonString(TString("{non_updatable=43;pool=pool}")));

    std::string updatedPool;

    auto configurator = NYsonStructUpdate::TConfigurator<TSpecWithPool>();
    {
        configurator.Field("pool", &TSpecBase::Pool)
            .Updater(BIND([&] (const std::string& newPool) {
                updatedPool = newPool;
            }));
    }

    EXPECT_ANY_THROW({
        NYsonStructUpdate::Update(configurator, oldSpec, newSpec);
    });
}


TEST(TUpdateYsonStructTest, Inherited)
{
    auto oldSpec = ConvertTo<TSpecBasePtr>(TYsonString(TString("{pool=pool;}")));
    auto newSpec = ConvertTo<TSpecBasePtr>(TYsonString(TString("{pool=new_pool;}")));

    std::string updatedPool;

    auto configurator = NYsonStructUpdate::TConfigurator<TSpecBase>();
    {
        NYsonStructUpdate::TConfigurator<TSpecWithPool> parentRegistrar = configurator;
        parentRegistrar.Field("pool", &TSpecBase::Pool)
            .Updater(BIND([&] (const std::string& newPool) {
                updatedPool = newPool;
            }));
    }

    NYsonStructUpdate::Update(configurator, oldSpec, newSpec);

    EXPECT_EQ(updatedPool, "new_pool");
}

TEST(TUpdateYsonStructTest, Nested)
{
    auto oldSpec = ConvertTo<TSpecBasePtr>(TYsonString(TString("{pool=pool;    mapper={command=cat};}")));
    auto newSpec = ConvertTo<TSpecBasePtr>(TYsonString(TString("{pool=new_pool;mapper={command=sort};}")));

    std::string updatedPool;
    std::string updatedCommand;

    auto configurator = NYsonStructUpdate::TConfigurator<TSpecBase>();
    {
        NYsonStructUpdate::TConfigurator<TSpecWithPool> parentRegistrar = configurator;
        parentRegistrar.Field("pool", &TSpecBase::Pool)
            .Updater(BIND([&] (const std::string& newPool) {
                updatedPool = newPool;
            }));
    }
    configurator.Field("mapper", &TSpecBase::Mapper)
        .NestedUpdater(BIND([&](NYsonStructUpdate::TConfigurator<TMapperSpec> configurator) {
            configurator.Field("command", &TMapperSpec::Command)
                .Updater(BIND([&] (const std::string& newCommand) {
                    updatedCommand = newCommand;
                }));
        }));

    NYsonStructUpdate::Update(configurator, oldSpec, newSpec);

    EXPECT_EQ(updatedPool, "new_pool");
    EXPECT_EQ(updatedCommand, "sort");
}

} // namespace
} // namespace NYT::NYTree
