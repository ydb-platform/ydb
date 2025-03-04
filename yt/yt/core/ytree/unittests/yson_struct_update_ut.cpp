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

} // namespace
} // namespace NYT::NYTree
