#include "s3_recipe_ut_helpers.h"

#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>

namespace NTestUtils {

using std::shared_ptr;
using namespace NKikimr::NKqp;

const TString TEST_SCHEMA = R"(["StructType";[["key";["DataType";"Utf8";];];["value";["DataType";"Utf8";];];];])";
const TString TEST_SCHEMA_IDS = R"(["StructType";[["key";["DataType";"Utf8";];];];])";

shared_ptr<TKikimrRunner> MakeKikimrRunner(std::optional<NKikimrConfig::TAppConfig> appConfig, const TString& domainRoot)
{
    return NFederatedQueryTest::MakeKikimrRunner(true, nullptr, nullptr, appConfig, NYql::NDq::CreateS3ActorsFactory(), domainRoot);
}

}
