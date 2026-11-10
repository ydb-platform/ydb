#include <yql/essentials/tools/purebench/gbenchmark/register.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/yt/misc/static_initializer.h>

namespace NYql::NGBenchmark::NSmoke {

YT_STATIC_INITIALIZER(RegisterSuite("smoke", SRC_("suites")));

} // namespace NYql::NGBenchmark::NSmoke
