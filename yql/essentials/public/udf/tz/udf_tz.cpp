#include "udf_tz.h"

namespace NYql {
namespace NUdf {

namespace {

static constexpr std::initializer_list<const std::string_view> TimezonesInit = {
#include "udf_tz.gen"
};

static constexpr TArrayRef<const std::string_view> Timezones(TimezonesInit);

}

TArrayRef<const std::string_view> GetTimezones() {
    return Timezones;
}
}
}
