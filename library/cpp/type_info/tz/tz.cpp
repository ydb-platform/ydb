#include "tz.h"

namespace NTi {

namespace {

static constexpr std::initializer_list<const std::string_view> TimezonesInit = {
#include "tz.gen"
};

static constexpr TArrayRef<const std::string_view> Timezones(TimezonesInit);

}

TArrayRef<const std::string_view> GetTimezones() {
    return Timezones;
}

}
