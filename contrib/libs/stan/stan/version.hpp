#ifndef STAN_VERSION_HPP
#define STAN_VERSION_HPP

#include <string>

#ifndef STAN_STRING_EXPAND
#define STAN_STRING_EXPAND(s) #s
#endif

#ifndef STAN_STRING
#define STAN_STRING(s) STAN_STRING_EXPAND(s)
#endif

#define STAN_MAJOR 2
#define STAN_MINOR 19
#define STAN_PATCH 1

namespace stan {

  /** Major version number for Stan package. */
  const std::string MAJOR_VERSION = STAN_STRING(STAN_MAJOR);

  /** Minor version number for Stan package. */
  const std::string MINOR_VERSION = STAN_STRING(STAN_MINOR);

  /** Patch version for Stan package. */
  const std::string PATCH_VERSION = STAN_STRING(STAN_PATCH);

}

#endif
