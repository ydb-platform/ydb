#ifndef STAN_SERVICES_UTIL_CREATE_RNG_HPP
#define STAN_SERVICES_UTIL_CREATE_RNG_HPP

#include <boost/random/additive_combine.hpp>

namespace stan {
  namespace services {
    namespace util {

      /**
       * Creates a pseudo random number generator from a random seed
       * and a chain id by initializing the PRNG with the seed and
       * then advancing past pow(2, 50) times the chain ID draws to
       * ensure different chains sample from different segments of the
       * pseudo random number sequence.
       *
       * Chain IDs should be kept to larger values than one to ensure
       * that the draws used to initialized transformed data are not
       * duplicated.
       *
       * @param[in] seed the random seed
       * @param[in] chain the chain id
       * @return a boost::ecuyer1988 instance
       */
      inline boost::ecuyer1988 create_rng(unsigned int seed,
                                          unsigned int chain) {
        using boost::uintmax_t;
        static uintmax_t DISCARD_STRIDE = static_cast<uintmax_t>(1) << 50;
        boost::ecuyer1988 rng(seed);
        rng.discard(DISCARD_STRIDE * chain);
        return rng;
      }

    }
  }
}
#endif
