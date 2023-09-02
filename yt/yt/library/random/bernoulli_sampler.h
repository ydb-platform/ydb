#pragma once

#include <yt/yt/core/misc/public.h>

#include <random>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A simple helper that is used in sampling routines.
//! It is deterministic and persistable (as POD).
class TBernoulliSampler
{
public:
    explicit TBernoulliSampler(
        std::optional<double> samplingRate = std::nullopt,
        std::optional<ui64> seed = std::nullopt);

    bool Sample();

    //! Result of this sampling depends on `salt' and `seed' only
    //! and not depends on previous sample calls.
    bool Sample(ui64 salt);

    void Persist(const TStreamPersistenceContext& context);

private:
    std::optional<double> SamplingRate_;
    std::optional<ui64> Seed_;
    std::mt19937 Generator_;
    std::bernoulli_distribution Distribution_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
