#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A thread-safe generator producing increasing sequence of numbers.
class TIdGenerator
{
public:
    ui64 Next();
    void Reset();

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

private:
    std::atomic<ui64> Current_ = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
