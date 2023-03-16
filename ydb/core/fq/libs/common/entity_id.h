#pragma once

#include <util/generic/string.h>
#include <util/datetime/base.h>

namespace NFq {

enum class EEntityType : char {
    UNDEFINED = 'u',
    QUERY = 'q',
    JOB = 'j',
    RESULT = 'r',
    CONNECTION = 'c',
    BINDING = 'b',
    CHECKPOINT_GRAPH_DESCRIPTION = 'g',
};

TString GetEntityIdAsString(const TString& prefix, EEntityType type);
TString GetEntityIdAsString(const TString& prefix, EEntityType type, TInstant now, ui32 rnd);

struct IEntityIdGenerator : public TThrRefBase {
    using TPtr = TIntrusivePtr<IEntityIdGenerator>;

    virtual TString Generate(EEntityType type) = 0;
};

IEntityIdGenerator::TPtr CreateEntityIdGenerator(const TString& prefix);

} // namespace NFq
