#pragma once

#include "log.h"

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

class TSerializableLogger
    : public TLogger
{
public:
    using TLogger::TLogger;
    TSerializableLogger(TLogger logger);

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
