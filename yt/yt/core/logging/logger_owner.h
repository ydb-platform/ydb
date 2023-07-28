#pragma once

#include "serializable_logger.h"

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

//! Typically serves as a virtual base for classes that need a member logger.
class TLoggerOwner
{
protected:
    TSerializableLogger Logger;

    TLoggerOwner() = default;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);
    void Persist(const TStreamPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
