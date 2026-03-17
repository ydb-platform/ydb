#pragma once

#include <Interpreters/Context_fwd.h>

namespace DBPoco
{

namespace Util
{
class LayeredConfiguration;
}

class Logger;

}


namespace DB
{

class IServer
{
public:
    /// Returns the application's configuration.
    virtual DBPoco::Util::LayeredConfiguration & config() const = 0;

    /// Returns the application's logger.
    virtual DBPoco::Logger & logger() const = 0;

    /// Returns global application's context.
    virtual ContextMutablePtr context() const = 0;

    /// Returns true if shutdown signaled.
    virtual bool isCancelled() const = 0;

    virtual ~IServer() = default;
};

}
