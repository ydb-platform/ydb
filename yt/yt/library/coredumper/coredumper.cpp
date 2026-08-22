#include "coredumper.h"

#include <yt/yt/core/ytree/ephemeral_node_factory.h>

namespace NYT::NCoreDump {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

struct TMockCoreDumper
    : public ICoreDumper
{
    virtual TCoreDump WriteCoreDump(const std::vector<TString>& /*notes*/, const TString& /*reason*/)
    {
        THROW_ERROR_EXCEPTION("Coredumper library is not available under this build configuration");
    }

    virtual const NYTree::IYPathServicePtr& CreateOrchidService() const
    {
        static const NYTree::IYPathServicePtr EmptyMapNode = GetEphemeralNodeFactory()->CreateMap();
        return EmptyMapNode;
    }
};

DEFINE_REFCOUNTED_TYPE(TMockCoreDumper)

Y_WEAK ICoreDumperPtr CreateCoreDumper(TCoreDumperConfigPtr /*config*/)
{
    // This implementation is used when core_dumper_impl.cpp is not linked.

    return New<TMockCoreDumper>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCoreDump
