#pragma once

#include "public.h"
#include "interned_attributes.h"
#include "ypath_service.h"

#include <yt/yt/core/yson/producer.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

//! A map ypath service whose items are assembled programmatically: static child services, batches
//! of items produced together, and system attributes. The mutators chain (each returns |this|).
struct ICompositeMapService
    : public virtual IYPathService
{
    //! Adds a single item #service under #key.
    virtual TIntrusivePtr<ICompositeMapService> AddChild(const std::string& key, IYPathServicePtr service) = 0;

    //! Splices the root-level items emitted by #producer (which must emit a map) into this map, as
    //! if each were added via #AddChild. Handy for grouping a batch of items produced together.
    //! NB: #producer is materialized eagerly on every access, so this suits only small portions of
    //! data (e.g. a handful of scalar fields), not large or hot subtrees.
    virtual TIntrusivePtr<ICompositeMapService> AddChildren(NYson::TYsonProducer producer) = 0;

    //! Adds a system attribute #key served by #producer.
    virtual TIntrusivePtr<ICompositeMapService> AddAttribute(TInternedAttributeKey key, NYson::TYsonCallback producer) = 0;

    //! Sets whether the map is opaque, i.e. renders as an entity (rather than inline) when an
    //! ancestor is fetched wholesale.
    virtual TIntrusivePtr<ICompositeMapService> SetOpaque(bool opaque) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICompositeMapService)

////////////////////////////////////////////////////////////////////////////////

ICompositeMapServicePtr CreateCompositeMapService();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
