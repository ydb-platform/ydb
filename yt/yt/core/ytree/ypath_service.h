#pragma once

#include "public.h"
#include "permission.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/library/profiling/sensor.h>

#include <variant>


namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Represents an abstract way of handling YPath requests.
/*!
 *  To handle a given YPath request one must first resolve the target.
 *
 *  We start with some root service and call #Resolve. The latter either replies "here", in which case
 *  the resolution is finished, or "there", in which case a new candidate target is provided.
 *  At each resolution step the current path may be altered by specifying a new one
 *  as a part of the result.
 *
 *  Once the request is resolved, #Invoke is called for the target service.
 *
 *  This interface also provides means for inspecting attributes associated with the service.
 *
 */
struct IYPathService
    : public virtual TRefCounted
{
    //! A result indicating that resolution is finished.
    struct TResolveResultHere
    {
        TYPath Path;
    };

    //! A result indicating that resolution must proceed.
    struct TResolveResultThere
    {
        IYPathServicePtr Service;
        TYPath Path;
    };

    using TResolveResult = std::variant<
        TResolveResultHere,
        TResolveResultThere
    >;

    //! Resolves the given path by either returning "here" or "there" result.
    virtual TResolveResult Resolve(const TYPath& path, const IYPathServiceContextPtr& context) = 0;

    //! Executes a given request.
    virtual void Invoke(const IYPathServiceContextPtr& context) = 0;

    //! Writes a map fragment consisting of attributes conforming to #filter into #consumer.
    /*!
     *  If #stable is |true| then the implementation must ensure a stable result.
     */
    void WriteAttributesFragment(
        NYson::IAsyncYsonConsumer* consumer,
        const TAttributeFilter& attributeFilter,
        bool stable);

    //! Wraps WriteAttributesFragment by enclosing attributes with angle brackets.
    //! If WriteAttributesFragment writes nothing then this method also does nothing.
    void WriteAttributes(
        NYson::IAsyncYsonConsumer* consumer,
        const TAttributeFilter& attributeFilter,
        bool stable);

    //! Manages strategy of writing attributes if attribute keys are null.
    virtual bool ShouldHideAttributes() = 0;

    // Extension methods

    //! Creates a YPath service from a YSON producer.
    /*!
     *  Each time a request is executed, producer is called, its output is turned into
     *  an ephemeral tree, and the request is forwarded to that tree.
     */
    static IYPathServicePtr FromProducer(
        NYson::TYsonProducer producer,
        TDuration cachePeriod = {});

    //! Creates a YPathDesignated service from a YSON producer.
    /*!
     *  Optimized version of previous service.
     *  Tries to avoid constructing an ephemeral tree using lazy YPath service.
     */
    static IYPathServicePtr FromProducerLazy(
        NYson::TYsonProducer producer);

    //! Creates a YPath service from an extended YSON producer.
    /*!
     *  Each time a request is executed, producer is called, its output is turned into
     *  an ephemeral tree, and the request is forwarded to that tree.
     */
    static IYPathServicePtr FromProducer(
        NYson::TExtendedYsonProducer<const IAttributeDictionaryPtr&> producer);

    //! Creates a producer from YPath service.
    /*!
     *  Takes a periodic snapshot of the service's root and returns it
     *  synchronously upon request. Snapshotting takes place in #workerInvoker.
     */
    NYson::TYsonProducer ToProducer(
        IInvokerPtr workerInvoker,
        TDuration updatePeriod);

    //! Creates a YPath service from a class method returning serializable value.
    template <class T, class R>
    static IYPathServicePtr FromMethod(
        R (std::remove_cv_t<T>::*method) () const,
        const TWeakPtr<T>& weak);

    //! Creates a YPath service from a class producer method.
    template <class T>
    static IYPathServicePtr FromMethod(
        void (std::remove_cv_t<T>::*producer) (NYson::IYsonConsumer*) const,
        const TWeakPtr<T>& weak);


    //! Creates a wrapper that handles all requests via the given invoker.
    IYPathServicePtr Via(IInvokerPtr invoker);

    //! Creates a wrapper that makes ephemeral snapshots to cache
    //! the underlying service.
    //! Building tree from underlying service is performed in #invoker,
    IYPathServicePtr Cached(
        TDuration updatePeriod,
        IInvokerPtr workerInvoker,
        const NProfiling::TProfiler& profiler = {});

    using TPermissionValidator = TCallback<void(const std::string& user, EPermission permission)>;
    //! Creates a wrapper that calls given callback on each invocation
    //! in order to validate user permission to query the ypath service.
    IYPathServicePtr WithPermissionValidator(TPermissionValidator validator);

protected:
    //! Implementation method for WriteAttributesFragment.
    //! It always write requested attributes and call ShouldHideAttributes.
    virtual void DoWriteAttributesFragment(
        NYson::IAsyncYsonConsumer* consumer,
        const TAttributeFilter& attributeFilter,
        bool stable) = 0;
};

DEFINE_REFCOUNTED_TYPE(IYPathService)

////////////////////////////////////////////////////////////////////////////////

struct ICachedYPathService
    : public virtual IYPathService
{
    virtual void SetCachePeriod(TDuration period) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICachedYPathService)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
