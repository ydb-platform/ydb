#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

#include "attribute_consumer.h"
#include "serialize.h"
#include "convert.h"

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T, class R>
IYPathServicePtr IYPathService::FromMethod(
    R (std::remove_cv_t<T>::*method) () const,
    const TWeakPtr<T>& weak)
{
    auto boundProducer = NYson::TYsonProducer(BIND_NO_PROPAGATE([=] (NYson::IYsonConsumer* consumer) {
        auto strong = weak.Lock();
        if (strong) {
            Serialize((strong.Get()->*method)(), consumer);
        } else {
            consumer->OnBeginAttributes();
            consumer->OnKeyedItem("object_destroyed");
            consumer->OnBooleanScalar(true);
            consumer->OnEndAttributes();
            consumer->OnEntity();
        }
    }));

    return FromProducer(std::move(boundProducer));
}

template <class T>
IYPathServicePtr IYPathService::FromMethod(
    void (std::remove_cv_t<T>::*producer) (NYson::IYsonConsumer*) const,
    const TWeakPtr<T>& weak)
{
    auto boundProducer = NYson::TYsonProducer(BIND_NO_PROPAGATE([=] (NYson::IYsonConsumer* consumer) {
        auto strong = weak.Lock();
        if (strong) {
            (strong.Get()->*producer)(consumer);
        } else {
            consumer->OnBeginAttributes();
            consumer->OnKeyedItem("object_destroyed");
            consumer->OnBooleanScalar(true);
            consumer->OnEndAttributes();
            consumer->OnEntity();
        }
    }));

    return FromProducer(std::move(boundProducer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
