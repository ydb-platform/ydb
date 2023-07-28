#include "http.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NHttp {

////////////////////////////////////////////////////////////////////////////////

TRequestMock::TRequestMock()
    : Headers(New<THeaders>())
{
    ON_CALL(*this, GetMethod()).WillByDefault(::testing::Return(EMethod::Get));
    ON_CALL(*this, GetUrl()).WillByDefault(::testing::ReturnRef(Url));
    ON_CALL(*this, GetHeaders()).WillByDefault(::testing::ReturnRef(Headers));
    ON_CALL(*this, GetRemoteAddress()).WillByDefault(::testing::ReturnRef(Address));
    ON_CALL(*this, GetStartTime()).WillByDefault(::testing::Return(TInstant::Now()));
}

////////////////////////////////////////////////////////////////////////////////

TResponseWriterMock::TResponseWriterMock()
    : Headers(New<THeaders>())
    , Trailers(New<THeaders>())
{
    ON_CALL(*this, GetHeaders()).WillByDefault(::testing::ReturnRef(Headers));
    ON_CALL(*this, GetTrailers()).WillByDefault(::testing::ReturnRef(Trailers));

    ON_CALL(*this, GetStatus()).WillByDefault(::testing::Return(std::make_optional(EStatusCode::InternalServerError)));
    ON_CALL(*this, Write(::testing::_)).WillByDefault(::testing::Return(VoidFuture));
    ON_CALL(*this, WriteBody(::testing::_)).WillByDefault(::testing::Return(VoidFuture));
    ON_CALL(*this, Flush()).WillByDefault(::testing::Return(VoidFuture));
    ON_CALL(*this, Close()).WillByDefault(::testing::Return(VoidFuture));
}

////////////////////////////////////////////////////////////////////////////////

TResponseMock::TResponseMock()
    : Headers(New<THeaders>())
    , Trailers(New<THeaders>())
{
    ON_CALL(*this, GetHeaders()).WillByDefault(::testing::ReturnRef(Headers));
    ON_CALL(*this, GetTrailers()).WillByDefault(::testing::ReturnRef(Trailers));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
