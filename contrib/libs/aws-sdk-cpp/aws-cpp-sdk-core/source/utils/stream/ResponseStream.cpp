/** 
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. 
 * SPDX-License-Identifier: Apache-2.0. 
 */ 

#include <aws/core/utils/stream/ResponseStream.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>

#if defined(_GLIBCXX_FULLY_DYNAMIC_STRING) && _GLIBCXX_FULLY_DYNAMIC_STRING == 0 && defined(__ANDROID__)
#include <aws/core/utils/stream/SimpleStreamBuf.h>
using DefaultStreamBufType = Aws::Utils::Stream::SimpleStreamBuf;
#else
using DefaultStreamBufType = Aws::StringBuf;
#endif

using namespace Aws::Utils::Stream;

ResponseStream::ResponseStream(void) :
    m_underlyingStream(nullptr)
{
}

ResponseStream::ResponseStream(Aws::IOStream* underlyingStreamToManage) :
    m_underlyingStream(underlyingStreamToManage)
{
}

ResponseStream::ResponseStream(const Aws::IOStreamFactory& factory) :
    m_underlyingStream(factory())
{
}

ResponseStream::ResponseStream(ResponseStream&& toMove) : m_underlyingStream(toMove.m_underlyingStream)
{
    toMove.m_underlyingStream = nullptr;
}

ResponseStream& ResponseStream::operator=(ResponseStream&& toMove)
{
    if(m_underlyingStream == toMove.m_underlyingStream)
    {
        return *this;
    }

    ReleaseStream();
    m_underlyingStream = toMove.m_underlyingStream;
    toMove.m_underlyingStream = nullptr;

    return *this;
}

ResponseStream::~ResponseStream()
{
    ReleaseStream();
}

void ResponseStream::ReleaseStream()
{
    if (m_underlyingStream)
    {
        m_underlyingStream->flush();
        Aws::Delete(m_underlyingStream);
    }

    m_underlyingStream = nullptr;
}

static const char *DEFAULT_STREAM_TAG = "DefaultUnderlyingStream";

DefaultUnderlyingStream::DefaultUnderlyingStream() :
    Base( Aws::New< DefaultStreamBufType >( DEFAULT_STREAM_TAG ) )
{}

DefaultUnderlyingStream::DefaultUnderlyingStream(Aws::UniquePtr<std::streambuf> buf) :
    Base(buf.release())
{}

DefaultUnderlyingStream::~DefaultUnderlyingStream()
{
    if( rdbuf() )
    {
        Aws::Delete( rdbuf() );
    }
}

static const char* RESPONSE_STREAM_FACTORY_TAG = "ResponseStreamFactory";

Aws::IOStream* Aws::Utils::Stream::DefaultResponseStreamFactoryMethod() 
{
    return Aws::New<Aws::Utils::Stream::DefaultUnderlyingStream>(RESPONSE_STREAM_FACTORY_TAG);
}
