/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/core/utils/event/EventEncoderStream.h>
#include <iostream>

namespace Aws
{
    namespace Utils
    {
        namespace Event
        {
            EventEncoderStream::EventEncoderStream(size_t bufferSize) :
                Aws::IOStream(&m_streambuf),
                m_streambuf(bufferSize)
            {
            }

            EventEncoderStream& EventEncoderStream::WriteEvent(const Aws::Utils::Event::Message& msg)
            {
                auto bits = m_encoder.EncodeAndSign(msg);
                write(reinterpret_cast<char*>(bits.data()), bits.size());
                return *this;
            }
        }
    }
}
