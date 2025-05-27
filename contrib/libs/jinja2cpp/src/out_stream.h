#ifndef JINJA2CPP_SRC_OUT_STREAM_H
#define JINJA2CPP_SRC_OUT_STREAM_H

#include "internal_value.h"

#include <functional>
#include <iostream>
#include <sstream>

namespace jinja2
{
class OutStream
{
public:
    struct StreamWriter
    {
        virtual ~StreamWriter() {}

        virtual void WriteBuffer(const void* ptr, size_t length) = 0;
        virtual void WriteValue(const InternalValue &val) = 0;
    };

    OutStream(std::function<StreamWriter*()> writerGetter)
        : m_writerGetter(std::move(writerGetter))
    {}

    void WriteBuffer(const void* ptr, size_t length)
    {
        m_writerGetter()->WriteBuffer(ptr, length);
    }

    void WriteValue(const InternalValue& val)
    {
        m_writerGetter()->WriteValue(val);
    }

private:
    std::function<StreamWriter*()> m_writerGetter;
};

} // namespace jinja2

#endif // JINJA2CPP_SRC_OUT_STREAM_H
