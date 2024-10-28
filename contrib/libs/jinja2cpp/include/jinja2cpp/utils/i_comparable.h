#ifndef JINJA2CPP_ICOMPARABLE_H
#define JINJA2CPP_ICOMPARABLE_H

#include <jinja2cpp/config.h>

namespace jinja2 {

struct JINJA2CPP_EXPORT IComparable
{
    virtual ~IComparable() {}
    virtual bool IsEqual(const IComparable& other) const = 0;
};

} // namespace jinja2

#endif // JINJA2CPP_ICOMPARABLE_H
