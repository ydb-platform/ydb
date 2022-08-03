#pragma once

#include <util/str_stl.h>

#include <memory>
#include <string_view>

namespace NYql {

class IOutputQueue {
public:
    using TPtr = std::unique_ptr<IOutputQueue>;

    static TPtr Make(const std::string_view& compression = {});

    virtual ~IOutputQueue() = default;

    virtual void Push(TString&& item) = 0;

    virtual TString Pop() = 0; // return empty in case of too small and not sealed.

    virtual void Seal() = 0;

    virtual size_t Size() const = 0;

    virtual bool Empty() const = 0;

    virtual bool IsSealed() const = 0;

    virtual size_t Volume() const = 0;
};

}
