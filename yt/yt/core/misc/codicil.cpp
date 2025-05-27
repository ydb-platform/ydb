#include "codicil.h"

#include <yt/yt/core/concurrency/fls.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

using TCodicilBuilderStack = TCompactVector<TCodicilBuilder, 16>;

NConcurrency::TFlsSlot<TCodicilBuilderStack>& CodicilBuilderStackSlot()
{
    static NConcurrency::TFlsSlot<TCodicilBuilderStack> result;
    return result;
}

} // namespace NDetail

void PushCodicilBuilder(TCodicilBuilder&& builder)
{
    NDetail::CodicilBuilderStackSlot()->push_back(std::move(builder));
}

void PopCodicilBuilder()
{
    auto& stack = *NDetail::CodicilBuilderStackSlot();
    YT_ASSERT(!stack.empty());
    stack.pop_back();
}

TRange<TCodicilBuilder> GetCodicilBuilders()
{
    // NB: Don't forcefully construct FLS slot to avoid allocations;
    // these may lead to deadlocks if the program crashes during an allocation itself.
    if (!NDetail::CodicilBuilderStackSlot().IsInitialized()) {
        return {};
    }
    return TRange(*NDetail::CodicilBuilderStackSlot());
}

std::vector<std::string> BuildCodicils()
{
    auto builders = GetCodicilBuilders();
    std::vector<std::string> result;
    result.reserve(builders.size());
    TCodicilFormatter formatter;
    for (const auto& builder : builders) {
        formatter.Reset();
        builder(&formatter);
        result.emplace_back(formatter.GetBuffer());
    }
    return result;
}

TCodicilBuilder MakeNonOwningCodicilBuilder(TStringBuf codicil)
{
    return [codicil] (TCodicilFormatter* formatter) {
        formatter->AppendString(codicil);
    };
}

TCodicilBuilder MakeOwningCodicilBuilder(std::string codicil)
{
    return [codicil = std::move(codicil)] (TCodicilFormatter* formatter) {
        formatter->AppendString(codicil);
    };
}

////////////////////////////////////////////////////////////////////////////////

TCodicilGuard::TCodicilGuard(TCodicilBuilder&& builder)
    : Active_(true)
{
    PushCodicilBuilder(std::move(builder));
}

TCodicilGuard::~TCodicilGuard()
{
    Release();
}

TCodicilGuard::TCodicilGuard(TCodicilGuard&& other)
    : Active_(other.Active_)
{
    other.Active_ = false;
}

TCodicilGuard& TCodicilGuard::operator=(TCodicilGuard&& other)
{
    if (this != &other) {
        Release();
        Active_ = other.Active_;
        other.Active_ = false;
    }
    return *this;
}

void TCodicilGuard::Release()
{
    if (Active_) {
        PopCodicilBuilder();
        Active_ = false;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
