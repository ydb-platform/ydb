#pragma once

#include <util/system/yassert.h>

#include <exception>

/// @cond Doxygen_Suppress
namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void FinishOrDie(T* pThis, bool autoFinish, const char* className) noexcept
{
    if (!autoFinish) {
        return;
    }

    auto fail = [&] (const char* what) {
        Y_ABORT(
            "\n\n"
            "Destructor of %s caught exception during Finish: %s.\n"
            "Some data is probably has not been written.\n"
            "In order to handle such exceptions consider explicitly call Finish() method.\n",
            className,
            what);
    };

    try {
        pThis->Finish();
    } catch (const std::exception& ex) {
        if (!std::uncaught_exceptions()) {
            fail(ex.what());
        }
    } catch (...) {
        if (!std::uncaught_exceptions()) {
            fail("<unknown exception>");
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
/// @endcond
