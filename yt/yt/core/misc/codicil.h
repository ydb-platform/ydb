#pragma once

#include <library/cpp/yt/string/raw_formatter.h>

#include <library/cpp/yt/memory/range.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/*
 * "Codicils" are short human- and machine-readable strings organized
 * into a per-fiber stack.
 *
 * When the crash handler is invoked, it dumps (alongside with the other
 * useful stuff like backtrace) the codicils.
 *
 * For performance reasons, codicils are constructed lazily when needed.
 */

constexpr auto MaxCodicilLength = 256;

//! A formatter used to build codicils.
using TCodicilFormatter = TRawFormatter<MaxCodicilLength>;

//! A callback that constructs a codicil.
using TCodicilBuilder = std::function<void(TCodicilFormatter* formatter)>;

//! Installs a new codicil builder onto the stack.
void PushCodicilBuilder(TCodicilBuilder&& builder);

//! Removes the top codicil builder from the stack.
void PopCodicilBuilder();

//! Returns the list of all currently installed codicil builders.
TRange<TCodicilBuilder> GetCodicilBuilders();

//! Invokes all registered codicil builders to construct codicils.
//! Not signal-safe; must only be used for testing purposes.
std::vector<std::string> BuildCodicils();

//! Creates a codicil builder holding a given string view.
TCodicilBuilder MakeNonOwningCodicilBuilder(TStringBuf codicil);

//! Creates a codicil builder holding a given string.
TCodicilBuilder MakeOwningCodicilBuilder(std::string codicil);

////////////////////////////////////////////////////////////////////////////////

//! Invokes #PushCodicilBuilder in ctor and #PopCodicilBuilder in dtor.
class TCodicilGuard
{
public:
    TCodicilGuard() = default;
    TCodicilGuard(TCodicilBuilder&& builder);

    ~TCodicilGuard();

    TCodicilGuard(const TCodicilGuard& othter) = delete;
    TCodicilGuard(TCodicilGuard&& other);

    TCodicilGuard& operator=(const TCodicilGuard& other) = delete;
    TCodicilGuard& operator=(TCodicilGuard&& other);

private:
    bool Active_ = false;

    void Release();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
