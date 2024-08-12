#pragma once

#include <util/generic/fwd.h>
#include <util/generic/ptr.h>
#include <util/stream/fwd.h>

/**
 * Peeks into the provided input stream to determine its compression format,
 * if any, and returns a corresponding decompressing stream. If the stream is
 * not compressed, then returns a simple pass-through proxy stream.
 *
 * Note that returned stream doesn't own the provided input stream, thus it's
 * up to the user to free them both.
 *
 * @param input                         Input stream.
 * @returns                             Newly constructed stream.
 */
THolder<IInputStream> OpenMaybeCompressedInput(IInputStream* input);

/**
 * Same as `OpenMaybeCompressedInput`, but returned stream owns the one passed
 * into this function.
 *
 * @param input                         Input stream.
 * @returns                             Newly constructed stream.
 * @see OpenMaybeCompressedInput(IInputStream*)
 */
THolder<IInputStream> OpenOwnedMaybeCompressedInput(THolder<IInputStream> input);

/**
 * @param input                         Input stream.
 * @returns                             Newly constructed stream.
 * @see OpenMaybeCompressedInput(IInputStream*)
 */
THolder<IInputStream> OpenMaybeCompressedInput(const TString& path);

THolder<IInputStream> OpenMaybeCompressedInput(const TString& path, ui32 bufSize);
