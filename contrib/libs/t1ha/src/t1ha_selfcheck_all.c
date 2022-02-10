/*
 *  Copyright (c) 2016-2020 Positive Technologies, https://www.ptsecurity.com,
 *  Fast Positive Hash.
 *
 *  Portions Copyright (c) 2010-2020 Leonid Yuriev <leo@yuriev.ru>,
 *  The 1Hippeus project (t1h).
 *
 *  This software is provided 'as-is', without any express or implied
 *  warranty. In no event will the authors be held liable for any damages
 *  arising from the use of this software.
 *
 *  Permission is granted to anyone to use this software for any purpose,
 *  including commercial applications, and to alter it and redistribute it
 *  freely, subject to the following restrictions:
 *
 *  1. The origin of this software must not be misrepresented; you must not
 *     claim that you wrote the original software. If you use this software
 *     in a product, an acknowledgement in the product documentation would be
 *     appreciated but is not required.
 *  2. Altered source versions must be plainly marked as such, and must not be
 *     misrepresented as being the original software.
 *  3. This notice may not be removed or altered from any source distribution.
 */

/*
 * t1ha = { Fast Positive Hash, aka "Позитивный Хэш" }
 * by [Positive Technologies](https://www.ptsecurity.ru)
 *
 * Briefly, it is a 64-bit Hash Function:
 *  1. Created for 64-bit little-endian platforms, in predominantly for x86_64,
 *     but portable and without penalties it can run on any 64-bit CPU.
 *  2. In most cases up to 15% faster than City64, xxHash, mum-hash, metro-hash
 *     and all others portable hash-functions (which do not use specific
 *     hardware tricks).
 *  3. Not suitable for cryptography.
 *
 * The Future will (be) Positive. Всё будет хорошо.
 *
 * ACKNOWLEDGEMENT:
 * The t1ha was originally developed by Leonid Yuriev (Леонид Юрьев)
 * for The 1Hippeus project - zerocopy messaging in the spirit of Sparta!
 */

#include "t1ha_bits.h"
#include "t1ha_selfcheck.h"

__cold int t1ha_selfcheck__all_enabled(void) {
  int rc = 0;

#ifndef T1HA2_DISABLED
  rc |= t1ha_selfcheck__t1ha2();
#endif /* T1HA2_DISABLED */

#ifndef T1HA1_DISABLED
  rc |= t1ha_selfcheck__t1ha1();
#endif /* T1HA1_DISABLED */

#ifndef T1HA0_DISABLED
  rc |= t1ha_selfcheck__t1ha0();
#endif /* T1HA0_DISABLED */

  return rc;
}
