#pragma once

#include "string.h"

#include <yt/yt/core/ypath/public.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

//! Given a YSON string and a family of YPaths, returns a smallest YSON containing
//! all subYSONs matching the given paths.
/*!
 * Paths not present in YSON are ignored.
 *
 * Current implementation does not support YPaths accessing attributes, those
 * will be ignored.
 *
 * Attributes of partially included objects (strictly speaking, of those
 * that are not explicitly filtered by any YPath) are dropped.
 *
 * If no matches are found, behavior is controlled by #allowNullResult flag.
 * - If #allowNullResult is set, the null YSON string is returned.
 * - Otherwise, an empty collection of the same type as #yson is returned
 *   (in case of a map or a list), or the original YSON itself (in case of a
 *   scalar type).
 */
TYsonString FilterYsonString(
    const std::vector<NYPath::TYPath>& paths,
    TYsonStringBuf yson,
    bool allowNullResult = true);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
