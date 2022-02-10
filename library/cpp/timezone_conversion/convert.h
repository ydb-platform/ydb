#pragma once

#include "civil.h"

#include <contrib/libs/cctz/include/cctz/time_zone.h>
#include <util/datetime/base.h>
#include <util/draft/datetime.h>

namespace NDatetime {
    /**
     * @return The mother of all time zones.
     * @see    https://en.wikipedia.org/wiki/Coordinated_Universal_Time
     */
    TTimeZone GetUtcTimeZone();

    /**
     * @return The time zone that is curently set on your machine.
     */
    TTimeZone GetLocalTimeZone();

    /**
     * @param absoluteTime  A TInstant representing a number of seconds elapsed
     *                      since The Epoch (the microsecond part is ignored).
     * @param tz            The time zone to use for conversion.
     * @return              The civil time corresponding to absoluteTime.
     * @note                This conversion is always well-defined (i.e., there
     *                      is exactly one civil time which corresponds to
     *                      absoluteTime).
     * @see                 https://en.wikipedia.org/wiki/Unix_time
     */
    TSimpleTM ToCivilTime(const TInstant& absoluteTime, const TTimeZone& tz);

    /**
     * Creates civil time in place with respect of given timezone.
     * @param[in] tz        The time zone to use for creation.
     * @param[in] year      The year of the creation time.
     * @param[in] mon       The month of the creation time.
     * @param[in] day       The day of the creation time.
     * @param[in] h         The hour of the creation time.
     * @param[in] m         The minute of the creation time.
     * @param[in] s         The second of the creation time.
     * @return a civil time
     */
    TSimpleTM CreateCivilTime(const TTimeZone& tz, ui32 year, ui32 mon, ui32 day, ui32 h = 0, ui32 m = 0, ui32 s = 0);

    /**
     * @param civilTime     A human-readable date and time (the following fields
     *                      are used by this function: {Year,Mon,MDay,Hour,Min,Sec}).
     * @param tz            The time zone to use for conversion.
     * @return              Some absolute time corresponding to civilTime.
     * @note                If multiple absolute times match civilTime, the earliest
     *                      if returned.
     *                      If civilTime doesn't exist due to discontinuity in time
     *                      (e.g., DST happened) we pretend the discontinuity isn't
     *                      there (i.e., if we skipped from 1:59AM to 3:00AM then
     *                      ToAbsoluteTime(2:30AM) == ToAbsoluteTime(3:30AM)).
     * @see                 https://en.wikipedia.org/wiki/Daylight_saving_time
     */
    TInstant ToAbsoluteTime(const TSimpleTM& civilTime, const TTimeZone& tz);
}
