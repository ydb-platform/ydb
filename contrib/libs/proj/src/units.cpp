/* definition of standard cartesian units */

#include <stddef.h>

#include "proj.h"

#include "proj_internal.h"

/* Field 2 that contains the multiplier to convert named units to meters
** may be expressed by either a simple floating point constant or a
** numerator/denomenator values (e.g. 1/1000) */
static const struct PJ_UNITS pj_units[] = {
    {"km", "1000", "Kilometer", 1000.0},
    {"m", "1", "Meter", 1.0},
    {"dm", "1/10", "Decimeter", 0.1},
    {"cm", "1/100", "Centimeter", 0.01},
    {"mm", "1/1000", "Millimeter", 0.001},
    {"kmi", "1852", "International Nautical Mile", 1852.0},
    {"in", "0.0254", "International Inch", 0.0254},
    {"ft", "0.3048", "International Foot", 0.3048},
    {"yd", "0.9144", "International Yard", 0.9144},
    {"mi", "1609.344", "International Statute Mile", 1609.344},
    {"fath", "1.8288", "International Fathom", 1.8288},
    {"ch", "20.1168", "International Chain", 20.1168},
    {"link", "0.201168", "International Link", 0.201168},
    {"us-in", "1/39.37", "U.S. Surveyor's Inch", 100 / 3937.0},
    {"us-ft", "0.304800609601219", "U.S. Surveyor's Foot", 1200 / 3937.0},
    {"us-yd", "0.914401828803658", "U.S. Surveyor's Yard", 3600 / 3937.0},
    {"us-ch", "20.11684023368047", "U.S. Surveyor's Chain", 79200 / 3937.0},
    {"us-mi", "1609.347218694437", "U.S. Surveyor's Statute Mile",
     6336000 / 3937.0},
    {"ind-yd", "0.91439523", "Indian Yard", 0.91439523},
    {"ind-ft", "0.30479841", "Indian Foot", 0.30479841},
    {"ind-ch", "20.11669506", "Indian Chain", 20.11669506},
    {nullptr, nullptr, nullptr, 0.0}};

// For internal use
const PJ_UNITS *pj_list_linear_units() { return pj_units; }

const PJ_UNITS *proj_list_units() { return pj_units; }

/* M_PI / 200 */
#define GRAD_TO_RAD 0.015707963267948967

const struct PJ_UNITS pj_angular_units[] = {
    {"rad", "1.0", "Radian", 1.0},
    {"deg", "0.017453292519943296", "Degree", DEG_TO_RAD},
    {"grad", "0.015707963267948967", "Grad", GRAD_TO_RAD},
    {nullptr, nullptr, nullptr, 0.0}};

// For internal use
const PJ_UNITS *pj_list_angular_units() { return pj_angular_units; }

const PJ_UNITS *proj_list_angular_units() { return pj_angular_units; }
