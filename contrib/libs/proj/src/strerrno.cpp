/* list of projection system errno values */

#include <stddef.h>
#include <stdio.h>
#include <string.h>

#include "proj.h"
#include "proj_config.h"
#include "proj_internal.h"

const char *proj_errno_string(int err) {
    return proj_context_errno_string(pj_get_default_ctx(), err);
}

static const struct {
    int num;
    const char *str;
} error_strings[] = {

    {PROJ_ERR_INVALID_OP_WRONG_SYNTAX, _("Invalid PROJ string syntax")},
    {PROJ_ERR_INVALID_OP_MISSING_ARG, _("Missing argument")},
    {PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE, _("Invalid value for an argument")},
    {PROJ_ERR_INVALID_OP_MUTUALLY_EXCLUSIVE_ARGS,
     _("Mutually exclusive arguments")},
    {PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID,
     _("File not found or invalid")},
    {PROJ_ERR_COORD_TRANSFM_INVALID_COORD, _("Invalid coordinate")},
    {PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN,
     _("Point outside of projection domain")},
    {PROJ_ERR_COORD_TRANSFM_NO_OPERATION,
     _("No operation matching criteria found for coordinate")},
    {PROJ_ERR_COORD_TRANSFM_OUTSIDE_GRID,
     _("Coordinate to transform falls outside grid")},
    {PROJ_ERR_COORD_TRANSFM_GRID_AT_NODATA,
     _("Coordinate to transform falls into a grid cell that evaluates to "
       "nodata")},
    {PROJ_ERR_COORD_TRANSFM_NO_CONVERGENCE,
     _("Iterative method fails to converge on coordinate to transform")},
    {PROJ_ERR_COORD_TRANSFM_MISSING_TIME,
     _("Coordinate to transform lacks time")},
    {PROJ_ERR_OTHER_API_MISUSE, _("API misuse")},
    {PROJ_ERR_OTHER_NO_INVERSE_OP, _("No inverse operation")},
    {PROJ_ERR_OTHER_NETWORK_ERROR,
     _("Network error when accessing a remote resource")},
};

const char *proj_context_errno_string(PJ_CONTEXT *ctx, int err) {
    if (ctx == nullptr)
        ctx = pj_get_default_ctx();

    if (0 == err)
        return nullptr;

    const char *str = nullptr;
    for (const auto &num_str_pair : error_strings) {
        if (err == num_str_pair.num) {
            str = num_str_pair.str;
            break;
        }
    }

    if (str == nullptr && err > 0 && (err & PROJ_ERR_INVALID_OP) != 0) {
        str = _(
            "Unspecified error related to coordinate operation initialization");
    }
    if (str == nullptr && err > 0 && (err & PROJ_ERR_COORD_TRANSFM) != 0) {
        str = _("Unspecified error related to coordinate transformation");
    }

    if (str) {
        ctx->lastFullErrorMessage = str;
    } else {
        ctx->lastFullErrorMessage.resize(50);
        snprintf(&ctx->lastFullErrorMessage[0],
                 ctx->lastFullErrorMessage.size(), _("Unknown error (code %d)"),
                 err);
        ctx->lastFullErrorMessage.resize(
            strlen(ctx->lastFullErrorMessage.data()));
    }
    return ctx->lastFullErrorMessage.c_str();
}
