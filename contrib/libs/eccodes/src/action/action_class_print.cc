/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "action_class_print.h"

grib_action* grib_action_create_print(grib_context* context, const char* name, char* outname)
{
    return new eccodes::action::Print(context, name, outname);
}

namespace eccodes::action
{

Print::Print(grib_context* context, const char* name, char* outname)
{
    char buf[1024];

    class_name_ = "action_class_print";
    op_         = grib_context_strdup_persistent(context, "section");
    context_    = context;
    name2_      = grib_context_strdup_persistent(context, name);

    if (outname) {
        FILE* out = nullptr;
        int ioerr = 0;
        outname_  = grib_context_strdup_persistent(context, outname);
        out       = fopen(outname, "w");
        ioerr     = errno;
        if (!out) {
            grib_context_log(context_, (GRIB_LOG_ERROR) | (GRIB_LOG_PERROR),
                             "IO ERROR: %s: %s", strerror(ioerr), outname);
        }
        if (out)
            fclose(out);
    }

    snprintf(buf, 1024, "print%p", (void*)name2_);

    name_ = grib_context_strdup_persistent(context, buf);
}

Print::~Print()
{
    grib_context_free_persistent(context_, name_);
    grib_context_free_persistent(context_, name2_);
    grib_context_free_persistent(context_, op_);
}

int Print::execute(grib_handle* h)
{
    int err   = 0;
    FILE* out = nullptr;
    int ioerr = 0;

    if (outname_) {
        out   = fopen(outname_, "a");
        ioerr = errno;
        if (!out) {
            grib_context_log(context_, (GRIB_LOG_ERROR) | (GRIB_LOG_PERROR),
                             "IO ERROR: %s: %s", strerror(ioerr), outname_);
            return GRIB_IO_PROBLEM;
        }
    }
    else {
        out = stdout;
    }

    err = grib_recompose_print(h, nullptr, name2_, 0, out);

    if (outname_)
        fclose(out);

    return err;
}

int Print::create_accessor(grib_section* p, grib_loader* h)
{
    // ECC-1929: A print statement within the definitions does not
    // actually create an accessor. So we just run it

    const int err = execute(p->h);
    if (err)
        grib_context_log(context_, GRIB_LOG_ERROR, "Print: '%s' (%s)", name_, grib_get_error_message(err));
    return err;

    // We may want to be forgiving and ignore the error
    // if (context_->debug) {
    //     return err;
    // }
    // return GRIB_SUCCESS;
}

}  // namespace eccodes::action
