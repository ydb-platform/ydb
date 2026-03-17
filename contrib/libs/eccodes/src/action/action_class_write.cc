/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "action_class_write.h"

/*extern int errno;*/

grib_action* grib_action_create_write(grib_context* context, const char* name, int append, int padtomultiple)
{
    return new eccodes::action::Write(context, name, append, padtomultiple);
}

namespace eccodes::action
{

Write::Write(grib_context* context, const char* name, int append, int padtomultiple)
{
    char buf[1024];

    class_name_ = "action_class_write";
    op_         = grib_context_strdup_persistent(context, "section");
    context_    = context;
    name2_      = grib_context_strdup_persistent(context, name);

    snprintf(buf, 1024, "write%p", (void*)name_);

    name_          = grib_context_strdup_persistent(context, buf);
    append_        = append;
    padtomultiple_ = padtomultiple;
}

Write::~Write()
{
    grib_context_free_persistent(context_, name2_);
    grib_context_free_persistent(context_, name_);
    grib_context_free_persistent(context_, op_);
}

int Write::execute(grib_handle* h)
{
    int err              = GRIB_SUCCESS;
    size_t size          = 0;
    const void* buffer   = NULL;
    const char* filename = NULL;
    char string[1024]    = {0, };

    grib_file* of = NULL;

    if ((err = grib_get_message(h, &buffer, &size)) != GRIB_SUCCESS) {
        grib_context_log(context_, GRIB_LOG_ERROR, "unable to get message");
        return err;
    }

    if (strlen(name2_) != 0) {
        err      = grib_recompose_name(h, NULL, name2_, string, 0);
        filename = string;
    }
    else {
        if (context_->outfilename) {
            filename = context_->outfilename;
            err      = grib_recompose_name(h, NULL, context_->outfilename, string, 0);
            if (!err)
                filename = string;
        }
        else {
            filename = "filter.out";
        }
    }

    ECCODES_ASSERT(filename);
    if (append_)
        of = grib_file_open(filename, "a", &err);
    else
        of = grib_file_open(filename, "w", &err);

    if (!of || !of->handle) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Unable to open file '%s' for %s",
                         filename, (append_ ? "appending" : "writing"));
        return GRIB_IO_PROBLEM;
    }

    if (h->gts_header) {
        if (fwrite(h->gts_header, 1, h->gts_header_len, of->handle) != h->gts_header_len) {
            grib_context_log(context_, (GRIB_LOG_ERROR) | (GRIB_LOG_PERROR),
                             "Error writing GTS header to '%s'", filename);
            return GRIB_IO_PROBLEM;
        }
    }

    if (fwrite(buffer, 1, size, of->handle) != size) {
        grib_context_log(context_, (GRIB_LOG_ERROR) | (GRIB_LOG_PERROR),
                         "Error writing to '%s'", filename);
        return GRIB_IO_PROBLEM;
    }

    if (padtomultiple_) {
        char* zeros = NULL;
        if (padtomultiple_ < 0)
            return GRIB_INVALID_ARGUMENT;
        size_t padding = padtomultiple_ - size % padtomultiple_;
        /* printf("XXX padding=%zu size=%zu padtomultiple=%d\n", padding, size,padtomultiple_); */
        zeros = (char*)calloc(padding, 1);
        if (!zeros)
            return GRIB_OUT_OF_MEMORY;
        if (fwrite(zeros, 1, padding, of->handle) != padding) {
            grib_context_log(context_, (GRIB_LOG_ERROR) | (GRIB_LOG_PERROR),
                             "Error writing to '%s'", filename);
            free(zeros);
            return GRIB_IO_PROBLEM;
        }
        free(zeros);
    }

    if (h->gts_header) {
        const char gts_trailer[4] = { '\x0D', '\x0D', '\x0A', '\x03' };
        if (fwrite(gts_trailer, 1, 4, of->handle) != 4) {
            grib_context_log(context_, (GRIB_LOG_ERROR) | (GRIB_LOG_PERROR),
                             "Error writing GTS trailer to '%s'", filename);
            return GRIB_IO_PROBLEM;
        }
    }

    grib_file_close(filename, 0, &err);
    if (err != GRIB_SUCCESS) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Unable to write message");
        return err;
    }

    return err;
}

}  // namespace eccodes::action
