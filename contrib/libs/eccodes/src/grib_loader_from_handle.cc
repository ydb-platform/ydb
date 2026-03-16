/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_api_internal.h"

#if defined DEBUG && ! defined GRIB_PTHREADS
  #define MY_DEBUG
#endif

static int copy_values(grib_handle* h, grib_accessor* ga)
{
    int i, j, k;
    /* printf("copy_values stack is %ld\n",(long)h->values_stack);*/
    for (j = 0; j < h->values_stack; j++) {
        for (i = 0; i < h->values_count[j]; i++) {
            for (k = 0; (k < MAX_ACCESSOR_NAMES) && (ga->all_names_[k] != NULL); k++) {
                /*printf("copy_values: %s %s\n",h->values[j][i].name,ga->all_names[k]);*/
                if (strcmp(h->values[j][i].name, ga->all_names_[k]) == 0) {
                    size_t len = 1;
                    /*printf("SET VALUES %s\n",h->values[j][i].name);*/
                    switch (h->values[j][i].type) {
                        case GRIB_TYPE_LONG:
                            return ga->pack_long(&h->values[j][i].long_value, &len);

                        case GRIB_TYPE_DOUBLE:
                            return ga->pack_double(&h->values[j][i].double_value, &len);

                        case GRIB_TYPE_STRING:
                            len = strlen(h->values[j][i].string_value);
                            return ga->pack_string(h->values[j][i].string_value, &len);
                    }
                }
            }
        }
    }

    return GRIB_NOT_FOUND;
}

int grib_lookup_long_from_handle(grib_context* gc, grib_loader* loader, const char* name, long* value)
{
    grib_handle* h   = (grib_handle*)loader->data;
    grib_accessor* b = grib_find_accessor(h, name);
    size_t len       = 1;
    if (b)
        return b->unpack_long(value, &len);

    /* TODO: fix me. For now, we don't fail on a lookup. */
    *value = -1;
    return GRIB_SUCCESS;
}

int grib_init_accessor_from_handle(grib_loader* loader, grib_accessor* ga, grib_arguments* default_value)
{
    grib_handle* h      = (grib_handle*)loader->data;
    int ret             = GRIB_SUCCESS;
    size_t len          = 0;
    char* sval          = NULL;
    unsigned char* uval = NULL;
    long* lval          = NULL;
    double* dval        = NULL;
#ifdef MY_DEBUG
    static int first           = 1;
    static const char* missing = 0;
#endif
    const char* name = NULL;
    int k            = 0;
    grib_handle* g;
    grib_accessor* ao = NULL;
    int e, pack_missing = 0;
    grib_context_log(h->context, GRIB_LOG_DEBUG, "XXXXX Copying  %s", ga->name_);

    if (default_value) {
        grib_context_log(h->context, GRIB_LOG_DEBUG, "Copying:  setting %s to default value",
                         ga->name_);
        ga->pack_expression(default_value->get_expression(h, 0));
    }

    if ((ga->flags_ & GRIB_ACCESSOR_FLAG_NO_COPY) ||
        ((ga->flags_ & GRIB_ACCESSOR_FLAG_EDITION_SPECIFIC) &&
         loader->changing_edition) ||
        (ga->flags_ & GRIB_ACCESSOR_FLAG_FUNCTION) ||
        ((ga->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) &&
         !(ga->flags_ & GRIB_ACCESSOR_FLAG_COPY_OK))) {
        grib_context_log(h->context, GRIB_LOG_DEBUG, "Copying %s ignored", ga->name_);
        return GRIB_SUCCESS;
    }

//     if(h->values)
//         if(copy_values(h,ga) == GRIB_SUCCESS)
//         {
//             grib_context_log(h->context,GRIB_LOG_DEBUG, "Copying: setting %s to multi-set-value",   ga->name);
//             return GRIB_SUCCESS;
//         }

#if 0
    if(h->loader)
        h->loader->init_accessor(h->loader,ga,default_value);
#else
    /* COMEBACK here
     this is needed if we reparse during reparse....
     */

    g = h;
    while (g) {
        if (copy_values(g, ga) == GRIB_SUCCESS) {
            grib_context_log(h->context, GRIB_LOG_DEBUG, "Copying: setting %s to multi-set-value", ga->name_);
            return GRIB_SUCCESS;
        }
        g = g->main;
    }
#endif

    /* Check if the same name exists in the original message ... */
    k = 0;
    while ((k < MAX_ACCESSOR_NAMES) &&
           ((name = ga->all_names_[k]) != NULL) &&
           ((ret = grib_get_size(h, name, &len)) != GRIB_SUCCESS))
        k++;

    if (ret != GRIB_SUCCESS) {
        name = ga->name_;
#ifdef MY_DEBUG
        if (first) {
            missing = codes_getenv("ECCODES_PRINT_MISSING");
            first   = 0;
        }
#endif
        grib_context_log(h->context, GRIB_LOG_DEBUG, "Copying [%s] failed: %s",
                         name, grib_get_error_message(ret));
#ifdef MY_DEBUG
        if (missing) {
            fprintf(stdout, "REPARSE: no value for %s", name);
            if (default_value)
                fprintf(stdout, " (default value)");
            fprintf(stdout, "\n");
        }
#endif
        return GRIB_SUCCESS;
    }

    /* we copy also virtual keys*/
    if (len == 0) {
        grib_context_log(h->context, GRIB_LOG_DEBUG, "Copying %s failed, length is 0", name);
        return GRIB_SUCCESS;
    }

    if ((ga->flags_ & GRIB_ACCESSOR_FLAG_CAN_BE_MISSING) && grib_is_missing(h, name, &e) && e == GRIB_SUCCESS && len == 1) {
        ga->pack_missing();
        pack_missing = 1;
    }

    long ga_type = ga->get_native_type();

    if ((ga->flags_ & GRIB_ACCESSOR_FLAG_COPY_IF_CHANGING_EDITION) && !loader->changing_edition) {
        // See ECC-1560 and ECC-1644
        grib_context_log(h->context, GRIB_LOG_DEBUG, "Skipping %s (only copied if changing edition)", ga->name_);
        return GRIB_SUCCESS;
    }

    // ECC-2013: Unfortunately we mapped some keys to be "string_type" in the definitions,
    // e.g., the grib2 key typeOfFirstFixedSurface. Its code table 4.5 has multiple
    // abbreviations of "sfc" etc which clash! So we mark it with this flag to revert to integers which are unique
    if ( ga->flags_ & GRIB_ACCESSOR_FLAG_COPY_AS_INT ) {
        ga_type = GRIB_TYPE_LONG;
    }

    switch (ga_type) {
        case GRIB_TYPE_STRING:
            /*grib_get_string_length_acc(ga,&len);  See ECC-490 */
            grib_get_string_length(h, name, &len);
            sval = (char*)grib_context_malloc(h->context, len);
            ret  = grib_get_string_internal(h, name, sval, &len);
            if (ret == GRIB_SUCCESS) {
                grib_context_log(h->context, GRIB_LOG_DEBUG, "Copying string %s to %s", sval, name);
                ret = ga->pack_string(sval, &len);
            }
            grib_context_free(h->context, sval);

            break;

        case GRIB_TYPE_LONG:
            lval = (long*)grib_context_malloc(h->context, len * sizeof(long));
            ret  = grib_get_long_array_internal(h, name, lval, &len);
            if (ret == GRIB_SUCCESS) {
                grib_context_log(h->context, GRIB_LOG_DEBUG, "Copying %d long(s) %d to %s", len, lval[0], name);
                if (ga->same_) {
                    ret = grib_set_long_array(grib_handle_of_accessor(ga), ga->name_, lval, len);

                    /* Allow for lists to be resized */
                    if ((ret == GRIB_WRONG_ARRAY_SIZE || ret == GRIB_ARRAY_TOO_SMALL) && loader->list_is_resized)
                        ret = GRIB_SUCCESS;
                }
                else {
                    /* See GRIB-492. This is NOT an ideal solution! */
                    if (*lval == GRIB_MISSING_LONG || pack_missing) {
                        ; /* No checks needed */
                    }
                    else {
                        /* If we have just one key of type long which has one octet, then do not exceed maximum value */
                        const long num_octets = ga->length_;
                        if (len == 1 && num_octets == 1 && *lval > 255) {
                            *lval = 0; /* Reset to a reasonable value */
                        }
                    }
                    ret = ga->pack_long(lval, &len);
                }
            }

            grib_context_free(h->context, lval);

            break;

        case GRIB_TYPE_DOUBLE:
            dval = (double*)grib_context_malloc(h->context, len * sizeof(double));
            ret  = grib_get_double_array(h, name, dval, &len); /* GRIB-898 */
            if (ret == GRIB_SUCCESS) {
                grib_context_log(h->context, GRIB_LOG_DEBUG, "Copying %d double(s) %g to %s", len, dval[0], name);
                if (ga->same_) {
                    ret = grib_set_double_array(grib_handle_of_accessor(ga), ga->name_, dval, len);

                    /* Allow for lists to be resized */
                    if ((ret == GRIB_WRONG_ARRAY_SIZE || ret == GRIB_ARRAY_TOO_SMALL) && loader->list_is_resized)
                        ret = GRIB_SUCCESS;
                }
                else
                    ret = ga->pack_double(dval, &len);
            }

            grib_context_free(h->context, dval);
            break;

        case GRIB_TYPE_BYTES:

            ao   = grib_find_accessor(h, name);
            len  = ao->byte_count();
            uval = (unsigned char*)grib_context_malloc(h->context, len * sizeof(char));
            ret  = ao->unpack_bytes(uval, &len);
            /* ret = grib_get_bytes_internal(h,name,uval,&len); */
            if (ret == GRIB_SUCCESS) {
                grib_context_log(h->context, GRIB_LOG_DEBUG, "Copying %d byte(s) to %s", len, name);
                ret = ga->pack_bytes(uval, &len);
            }

            grib_context_free(h->context, uval);

            break;

        case GRIB_TYPE_LABEL:
            break;

        default:
            grib_context_log(h->context, GRIB_LOG_ERROR,
                "Copying %s, cannot establish type %ld [%s]", name, ga->get_native_type(), ga->creator_->class_name_);
            break;
    }

    return ret;
}
