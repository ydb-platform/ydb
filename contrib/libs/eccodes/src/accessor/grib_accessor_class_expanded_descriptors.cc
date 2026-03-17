/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_expanded_descriptors.h"
#include "grib_scaling.h"

grib_accessor_expanded_descriptors_t _grib_accessor_expanded_descriptors{};
grib_accessor* grib_accessor_expanded_descriptors = &_grib_accessor_expanded_descriptors;

#define MYDEBUG        0
#define DESC_SIZE_INIT 400 /* Initial size for grib_bufr_descriptors_array_new */
#define DESC_SIZE_INCR 400 /* Increment size for grib_bufr_descriptors_array_new */

/* Handy macro to catch errors.
 * Arguments: array is a pointer to 'bufr_descriptors_array', result is pointer to 'bufr_descriptor' */
#define DESCRIPTORS_POP_FRONT_OR_RETURN(array, result)         \
    {                                                          \
        if (array->n == 0) {                                   \
            *err = GRIB_INTERNAL_ERROR;                        \
            return;                                            \
        }                                                      \
        result = grib_bufr_descriptors_array_pop_front(array); \
    }

void grib_accessor_expanded_descriptors_t::init(const long len, grib_arguments* args)
{
    grib_accessor_long_t::init(len, args);
    int n               = 0;
    grib_handle* hand   = grib_handle_of_accessor(this);
    tablesAccessorName_ = args->get_name(hand, n++);
    expandedName_       = args->get_name(hand, n++);
    rank_               = args->get_long(hand, n++);
    if (rank_ != 0) {
        expandedAccessor_ = dynamic_cast<grib_accessor_expanded_descriptors_t*>(grib_find_accessor(hand, expandedName_));
    }
    else {
        expandedAccessor_ = 0;
    }
    unexpandedDescriptors_ = args->get_name(hand, n++);
    sequence_              = args->get_name(hand, n++);
    do_expand_             = 1;
    expanded_              = 0;
    length_                = 0;

    tablesAccessor_ = NULL;
}

#define BUFR_DESCRIPTORS_ARRAY_USED_SIZE(v) ((v)->n)
#define SILENT                              1

#if MYDEBUG
static int global_depth = -1;

static const char* descriptor_type_name(int dtype)
{
    switch (dtype) {
        case BUFR_DESCRIPTOR_TYPE_STRING:
            return "string";
        case BUFR_DESCRIPTOR_TYPE_LONG:
            return "long";
        case BUFR_DESCRIPTOR_TYPE_DOUBLE:
            return "double";
        case BUFR_DESCRIPTOR_TYPE_TABLE:
            return "table";
        case BUFR_DESCRIPTOR_TYPE_FLAG:
            return "flag";
        case BUFR_DESCRIPTOR_TYPE_UNKNOWN:
            return "unknown";
        case BUFR_DESCRIPTOR_TYPE_REPLICATION:
            return "replication";
        case BUFR_DESCRIPTOR_TYPE_OPERATOR:
            return "operator";
        case BUFR_DESCRIPTOR_TYPE_SEQUENCE:
            return "sequence";
    }
    ECCODES_ASSERT(!"bufr_descriptor_type_name failed");
    return "unknown";
}
#endif

void grib_accessor_expanded_descriptors_t::__expand(bufr_descriptors_array* unexpanded, bufr_descriptors_array* expanded, change_coding_params* ccp, int* err)
{
    int k, j, i;
    size_t size         = 0;
    long* v_array       = NULL;
    bufr_descriptor* u  = NULL;
    bufr_descriptor* vv = NULL;
    /* ECC-1422: 'ur' is the array of bufr_descriptor pointers for replications.
     * Its max size is X (from FXY) which is 6 bits so no need for malloc */
    bufr_descriptor* ur[65] = {0,};
    bufr_descriptor* urc                     = NULL;
    size_t idx                               = 0;
    bufr_descriptor* u0                      = NULL;
    grib_context* c                          = context_;
    bufr_descriptor* us                      = NULL;
    bufr_descriptors_array* inner_expanded   = NULL;
    bufr_descriptors_array* inner_unexpanded = NULL;
    grib_handle* hand                        = grib_handle_of_accessor(this);
#if MYDEBUG
    int idepth;
#endif

    if (BUFR_DESCRIPTORS_ARRAY_USED_SIZE(unexpanded) == 0)
        return;

    us          = grib_bufr_descriptor_clone(grib_bufr_descriptors_array_get(unexpanded, 0));
    us->context = c;

    *err = 0;
#if MYDEBUG
    for (idepth = 0; idepth < global_depth; idepth++)
        printf("\t");
    printf("expanding ==> %d-%02d-%03d\n", us->F, us->X, us->Y);
#endif
    switch (us->F) {
        case 3:
            /* sequence */
            DESCRIPTORS_POP_FRONT_OR_RETURN(unexpanded, u);
#if MYDEBUG
            for (idepth = 0; idepth < global_depth; idepth++)
                printf("\t");
            printf("+++ pop  %06ld [%s]\n", u->code, descriptor_type_name(u->type));
#endif
            /*this is to get the sequence elements of the sequence unexpanded[i] */
            *err = grib_set_long(hand, sequence_, u->code);
            *err = grib_get_size(hand, sequence_, &size);
            grib_bufr_descriptor_delete(u);
            if (*err)
                goto cleanup;
            v_array = (long*)grib_context_malloc_clear(c, sizeof(long) * size);
            *err    = grib_get_long_array(hand, sequence_, v_array, &size);
            if (*err)
                goto cleanup;

            inner_unexpanded = grib_bufr_descriptors_array_new(DESC_SIZE_INIT, DESC_SIZE_INCR);
            for (i = 0; i < size; i++) {
                vv               = grib_bufr_descriptor_new(tablesAccessor_, v_array[i], !SILENT, err);
                inner_unexpanded = grib_bufr_descriptors_array_push(inner_unexpanded, vv);
            }
            grib_context_free(c, v_array);
            inner_expanded = do_expand(inner_unexpanded, ccp, err);
            if (*err)
                return;
            grib_bufr_descriptors_array_delete(inner_unexpanded);
#if MYDEBUG
            for (i = 0; i < inner_expanded->n; i++) {
                for (idepth = 0; idepth < global_depth; idepth++)
                    printf("\t");
                printf("+++ push %06ld\n", inner_expanded->v[i]->code);
            }
#endif
            size = BUFR_DESCRIPTORS_ARRAY_USED_SIZE(inner_expanded);
            grib_bufr_descriptors_array_append(expanded, inner_expanded);
            break;

        case 1:
            if (us->Y == 0) {
                /* delayed replication */
                bufr_descriptor* uidx = 0;
                DESCRIPTORS_POP_FRONT_OR_RETURN(unexpanded, u);
#if MYDEBUG
                for (idepth = 0; idepth < global_depth; idepth++)
                    printf("\t");
                printf("+++ pop  %06ld [%s]\n", u->code, descriptor_type_name(u->type));
                for (idepth = 0; idepth < global_depth; idepth++)
                    printf("\t");
                printf("+++ push %06ld\n", u->code);
#endif
                grib_bufr_descriptors_array_push(expanded, u);
                idx              = expanded->n - 1;
                size             = 0;
                inner_unexpanded = grib_bufr_descriptors_array_new(DESC_SIZE_INIT, DESC_SIZE_INCR);

                /* Number of descriptors to replicate cannot be more than what's left */
                if (us->X + 1 > unexpanded->n) {
                    grib_context_log(c, GRIB_LOG_ERROR,
                                     "Delayed replication: %06ld: expected %d but only found %lu element(s)",
                                     u->code, us->X, unexpanded->n - 1);
                    *err = GRIB_DECODING_ERROR;
                    return;
                }
                for (j = 0; j < us->X + 1; j++) {
                    DESCRIPTORS_POP_FRONT_OR_RETURN(unexpanded, u0);
                    grib_bufr_descriptors_array_push(inner_unexpanded, u0);
#if MYDEBUG
                    for (idepth = 0; idepth < global_depth; idepth++)
                        printf("\t");
                    printf("+++ pop  %06ld [%s]\n", u0->code, descriptor_type_name(u0->type));
#endif
                }
                inner_expanded = do_expand(inner_unexpanded, ccp, err);
                if (*err)
                    return;
                grib_bufr_descriptors_array_delete(inner_unexpanded);
                size = BUFR_DESCRIPTORS_ARRAY_USED_SIZE(inner_expanded);
#if MYDEBUG
                for (i = 0; i < inner_expanded->n; i++) {
                    for (idepth = 0; idepth < global_depth; idepth++)
                        printf("\t");
                    printf("+++ push %06ld\n", inner_expanded->v[i]->code);
                }
#endif
                expanded = grib_bufr_descriptors_array_append(expanded, inner_expanded);
                uidx     = grib_bufr_descriptors_array_get(expanded, idx);
                ECCODES_ASSERT( uidx->type == BUFR_DESCRIPTOR_TYPE_REPLICATION );
                ECCODES_ASSERT( uidx->F == 1 );
                ECCODES_ASSERT( uidx->Y == 0 );
                // ECC-1958 and ECC-1054:
                // Here X is used to store the size which can exceed 63. The normal X is 6 bits wide so max=63
                // We need to set X but not the descriptor code
                uidx->X = (int)(size - 1);
                if (size < 64)
                    uidx->code = (size - 1) * 1000 + 100000;
                //grib_bufr_descriptor_set_code(uidx, (size - 1) * 1000 + 100000);
                size++;
            }
            else {
                /* replication with fixed number of descriptors (non-delayed) */
                DESCRIPTORS_POP_FRONT_OR_RETURN(unexpanded, u);
#if MYDEBUG
                for (idepth = 0; idepth < global_depth; idepth++)
                    printf("\t");
                printf("+++ pop  %06ld [%s]\n", u->code, descriptor_type_name(u->type));
#endif
                grib_bufr_descriptor_delete(u);
                size = us->X * us->Y;
                memset(ur, 0, us->X); /* ECC-1422 */
                if (us->X > unexpanded->n) {
                    grib_context_log(c, GRIB_LOG_ERROR, "Replication descriptor %06ld: expected %d but only found %zu element(s)",
                                     us->code, us->X, unexpanded->n);
                }
                for (j = 0; j < us->X; j++) {
                    DESCRIPTORS_POP_FRONT_OR_RETURN(unexpanded, ur[j]);
#if MYDEBUG
                    for (idepth = 0; idepth < global_depth; idepth++)
                        printf("\t");
                    printf("+++ pop  %06ld [%s]\n", ur[j]->code, descriptor_type_name(ur[j]->type));
#endif
                }
                inner_unexpanded = grib_bufr_descriptors_array_new(DESC_SIZE_INIT, DESC_SIZE_INCR);
                for (j = 0; j < us->X; j++) {
                    urc = grib_bufr_descriptor_clone(ur[j]);
                    grib_bufr_descriptors_array_push(inner_unexpanded, urc);
                }
                for (k = 1; k < us->Y; k++) {
                    for (j = 0; j < us->X; j++) {
                        urc = grib_bufr_descriptor_clone(ur[j]);
                        grib_bufr_descriptors_array_push(inner_unexpanded, urc);
                    }
                }
                for (i = 0; i < us->X; i++)
                    grib_bufr_descriptor_delete(ur[i]);

                inner_expanded = do_expand(inner_unexpanded, ccp, err);
                if (*err)
                    return;
                grib_bufr_descriptors_array_delete(inner_unexpanded);
#if MYDEBUG
                for (i = 0; i < inner_expanded->n; i++) {
                    for (idepth = 0; idepth < global_depth; idepth++)
                        printf("\t");
                    printf("+++ push %06ld\n", inner_expanded->v[i]->code);
                }
#endif
                size = BUFR_DESCRIPTORS_ARRAY_USED_SIZE(inner_expanded);
                grib_bufr_descriptors_array_append(expanded, inner_expanded);
            }
            break;

        case 0:
            DESCRIPTORS_POP_FRONT_OR_RETURN(unexpanded, u);
            size = 1;
            if (ccp->associatedFieldWidth && u->X != 31) {
                bufr_descriptor* au = grib_bufr_descriptor_new(tablesAccessor_, 999999, !SILENT, err);
                au->width           = ccp->associatedFieldWidth;
                grib_bufr_descriptor_set_scale(au, 0);
                strcpy(au->shortName, "associatedField");
                /* au->name=grib_context_strdup(c,"associated field");  See ECC-489 */
                strcpy(au->units, "associated units");
#if MYDEBUG
                for (idepth = 0; idepth < global_depth; idepth++)
                    printf("\t");
                printf("+++ push %06ld (s=%ld r=%ld w=%ld)", au->code, au->scale, au->reference, au->width);
#endif
                grib_bufr_descriptors_array_push(expanded, au);
                size++;
            }
#if MYDEBUG
            for (idepth = 0; idepth < global_depth; idepth++)
                printf("\t");
            printf("+++ pop  %06ld [%s]\n", u->code, descriptor_type_name(u->type));
            for (idepth = 0; idepth < global_depth; idepth++)
                printf("\t");
            printf("+++ push %06ld [%s] (s=%ld r=%ld w=%ld)",
                   u->code, descriptor_type_name(u->type), u->scale, u->reference, u->width);
#endif
            if (u->type != BUFR_DESCRIPTOR_TYPE_FLAG &&
                u->type != BUFR_DESCRIPTOR_TYPE_TABLE &&
                u->type != BUFR_DESCRIPTOR_TYPE_STRING) {
                if (ccp->localDescriptorWidth > 0) {
                    u->width     = ccp->localDescriptorWidth;
                    u->reference = 0;
                    grib_bufr_descriptor_set_scale(u, 0);
                    ccp->localDescriptorWidth = 0;
                }
                else {
                    u->width += ccp->extraWidth;
                    u->reference *= ccp->referenceFactor;
                    grib_bufr_descriptor_set_scale(u, u->scale + ccp->extraScale);
                }
            }
            else if (u->type == BUFR_DESCRIPTOR_TYPE_STRING && ccp->newStringWidth != 0) {
                u->width = ccp->newStringWidth;
            }
#if MYDEBUG
            printf("->(s=%ld r=%ld w=%ld)\n", u->scale, u->reference, u->width);
#endif
            grib_bufr_descriptors_array_push(expanded, u);
            break;

        case 2:
            DESCRIPTORS_POP_FRONT_OR_RETURN(unexpanded, u);
#if MYDEBUG
            for (idepth = 0; idepth < global_depth; idepth++)
                printf("\t");
            printf("+++ pop  %06ld [%s]\n", u->code, descriptor_type_name(u->type));
#endif
            switch (us->X) {
                case 1:
                    ccp->extraWidth = us->Y ? us->Y - 128 : 0;
                    size            = 0;
                    grib_bufr_descriptor_delete(u);
                    break;
                case 2:
                    ccp->extraScale = us->Y ? us->Y - 128 : 0;
                    size            = 0;
                    grib_bufr_descriptor_delete(u);
                    break;
                case 4:
                    /* associated field*/
                    ccp->associatedFieldWidth = us->Y;
                    grib_bufr_descriptor_delete(u);
                    break;
                case 6:
                    /*signify data width*/
                    ccp->localDescriptorWidth = us->Y;
                    size                      = 0;
                    grib_bufr_descriptor_delete(u);
                    break;
                case 7:
                    if (us->Y) {
                        ccp->extraScale      = us->Y;
                        ccp->referenceFactor = codes_power<double>(us->Y, 10);
                        ccp->extraWidth      = ((10 * us->Y) + 2) / 3;
                    }
                    else {
                        ccp->extraWidth      = 0;
                        ccp->extraScale      = 0;
                        ccp->referenceFactor = 1;
                    }
                    size = 0;
                    grib_bufr_descriptor_delete(u);
                    break;
                case 8:
                    ccp->newStringWidth = us->Y * 8;
                    break;
                default:
#if MYDEBUG
                    for (idepth = 0; idepth < global_depth; idepth++)
                        printf("\t");
                    printf("+++ push %06ld\n", u->code);
#endif
                    grib_bufr_descriptors_array_push(expanded, u);
                    size = 1;
            }
            break;

        default:
            DESCRIPTORS_POP_FRONT_OR_RETURN(unexpanded, u);
#if MYDEBUG
            for (idepth = 0; idepth < global_depth; idepth++)
                printf("\t");
            printf("+++ pop  %06ld [%s]\n", u->code, descriptor_type_name(u->type));
            for (idepth = 0; idepth < global_depth; idepth++)
                printf("\t");
            printf("+++ push %06ld\n", u->code);
#endif
            grib_bufr_descriptors_array_push(expanded, u);
            size = 1;
    }
#if MYDEBUG
    for (idepth = 0; idepth < global_depth; idepth++)
        printf("\t");
    printf("expanding <== %d-%.2d-%.3d (size=%ld)\n\n", us->F, us->X, us->Y, size);
#endif
cleanup:
    if (us) grib_bufr_descriptor_delete(us);
}

bufr_descriptors_array* grib_accessor_expanded_descriptors_t::do_expand(bufr_descriptors_array* unexpanded, change_coding_params* ccp, int* err)
{
    bufr_descriptors_array* expanded = NULL;

#if MYDEBUG
    int idepth;
    global_depth++;
#endif

    expanded = grib_bufr_descriptors_array_new(DESC_SIZE_INIT, DESC_SIZE_INCR);

#if MYDEBUG
    {
        int i;
        for (idepth = 0; idepth < global_depth; idepth++)
            printf("\t");
        printf("to be expanded ==> \n");
        for (i = 0; i < unexpanded->n; i++) {
            bufr_descriptor* xx = grib_bufr_descriptors_array_get(unexpanded, i);
            for (idepth = 0; idepth < global_depth; idepth++)
                printf("\t");
            printf("%06ld\n", xx->code);
        }
        for (idepth = 0; idepth < global_depth; idepth++)
            printf("\t");
        printf("to be expanded <== \n\n");
    }
#endif
    while (unexpanded->n) {
        __expand(unexpanded, expanded, ccp, err);
        if (*err) {
            grib_bufr_descriptors_array_delete(expanded);
            return NULL;
        }
    }
#if MYDEBUG
    {
        int i;
        for (idepth = 0; idepth < global_depth; idepth++)
            printf("\t");
        printf("expanded ==> \n");
        for (i = 0; i < expanded->n; i++) {
            bufr_descriptor* xx = grib_bufr_descriptors_array_get(expanded, i);
            for (idepth = 0; idepth < global_depth; idepth++)
                printf("\t");
            printf("==  %-6d== %06ld ", i, xx->code);
            printf("s=%ld r=%ld w=%ld", xx->scale, xx->reference, xx->width);
            printf("\n");
        }
        for (idepth = 0; idepth < global_depth; idepth++)
            printf("\t");
        printf("expanded <== \n\n");
    }
#endif

#if MYDEBUG
    global_depth--;
#endif

    return expanded;
}

int grib_accessor_expanded_descriptors_t::expand()
{
    int err               = 0;
    size_t unexpandedSize = 0;
    /* grib_iarray* unexp=0; */
    int i;
    long* u      = 0;
    char key[50] = {0,};
    long centre, masterTablesVersionNumber, localTablesVersionNumber, masterTablesNumber;
    change_coding_params ccp;
    bufr_descriptors_array* unexpanded      = NULL;
    bufr_descriptors_array* unexpanded_copy = NULL;
    bufr_descriptors_array* expanded        = NULL;
    grib_context* c                         = context_;
    const grib_handle* h                    = grib_handle_of_accessor(this);
    int operator206yyy_width                = 0; /* width specified by operator 206YYY */

    if (!do_expand_) {
        return err;
    }
    do_expand_ = 0;
    if (rank_ != 0) {
        err       = expandedAccessor_->expand();
        expanded_ = ((grib_accessor_expanded_descriptors_t*)expandedAccessor_)->expanded_;
        return err;
    }

    err = grib_get_size(h, unexpandedDescriptors_, &unexpandedSize);
    if (err)
        return err;
    if (unexpandedSize == 0) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Unexpanded size is zero!", name_);
        return GRIB_DECODING_ERROR;
    }

    u = (long*)grib_context_malloc_clear(c, sizeof(long) * unexpandedSize);
    if (!u) {
        err = GRIB_OUT_OF_MEMORY;
        return err;
    }
    err = grib_get_long_array(h, unexpandedDescriptors_, u, &unexpandedSize);
    if (err)
        return err;

    err = grib_get_long(h, "bufrHeaderCentre", &centre);
    if (err)
        return err;
    err = grib_get_long(h, "masterTablesVersionNumber", &masterTablesVersionNumber);
    if (err)
        return err;
    err = grib_get_long(h, "localTablesVersionNumber", &localTablesVersionNumber);
    if (err)
        return err;
    err = grib_get_long(h, "masterTableNumber", &masterTablesNumber);
    if (err)
        return err;

    snprintf(key, sizeof(key), "%ld_%ld_%ld_%ld_%ld", centre, masterTablesVersionNumber, localTablesVersionNumber, masterTablesNumber, u[0]);
    expanded = grib_context_expanded_descriptors_list_get(c, key, u, unexpandedSize);
    if (expanded) {
        expanded_ = expanded;
        grib_context_free(c, u);
        return GRIB_SUCCESS;
    }

    if (!tablesAccessor_) {
        tablesAccessor_ = grib_find_accessor(h, tablesAccessorName_);
        ECCODES_ASSERT(tablesAccessor_);
    }

    unexpanded           = grib_bufr_descriptors_array_new(unexpandedSize, DESC_SIZE_INCR);
    unexpanded_copy      = grib_bufr_descriptors_array_new(unexpandedSize, DESC_SIZE_INCR);
    operator206yyy_width = 0;
    for (i = 0; i < unexpandedSize; i++) {
        bufr_descriptor *aDescriptor1, *aDescriptor2;
        /* ECC-1274: clear error and only issue msg once */
        err          = 0;
        aDescriptor1 = grib_bufr_descriptor_new(tablesAccessor_, u[i], SILENT, &err);
        err          = 0;
        aDescriptor2 = grib_bufr_descriptor_new(tablesAccessor_, u[i], !SILENT, &err);

        /* ECC-433: Operator 206YYY */
        if (aDescriptor1->F == 2 && aDescriptor1->X == 6) {
            ECCODES_ASSERT(aDescriptor1->type == BUFR_DESCRIPTOR_TYPE_OPERATOR);
            operator206yyy_width = aDescriptor1->Y; /* Store the width for the following descriptor */
            DEBUG_ASSERT(operator206yyy_width > 0);
        }
        else if (operator206yyy_width > 0) {
            if (err == GRIB_NOT_FOUND) {
                DEBUG_ASSERT(aDescriptor1->type == BUFR_DESCRIPTOR_TYPE_UNKNOWN);
                err                 = 0;                       /* Clear any error generated due to local descriptor */
                aDescriptor1->nokey = aDescriptor2->nokey = 1; /* Do not show this descriptor in dump */
            }
            /* The width specified by operator takes precedence over element's own width */
            aDescriptor1->width = aDescriptor2->width = operator206yyy_width;
            operator206yyy_width                      = 0; /* Restore. Operator no longer in scope */
        }

        grib_bufr_descriptors_array_push(unexpanded, aDescriptor1);
        grib_bufr_descriptors_array_push(unexpanded_copy, aDescriptor2);
    }

    grib_context_free(c, u);

    ccp.extraWidth           = 0;
    ccp.localDescriptorWidth = -1;
    ccp.extraScale           = 0;
    ccp.referenceFactor      = 1;
    ccp.associatedFieldWidth = 0;
    ccp.newStringWidth       = 0;
    expanded_                = do_expand(unexpanded, &ccp, &err);
    if (err) {
        grib_bufr_descriptors_array_delete(unexpanded);
        grib_bufr_descriptors_array_delete(unexpanded_copy);
        return err;
    }
    grib_context_expanded_descriptors_list_push(c, key, expanded_, unexpanded_copy);
    grib_bufr_descriptors_array_delete(unexpanded);

    return err;
}

int grib_accessor_expanded_descriptors_t::grib_accessor_expanded_descriptors_set_do_expand(long do_expand)
{
    do_expand_ = do_expand;
    return 0;
}

bufr_descriptors_array* grib_accessor_expanded_descriptors_t::grib_accessor_expanded_descriptors_get_expanded(int* err)
{
    *err = expand();
    return expanded_;
}

int grib_accessor_expanded_descriptors_t::unpack_double(double* val, size_t* len)
{
    int ret  = 0;
    size_t i = 0;
    size_t expandedSize;

    if (rank_ != 2) {
        long* lval = (long*)grib_context_malloc_clear(context_, *len * sizeof(long));
        ret        = unpack_long(lval, len);
        if (ret)
            return ret;
        for (i = 0; i < *len; i++)
            val[i] = (double)lval[i];
        grib_context_free(context_, lval);
    }
    else {
        ret = expand();
        if (ret)
            return ret;

        expandedSize = BUFR_DESCRIPTORS_ARRAY_USED_SIZE(expanded_);
        if (*len < expandedSize) {
            grib_context_log(context_, GRIB_LOG_ERROR,
                             "Wrong size (%ld) for %s, it contains %lu values", *len, name_, expandedSize);
            *len = 0;
            return GRIB_ARRAY_TOO_SMALL;
        }
        *len = expandedSize;
        for (i = 0; i < *len; i++)
            val[i] = expanded_->v[i]->reference;
    }
    return ret;
}

int grib_accessor_expanded_descriptors_t::unpack_long(long* val, size_t* len)
{
    int ret     = 0;
    size_t rlen = 0;
    size_t i    = 0;

    ret = expand();
    if (ret)
        return ret;
    if (!expanded_)
        return GRIB_DECODING_ERROR;
    rlen = BUFR_DESCRIPTORS_ARRAY_USED_SIZE(expanded_);

    if (*len < rlen) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "Wrong size (%ld) for %s, it contains %lu values", *len, name_, rlen);
        *len = 0;
        return GRIB_ARRAY_TOO_SMALL;
    }

    *len = rlen;
    switch (rank_) {
        case 0:
            for (i = 0; i < *len; i++)
                val[i] = expanded_->v[i]->code;
            break;
        case 1:
            for (i = 0; i < *len; i++)
                val[i] = expanded_->v[i]->scale;
            break;
        case 2:
            return GRIB_INVALID_TYPE;
        case 3:
            for (i = 0; i < *len; i++)
                val[i] = expanded_->v[i]->width;
            break;
        case 4:
            for (i = 0; i < *len; i++)
                val[i] = expanded_->v[i]->type;
            break;
    }

    return GRIB_SUCCESS;
}

int grib_accessor_expanded_descriptors_t::unpack_string_array(char** buffer, size_t* len)
{
    int err      = 0;
    long* v      = NULL;
    char buf[25] = {0,};
    long llen = 0;
    size_t i = 0, size = 0;
    const grib_context* c = context_;

    err = value_count(&llen);
    if (err) return err;

    size = llen;
    v    = (long*)grib_context_malloc_clear(c, sizeof(long) * size);
    err  = unpack_long(v, &size);
    if (err) return err;

    for (i = 0; i < size; i++) {
        snprintf(buf, sizeof(buf), "%06ld", v[i]);
        buffer[i] = grib_context_strdup(c, buf);
    }
    *len = size;
    grib_context_free(c, v);

    return GRIB_NOT_IMPLEMENTED;
}

int grib_accessor_expanded_descriptors_t::pack_long(const long* val, size_t* len)
{
    do_expand_ = 1;
    return GRIB_NOT_IMPLEMENTED;
}

int grib_accessor_expanded_descriptors_t::value_count(long* rlen)
{
    int err = 0;
    *rlen   = 0;

    err = expand();
    if (err) {
        grib_context_log(context_, GRIB_LOG_ERROR, "%s unable to compute size", name_);
        grib_bufr_descriptors_array_delete(expanded_);
        return err;
    }
    *rlen = BUFR_DESCRIPTORS_ARRAY_USED_SIZE(expanded_);
    return err;
}

void grib_accessor_expanded_descriptors_t::destroy(grib_context* c)
{
    grib_accessor_long_t::destroy(c);
    // grib_accessor_expanded_descriptors_t* self = (grib_accessor_expanded_descriptors_t*)a;
    // if (rank==0 && expanded_ )
    //  grib_bufr_descriptors_array_delete(expanded_ );
}

long grib_accessor_expanded_descriptors_t::get_native_type()
{
    if (rank_ == 2)
        return GRIB_TYPE_DOUBLE;
    else
        return GRIB_TYPE_LONG;
}
