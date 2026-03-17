/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

/***************************************************************************
 *   Jean Baptiste Filippi - 01.11.2005                                    *
 ***************************************************************************/
#include "grib_api_internal.h"

static grib_handle* grib_handle_new_from_file_no_multi(grib_context* c, FILE* f, int headers_only, int* error);
static grib_handle* grib_handle_new_from_file_multi(grib_context* c, FILE* f, int* error);
static bool grib2_get_next_section(unsigned char* msgbegin, size_t msglen, unsigned char** secbegin, size_t* seclen, int* secnum, int* err);
static bool grib2_has_next_section(unsigned char* msgbegin, size_t msglen, unsigned char* secbegin, size_t seclen, int* err);
static void grib2_build_message(grib_context* context, unsigned char* sections[], size_t sections_len[], void** data, size_t* msglen);
static grib_multi_support* grib_get_multi_support(grib_context* c, FILE* f);
static grib_multi_support* grib_multi_support_new(grib_context* c);
static grib_handle* grib_handle_new_multi(grib_context* c, unsigned char** idata, size_t* buflen, int* error);

/* Note: A fast cut-down version of strcmp which does NOT return -1 */
/* 0 means input strings are equal and 1 means not equal */
static GRIB_INLINE int grib_inline_strcmp(const char* a, const char* b)
{
    if (*a != *b)
        return 1;
    while ((*a != 0 && *b != 0) && *(a) == *(b)) {
        a++;
        b++;
    }
    return (*a == 0 && *b == 0) ? 0 : 1;
}

grib_section* grib_section_create(grib_handle* h, grib_accessor* owner)
{
    grib_section* s = (grib_section*)grib_context_malloc_clear(h->context, sizeof(grib_section));
    s->owner        = owner;
    s->aclength     = NULL;
    s->h            = h;
    s->block        = (grib_block_of_accessors*)grib_context_malloc_clear(h->context, sizeof(grib_block_of_accessors));
    return s;
}

static void update_sections(grib_section* s, grib_handle* h, long offset)
{
    grib_accessor* a = s ? s->block->first : NULL;
    if (s)
        s->h = h;
    while (a) {
        a->offset_ += offset;
        /* update_sections ( grib_get_sub_section ( a ),h,offset ); */
        update_sections(a->sub_section_, h, offset);
        a = a->next_;
    }
}

void grib_swap_sections(grib_section* the_old, grib_section* the_new)
{
    grib_accessor* a;
    grib_block_of_accessors* b = the_old->block;

    /* printf("SWAPPING -----\n"); grib_dump_section_content(new,stdout); */
    the_old->block = the_new->block;
    the_new->block = b;

    a                 = the_old->aclength;
    the_old->aclength = the_new->aclength;
    the_new->aclength = a;

    a = the_old->block->first;
    while (a) {
        a->parent_ = the_old;
        a         = a->next_;
    }

    update_sections(the_old, the_old->h, the_old->owner->offset_);
    /* update_sections(new,new->h,new->owner->offset); */

    /* printf("SWAPPING -----\n"); grib_dump_section_content(old,stdout); */
}

void grib_empty_section(grib_context* c, grib_section* b)
{
    grib_accessor* current = NULL;
    if (!b)
        return;

    b->aclength = NULL;

    current = b->block->first;

    while (current) {
        grib_accessor* next = current->next_;
        if (current->sub_section_) {
            grib_section_delete(c, current->sub_section_);
            current->sub_section_ = 0;
        }
        current->destroy(c);
        delete current;
        current = next;
    }
    b->block->first = b->block->last = 0;
}

void grib_section_delete(grib_context* c, grib_section* b)
{
    if (!b)
        return;

    grib_empty_section(c, b);
    grib_context_free(c, b->block);
    /* printf("++++ deleted %p\n",b); */
    grib_context_free(c, b);
}

int grib_handle_delete(grib_handle* h)
{
    if (h != NULL) {
        grib_context* ct   = h->context;
        grib_dependency* d = h->dependencies;
        grib_dependency* n;

        if (h->kid != NULL)
            return GRIB_INTERNAL_ERROR;

        while (d) {
            n = d->next;
            grib_context_free(ct, d);
            d = n;
        }
        h->dependencies = 0;

        grib_buffer_delete(ct, h->buffer);
        grib_section_delete(ct, h->root);
        grib_context_free(ct, h->gts_header);

        grib_context_log(ct, GRIB_LOG_DEBUG, "grib_handle_delete: deleting handle %p", (void*)h);
        grib_context_free(ct, h);
        h = NULL;
    }
    return GRIB_SUCCESS;
}

grib_handle* grib_new_handle(grib_context* c)
{
    grib_handle* g = NULL;
    if (c == NULL)
        c = grib_context_get_default();
    g = (grib_handle*)grib_context_malloc_clear(c, sizeof(grib_handle));

    if (g == NULL) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Cannot allocate handle", __func__);
    }
    else {
        g->context      = c;
        g->product_kind = PRODUCT_ANY; /* Default. Will later be set to a specific product */
    }

    grib_context_log(c, GRIB_LOG_DEBUG, "%s: Allocated handle %p", __func__, (void*)g);

    return g;
}

static grib_handle* grib_handle_create(grib_handle* gl, grib_context* c, const void* data, size_t buflen)
{
    grib_action* next = NULL;
    int err           = 0;

    if (gl == NULL)
        return NULL;

    gl->use_trie     = 1;
    gl->trie_invalid = 0;
    gl->buffer       = grib_new_buffer(gl->context, (const unsigned char*)data, buflen);

    if (gl->buffer == NULL) {
        grib_handle_delete(gl);
        return NULL;
    }

    gl->root = grib_create_root_section(gl->context, gl);

    if (!gl->root) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Cannot create root section", __func__);
        grib_handle_delete(gl);
        return NULL;
    }

    if (!gl->context->grib_reader || !gl->context->grib_reader->first) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Cannot create handle, no definitions found", __func__);
        grib_handle_delete(gl);
        return NULL;
    }

    gl->buffer->property = CODES_USER_BUFFER;

    next = gl->context->grib_reader->first->root;
    while (next) {
        if (next->create_accessor(gl->root, NULL) != GRIB_SUCCESS)
            break;
        next = next->next_;
    }

    err = grib_section_adjust_sizes(gl->root, 0, 0);
    if (err) {
        grib_handle_delete(gl);
        return NULL;
    }

    grib_section_post_init(gl->root);

    return gl;
}

grib_handle* codes_handle_new_from_samples(grib_context* c, const char* name)
{
    grib_handle* g = 0;
    if (c == NULL)
        c = grib_context_get_default();
    grib_context_set_handle_file_count(c, 0);
    grib_context_set_handle_total_count(c, 0);

    if (c->debug) {
        fprintf(stderr, "ECCODES DEBUG codes_handle_new_from_samples '%s'\n", name);
    }

    g = codes_external_sample(c, PRODUCT_ANY, name);
    if (!g) {
        grib_context_log(c, GRIB_LOG_ERROR,
                         "Unable to load sample file '%s.tmpl'\n"
                         "                   samples path='%s'\n"
                         "                   (ecCodes Version=%s)",
                         name, c->grib_samples_path, ECCODES_VERSION_STR);
    }

    return g;
}

grib_handle* grib_handle_new_from_samples(grib_context* c, const char* name)
{
    grib_handle* g = 0;
    if (c == NULL)
        c = grib_context_get_default();
    grib_context_set_handle_file_count(c, 0);
    grib_context_set_handle_total_count(c, 0);

    /*
     * g = grib_internal_sample(c,name);
     * if(g) return g;
     */
    if (c->debug) {
        fprintf(stderr, "ECCODES DEBUG grib_handle_new_from_samples '%s'\n", name);
    }

    g = codes_external_sample(c, PRODUCT_GRIB, name);
    if (!g)
        grib_context_log(c, GRIB_LOG_ERROR,
                         "Unable to load GRIB sample file '%s.tmpl'\n"
                         "                   from %s\n"
                         "                   (ecCodes Version=%s)",
                         name, c->grib_samples_path, ECCODES_VERSION_STR);

    return g;
}

grib_handle* codes_bufr_handle_new_from_samples(grib_context* c, const char* name)
{
    grib_handle* g = 0;
    if (c == NULL)
        c = grib_context_get_default();
    grib_context_set_handle_file_count(c, 0);
    grib_context_set_handle_total_count(c, 0);

    if (c->debug) {
        fprintf(stderr, "ECCODES DEBUG bufr_handle_new_from_samples '%s'\n", name);
    }

    g = codes_external_sample(c, PRODUCT_BUFR, name);
    if (!g) {
        grib_context_log(c, GRIB_LOG_ERROR,
                         "Unable to load BUFR sample file '%s.tmpl'\n"
                         "                   from %s\n"
                         "                   (ecCodes Version=%s)",
                         name, c->grib_samples_path, ECCODES_VERSION_STR);
    }

    return g;
}

int grib_write_message(const grib_handle* h, const char* file, const char* mode)
{
    FILE* fh = 0;
    int err;
    const void* buffer;
    size_t size;

    fh = fopen(file, mode);
    if (!fh) {
        perror(file);
        return GRIB_IO_PROBLEM;
    }
    err = grib_get_message(h, &buffer, &size);
    if (err) {
        fclose(fh);
        return err;
    }

    if (fwrite(buffer, 1, size, fh) != size) {
        perror(file);
        fclose(fh);
        return GRIB_IO_PROBLEM;
    }
    if (codes_flush_sync_close_file(fh) != GRIB_SUCCESS) {
        perror(file);
        return GRIB_IO_PROBLEM;
    }
    return 0;
}

grib_handle* grib_handle_clone(const grib_handle* h)
{
    grib_handle* result  = grib_handle_new_from_message_copy(h->context, h->buffer->data, h->buffer->ulength);
    result->product_kind = h->product_kind;
    return result;
}

static bool can_create_clone_headers_only(const grib_handle* h)
{
    // Only for GRIB, not BUFR etc
    if (h->product_kind != PRODUCT_GRIB) return false;

    // Spectral data does not have constant fields!
    long isGridded = 0;
    int err = grib_get_long(h, "isGridded", &isGridded);
    if (err || !isGridded) return false;

    return true;
}

// Clone the message but not its Bitmap and Data sections (only the meta-data)
grib_handle* grib_handle_clone_headers_only(const grib_handle* h)
{
    int err = 0;
    grib_handle* result = NULL;
    grib_context* c = h->context;

    if (!can_create_clone_headers_only(h)) {
        // Headers-only clone not possible. Do a normal clone
        return grib_handle_clone(h);
    }

    char sample_name[1024];
    long edition = 0;
    grib_get_long(h, "edition", &edition);
    snprintf(sample_name, sizeof(sample_name), "GRIB%ld", edition);
    grib_handle* h_sample = grib_handle_new_from_samples(c, sample_name);
    if (!h_sample) {
        grib_context_log(c, GRIB_LOG_ERROR, "Failed to create headers_only clone using sample %s", sample_name);
        return NULL;
    }

    // Must preserve the packingType
    char input_packing_type[100];
    size_t len = sizeof(input_packing_type);
    err = grib_get_string(h, "packingType", input_packing_type, &len);
    if (!err) {
        grib_set_string(h_sample, "packingType", input_packing_type, &len);
    }

    // Copy all sections except Bitmap and Data from h to h_sample
    const int sections_to_copy = GRIB_SECTION_PRODUCT | GRIB_SECTION_LOCAL | GRIB_SECTION_GRID;
    result = grib_util_sections_copy((grib_handle*)h, h_sample, sections_to_copy, &err);
    if (!result || err) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s failed: Unable to copy sections (%s)", __func__, grib_get_error_message(err));
        grib_handle_delete(h_sample);
        return NULL;
    }

    grib_handle_delete(h_sample);
    return result;
}

// grib_handle* grib_handle_clone_headers_only(const grib_handle* h)
// {
//     int err = 0;
//     size_t size1 = 0;
//     const void* msg1 = NULL;
//     long edition = 0;
//     // Only for GRIB, not BUFR etc
//     if (h->product_kind != PRODUCT_GRIB) {
//         grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Only supported for %s",
//                          __func__, codes_get_product_name(PRODUCT_GRIB));
//         return NULL;
//     }
//     err = grib_get_long(h, "edition", &edition);
//     if (!err && edition == 1) {
//         grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Edition not supported", __func__);
//         return NULL;
//     }
//     err = grib_get_message_headers(h, &msg1, &size1);
//     if (err) return NULL;
//     size1 += 4;
//     grib_handle* result  = grib_handle_new_from_partial_message_copy(h->context, msg1, size1);
//     result->buffer->data[ size1 - 4 ] = '7';
//     result->buffer->data[ size1 - 3 ] = '7';
//     result->buffer->data[ size1 - 2 ] = '7';
//     result->buffer->data[ size1 - 1 ] = '7';
//     result->buffer->ulength = size1;
//     result->product_kind = h->product_kind;
//     long off = 64; // This is only true for GRIB edition 2
//     err = grib_encode_unsigned_long( result->buffer->data, (unsigned long)size1, &off, 64);
//     if (err) {
//         printf("err=%s\n", grib_get_error_message(err));
//         return NULL;
//     }
//     return result;
// }

grib_handle* codes_handle_new_from_file(grib_context* c, FILE* f, ProductKind product, int* error)
{
    if (product == PRODUCT_GRIB)
        return grib_handle_new_from_file(c, f, error);
    if (product == PRODUCT_BUFR)
        return bufr_new_from_file(c, f, error);
    if (product == PRODUCT_METAR)
        return metar_new_from_file(c, f, error);
    if (product == PRODUCT_GTS)
        return gts_new_from_file(c, f, error);
    //if (product == PRODUCT_TAF)
    //    return taf_new_from_file(c, f, error);
    if (product == PRODUCT_ANY)
        return any_new_from_file(c, f, error);

    ECCODES_ASSERT(!"codes_handle_new_from_file: Invalid product");
    return NULL;
}

grib_handle* codes_grib_handle_new_from_file(grib_context* c, FILE* f, int* error)
{
    return grib_handle_new_from_file(c, f, error);
}
grib_handle* codes_bufr_handle_new_from_file(grib_context* c, FILE* f, int* error)
{
    return bufr_new_from_file(c, f, error);
}
grib_handle* codes_metar_handle_new_from_file(grib_context* c, FILE* f, int* error)
{
    return metar_new_from_file(c, f, error);
}
grib_handle* codes_gts_handle_new_from_file(grib_context* c, FILE* f, int* error)
{
    return gts_new_from_file(c, f, error);
}

static int determine_product_kind(grib_handle* h, ProductKind* prod_kind)
{
    int err    = 0;
    size_t len = 0;
    err        = grib_get_length(h, "identifier", &len);
    if (!err) {
        char id_str[64] = {0,};
        err = grib_get_string(h, "identifier", id_str, &len);
        if (grib_inline_strcmp(id_str, "GRIB") == 0)
            *prod_kind = PRODUCT_GRIB;
        else if (grib_inline_strcmp(id_str, "BUFR") == 0)
            *prod_kind = PRODUCT_BUFR;
        else if (grib_inline_strcmp(id_str, "METAR") == 0)
            *prod_kind = PRODUCT_METAR;
        else if (grib_inline_strcmp(id_str, "GTS") == 0)
            *prod_kind = PRODUCT_GTS;
        else if (grib_inline_strcmp(id_str, "TAF") == 0)
            *prod_kind = PRODUCT_TAF;
        else
            *prod_kind = PRODUCT_ANY;
    }
    return err;
}

grib_handle* grib_handle_new_from_message_copy(grib_context* c, const void* data, size_t size)
{
    grib_handle* g = NULL;
    void* copy     = NULL;
    if (c == NULL)
        c = grib_context_get_default();

    grib_context_set_handle_file_count(c, 0);
    grib_context_set_handle_total_count(c, 0);
    copy = grib_context_malloc(c, size);
    if (!copy) {
        return NULL;
    }

    memcpy(copy, data, size);

    g                   = grib_handle_new_from_message(c, copy, size);
    g->buffer->property = CODES_MY_BUFFER;

    return g;
}

grib_handle* grib_handle_new_from_partial_message_copy(grib_context* c, const void* data, size_t size)
{
    grib_handle* g = NULL;
    void* copy     = NULL;
    if (c == NULL)
        c = grib_context_get_default();
    grib_context_set_handle_file_count(c, 0);
    grib_context_set_handle_total_count(c, 0);
    copy = grib_context_malloc(c, size);
    if (!copy)
        return NULL;

    memcpy(copy, data, size);

    g = grib_handle_new_from_partial_message(c, copy, size);
    g->buffer->property = CODES_MY_BUFFER;

    return g;
}

grib_handle* grib_handle_new_from_partial_message(grib_context* c, const void* data, size_t buflen)
{
    grib_handle* gl = NULL;
    if (c == NULL)
        c = grib_context_get_default();
    grib_context_set_handle_file_count(c, 0);
    grib_context_set_handle_total_count(c, 0);
    gl          = grib_new_handle(c);
    gl->partial = 1;
    return grib_handle_create(gl, c, data, buflen);
}

grib_handle* grib_handle_new_from_message(grib_context* c, const void* data, size_t buflen)
{
    grib_handle* gl          = NULL;
    grib_handle* h           = NULL;
    ProductKind product_kind = PRODUCT_ANY;
    if (c == NULL)
        c = grib_context_get_default();
    gl               = grib_new_handle(c);
    gl->product_kind = PRODUCT_GRIB; /* See ECC-480 */
    h                = grib_handle_create(gl, c, data, buflen);
    if (!h) return NULL;

    /* See ECC-448 */
    if (determine_product_kind(h, &product_kind) == GRIB_SUCCESS) {
        h->product_kind = product_kind;
    }

    if (h->product_kind == PRODUCT_GRIB) {
        if (!grib_is_defined(h, "7777")) {
            grib_context_log(c, GRIB_LOG_ERROR, "%s: No final 7777 in message!", __func__);
            /* TODO: Return NULL. An incomplete message is no use to anyone.
             * But first check the MARS Client and other applications
             */
        }
    }
    return h;
}

grib_handle* grib_handle_new_from_multi_message(grib_context* c, void** data,
                                                size_t* buflen, int* error)
{
    grib_handle* h    = NULL;
    unsigned char** d = (unsigned char**)data;
    if (c == NULL)
        c = grib_context_get_default();

    if (c->multi_support_on)
        h = grib_handle_new_multi(c, d, buflen, error);
    else {
        size_t olen   = 0;
        void* message = NULL;
        *error        = grib_read_any_from_memory_alloc(c, d, buflen, &message, &olen);
        if (message == NULL)
            return NULL;
        h = grib_new_handle(c);
        grib_handle_create(h, c, message, olen);
    }

    return h;
}

grib_handle* grib_handle_new_from_file(grib_context* c, FILE* f, int* error)
{
    return grib_new_from_file(c, f, 0, error);
}

static grib_handle* grib_handle_new_multi(grib_context* c, unsigned char** data,
                                          size_t* buflen, int* error)
{
    void* message = NULL;
    size_t olen = 0, len = 0;
    grib_handle* gl         = NULL;
    long edition            = 0;
    size_t seclen           = 0;
    unsigned char* secbegin = 0;
    int secnum = 0;
    // int seccount = 0;
    int err = 0, i = 0;
    grib_multi_support* gm = NULL;

    if (c == NULL)
        c = grib_context_get_default();

    gm = grib_get_multi_support(c, 0);

    if (!gm->message) {
        *error             = grib_read_any_from_memory_alloc(c, data, buflen, &message, &olen);
        gm->message_length = olen;
        gm->message        = (unsigned char*)message;
        if (*error != GRIB_SUCCESS || !message) {
            if (*error == GRIB_END_OF_FILE)
                *error = GRIB_SUCCESS;
            gm->message_length = 0;
            return NULL;
        }
    }
    else {
        message = gm->message;
    }

    edition = grib_decode_unsigned_byte_long((const unsigned char*)message, 7, 1);

    if (edition == 2) {
        olen = gm->message_length;
        if (gm->section_number == 0) {
            gm->sections[0] = (unsigned char*)message;
        }
        secbegin = gm->sections[gm->section_number];
        seclen   = gm->sections_length[gm->section_number];
        secnum   = gm->section_number;
        // seccount = 0;
        while (grib2_get_next_section((unsigned char*)message, olen, &secbegin, &seclen, &secnum, &err)) {
            // seccount++;
            /*printf("   - %d - section %d length=%d\n",(int)seccount,(int)secnum,(int)seclen);*/
            gm->sections[secnum]        = secbegin;
            gm->sections_length[secnum] = seclen;

            if (secnum == 6) {
                /* Special case for inherited bitmaps */
                if (grib_decode_unsigned_byte_long(secbegin, 5, 1) == 254) {
                    if (!gm->bitmap_section) {
                        grib_context_log(c, GRIB_LOG_ERROR,
                                         "%s: Cannot create handle, missing bitmap", __func__);
                        return NULL;
                    }
                    gm->sections[secnum]        = gm->bitmap_section;
                    gm->sections_length[secnum] = gm->bitmap_section_length;
                }
                else {
                    if (gm->bitmap_section) {
                        grib_context_free(c, gm->bitmap_section);
                        gm->bitmap_section = NULL;
                    }
                    gm->bitmap_section        = (unsigned char*)grib_context_malloc(c, seclen);
                    gm->bitmap_section        = (unsigned char*)memcpy(gm->bitmap_section, secbegin, seclen);
                    gm->bitmap_section_length = seclen;
                }
            }

            if (secnum == 7) {
                void* p = message;
                len     = olen;
                grib2_build_message(c, gm->sections, gm->sections_length, &message, &len);

                if (grib2_has_next_section((unsigned char*)p, olen, secbegin, seclen, &err)) {
                    gm->message        = (unsigned char*)p;
                    gm->section_number = secnum;
                    olen               = len;
                }
                else {
                    grib_context_free(c, gm->message);
                    gm->message = NULL;
                    for (i = 0; i < 8; i++)
                        gm->sections[i] = NULL;
                    gm->section_number = 0;
                    gm->message_length = 0;
                    olen               = len;
                }

                break;
            }
        }
        // ECC-782
        if (err == GRIB_INVALID_SECTION_NUMBER) {
            grib_context_log(c, GRIB_LOG_ERROR, "%s: Failed to get section info (%s)", __func__, grib_get_error_message(err));
            return NULL;
        }
    }
    else if (edition == 3) {
        *error = GRIB_UNSUPPORTED_EDITION;
        return NULL;
    }
    else {
        gm->message_length = 0;
        gm->message        = NULL;
    }

    gl = grib_handle_new_from_message(c, message, olen);
    if (!gl) {
        *error = GRIB_DECODING_ERROR;
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Cannot create handle", __func__);
        return NULL;
    }

    gl->buffer->property = CODES_MY_BUFFER;
    grib_context_increment_handle_file_count(c);
    grib_context_increment_handle_total_count(c);

    return gl;
}

static grib_handle* grib_handle_new_from_file_multi(grib_context* c, FILE* f, int* error)
{
    void *data = NULL, *old_data = NULL;
    size_t olen = 0, len = 0;
    grib_handle* gl         = NULL;
    long edition            = 0;
    size_t seclen           = 0;
    unsigned char* secbegin = 0;
    int secnum = 0;
    // int seccount = 0;
    int err = 0, i = 0;
    grib_multi_support* gm  = NULL;
    off_t gts_header_offset = 0;
    off_t end_msg_offset = 0, offset = 0;
    char *gts_header = 0, *save_gts_header = 0;
    int gtslen = 0;

    *error = GRIB_SUCCESS;

    if (c == NULL)
        c = grib_context_get_default();

    gm = grib_get_multi_support(c, f);

    if (!gm->message) {
        gts_header_offset = grib_context_tell(c, f);
        data              = wmo_read_grib_from_file_malloc(f, 0, &olen, &offset, error);
        end_msg_offset    = grib_context_tell(c, f);

        gm->message_length = olen;
        gm->message        = (unsigned char*)data;
        gm->offset         = offset;
        if (*error != GRIB_SUCCESS || !data) {
            if (data)
                grib_context_free(c, data);

            if (*error == GRIB_END_OF_FILE)
                *error = GRIB_SUCCESS;
            gm->message_length = 0;
            gm->message        = NULL;
            return NULL;
        }
        if (c->gts_header_on) {
            int g = 0;
            grib_context_seek(c, gts_header_offset, SEEK_SET, f);
            gtslen          = offset - gts_header_offset;
            gts_header      = (char*)grib_context_malloc_clear(c, sizeof(unsigned char) * gtslen);
            save_gts_header = gts_header;
            grib_context_read(c, gts_header, gtslen, f);
            g = gtslen;
            while (gts_header != NULL && g != 0 && *gts_header != '\03') {
                /*printf("--------%d %X \n",gtslen,*gts_header);*/
                gts_header++;
                g--;
            }
            if (g > 8) {
                gts_header++;
                gtslen = g - 1;
            }
            else
                gts_header = save_gts_header;
            grib_context_seek(c, end_msg_offset, SEEK_SET, f);
        }
    }
    else
        data = gm->message;

    edition = grib_decode_unsigned_byte_long((const unsigned char*)data, 7, 1);

    if (edition == 2) {
        olen = gm->message_length;
        if (gm->section_number == 0) {
            gm->sections[0] = (unsigned char*)data;
        }
        secbegin = gm->sections[gm->section_number];
        seclen   = gm->sections_length[gm->section_number];
        secnum   = gm->section_number;
        // seccount = 0;
        while (grib2_get_next_section((unsigned char*)data, olen, &secbegin, &seclen, &secnum, &err)) {
            // seccount++;
            /*printf("   - %d - section %d length=%d\n",(int)seccount,(int)secnum,(int)seclen);*/

            gm->sections[secnum]        = secbegin;
            gm->sections_length[secnum] = seclen;

            if (secnum == 6) {
                /* Special case for inherited bitmaps */
                if (grib_decode_unsigned_byte_long(secbegin, 5, 1) == 254) {
                    if (!gm->bitmap_section) {
                        grib_context_log(c, GRIB_LOG_ERROR, "%s: Cannot create handle, missing bitmap", __func__);
                        grib_context_free(c, data);
                        return NULL;
                    }
                    gm->sections[secnum]        = gm->bitmap_section;
                    gm->sections_length[secnum] = gm->bitmap_section_length;
                }
                else {
                    if (gm->bitmap_section) {
                        grib_context_free(c, gm->bitmap_section);
                        gm->bitmap_section = NULL;
                    }
                    gm->bitmap_section        = (unsigned char*)grib_context_malloc(c, seclen);
                    gm->bitmap_section        = (unsigned char*)memcpy(gm->bitmap_section, secbegin, seclen);
                    gm->bitmap_section_length = seclen;
                }
            }

            if (secnum == 7) {
                old_data = data;
                len      = olen;
                grib2_build_message(c, gm->sections, gm->sections_length, &data, &len);

                if (grib2_has_next_section((unsigned char*)old_data, olen, secbegin, seclen, &err)) {
                    gm->message        = (unsigned char*)old_data;
                    gm->section_number = secnum;
                    olen               = len;
                }
                else {
                    if (gm->message)
                        grib_context_free(c, gm->message);
                    gm->message = NULL;
                    for (i = 0; i < 8; i++)
                        gm->sections[i] = NULL;
                    gm->section_number = 0;
                    gm->message_length = 0;
                    olen               = len;
                }
                break;
            }
        }
        // ECC-782
        if (err == GRIB_INVALID_SECTION_NUMBER) {
            grib_context_log(c, GRIB_LOG_ERROR, "%s: Failed to get section info (%s)", __func__, grib_get_error_message(err));
            return NULL;
        }
    }
    else if (edition == 3) {
        /* GRIB3: Multi-field mode not yet supported */
        printf("WARNING: %s: GRIB3 multi-field mode not yet implemented! Reverting to single-field mode", __func__);
        gm->message_length = 0;
        gm->message        = NULL;
    }
    else {
        gm->message_length = 0;
        gm->message        = NULL;
    }

    gl = grib_handle_new_from_message(c, data, olen);
    if (!gl) {
        *error = GRIB_DECODING_ERROR;
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Cannot create handle", __func__);
        grib_context_free(c, data);
        return NULL;
    }

    gl->offset           = gm->offset;
    gl->buffer->property = CODES_MY_BUFFER;
    grib_context_increment_handle_file_count(c);
    grib_context_increment_handle_total_count(c);

    if (c->gts_header_on && gtslen >= 8) {
        gl->gts_header = (char*)grib_context_malloc_clear(c, sizeof(unsigned char) * gtslen);
        DEBUG_ASSERT(gts_header);
        if (gts_header) memcpy(gl->gts_header, gts_header, gtslen);
        gl->gts_header_len = gtslen;
        grib_context_free(c, save_gts_header);
    }
    else {
        gl->gts_header = NULL;
    }

    return gl;
}

grib_handle* grib_new_from_file(grib_context* c, FILE* f, int headers_only, int* error)
{
    grib_handle* h = 0;
    if (!f) {
        *error = GRIB_IO_PROBLEM;
        return NULL;
    }

    if (c == NULL)
        c = grib_context_get_default();

    if (c->multi_support_on)
        h = grib_handle_new_from_file_multi(c, f, error);
    else
        h = grib_handle_new_from_file_no_multi(c, f, headers_only, error);

    if (h && h->offset == 0)
        grib_context_set_handle_file_count(c, 1);

    if (h) {
        h->product_kind = PRODUCT_GRIB;
    }

    if (!c->no_fail_on_wrong_length && *error == GRIB_WRONG_LENGTH) {
        grib_handle_delete(h);
        h = NULL;
    }

    return h;
}

grib_handle* gts_new_from_file(grib_context* c, FILE* f, int* error)
{
    void* data      = NULL;
    size_t olen     = 0;
    grib_handle* gl = NULL;
    off_t offset    = 0;

    if (c == NULL)
        c = grib_context_get_default();

    data = wmo_read_gts_from_file_malloc(f, 0, &olen, &offset, error);

    if (*error != GRIB_SUCCESS) {
        if (data)
            grib_context_free(c, data);

        if (*error == GRIB_END_OF_FILE)
            *error = GRIB_SUCCESS;
        return NULL;
    }

    gl = grib_handle_new_from_message(c, data, olen);

    if (!gl) {
        *error = GRIB_DECODING_ERROR;
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Cannot create handle", __func__);
        grib_context_free(c, data);
        return NULL;
    }

    gl->offset           = offset;
    gl->buffer->property = CODES_MY_BUFFER;
    gl->product_kind     = PRODUCT_GTS;
    grib_context_increment_handle_file_count(c);
    grib_context_increment_handle_total_count(c);
    if (gl->offset == 0)
        grib_context_set_handle_file_count(c, 1);

    return gl;
}

grib_handle* taf_new_from_file(grib_context* c, FILE* f, int* error)
{
    void* data      = NULL;
    size_t olen     = 0;
    grib_handle* gl = NULL;
    off_t offset    = 0;

    if (c == NULL)
        c = grib_context_get_default();

    data = wmo_read_taf_from_file_malloc(f, 0, &olen, &offset, error);

    if (*error != GRIB_SUCCESS) {
        if (data)
            grib_context_free(c, data);

        if (*error == GRIB_END_OF_FILE)
            *error = GRIB_SUCCESS;
        return NULL;
    }

    gl = grib_handle_new_from_message(c, data, olen);

    if (!gl) {
        *error = GRIB_DECODING_ERROR;
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Cannot create handle", __func__);
        grib_context_free(c, data);
        return NULL;
    }

    gl->offset           = offset;
    gl->buffer->property = CODES_MY_BUFFER;
    gl->product_kind     = PRODUCT_TAF;
    grib_context_increment_handle_file_count(c);
    grib_context_increment_handle_total_count(c);
    if (gl->offset == 0)
        grib_context_set_handle_file_count(c, 1);

    return gl;
}

grib_handle* metar_new_from_file(grib_context* c, FILE* f, int* error)
{
    void* data      = NULL;
    size_t olen     = 0;
    grib_handle* gl = NULL;
    off_t offset    = 0;

    if (c == NULL)
        c = grib_context_get_default();

    data = wmo_read_metar_from_file_malloc(f, 0, &olen, &offset, error);

    if (*error != GRIB_SUCCESS) {
        if (data)
            grib_context_free(c, data);

        if (*error == GRIB_END_OF_FILE)
            *error = GRIB_SUCCESS;
        return NULL;
    }

    gl = grib_handle_new_from_message(c, data, olen);

    if (!gl) {
        *error = GRIB_DECODING_ERROR;
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Cannot create handle", __func__);
        grib_context_free(c, data);
        return NULL;
    }

    gl->offset           = offset;
    gl->buffer->property = CODES_MY_BUFFER;
    gl->product_kind     = PRODUCT_METAR;
    grib_context_increment_handle_file_count(c);
    grib_context_increment_handle_total_count(c);
    if (gl->offset == 0)
        grib_context_set_handle_file_count(c, 1);

    return gl;
}

grib_handle* bufr_new_from_file(grib_context* c, FILE* f, int* error)
{
    void* data              = NULL;
    size_t olen             = 0;
    grib_handle* gl         = NULL;
    off_t gts_header_offset = 0;
    off_t offset = 0, end_msg_offset = 0;
    char *gts_header = 0, *save_gts_header = 0;
    int gtslen = 0;

    if (c == NULL)
        c = grib_context_get_default();

    gts_header_offset = grib_context_tell(c, f);
    data              = wmo_read_bufr_from_file_malloc(f, 0, &olen, &offset, error);
    end_msg_offset    = grib_context_tell(c, f);

    if (*error != GRIB_SUCCESS) {
        if (data)
            grib_context_free(c, data);

        if (*error == GRIB_END_OF_FILE)
            *error = GRIB_SUCCESS;
        return NULL;
    }

    if (c->gts_header_on) {
        int g = 0;
        grib_context_seek(c, gts_header_offset, SEEK_SET, f);
        gtslen          = offset - gts_header_offset;
        gts_header      = (char*)grib_context_malloc(c, sizeof(unsigned char) * gtslen);
        save_gts_header = gts_header;
        grib_context_read(c, gts_header, gtslen, f);
        g = gtslen;
        while (gts_header != NULL && g != 0 && *gts_header != '\03') {
            /*printf("--------%d %X \n",gtslen,*gts_header);*/
            gts_header++;
            g--;
        }
        if (g > 8) {
            gts_header++;
            gtslen = g - 1;
        }
        else
            gts_header = save_gts_header;
        grib_context_seek(c, end_msg_offset, SEEK_SET, f);
    }

    gl = grib_handle_new_from_message(c, data, olen);

    if (!gl) {
        *error = GRIB_DECODING_ERROR;
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Cannot create handle", __func__);
        grib_context_free(c, data);
        return NULL;
    }

    gl->offset           = offset;
    gl->buffer->property = CODES_MY_BUFFER;
    gl->product_kind     = PRODUCT_BUFR;
    grib_context_increment_handle_file_count(c);
    grib_context_increment_handle_total_count(c);
    if (gl->offset == 0)
        grib_context_set_handle_file_count(c, 1);

    if (c->gts_header_on && gtslen >= 8) {
        gl->gts_header = (char*)grib_context_malloc(c, sizeof(unsigned char) * gtslen);
        DEBUG_ASSERT(gts_header);
        if (gts_header) memcpy(gl->gts_header, gts_header, gtslen);
        gl->gts_header_len = gtslen;
        grib_context_free(c, save_gts_header);
    }
    else {
        gl->gts_header = NULL;
    }

    return gl;
}

grib_handle* any_new_from_file(grib_context* c, FILE* f, int* error)
{
    void* data      = NULL;
    size_t olen     = 0;
    grib_handle* gl = NULL;
    off_t offset    = 0;

    if (c == NULL)
        c = grib_context_get_default();

    data = wmo_read_any_from_file_malloc(f, 0, &olen, &offset, error);

    if (*error != GRIB_SUCCESS) {
        if (data)
            grib_context_free(c, data);

        if (*error == GRIB_END_OF_FILE)
            *error = GRIB_SUCCESS;
        return NULL;
    }

    gl = grib_handle_new_from_message(c, data, olen);

    if (!gl) {
        *error = GRIB_DECODING_ERROR;
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Cannot create handle", __func__);
        grib_context_free(c, data);
        return NULL;
    }

    gl->offset           = offset;
    gl->buffer->property = CODES_MY_BUFFER;
    gl->product_kind     = PRODUCT_ANY;
    grib_context_increment_handle_file_count(c);
    grib_context_increment_handle_total_count(c);
    if (gl->offset == 0)
        grib_context_set_handle_file_count(c, 1);

    return gl;
}

static grib_handle* grib_handle_new_from_file_no_multi(grib_context* c, FILE* f, int headers_only, int* error)
{
    void* data              = NULL;
    size_t olen             = 0;
    grib_handle* gl         = NULL;
    off_t gts_header_offset = 0;
    off_t offset = 0, end_msg_offset = 0;
    char *gts_header = 0, *save_gts_header = 0;
    int gtslen = 0;

    if (c == NULL)
        c = grib_context_get_default();

    gts_header_offset = grib_context_tell(c, f);
    data              = wmo_read_grib_from_file_malloc(f, headers_only, &olen, &offset, error);
    end_msg_offset    = grib_context_tell(c, f);

    if (*error != GRIB_SUCCESS) {
        if (data)
            grib_context_free(c, data);

        if (*error == GRIB_END_OF_FILE)
            *error = GRIB_SUCCESS;
        return NULL;
    }

    if (c->gts_header_on) {
        int g = 0;
        grib_context_seek(c, gts_header_offset, SEEK_SET, f);
        gtslen          = offset - gts_header_offset;
        gts_header      = (char*)grib_context_malloc(c, sizeof(unsigned char) * gtslen);
        save_gts_header = gts_header;
        grib_context_read(c, gts_header, gtslen, f);
        g = gtslen;
        while (gts_header != NULL && g != 0 && *gts_header != '\03') {
            /*printf("--------%d %X \n",gtslen,*gts_header);*/
            gts_header++;
            g--;
        }
        if (g > 8) {
            gts_header++;
            gtslen = g - 1;
        }
        else
            gts_header = save_gts_header;
        grib_context_seek(c, end_msg_offset, SEEK_SET, f);
    }

    if (headers_only) {
        gl = grib_handle_new_from_partial_message(c, data, olen);
    }
    else {
        gl = grib_handle_new_from_message(c, data, olen);
    }

    if (!gl) {
        *error = GRIB_DECODING_ERROR;
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Cannot create handle", __func__);
        grib_context_free(c, data);
        return NULL;
    }

    gl->offset           = offset;
    gl->buffer->property = CODES_MY_BUFFER;

    grib_context_increment_handle_file_count(c);
    grib_context_increment_handle_total_count(c);

    if (c->gts_header_on && gtslen >= 8) {
        gl->gts_header = (char*)grib_context_malloc(c, sizeof(unsigned char) * gtslen);
        DEBUG_ASSERT(gts_header);
        if (gts_header) memcpy(gl->gts_header, gts_header, gtslen);
        gl->gts_header_len = gtslen;
        grib_context_free(c, save_gts_header);
    }
    else {
        gl->gts_header = NULL;
    }

    return gl;
}

grib_multi_handle* grib_multi_handle_new(grib_context* c)
{
    grib_multi_handle* h;
    if (c == NULL)
        c = grib_context_get_default();
    if (!c->multi_support_on) {
        grib_context_log(c, GRIB_LOG_DEBUG, "grib_multi_handle_new: Setting multi_support_on = 1");
        c->multi_support_on = 1;
    }
    h = (grib_multi_handle*)grib_context_malloc_clear(c, sizeof(grib_multi_handle));
    if (h == NULL) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Unable to allocate memory", __func__);
        return NULL;
    }
    h->buffer          = grib_create_growable_buffer(c);
    h->buffer->ulength = 0;
    h->context         = c;

    return h;
}

int grib_multi_handle_delete(grib_multi_handle* h)
{
    if (h != NULL) {
        grib_buffer_delete(h->context, h->buffer);
        grib_context_free(h->context, h);
    }
    return GRIB_SUCCESS;
}

int grib_multi_handle_append(grib_handle* h, int start_section, grib_multi_handle* mh)
{
    const void* mess = NULL;
    unsigned char* p = NULL;
    int err          = 0;
    size_t mess_len  = 0;
    size_t total_len = 0;

    if (!h)
        return GRIB_NULL_HANDLE;
    if (!mh)
        return GRIB_NULL_HANDLE;

    if (start_section == 0 || mh->buffer->ulength == 0) {
        err = grib_get_message(h, &mess, &mess_len);
        if (err != 0)
            return err;
        total_len = mh->buffer->ulength + mess_len;

        if (total_len > mh->buffer->length)
            grib_grow_buffer(h->context, mh->buffer, total_len);

        p = mh->buffer->data + mh->buffer->ulength;
        memcpy(p, mess, mess_len);
        mh->offset          = mh->buffer->ulength;
        mh->buffer->ulength = total_len;
        mh->length          = mess_len;
    }
    else {
        long off = 0;
        err      = grib_get_partial_message(h, &mess, &mess_len, start_section);
        if (err != 0)
            return err;
        total_len = mh->buffer->ulength + mess_len - 4;

        while (total_len > mh->buffer->length)
            grib_grow_buffer(h->context, mh->buffer, total_len);

        p = mh->buffer->data + mh->buffer->ulength - 4;
        memcpy(p, mess, mess_len);
        mh->length += mess_len - 4;

        off = mh->offset + 64;

        grib_encode_unsigned_long(mh->buffer->data, mh->length, &off, 64);
        mh->buffer->ulength = total_len;
    }
    return err;
}

int grib_multi_handle_write(grib_multi_handle* h, FILE* f)
{
    if (f == NULL)
        return GRIB_INVALID_FILE;
    if (h == NULL)
        return GRIB_INVALID_GRIB;

    if (fwrite(h->buffer->data, 1, h->buffer->ulength, f) != h->buffer->ulength) {
        grib_context_log(h->context, GRIB_LOG_PERROR, "%s failed", __func__);
        return GRIB_IO_PROBLEM;
    }

    return GRIB_SUCCESS;
}

int grib_get_partial_message(grib_handle* h, const void** msg, size_t* len, int start_section)
{
    size_t partial_len  = 0;
    long section_offset = 0;
    if (!h)
        return GRIB_NULL_HANDLE;

    if (start_section > h->sections_count)
        return GRIB_INVALID_SECTION_NUMBER;

    grib_get_long(h, h->section_offset[start_section], &section_offset);
    partial_len = h->buffer->ulength - section_offset;

    *len = partial_len;
    *msg = h->buffer->data + section_offset;

    return GRIB_SUCCESS;
}

int grib_get_partial_message_copy(grib_handle* h, void* message, size_t* len,
                                  int start_section)
{
    size_t partial_len  = 0;
    long section_offset = 0;
    if (!h)
        return GRIB_NULL_HANDLE;

    if (start_section > h->sections_count)
        return GRIB_INVALID_SECTION_NUMBER;

    grib_get_long(h, h->section_offset[start_section], &section_offset);
    partial_len = h->buffer->ulength - section_offset;

    if (*len < partial_len)
        return GRIB_BUFFER_TOO_SMALL;

    *len = partial_len;

    memcpy(message, h->buffer->data + section_offset, *len);
    return GRIB_SUCCESS;
}

int grib_get_message_copy(const grib_handle* h, void* message, size_t* len)
{
    if (!h)
        return GRIB_NOT_FOUND;

    if (*len < h->buffer->ulength)
        return GRIB_BUFFER_TOO_SMALL;

    *len = h->buffer->ulength;

    memcpy(message, h->buffer->data, *len);
    return GRIB_SUCCESS;
}

int grib_get_message_offset(const grib_handle* h, off_t* offset)
{
    if (h)
        *offset = h->offset;
    else
        return GRIB_NULL_HANDLE;

    return GRIB_SUCCESS;
}

int codes_get_product_kind(const grib_handle* h, ProductKind* product_kind)
{
    if (h) {
        *product_kind = h->product_kind;
        return GRIB_SUCCESS;
    }
    return GRIB_NULL_HANDLE;
}

int codes_check_message_header(const void* bytes, size_t length, ProductKind product)
{
    const char* p = ((const char*)bytes);
    ECCODES_ASSERT(p);
    ECCODES_ASSERT(product == PRODUCT_GRIB || product == PRODUCT_BUFR); /* Others not yet implemented */
    ECCODES_ASSERT(length > 4);
    if (product == PRODUCT_GRIB) {
        if (p[0] != 'G' || p[1] != 'R' || p[2] != 'I' || p[3] != 'B')
            return GRIB_INVALID_MESSAGE;
    }
    else if (product == PRODUCT_BUFR) {
        if (p[0] != 'B' || p[1] != 'U' || p[2] != 'F' || p[3] != 'R')
            return GRIB_INVALID_MESSAGE;
    }
    else {
        return GRIB_NOT_IMPLEMENTED;
    }

    return GRIB_SUCCESS;
}
int codes_check_message_footer(const void* bytes, size_t length, ProductKind product)
{
    const char* p = ((const char*)bytes);
    ECCODES_ASSERT(p);
    ECCODES_ASSERT(product == PRODUCT_GRIB || product == PRODUCT_BUFR); /* Others not yet implemented */

    if (p[length - 4] != '7' || p[length - 3] != '7' || p[length - 2] != '7' || p[length - 1] != '7') {
        return GRIB_7777_NOT_FOUND;
    }
    return GRIB_SUCCESS;
}

int grib_get_message_size(const grib_handle* ch, size_t* size)
{
    long totalLength = 0;
    int err          = 0;
    grib_handle* h   = (grib_handle*)ch;
    *size            = h->buffer->ulength;
    err              = grib_get_long(h, "totalLength", &totalLength);
    if (!err)
        *size = totalLength;
    return err;
}

int grib_get_message(const grib_handle* ch, const void** msg, size_t* size)
{
    long totalLength = 0;
    int err          = 0;
    grib_handle* h   = (grib_handle*)ch;
    *msg             = h->buffer->data;
    *size            = h->buffer->ulength;

    err = grib_get_long(h, "totalLength", &totalLength);
    if (!err)
        *size = totalLength;

    if (h->context->gts_header_on && h->gts_header) {
        char strbuf[10];
        snprintf(strbuf, sizeof(strbuf), "%.8d", (int)(h->buffer->ulength + h->gts_header_len - 6));
        memcpy(h->gts_header, strbuf, 8);
    }
    return 0;
}

int grib_get_message_headers(const grib_handle* h, const void** msg, size_t* size)
{
    int err = 0;
    size_t endOfHeadersMarker;
    *msg  = h->buffer->data;
    *size = h->buffer->ulength;

    if ((err = grib_get_offset(h, "endOfHeadersMarker", &endOfHeadersMarker)) != GRIB_SUCCESS) {
        grib_context_log(h->context, GRIB_LOG_ERROR,
                         "%s: Unable to get offset of endOfHeadersMarker", __func__);
        return err;
    }

    *size = endOfHeadersMarker;

    return err;
}

// grib_handle* grib_handle_new(grib_context* c)
// {
//     grib_handle* h;
//     if (!c)
//         c = grib_context_get_default();
//     h         = grib_new_handle(c);
//     h->buffer = grib_create_growable_buffer(c);
//     if (h->buffer == NULL) {
//         grib_handle_delete(h);
//         return NULL;
//     }
//     h->root = grib_create_root_section(h->context, h);
//     if (!h->root) {
//         grib_context_log(c, GRIB_LOG_ERROR, "grib_handle_new: cannot create root section");
//         grib_handle_delete(h);
//         return NULL;
//     }
//     if (!h->context->grib_reader || !h->context->grib_reader->first) {
//         grib_context_log(c, GRIB_LOG_ERROR, "grib_handle_new: cannot create handle, no definitions found");
//         grib_handle_delete(h);
//         return NULL;
//     }
//     h->buffer->property = CODES_USER_BUFFER;
//     h->header_mode = 1;
//     return h;
// }

grib_action* grib_action_from_filter(const char* filter)
{
    grib_action* a        = NULL;
    grib_context* context = grib_context_get_default();
    a                     = grib_parse_file(context, filter);

    if (context->grib_reader && context->grib_reader->first) {
        grib_context_free_persistent(context, context->grib_reader->first->filename);
        grib_context_free_persistent(context, context->grib_reader->first);
        grib_context_free_persistent(context, context->grib_reader);
    }

    context->grib_reader = nullptr;
    return a;
}

int grib_handle_apply_action(grib_handle* h, grib_action* a)
{
    int err;

    if (!a)
        return GRIB_SUCCESS; /* TODO: return error */

    while (a) {
        err = a->execute(h);
        if (err != GRIB_SUCCESS)
            return err;
        a = a->next_;
    }

    return GRIB_SUCCESS;
}

// int grib_handle_prepare_action(grib_handle* h, grib_action* a)
// {
//     int err;

//     if (!a)
//         return GRIB_SUCCESS; /* TODO: return error */

//     while (a) {
//         err = a->execute(h);
//         if (err != GRIB_SUCCESS)
//             return err;
//         a = a->next;
//     }

//     return GRIB_SUCCESS;
// }

static bool grib2_get_next_section(unsigned char* msgbegin, size_t msglen, unsigned char** secbegin, size_t* seclen, int* secnum, int* err)
{
    if (!grib2_has_next_section(msgbegin, msglen, *secbegin, *seclen, err))
        return false;

    *secbegin += *seclen;
    *seclen = grib_decode_unsigned_byte_long(*secbegin, 0, 4);
    *secnum = grib_decode_unsigned_byte_long(*secbegin, 4, 1);

    if (*secnum < 1 || *secnum > 7) {
        *err = GRIB_INVALID_SECTION_NUMBER;
        return false;
    }
    return true;
}

static bool grib2_has_next_section(unsigned char* msgbegin, size_t msglen, unsigned char* secbegin, size_t seclen, int* err)
{
    long next_seclen;
    *err = 0;

    next_seclen = (msgbegin + msglen) - (secbegin + seclen);

    if (next_seclen < 5) {
        if ((next_seclen > 3) && !strncmp((char*)secbegin, "7777", 4))
            *err = GRIB_SUCCESS;
        else
            *err = GRIB_7777_NOT_FOUND;
        return false;
    }

    /*secbegin += seclen;*/

    return true;
}

static void grib2_build_message(grib_context* context, unsigned char* sections[], size_t sections_len[], void** data, size_t* len)
{
    int i              = 0;
    const char* theEnd = "7777";
    unsigned char* p   = 0;
    size_t msglen      = 0;
    long bitp          = 64;
    if (!sections[0]) {
        *data = NULL;
        return;
    }

    for (i = 0; i < 8; i++)
        msglen += sections_len[i];
    msglen += 4;
    if (*len < msglen)
        msglen = *len;

    *data = (unsigned char*)grib_context_malloc(context, msglen);
    p     = (unsigned char*)*data;

    for (i = 0; i < 8; i++) {
        if (sections[i]) {
            memcpy(p, sections[i], sections_len[i]);
            p += sections_len[i];
        }
    }

    memcpy(p, theEnd, 4);

    grib_encode_unsigned_long((unsigned char*)*data, msglen, &bitp, 64);

    *len = msglen;
}

/* For multi support mode: Reset all file handles equal to f. See GRIB-249 */
void grib_multi_support_reset_file(grib_context* c, FILE* f)
{
    if (!c) c = grib_context_get_default();
    grib_multi_support* gm = c->multi_support;
    while (gm) {
        if (gm->file == f) {
            gm->file = NULL;
        }
        gm = gm->next;
    }
}

static grib_multi_support* grib_get_multi_support(grib_context* c, FILE* f)
{
    int i                    = 0;
    grib_multi_support* gm   = c->multi_support;
    grib_multi_support* prev = NULL;

    while (gm) {
        if (gm->file == f)
            return gm;
        prev = gm;
        gm   = gm->next;
    }

    if (!gm) {
        gm = grib_multi_support_new(c);
        if (!c->multi_support) {
            c->multi_support = gm;
        }
        else {
            if (prev)
                prev->next = gm;
        }
    }

    gm->next = 0;
    if (gm->message)
        grib_context_free(c, gm->message);
    gm->message            = NULL;
    gm->section_number     = 0;
    gm->sections_length[0] = 16;
    for (i = 1; i < 8; i++)
        gm->sections_length[i] = 0;
    gm->sections_length[8] = 4;
    gm->file               = f;

    return gm;
}

void grib_multi_support_reset(grib_context* c)
{
    if (!c) c = grib_context_get_default();
    const int GRIB2_END_SECTION = 8;

    grib_multi_support* gm = c->multi_support;
    while (gm) {
        if (gm->file)
            fclose(gm->file);
        if (gm->message)
            grib_context_free(c, gm->message);
        gm->message = NULL;
        for (int i = 0; i < GRIB2_END_SECTION; i++)
            gm->sections[i] = 0;
        if (gm->bitmap_section)
            grib_context_free(c, gm->bitmap_section);
        gm->bitmap_section = NULL;
        //grib_context_free(c, gm);
        gm = gm->next;
    }
}

static grib_multi_support* grib_multi_support_new(grib_context* c)
{
    // GRIB edition 2 has 9 sections ( 0 to 8 )
    const int GRIB2_END_SECTION = 8;
    int i = 0;
    grib_multi_support* gm =
        (grib_multi_support*)grib_context_malloc_clear(c, sizeof(grib_multi_support));
    gm->file                  = NULL;
    gm->message               = NULL;
    gm->message_length        = 0;
    gm->bitmap_section        = NULL;
    gm->bitmap_section_length = 0;
    gm->section_number        = 0;
    gm->next                  = 0;
    gm->sections_length[0]    = 16;

    for (i = 1; i < GRIB2_END_SECTION; i++)
        gm->sections_length[i] = 0;
    gm->sections_length[GRIB2_END_SECTION] = 4; // The 7777

    return gm;
}
