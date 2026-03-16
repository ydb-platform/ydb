/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "action_class_section.h"

namespace eccodes::action
{
// static void check_sections(grib_section *s,grib_handle* h)
// {
//     grib_accessor *a = s?s->block->first:NULL;
//     if(s) ECCODES_ASSERT(s->h == h);
//     while(a)
//     {
//       ECCODES_ASSERT(grib_handle_of_accessor(a) == h);
//       check_sections(a->sub_section_,h);
//       a = a->next;
//     }
// }

int Section::notify_change(grib_accessor* notified,
                           grib_accessor* changed)
{
    grib_loader loader = { 0, 0, 0, 0, 0 };

    grib_section* old_section = NULL;
    grib_handle* h            = grib_handle_of_accessor(notified);
    size_t len                = 0;
    size_t size               = 0;
    int err                   = 0;
    grib_handle* tmp_handle;
    int doit = 0;

    grib_action* la = NULL;

    if (h->context->debug > 0) {
        char debug_str[1024] = {0, };
        if (debug_info_) {
            snprintf(debug_str, 1024, " (%s)", debug_info_);
        }
        grib_context_log(h->context,
                         GRIB_LOG_DEBUG, "------------- SECTION action %s (%s) is triggered by [%s]%s",
                         name_, notified->name_, changed->name_, debug_str);
    }

    la          = reparse(notified, &doit);
    old_section = notified->sub_section_;
    if (!old_section) return GRIB_INTERNAL_ERROR;

    ECCODES_ASSERT(old_section->h == h);

    /* printf("old = %p\n",(void*)old_section->branch); */
    /* printf("new = %p\n",(void*)la); */

    grib_context_log(h->context,
                     GRIB_LOG_DEBUG, "------------- DOIT %ld OLD %p NEW %p",
                     doit, old_section->branch, la);


    if (!doit) {
        if (la != NULL || old_section->branch != NULL)
            if (la == old_section->branch) {
                grib_context_log(h->context, GRIB_LOG_DEBUG, "IGNORING TRIGGER action %s (%s) is triggered %p", name_, notified->name_, (void*)la);
                return GRIB_SUCCESS;
            }
    }

    loader.list_is_resized = (la == old_section->branch);

    if (!strcmp(changed->name_, "GRIBEditionNumber"))
        loader.changing_edition = 1;
    else
        loader.changing_edition = 0;

    old_section->branch = la;

    tmp_handle = grib_new_handle(h->context);
    if (!tmp_handle)
        return GRIB_OUT_OF_MEMORY;

    tmp_handle->buffer = grib_create_growable_buffer(h->context);
    ECCODES_ASSERT(tmp_handle->buffer); /* FIXME */

    loader.data          = h;
    loader.lookup_long   = grib_lookup_long_from_handle;
    loader.init_accessor = grib_init_accessor_from_handle;

    if (h->kid != NULL) {
        /* grib_handle_delete(tmp_handle); */
        return GRIB_INTERNAL_ERROR;
    }

    ECCODES_ASSERT(h->kid == NULL);
    tmp_handle->loader = &loader;
    tmp_handle->main   = h;
    h->kid             = tmp_handle;
    /* printf("tmp_handle- main %p %p\n",(void*)tmp_handle,(void*)h); */

    grib_context_log(h->context, GRIB_LOG_DEBUG, "------------- CREATE TMP BLOCK act=%s notified=%s", name_, notified->name_);
    tmp_handle->root = grib_section_create(tmp_handle, NULL);

    tmp_handle->use_trie = 1;

    err = create_accessor(tmp_handle->root, &loader);
    if (err) {
        if (err == GRIB_NOT_FOUND && strcmp(name_, "dataValues") == 0) {
            /* FIXME: Allow this error. Needed when changing some packingTypes e.g. CCSDS to Simple */
            /*err = GRIB_SUCCESS;*/
        }
        else {
            grib_handle_delete(tmp_handle);
            h->kid = NULL; /* ECC-1314: must set to NULL for grib_handle_delete(h) to work */
            return err;
        }
    }

    err = grib_section_adjust_sizes(tmp_handle->root, 1, 0);
    if (err) {
        /* grib_handle_delete(tmp_handle); h->kid = NULL; */
        return err;
    }
    grib_section_post_init(tmp_handle->root);

    /* grib_recompute_sections_lengths(tmp_handle->root); */
    grib_get_block_length(tmp_handle->root, &len);
    grib_context_log(h->context, GRIB_LOG_DEBUG, "-------------  TMP BLOCK IS sectlen=%d buffer=%d", len, tmp_handle->buffer->ulength);

    // if(h->context->debug > 10)
    //     grib_dump_content(tmp_handle,stdout,NULL,0,NULL);

    /* ECCODES_ASSERT(tmp_handle->buffer->ulength == len); */
    /* grib_empty_section(h->context,old_section); */

    grib_buffer_replace(notified, tmp_handle->buffer->data, tmp_handle->buffer->ulength, 0, 1);

    ECCODES_ASSERT(tmp_handle->root->block->first != NULL);
    grib_swap_sections(old_section,
                       tmp_handle->root->block->first->sub_section_);

    ECCODES_ASSERT(tmp_handle->dependencies == NULL);
    /* printf("grib_handle_delete %p\n",(void*)tmp_handle); */

    grib_handle_delete(tmp_handle);

    h->use_trie     = 1;
    h->trie_invalid = 1;
    h->kid          = NULL;

    err = grib_section_adjust_sizes(h->root, 1, 0);
    if (err)
        return err;

    grib_section_post_init(h->root);

    grib_get_block_length(old_section, &size);

    grib_context_log(h->context, GRIB_LOG_DEBUG, "-------------   BLOCK SIZE %ld, buffer len=%ld", size, len);
    if (h->context->debug > 10)
        grib_dump_content(h, stdout, "debug", ~0, NULL);

    ECCODES_ASSERT(size == len);

    grib_update_paddings(old_section);

    return GRIB_SUCCESS;
}

}  // namespace eccodes::action
