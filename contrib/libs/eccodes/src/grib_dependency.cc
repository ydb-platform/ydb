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
 *   Jean Baptiste Filippi - 01.11.2005
 ***************************************************************************/
#include "grib_api_internal.h"

grib_handle* grib_handle_of_accessor(const grib_accessor* a)
{
    if (a->parent_ == NULL) {
        return a->h_;
    }
    else {
        return a->parent_->h;
    }
}

static grib_handle* handle_of(grib_accessor* observed)
{
    grib_handle* h = NULL;
    DEBUG_ASSERT(observed);
    /* printf("+++++ %s->parent = %p\n",observed->name,observed->parent); */
    /* printf("+++++ %s = %p\n",observed->name,observed); */
    /* printf("+++++       h=%p\n",observed->h); */
    /* special case for BUFR attributes parentless */
    if (observed->parent_ == NULL) {
        return observed->h_;
    }
    h = observed->parent_->h;
    while (h->main)
        h = h->main;
    return h;
}

void grib_dependency_add(grib_accessor* observer, grib_accessor* observed)
{
    grib_handle* h        = NULL;
    grib_dependency* d    = NULL;
    grib_dependency* last = NULL;

    /*printf("grib_dependency_add: observe %p %p observed=%s observer=%s\n",
           (void*)observed, (void*)observer,
           observed ? observed->name : "NULL",
           observer ? observer->name : "NULL");*/

    if (!observer || !observed) {
        return;
    }
    h = handle_of(observed);
    d = h->dependencies;

    /* ECCODES_ASSERT(h == handle_of(observer)); */

    /* Check if already in list */
    while (d) {
        if (d->observer == observer && d->observed == observed)
            return;
        last = d;
        d    = d->next;
    }

//     d = h->dependencies;
//     while(d)
//     {
//         last = d;
//         d = d->next;
//     }

    d = (grib_dependency*)grib_context_malloc_clear(h->context, sizeof(grib_dependency));
    ECCODES_ASSERT(d);

    d->observed = observed;
    d->observer = observer;
    d->next     = 0;

    //printf("observe %p %p %s %s\n",(void*)observed,(void*)observer, observed->name,observer->name);
//     d->next     = h->dependencies;
//     h->dependencies = d;

    if (last)
        last->next = d;
    else
        h->dependencies = d;
}

void grib_dependency_remove_observed(grib_accessor* observed)
{
    grib_handle* h     = handle_of(observed);
    grib_dependency* d = h->dependencies;
    /* printf("%s\n",observed->name); */

    while (d) {
        if (d->observed == observed) {
            /*  TODO: Notify observer...*/
            d->observed = 0; /*printf("grib_dependency_remove_observed %s\n",observed->name); */
        }
        d = d->next;
    }
}

/* TODO: Notification must go from outer blocks to inner block */

int grib_dependency_notify_change(grib_accessor* observed)
{
    grib_handle* h     = handle_of(observed);
    grib_dependency* d = h->dependencies;
    int ret            = GRIB_SUCCESS;

    /*Do a two pass mark&sweep, in case some dependencies are added while we notify*/
    while (d) {
        d->run = (d->observed == observed && d->observer != 0);
        d      = d->next;
    }

    d = h->dependencies;
    while (d) {
        if (d->run) {
            /*printf("grib_dependency_notify_change %s %s %p\n", observed->name, d->observer ? d->observer->name : "?", (void*)d->observer);*/
            if (d->observer && (ret = d->observer->notify_change(observed)) != GRIB_SUCCESS)
                return ret;
        }
        d = d->next;
    }
    return ret;
}

/* This version takes in the handle so does not need to work it out from the 'observed' */
/* See ECC-778 */
int grib_dependency_notify_change_h(grib_handle* h, grib_accessor* observed)
{
    grib_dependency* d = h->dependencies;
    int ret            = GRIB_SUCCESS;

    /*Do a two pass mark&sweep, in case some dependencies are added while we notify*/
    while (d) {
        d->run = (d->observed == observed && d->observer != 0);
        d      = d->next;
    }

    d = h->dependencies;
    while (d) {
        if (d->run) {
            /*printf("grib_dependency_notify_change %s %s %p\n",observed->name,d->observer ? d->observer->name : "?", (void*)d->observer);*/
            if (d->observer && (ret = d->observer->notify_change(observed)) != GRIB_SUCCESS)
                return ret;
        }
        d = d->next;
    }
    return ret;
}

void grib_dependency_remove_observer(grib_accessor* observer)
{
    grib_handle* h     = NULL;
    grib_dependency* d = NULL;

    if (!observer)
        return;

    h = handle_of(observer);
    d = h->dependencies;

    while (d) {
        if (d->observer == observer) {
            d->observer = 0;
        }
        d = d->next;
    }
}

void grib_dependency_observe_expression(grib_accessor* observer, grib_expression* e)
{
    e->add_dependency(observer);
}

void grib_dependency_observe_arguments(grib_accessor* observer, grib_arguments* a)
{
    while (a) {
        grib_dependency_observe_expression(observer, a->expression_);
        a = a->next_;
    }
}
