/**
 * hdr_interval_recorder.h
 * Written by Michael Barker and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

#include "hdr_atomic.h"
#include "hdr_interval_recorder.h"

int hdr_interval_recorder_init(struct hdr_interval_recorder* r)
{
    return hdr_writer_reader_phaser_init(&r->phaser);
}

void hdr_interval_recorder_destroy(struct hdr_interval_recorder* r)
{
    hdr_writer_reader_phaser_destory(&r->phaser);
}

void hdr_interval_recorder_update(
    struct hdr_interval_recorder* r, 
    void(*update_action)(void*, void*), 
    void* arg)
{
    int64_t val = hdr_phaser_writer_enter(&r->phaser);

    void* active = hdr_atomic_load_pointer(&r->active);

    update_action(active, arg);

    hdr_phaser_writer_exit(&r->phaser, val);
}

void* hdr_interval_recorder_sample(struct hdr_interval_recorder* r)
{
    void* temp;

    hdr_phaser_reader_lock(&r->phaser);

    temp = r->inactive;

    // volatile read
    r->inactive = hdr_atomic_load_pointer(&r->active);

    // volatile write
    hdr_atomic_store_pointer(&r->active, temp);

    hdr_phaser_flip_phase(&r->phaser, 0);

    hdr_phaser_reader_unlock(&r->phaser);

    return r->inactive;
}
