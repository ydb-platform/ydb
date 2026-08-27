/*
 * Reliable-network baseline for block-erasure VPatch.
 *
 * DATA_PARTS and PARITY_PARTS are supplied by a wrapper model. One logical
 * Patch request has one actor per part. Blob bytes are abstracted away;
 * patched[id] means that the local VPut completed in one transition. The model
 * preserves the data-to-parity XOR fan-out and permits DIFF/XOR delivery in
 * either order at parity actors. It excludes disconnects, retries, timeouts,
 * fallback, and queue accounting.
 */

#define TOTAL_PARTS (DATA_PARTS + PARITY_PARTS)
#define CHANNEL_CAPACITY (TOTAL_PARTS + DATA_PARTS)
#define NO_PAYLOAD 0

mtype = { VPATCH_START, FOUND, VPATCH_DIFF, XOR_DIFF, PART_OK };

/* DSProxy and data VDisks write disk_in; each element has one VDisk reader. */
chan disk_in[TOTAL_PARTS] = [CHANNEL_CAPACITY] of { mtype, byte };

/* Every VDisk writes proxy_in; DSProxy is its sole reader. */
chan proxy_in = [CHANNEL_CAPACITY] of { mtype, byte };

/* DSProxy owns the client-visible request lifecycle. */
bool request_started;
bool client_replied;
bool client_ok;

/* Each VDisk owns patched[id] and atomically updates the shared exact count. */
byte patched_count;
byte patched[TOTAL_PARTS];

#define ALL_PARTS_PATCHED (patched_count == TOTAL_PARTS)

proctype VDisk(byte id) {
    byte payload;
    byte parity;
    byte xor_count = 0;
    bool got_diff = false;

    disk_in[id]?VPATCH_START,payload;
    proxy_in!FOUND,id;

    if
    :: id < DATA_PARTS ->
        disk_in[id]?VPATCH_DIFF,payload;

        /* One XOR delta from this data part to every parity part. */
        parity = DATA_PARTS;
        do
        :: parity < TOTAL_PARTS ->
            disk_in[parity]!XOR_DIFF,id;
            parity++
        :: else ->
            break
        od;

        atomic {
            assert(patched[id] == 0);
            patched[id] = 1;
            patched_count++
        };
        proxy_in!PART_OK,id

    :: id >= DATA_PARTS ->
        /* XOR may arrive before or after DSProxy's part diff. */
        do
        :: xor_count < DATA_PARTS ->
            if
            :: disk_in[id]?VPATCH_DIFF,payload ->
                assert(!got_diff);
                got_diff = true
            :: disk_in[id]?XOR_DIFF,payload ->
                assert(payload < DATA_PARTS);
                xor_count++
            fi
        :: else ->
            break
        od;

        if
        :: !got_diff ->
            disk_in[id]?VPATCH_DIFF,payload;
            got_diff = true
        :: else ->
            skip
        fi;

        assert(got_diff);
        assert(xor_count == DATA_PARTS);
        atomic {
            assert(patched[id] == 0);
            patched[id] = 1;
            patched_count++
        };
        proxy_in!PART_OK,id
    fi
}

proctype DSProxy() {
    byte i = 0;
    byte sender;
    byte found_count = 0;
    byte result_count = 0;

    request_started = true;

    do
    :: i < TOTAL_PARTS ->
        disk_in[i]!VPATCH_START,NO_PAYLOAD;
        i++
    :: else ->
        break
    od;

    do
    :: found_count < TOTAL_PARTS ->
        proxy_in?FOUND,sender;
        assert(sender < TOTAL_PARTS);
        found_count++
    :: else ->
        break
    od;

    i = 0;
    do
    :: i < TOTAL_PARTS ->
        disk_in[i]!VPATCH_DIFF,NO_PAYLOAD;
        i++
    :: else ->
        break
    od;

    do
    :: result_count < TOTAL_PARTS ->
        proxy_in?PART_OK,sender;
        assert(sender < TOTAL_PARTS);
        result_count++
    :: else ->
        break
    od;

    client_ok = true;
    client_replied = true;
    assert(ALL_PARTS_PATCHED)
}

ltl safe_ok_requires_all_parts { [] (client_ok -> ALL_PARTS_PATCHED) }

/* Verified with weak process fairness; see the run manifest in README.md. */
ltl live_eventual_reply { [] (request_started -> <> client_replied) }

init {
    byte i = 0;

    atomic {
        run DSProxy();
        do
        :: i < TOTAL_PARTS ->
            run VDisk(i);
            i++
        :: else ->
            break
        od
    }
}
