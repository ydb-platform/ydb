/*
 * Reliable-network baseline for block-erasure VPatch.
 *
 * DATA_PARTS and PARITY_PARTS are supplied by a wrapper model. Each part has
 * one actor. Blob bytes are abstracted away; patched[id] means the local VPut
 * completed successfully. The model preserves the data->parity XOR fan-out
 * and permits DIFF/XOR delivery in either order at parity actors.
 */

#define TOTAL_PARTS (DATA_PARTS + PARITY_PARTS)
#define CHANNEL_CAPACITY (TOTAL_PARTS + DATA_PARTS)

mtype = { VPATCH_START, FOUND, VPATCH_DIFF, XOR_DIFF, PART_OK };

chan disk_in[TOTAL_PARTS] = [CHANNEL_CAPACITY] of { mtype, byte };
chan proxy_in = [CHANNEL_CAPACITY] of { mtype, byte };

bool request_started;
bool client_replied;
bool client_ok;
byte patched_count;
byte patched[TOTAL_PARTS];

#define all_parts_patched (patched_count == TOTAL_PARTS)

proctype VDisk(byte id)
{
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

proctype DSProxy()
{
    byte i = 0;
    byte sender;
    byte found_count = 0;
    byte result_count = 0;

    request_started = true;

    do
    :: i < TOTAL_PARTS ->
        disk_in[i]!VPATCH_START,0;
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
        disk_in[i]!VPATCH_DIFF,0;
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
    assert(all_parts_patched)
}

init
{
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

ltl safe_ok_requires_all_parts { [] (client_ok -> all_parts_patched) }
ltl live_eventual_reply { [] (request_started -> <> client_replied) }
