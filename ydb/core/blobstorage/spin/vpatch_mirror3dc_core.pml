/*
 * Mirror-3-DC VPatch completion model.
 *
 * MODE_111 patches one part in every DC. MODE_22 patches two parts in each of
 * two DCs. The current implementation waits for an erasure-specific placement
 * quorum. Define LEGACY_COUNT_QUORUM to reproduce the old result-count rule
 * that replied after TotalPartCount()==3 successful results in MODE_22.
 */

#define MODE_111 1
#define MODE_22 2
#define MAX_SLOTS 4

mtype = { VPATCH_START, FOUND, VPATCH_DIFF, PART_OK };

chan disk_in[MAX_SLOTS] = [2] of { mtype, byte };
chan proxy_in = [MAX_SLOTS] of { mtype, byte };

byte mode;
byte selected;
byte slot_dc[MAX_SLOTS];
byte written_by_dc[3];
byte acked_by_dc[3];
byte result_count;

bool request_started;
bool client_replied;
bool client_ok;

#define durable_111_written \
    (written_by_dc[0] >= 1 && written_by_dc[1] >= 1 && written_by_dc[2] >= 1)
#define durable_22_written \
    ((written_by_dc[0] >= 2 && written_by_dc[1] >= 2) || \
     (written_by_dc[0] >= 2 && written_by_dc[2] >= 2) || \
     (written_by_dc[1] >= 2 && written_by_dc[2] >= 2))
#define durable_written \
    ((mode == MODE_111 && durable_111_written) || \
     (mode == MODE_22 && durable_22_written))

#define durable_111_acked \
    (acked_by_dc[0] >= 1 && acked_by_dc[1] >= 1 && acked_by_dc[2] >= 1)
#define durable_22_acked \
    ((acked_by_dc[0] >= 2 && acked_by_dc[1] >= 2) || \
     (acked_by_dc[0] >= 2 && acked_by_dc[2] >= 2) || \
     (acked_by_dc[1] >= 2 && acked_by_dc[2] >= 2))
#define durable_acked \
    ((mode == MODE_111 && durable_111_acked) || \
     (mode == MODE_22 && durable_22_acked))

proctype VDisk(byte slot)
{
    byte payload;
    byte dc = slot_dc[slot];

    disk_in[slot]?VPATCH_START,payload;
    proxy_in!FOUND,slot;
    disk_in[slot]?VPATCH_DIFF,payload;

    atomic {
        written_by_dc[dc]++;
        assert(written_by_dc[dc] <= 2)
    };
    proxy_in!PART_OK,slot
}

proctype DSProxy()
{
    byte i = 0;
    byte sender;
    byte found_count = 0;
    bool done = false;

    request_started = true;

    do
    :: i < selected ->
        disk_in[i]!VPATCH_START,0;
        i++
    :: else ->
        break
    od;

    do
    :: found_count < selected ->
        proxy_in?FOUND,sender;
        assert(sender < selected);
        found_count++
    :: else ->
        break
    od;

    i = 0;
    do
    :: i < selected ->
        disk_in[i]!VPATCH_DIFF,0;
        i++
    :: else ->
        break
    od;

    do
    :: !done ->
        proxy_in?PART_OK,sender;
        assert(sender < selected);
        result_count++;
        acked_by_dc[slot_dc[sender]]++;

#ifdef LEGACY_COUNT_QUORUM
        /* Pre-fix regression: ReceivedResults == TotalPartCount(). */
        if
        :: result_count == 3 -> done = true
        :: else -> skip
        fi
#else
        if
        :: durable_acked -> done = true
        :: else -> skip
        fi
#endif
    :: else ->
        break
    od;

    client_ok = true;
    client_replied = true;
    assert(durable_written);

    /*
     * A reply to a terminated actor is dropped by the actor runtime. Drain
     * already selected late replies here so that Spin does not report them as
     * an invalid end state unrelated to the completion-quorum property.
     */
    do
    :: result_count < selected ->
        proxy_in?PART_OK,sender;
        result_count++
    :: else ->
        break
    od
}

init
{
    byte i = 0;

    atomic {
        if
        :: mode = MODE_111;
           selected = 3;
           slot_dc[0] = 0;
           slot_dc[1] = 1;
           slot_dc[2] = 2
        :: mode = MODE_22;
           selected = 4;
           slot_dc[0] = 0;
           slot_dc[1] = 0;
           slot_dc[2] = 1;
           slot_dc[3] = 1
        fi;

        run DSProxy();
        do
        :: i < selected ->
            run VDisk(i);
            i++
        :: else ->
            break
        od
    }
}

ltl safe_ok_requires_durable { [] (client_ok -> durable_written) }
ltl live_eventual_reply { [] (request_started -> <> client_replied) }
