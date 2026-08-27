/*
 * Mirror-3-DC VPatch completion model.
 *
 * MODE_111 patches one part in every DC. MODE_22 patches two parts in each of
 * two DCs. The current implementation waits for an erasure-specific placement
 * quorum. Define LEGACY_COUNT_QUORUM to reproduce the old result-count rule
 * that replied after TotalPartCount() == 3 successful results in MODE_22. The
 * network is reliable, placement selection is reduced to the two relevant
 * quorum shapes, and each local write is one transition. Payload, failures,
 * retries, and topology changes are outside the model; completion is checked
 * after placement selection.
 */

#define MODE_111 1
#define MODE_22 2
#define DC_COUNT 3
#define MAX_SLOTS 4
#define DISK_QUEUE_CAPACITY 2
#define MAX_WRITES_PER_DC 2
#define LEGACY_RESULT_QUORUM 3
#define NO_PAYLOAD 0

mtype = { VPATCH_START, FOUND, VPATCH_DIFF, PART_OK };

/* DSProxy writes each disk queue, and its indexed VDisk is the sole reader. */
chan disk_in[MAX_SLOTS] = [DISK_QUEUE_CAPACITY] of { mtype, byte };

/* Every VDisk writes proxy_in, and DSProxy is its sole reader. */
chan proxy_in = [MAX_SLOTS] of { mtype, byte };

/* init owns the selected placement and uses one representative 2+2 DC pair. */
byte mode;
byte selected;
byte slot_dc[MAX_SLOTS];

/* Multiple VDisks atomically publish the completed-write count for each DC. */
byte written_by_dc[DC_COUNT];

/* DSProxy alone owns acknowledgement and result accounting. */
byte acked_by_dc[DC_COUNT];
byte result_count;

/* DSProxy owns the client-visible request outcome. */
bool request_started;
bool client_replied;
bool client_ok;

#define DURABLE_111_WRITTEN \
    (written_by_dc[0] >= 1 && written_by_dc[1] >= 1 && written_by_dc[2] >= 1)
#define DURABLE_22_WRITTEN \
    ((written_by_dc[0] >= 2 && written_by_dc[1] >= 2) || \
     (written_by_dc[0] >= 2 && written_by_dc[2] >= 2) || \
     (written_by_dc[1] >= 2 && written_by_dc[2] >= 2))
#define DURABLE_WRITTEN \
    ((mode == MODE_111 && DURABLE_111_WRITTEN) || \
     (mode == MODE_22 && DURABLE_22_WRITTEN))

#define DURABLE_111_ACKED \
    (acked_by_dc[0] >= 1 && acked_by_dc[1] >= 1 && acked_by_dc[2] >= 1)
#define DURABLE_22_ACKED \
    ((acked_by_dc[0] >= 2 && acked_by_dc[1] >= 2) || \
     (acked_by_dc[0] >= 2 && acked_by_dc[2] >= 2) || \
     (acked_by_dc[1] >= 2 && acked_by_dc[2] >= 2))
#define DURABLE_ACKED \
    ((mode == MODE_111 && DURABLE_111_ACKED) || \
     (mode == MODE_22 && DURABLE_22_ACKED))

proctype VDisk(byte slot) {
    byte payload;
    byte dc = slot_dc[slot];

    disk_in[slot]?VPATCH_START,payload;
    proxy_in!FOUND,slot;
    disk_in[slot]?VPATCH_DIFF,payload;

    /* Publish the VDisk write and its shared exact counter as one transition. */
    atomic {
        written_by_dc[dc]++;
        assert(written_by_dc[dc] <= MAX_WRITES_PER_DC)
    };
    proxy_in!PART_OK,slot
}

proctype DSProxy() {
    byte i = 0;
    byte sender;
    byte found_count = 0;
    bool done = false;

    request_started = true;

    do
    :: i < selected ->
        disk_in[i]!VPATCH_START,NO_PAYLOAD;
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
        disk_in[i]!VPATCH_DIFF,NO_PAYLOAD;
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
        :: result_count == LEGACY_RESULT_QUORUM ->
            done = true
        :: else ->
            skip
        fi
#else
        if
        :: DURABLE_ACKED ->
            done = true
        :: else ->
            skip
        fi
#endif
    :: else ->
        break
    od;

    client_ok = true;
    client_replied = true;
    assert(DURABLE_WRITTEN);

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

ltl safe_ok_requires_durable { [] (client_ok -> DURABLE_WRITTEN) }

/* Verified with weak process fairness; see the run manifest in README.md. */
ltl live_eventual_reply { [] (request_started -> <> client_replied) }

init {
    byte i = 0;

    /* Select one placement and publish the complete actor set without interleaving. */
    atomic {
        if
        :: mode = MODE_111;
           selected = DC_COUNT;
           slot_dc[0] = 0;
           slot_dc[1] = 1;
           slot_dc[2] = 2
        :: mode = MODE_22;
           selected = MAX_SLOTS;
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
