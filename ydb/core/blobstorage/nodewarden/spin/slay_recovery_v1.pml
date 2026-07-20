/*
 * Bounded model of WIPE/DESTROY recovery for one stable VSlotId.
 * Version v1 models action-aware SlayInFlight recovery from this branch.
 *
 * The checked-in v0 and v1 files share BSC/PDisk/environment code and differ
 * only in NodeWarden: v0 stores a round; v1 stores {VDiskId, Action, Round},
 * replays after restart, and schedules confirmation retries.
 *
 * BSC, PDisk and the fault environment are identical in both versions.
 * A VDisk generation changes from 1 to 2, while its short identity and
 * VSlotId remain constant.  BSC and PDisk therefore intentionally do not use
 * generation equality as slot identity equality.
 *
 * PDisk follows the production path:
 *
 *   actor mailbox -> TPDisk::InputQueue -> executing request
 *                 -> completion queue -> NodeWarden mailbox
 *
 * Restart uses the same actor mailbox as TEvSlay.  Stop() destroys every
 * incarnation-owned stage, but cannot roll back a committed owner removal or
 * retract a result already delivered to NodeWarden's mailbox.
 *
 * VDisk restart is deliberately asynchronous.  NodeWarden first poisons the
 * old actor; a later actor incarnation allocates InitOwnerRound and sends
 * TEvYardInit through the same PDisk actor mailbox as TEvSlay.  Poison cannot
 * retract a YardInit which the VDisk actor has already sent.
 *
 * The bounded scope admits one outstanding YardInit.  PDisk Stop drains it
 * together with the other incarnation-owned stages.  ENABLE_NW_RESTART is a
 * logical NodeWarden actor/session restart with a preserved monotonic owner-
 * round allocator; a full host restart (which also recreates PDisk) is a
 * separate fault profile and is not claimed here.
 */

#ifndef PDISK_MAILBOX_CAP
#define PDISK_MAILBOX_CAP 6
#endif
#ifndef PDISK_INPUT_CAP
#define PDISK_INPUT_CAP 1
#endif
#ifndef PDISK_COMPLETION_CAP
#define PDISK_COMPLETION_CAP 1
#endif
#ifndef NW_MAILBOX_CAP
#define NW_MAILBOX_CAP 12
#endif
#ifndef BSC_MAILBOX_CAP
#define BSC_MAILBOX_CAP 8
#endif
#ifndef RAW_SLAY_TIMER_CAP
#define RAW_SLAY_TIMER_CAP 3
#endif
#ifndef ENABLE_CONFIRMATION_RETRY
/* Set to 0 only for the exhaustive core-recovery profile.  The faithful
 * production retry profile keeps this enabled and is checked separately. */
#define ENABLE_CONFIRMATION_RETRY 1
#endif
#ifndef ENABLE_BSC_RESTART
#define ENABLE_BSC_RESTART 0
#endif
#ifndef ENABLE_NW_RESTART
#define ENABLE_NW_RESTART 0
#endif

/* Profile controls split the Cartesian product into small exhaustive runs.
 * Timing stays unrestricted inside every run.
 *   OPERATION_MODE: 0=both, 1=direct DESTROY, 2=WIPE then DESTROY
 *   PDISK_RESTART_MODE: 0=none/BSC/self, 1=none, 2=BSC, 3=self */
#ifndef OPERATION_MODE
#define OPERATION_MODE 0
#endif
#ifndef PDISK_RESTART_MODE
#define PDISK_RESTART_MODE 0
#endif

mtype = {
    /* BSC and NodeWarden protocol. */
    START_WIPE,
    START_DESTROY,
    SERVICE_SET_WIPE,
    SERVICE_SET_DESTROY,
    WIPED,
    DESTROYED,
    REGISTER_NODE,
    BSC_RECONNECTED,

    /* PDisk restart protocol. */
    REQUEST_PDISK_RESTART,
    ASK_WARDEN_RESTART,
    ASK_BSC_RESTART,
    PDISK_RESTARTED,

    /* VDisk/PDisk protocol. */
    POISON,
    YARD_INIT,
    YARD_INIT_OK,
    SLAY,
    SLAY_OK,
    SLAY_ALREADY,
    SLAY_NOTREADY,
    SLAY_RACE,
    RETRY_SLAY,

    /* Persistent BSC intent and volatile NodeWarden action. */
    MOOD_NORMAL,
    MOOD_WIPE,
    MOOD_DELETE,
    ACTION_NONE,
    ACTION_WIPE,
    ACTION_DESTROY,

    /* Control-plane actor lifecycle. */
    ACTOR_DOWN,
    ACTOR_STARTING,
    ACTOR_UP,

    /* Volatile VDisk actor lifecycle. */
    VDISK_RUNNING,
    VDISK_STARTING,
    VDISK_STOPPED,

    /* PDisk lifecycle and worker state. */
    PDISK_RUNNING,
    PDISK_RESTARTING,
    PDISK_STARTING,
    RESTART_STOPPING,
    RESTART_DRAINING,
    RESTART_CREATING,
    EXEC_EMPTY,
    EXEC_ACCEPTED,
    EXEC_RESULT_READY,
    RESULT_NONE,
    RESULT_OK,
    RESULT_ALREADY,
    RESULT_NOTREADY,
    RESULT_RACE
};

/* Actor mailboxes.  Different producers race when enqueuing, then the actor
 * observes the resulting FIFO order.  The byte fields are message-specific. */
chan bsc_mailbox = [BSC_MAILBOX_CAP] of { mtype, byte, byte, byte };
chan nw_mailbox = [NW_MAILBOX_CAP] of { mtype, byte, byte, byte, byte };
chan vdisk_mailbox = [4] of { mtype, byte };
/* NodeWarden sends Slay/restart and a VDisk actor sends YardInit.  Enqueue
 * ordering between producers is nondeterministic, then this mailbox is FIFO. */
chan pdisk_actor_mailbox = [PDISK_MAILBOX_CAP] of {
    mtype, byte, byte, byte, byte
};

/* These two queues and the executing request belong to a PDisk incarnation
 * and are synchronously discarded by Stop(). */
chan pdisk_input_queue = [PDISK_INPUT_CAP] of {
    mtype, byte, byte, byte, byte
};
chan pdisk_completion_queue = [PDISK_COMPLETION_CAP] of {
    mtype, byte, byte, byte, byte
};


/* OwnerRound is an unbounded monotonic ui64 in production.  The model uses
 * nominal tokens instead: a token may be reused only after every current
 * record, message, pipeline stage, and timer released its reference.  The
 * extra token guarantees that allocation can be fresh even when every holder
 * contains a different live round. */
#define ROUND_POOL (PDISK_MAILBOX_CAP + PDISK_INPUT_CAP + \
    PDISK_COMPLETION_CAP + NW_MAILBOX_CAP + RAW_SLAY_TIMER_CAP + 5)
#define NO_ROUND 0
byte round_refs[ROUND_POOL + 1];
bool round_after_owner[ROUND_POOL + 1];
/* Exact ordering cut for one allocated but not yet committed InitOwnerRound.
 * Existing Slay tokens are older than the cut; subsequently allocated tokens
 * are newer.  Commit copies this relation to the current PDisk owner. */
bool round_after_pending_init[ROUND_POOL + 1];

/* BSC persistent state. */
mtype bsc_mood = MOOD_NORMAL;
byte bsc_generation = 1;
byte bsc_epoch = 1;
byte bsc_registered_nw_epoch = 1;
bool bsc_wipe_pending = false;
bool bsc_delete_pending = false;
bool bsc_vslot_allocated = true;
bool bsc_wipe_completed = false;
bool bsc_reconnect_pending = false;
bool bsc_processing_event = false;
mtype bsc_actor_state = ACTOR_UP;
bool nw_register_pending = false;
byte nw_register_source_epoch = 0;
byte nw_register_target_bsc_epoch = 0;

/* NodeWarden volatile state for the one stable TVSlotId. */
byte nw_epoch = 1;
byte nw_known_bsc_epoch = 1;
bool nw_processing_event = false;
mtype nw_actor_state = ACTOR_UP;
bool nw_local_vdisk = true;
byte nw_current_generation = 1;
bool nw_slay_inflight = false;
byte nw_slay_generation = 0;
mtype nw_slay_action = ACTION_NONE;
byte nw_round = NO_ROUND;
byte nw_last_issue_pdisk_epoch = 0;
bool nw_pdisk_restart_inflight = false;

/* LocalVDisks membership is separate from the actor lifecycle. */
mtype vdisk_state = VDISK_RUNNING;
byte vdisk_epoch = 1;
bool vdisk_poison_queued = false;
bool vdisk_start_requested = false;
bool vdisk_init_result_pending = false;
byte vdisk_init_result_epoch = 0;

/* One post-WIPE VDisk incarnation and one allocated InitOwnerRound are enough
 * for the core WIPE->DESTROY counterexample.  Replacement starts serialize
 * behind this cut; profiles requiring two concurrent YardInit requests need a
 * second cut and are intentionally outside this bounded owner-round model. */
bool init_cut_active = false;
bool init_sent = false;
byte init_generation = 0;
byte init_vdisk_epoch = 0;
byte init_target_pdisk_epoch = 0;

/* IssueSlay and the NOTREADY handler schedule distinct production timers.
 * Keeping separate slots preserves their multiplicity.  A slot is reclaimed
 * only after its event becomes stale, never by merging live events. */
bool issue_timer_armed = false;
byte issue_timer_round = NO_ROUND;
byte issue_timer_nw_epoch = 0;
byte issue_timer_generation = 0;
bool notready_timer_armed = false;
byte notready_timer_round = NO_ROUND;
byte notready_timer_nw_epoch = 0;
byte notready_timer_generation = 0;

/* Ghost evidence used by the restart-recovery assertion.  It is true only
 * when the result for NodeWarden's current round has left PDisk and is already
 * in the current NodeWarden incarnation's mailbox. */
bool nw_current_result_available = false;
bool nw_current_attempt_outstanding = false;
#ifdef REQUIRE_INPUT_QUEUE_LOSS
/* Trace-selection ghost only: it never enables a component transition. */
bool trace_input_queue_loss = false;
#endif

/* PDisk lifecycle and persistent physical owner. */
mtype pdisk_state = PDISK_RUNNING;
mtype pdisk_restart_phase = RESTART_STOPPING;
byte pdisk_epoch = 1;
byte pdisk_restart_reply_nw_epoch = 0;
bool pdisk_owner_present = true;
bool pdisk_callback_pending = false;
byte pdisk_callback_epoch = 0;
byte pdisk_callback_target_nw_epoch = 0;

/* YardInitStart removes the request from the ordinary execution slot, then
 * PendingYardInits may commit it asynchronously. */
bool pdisk_yard_init_pending = false;
byte pdisk_yard_init_generation = 0;
byte pdisk_yard_init_vdisk_epoch = 0;
byte pdisk_yard_init_epoch = 0;

/* At most one request executes for the modeled VSlot. */
mtype pdisk_exec_phase = EXEC_EMPTY;
mtype pdisk_exec_kind = SLAY;
byte pdisk_exec_round = 0;
byte pdisk_exec_generation = 0;
byte pdisk_exec_epoch = 0;
byte pdisk_exec_nw_epoch = 0;
mtype pdisk_exec_result = RESULT_NONE;

inline retain_round(round) {
    assert(round > NO_ROUND && round <= ROUND_POOL);
    assert(round_refs[round] < 255);
    round_refs[round]++
}

inline release_round(round) {
    assert(round > NO_ROUND && round <= ROUND_POOL);
    assert(round_refs[round] > 0);
    round_refs[round]--;
    if
    :: round_refs[round] == 0 ->
        /* Canonicalize an unused token before alpha-renaming it. */
        round_after_owner[round] = false;
        round_after_pending_init[round] = false
    :: else -> skip
    fi
}

inline allocate_fresh_round(round) {
    round = 1;
    do
    :: round > ROUND_POOL ->
        /* ROUND_POOL is holder-count + one; reaching this means a missing
         * retain/release or an unaccounted token-bearing holder. */
        assert(false);
        break
    :: else ->
        if
        :: round_refs[round] == 0 -> break
        :: else -> round++
        fi
    od;
    assert(round > NO_ROUND && round <= ROUND_POOL);
    assert(round_refs[round] == 0);
    round_refs[round] = 1; /* SlayInFlight.Round owns the first reference. */
    round_after_owner[round] = true;
    /* A Slay allocated while InitOwnerRound is outstanding is strictly newer
     * than that init.  Otherwise this bit is canonical but semantically idle. */
    round_after_pending_init[round] = init_cut_active
}

inline allocate_init_cut(index) {
    assert(!init_cut_active);
    init_cut_active = true;
    index = 1;
    do
    :: index <= ROUND_POOL ->
        if
        :: round_refs[index] > 0 ->
            /* Every already allocated Slay is older than this InitOwnerRound. */
            round_after_pending_init[index] = false
        :: round_refs[index] == 0 ->
            round_after_pending_init[index] = false
        fi;
        index++
    :: index > ROUND_POOL -> break
    od
}

inline commit_init_cut(index) {
    assert(init_cut_active);
    index = 1;
    do
    :: index <= ROUND_POOL ->
        if
        :: round_refs[index] > 0 ->
            /* YardInit publishes its allocated OwnerRound. */
            round_after_owner[index] = round_after_pending_init[index]
        :: round_refs[index] == 0 ->
            round_after_owner[index] = false
        fi;
        round_after_pending_init[index] = false;
        index++
    :: index > ROUND_POOL -> break
    od;
    init_cut_active = false;
    init_sent = false;
    init_generation = 0;
    init_vdisk_epoch = 0;
    init_target_pdisk_epoch = 0
}

inline drop_init_cut(index) {
    assert(init_cut_active);
    index = 1;
    do
    :: index <= ROUND_POOL ->
        round_after_pending_init[index] = false;
        index++
    :: index > ROUND_POOL -> break
    od;
    init_cut_active = false;
    init_sent = false;
    init_generation = 0;
    init_vdisk_epoch = 0;
    init_target_pdisk_epoch = 0
}

/* A restarted TPDisk restores the VDisk identity from syslog but not its
 * volatile TOwnerData::OwnerRound (which starts at zero).  Every Slay round
 * still held by NodeWarden/transport is therefore newer than a restored
 * physical owner. */
inline reset_owner_relation_after_pdisk_restart(index) {
    /* Stop drains every YardInit already sent to the old PDisk.  A VDisk may,
     * however, have allocated InitOwnerRound without sending YardInit yet;
     * that VDisk-owned cut survives and may target the new incarnation. */
    assert(!init_cut_active || !init_sent);
    index = 1;
    do
    :: index <= ROUND_POOL ->
        if
        :: round_refs[index] > 0 -> round_after_owner[index] = true
        :: round_refs[index] == 0 -> round_after_owner[index] = false
        fi;
        if
        :: !init_cut_active -> round_after_pending_init[index] = false
        :: init_cut_active -> skip
        fi;
        index++
    :: index > ROUND_POOL -> break
    od
}

inline discard_stale_fixed_timers() {
    if
    :: issue_timer_armed ->
        assert(issue_timer_round != nw_round ||
            issue_timer_nw_epoch != nw_epoch);
        release_round(issue_timer_round);
        issue_timer_armed = false;
        issue_timer_round = NO_ROUND
    :: else -> skip
    fi;
    if
    :: notready_timer_armed ->
        assert(notready_timer_round != nw_round ||
            notready_timer_nw_epoch != nw_epoch);
        release_round(notready_timer_round);
        notready_timer_armed = false;
        notready_timer_round = NO_ROUND
    :: else -> skip
    fi
}

inline arm_issue_timer() {
    assert(!issue_timer_armed);
    retain_round(nw_round);
    issue_timer_armed = true;
    issue_timer_round = nw_round;
    issue_timer_nw_epoch = nw_epoch;
    issue_timer_generation = nw_slay_generation
}

/* The caller transfers the reference owned by SLAY_NOTREADY into this slot. */
inline arm_notready_timer_from_result(round, generation, target_epoch) {
    if
    :: notready_timer_armed ->
        /* A unique IssueSlay produces one Slay and one NOTREADY response. */
        assert(notready_timer_round != round ||
            notready_timer_nw_epoch != target_epoch);
        release_round(notready_timer_round)
    :: else -> skip
    fi;
    notready_timer_armed = true;
    notready_timer_round = round;
    notready_timer_nw_epoch = target_epoch;
    notready_timer_generation = generation
}

inline send_bsc_service_set(kind, generation, target_nw) {
    /* Do not let a ghost queue-membership bit suppress a real controller
     * replay.  The bounded transport applies backpressure when it is full. */
    if
    :: target_nw == 0 ->
        printf("[BSC] defer ServiceSet until RegisterNode\n")
    :: target_nw != 0 ->
        nfull(nw_mailbox);
        nw_mailbox!kind(generation, target_nw, bsc_epoch, 0)
    fi
}

inline send_slay(generation) {
    byte old_round;
    byte fresh_round;
    byte target_pdisk_epoch;
    bool issued_while_restarting;

    /* OwnerRound allocation is part of IssueSlay and must not depend on
     * bounded transport capacity: in production Send() itself is nonblocking.
     * Snapshot the target incarnation before a full abstract mailbox can make
     * this handler wait. */
    target_pdisk_epoch = pdisk_epoch;
    issued_while_restarting = pdisk_state == PDISK_RESTARTING;
    old_round = nw_round;
    allocate_fresh_round(fresh_round);
    nw_round = fresh_round;
    nw_current_result_available = false;
    nw_current_attempt_outstanding = false;
    if
    :: old_round != NO_ROUND -> release_round(old_round)
    :: else -> skip
    fi;

    nw_last_issue_pdisk_epoch = target_pdisk_epoch;
    printf("[NodeWarden] IssueSlay roundToken=%d generation=%d pdiskEpoch=%d nwEpoch=%d\n",
        nw_round, generation, target_pdisk_epoch, nw_epoch);

    if
    :: issued_while_restarting ->
        /* The old incarnation will destroy this send synchronously. */
        printf("[NodeWarden] Slay roundToken=%d is lost in restarting PDisk actor\n",
            nw_round)
    :: !issued_while_restarting ->
        nfull(pdisk_actor_mailbox);
        retain_round(nw_round);
        pdisk_actor_mailbox!SLAY(nw_round, generation,
            target_pdisk_epoch, nw_epoch);
        nw_current_attempt_outstanding = true
    fi;

    discard_stale_fixed_timers();
#if ENABLE_CONFIRMATION_RETRY
    arm_issue_timer()
#else
    skip
#endif
}

inline poison_local_vdisk() {
    if
    :: !vdisk_poison_queued &&
            (vdisk_state == VDISK_RUNNING || vdisk_state == VDISK_STARTING) ->
        nfull(vdisk_mailbox);
        vdisk_mailbox!POISON(vdisk_epoch);
        vdisk_poison_queued = true
    :: else -> skip
    fi
}

proctype BSC() {
    mtype message;
    byte generation;
    byte source_epoch;
    byte target_epoch;

end_bsc:
    do
    :: atomic {
        bsc_actor_state == ACTOR_UP && len(bsc_mailbox) > 0;
        bsc_mailbox?message(generation, source_epoch, target_epoch);
        bsc_processing_event = true;
            if
            :: message == START_WIPE ->
                assert(bsc_vslot_allocated);
                bsc_mood = MOOD_WIPE;
                bsc_generation = generation;
                bsc_wipe_pending = true;
                bsc_wipe_completed = false;
                printf("[BSC] persist WIPE generation=%d\n", generation);
                send_bsc_service_set(SERVICE_SET_WIPE, generation,
                    bsc_registered_nw_epoch)

            :: message == START_DESTROY ->
                assert(bsc_vslot_allocated);
                bsc_mood = MOOD_DELETE;
                bsc_generation = generation;
                bsc_wipe_pending = false;
                bsc_delete_pending = true;
                printf("[BSC] persist DELETE generation=%d\n", generation);
                send_bsc_service_set(SERVICE_SET_DESTROY, generation,
                    bsc_registered_nw_epoch)

            :: message == REQUEST_PDISK_RESTART ->
                printf("[BSC] authorize controller-initiated PDisk restart\n");
                send_bsc_service_set(REQUEST_PDISK_RESTART, 0,
                    bsc_registered_nw_epoch)

            :: message == ASK_BSC_RESTART ->
                if
                :: target_epoch == bsc_epoch ->
                    printf("[BSC] authorize PDisk self-restart request\n");
                    send_bsc_service_set(REQUEST_PDISK_RESTART, 0, source_epoch)
                :: target_epoch != bsc_epoch ->
                    printf("[BSC] drop stale restart request for epoch=%d current=%d\n",
                        target_epoch, bsc_epoch)
                fi

            :: message == REGISTER_NODE ->
                if
                :: target_epoch == bsc_epoch ->
                    bsc_registered_nw_epoch = source_epoch;
                    printf("[BSC] RegisterNode from nwEpoch=%d; replay persistent ServiceSet\n",
                        source_epoch);
                    if
                    :: bsc_vslot_allocated && bsc_delete_pending ->
                        send_bsc_service_set(SERVICE_SET_DESTROY, bsc_generation, source_epoch)
                    :: bsc_vslot_allocated && bsc_wipe_pending ->
                        send_bsc_service_set(SERVICE_SET_WIPE, bsc_generation, source_epoch)
                    :: else -> skip
                    fi
                :: target_epoch != bsc_epoch ->
                    printf("[BSC] drop RegisterNode for old bscEpoch=%d current=%d\n",
                        target_epoch, bsc_epoch)
                fi

            :: message == WIPED ->
                if
                :: target_epoch != bsc_epoch ->
                    printf("[BSC] drop stale WIPED for epoch=%d current=%d\n",
                        target_epoch, bsc_epoch)
                :: bsc_mood == MOOD_WIPE && bsc_wipe_pending ->
                    /* Generation may differ: TVSlotInfo::IsSameVDisk compares
                     * stable short identity, not VDisk generation. */
                    assert(bsc_vslot_allocated);
                    bsc_mood = MOOD_NORMAL;
                    bsc_wipe_pending = false;
                    bsc_wipe_completed = true;
                    printf("[BSC] accept WIPED generation=%d; keep VSlotId allocated\n",
                        generation)
                :: else ->
                    printf("[BSC] ignore WIPED generation=%d while mood=", generation);
                    printm(bsc_mood);
                    printf("\n")
                fi

            :: message == DESTROYED ->
                if
                :: target_epoch != bsc_epoch ->
                    printf("[BSC] drop stale DESTROYED for epoch=%d current=%d\n",
                        target_epoch, bsc_epoch)
                :: bsc_mood == MOOD_DELETE && bsc_delete_pending ->
                    /* The report may carry an older generation of this same
                     * short VDisk identity, which production accepts. */
                    assert(!pdisk_owner_present);
                    assert(!init_cut_active);
                    bsc_delete_pending = false;
                    bsc_vslot_allocated = false;
                    printf("[BSC] accept DESTROYED generation=%d and free VSlotId\n",
                        generation)
                :: else ->
                    printf("[BSC] ignore obsolete DESTROYED generation=%d\n", generation)
                fi

            :: else -> assert(false)
            fi;
        bsc_processing_event = false
        }
    od
}

proctype NodeWarden() {
    mtype message;
    byte first;
    byte second;
    byte third;
    byte fourth;
    mtype completed_action;
    byte completed_generation;

end_node_warden:
    do
    :: atomic {
        nw_actor_state == ACTOR_UP && len(nw_mailbox) > 0;
        nw_mailbox?message(first, second, third, fourth);
        nw_processing_event = true;
            if
            :: message == SERVICE_SET_WIPE ->
                if
                :: second != nw_epoch || third != nw_known_bsc_epoch ->
                    printf("[NodeWarden] drop stale ServiceSet(WIPE) nwEpoch=%d bscEpoch=%d\n",
                        second, third)
                :: second == nw_epoch && third == nw_known_bsc_epoch ->
                    printf("[NodeWarden] apply ServiceSet(WIPE) generation=%d\n", first);
                    nw_local_vdisk = true;
                    nw_current_generation = first;
                    if
                    :: !nw_slay_inflight ->
                        poison_local_vdisk();
                        nw_slay_inflight = true;
                        nw_slay_action = ACTION_WIPE;
                        nw_slay_generation = first;
                        send_slay(first)
                    :: nw_slay_inflight ->
                        /* Production refreshes VDiskId for every request with
                         * the same short identity, even after DESTROY won. */
                        nw_slay_generation = first;
                        assert(nw_slay_action == ACTION_WIPE ||
                            nw_slay_action == ACTION_DESTROY)
                        printf("[NodeWarden] merge WIPE with current SlayInFlight\n")
                    fi
                fi

            :: message == SERVICE_SET_DESTROY ->
                if
                :: second != nw_epoch || third != nw_known_bsc_epoch ->
                    printf("[NodeWarden] drop stale ServiceSet(DESTROY) nwEpoch=%d bscEpoch=%d\n",
                        second, third)
                :: second == nw_epoch && third == nw_known_bsc_epoch ->
                    printf("[NodeWarden] apply ServiceSet(DESTROY) generation=%d\n", first);
                    nw_current_generation = first;
                    if
                    :: !nw_slay_inflight ->
                        poison_local_vdisk();
                        nw_slay_inflight = true;
                        nw_slay_action = ACTION_DESTROY;
                        nw_slay_generation = first;
                        send_slay(first)
                    :: nw_slay_inflight ->
                        if
                        :: nw_slay_action == ACTION_WIPE ->
                            nw_slay_action = ACTION_DESTROY;
                            nw_slay_generation = first;
                            printf("[NodeWarden] upgrade WIPE to DESTROY and issue a new round\n");
                            send_slay(first)
                        :: nw_slay_action == ACTION_DESTROY ->
                            nw_slay_generation = first;
                            printf("[NodeWarden] merge duplicate DESTROY\n")
                        fi;
                        assert(nw_slay_action == ACTION_DESTROY)
                    fi;
                    nw_local_vdisk = false;
                    vdisk_start_requested = false;
                fi

            :: message == REQUEST_PDISK_RESTART ->
                if
                :: second != nw_epoch || third != nw_known_bsc_epoch ->
                    printf("[NodeWarden] drop stale ServiceSet(RESTART)\n")
                :: second == nw_epoch && third == nw_known_bsc_epoch ->
                    printf("[NodeWarden] send authorized restart through PDisk actor FIFO\n");
                    nw_pdisk_restart_inflight = true;
                    nfull(pdisk_actor_mailbox);
                    pdisk_actor_mailbox!REQUEST_PDISK_RESTART(0, 0, pdisk_epoch, nw_epoch)
                fi

            :: message == ASK_WARDEN_RESTART ->
                if
                :: second == nw_epoch ->
                    printf("[NodeWarden] forward AskWardenRestartPDisk to BSC\n");
                    nfull(bsc_mailbox);
                    bsc_mailbox!ASK_BSC_RESTART(0, nw_epoch,
                        nw_known_bsc_epoch)
                :: else ->
                    printf("[NodeWarden] drop self-restart request for old incarnation\n")
                fi

            :: message == BSC_RECONNECTED ->
                if
                :: second == nw_epoch ->
                    printf("[NodeWarden] BSC reconnected; schedule RegisterNode\n");
                    nw_known_bsc_epoch = first;
                    nw_register_pending = true;
                    nw_register_source_epoch = nw_epoch;
                    nw_register_target_bsc_epoch = first
                :: else ->
                    printf("[NodeWarden] drop BSC reconnect for old incarnation\n")
                fi

            :: message == PDISK_RESTARTED ->
                if
                :: second != nw_epoch ->
                    printf("[NodeWarden] drop PDisk callback for old nwEpoch=%d current=%d\n",
                        second, nw_epoch)
                :: !nw_pdisk_restart_inflight ->
                    printf("[NodeWarden] ignore unexpected PDisk restart callback\n")
                :: nw_pdisk_restart_inflight ->
                    nw_pdisk_restart_inflight = false;
                    printf("[NodeWarden] PDisk restarted at epoch=%d\n", first);
                    /* Production first poisons every local VDisk over this
                     * PDisk, then replays Slay, then starts local VDisks which
                     * have no SlayInFlight.  This is essential when Stop()
                     * discarded a YardInit from a STARTING VDisk actor. */
                    if
                    :: nw_local_vdisk -> poison_local_vdisk()
                    :: else -> skip
                    fi;
                    if
                    :: nw_slay_inflight ->
                        printf("[NodeWarden] replay stored action=");
                        printm(nw_slay_action);
                        printf(" generation=%d\n", nw_slay_generation);
                        send_slay(nw_slay_generation)
                    :: else -> skip
                    fi
                    if
                    :: nw_local_vdisk && !nw_slay_inflight ->
                        vdisk_start_requested = true
                    :: else -> skip
                    fi;
                    /* If Slay remains in flight, its recovery evidence must be
                     * either a request for the new PDisk incarnation or a
                     * matching result that already escaped the old actor. */
#ifdef REQUIRE_INPUT_QUEUE_LOSS
                    if
                    :: trace_input_queue_loss ->
                        assert(!nw_slay_inflight ||
                            nw_last_issue_pdisk_epoch == pdisk_epoch ||
                            nw_current_result_available)
                    :: !trace_input_queue_loss -> skip
                    fi
#else
                    assert(!nw_slay_inflight ||
                        nw_last_issue_pdisk_epoch == pdisk_epoch ||
                        nw_current_result_available)
#endif
                fi

            :: message == RETRY_SLAY ->
                if
                :: second == nw_epoch && nw_slay_inflight && first == nw_round ->
                    printf("[NodeWarden] retry current TEvSlay roundToken=%d\n", first);
                    /* Consume the scheduled-event reference.  The current
                     * SlayInFlight record independently retains this round. */
                    release_round(first);
                    send_slay(nw_slay_generation)
                :: else ->
                    printf("[NodeWarden] ignore stale retry roundToken=%d\n", first);
                    release_round(first)
                fi

            :: message == SLAY_NOTREADY ->
                if
                :: third != nw_epoch || !nw_slay_inflight || first != nw_round ->
                    printf("[NodeWarden] ignore stale NOTREADY roundToken=%d\n", first);
                    release_round(first)
                :: else ->
                    printf("[NodeWarden] accept NOTREADY roundToken=%d and schedule retry\n",
                        first);
                    nw_current_result_available = false;
                    nw_current_attempt_outstanding = false;
                    /* Transfer the result-message reference to the distinct
                     * NOTREADY timer; the IssueSlay timer remains independent. */
                    arm_notready_timer_from_result(first, second, third)
                fi

            :: message == SLAY_OK || message == SLAY_ALREADY || message == SLAY_RACE ->
                if
                :: third != nw_epoch || !nw_slay_inflight || first != nw_round ->
                    printf("[NodeWarden] ignore stale result=");
                    printm(message);
                    printf(" roundToken=%d expected=%d\n", first, nw_round);
                    release_round(first)
                :: message == SLAY_RACE ->
                    /* Production treats a matching RACE as impossible. */
                    release_round(first);
                    assert(false)
                :: else ->
                    release_round(first);    /* result message */
                    release_round(nw_round); /* SlayInFlight.Round */
                    nw_round = NO_ROUND;
                    nw_current_result_available = false;
                    nw_current_attempt_outstanding = false;
                    if
                    :: issue_timer_armed ->
                        release_round(issue_timer_round);
                        issue_timer_armed = false;
                        issue_timer_round = NO_ROUND
                    :: else -> skip
                    fi;
                    if
                    :: notready_timer_armed ->
                        release_round(notready_timer_round);
                        notready_timer_armed = false;
                        notready_timer_round = NO_ROUND
                    :: else -> skip
                    fi;
                    completed_action = nw_slay_action;
                    completed_generation = nw_slay_generation;
                    nw_slay_inflight = false;
                    nw_slay_action = ACTION_NONE;
                    nw_slay_generation = 0;
                    nw_current_attempt_outstanding = false;
                    printf("[NodeWarden] complete action=");
                    printm(completed_action);
                    printf(" from result=");
                    printm(message);
                    printf(" roundToken=%d\n", first);
                    nfull(bsc_mailbox);
                    if
                    :: completed_action == ACTION_DESTROY ->
                        bsc_mailbox!DESTROYED(completed_generation, nw_epoch,
                            nw_known_bsc_epoch)
                    :: completed_action == ACTION_WIPE ->
                        bsc_mailbox!WIPED(completed_generation, nw_epoch,
                            nw_known_bsc_epoch);
                        if
                        :: nw_local_vdisk ->
                            /* StartLocalVDiskActor may have to wait for the
                             * poisoned incarnation's Gone notification. */
                            vdisk_start_requested = true
                        :: else -> skip
                        fi
                    fi
                fi

            :: else -> assert(false)
            fi;
        nw_processing_event = false
        }
    od
}

proctype VDisk() {
    mtype message;
    byte target_vdisk_epoch;
    byte cut_index;

end_vdisk:
    do
    /* Messages from the one VDisk mailbox stay adjacent: their FIFO order is
     * fixed, while the next actor action may still race with its head. */
    :: vdisk_mailbox?message(target_vdisk_epoch) ->
        atomic {
            if
            :: message == POISON ->
                if
                :: target_vdisk_epoch == vdisk_epoch ->
                    vdisk_poison_queued = false;
                    vdisk_state = VDISK_STOPPED;
                    printf("[VDisk] consume Poison epoch=%d and stop actor\n",
                        target_vdisk_epoch);
                    if
                    :: init_cut_active &&
                            init_vdisk_epoch == target_vdisk_epoch && !init_sent ->
                        /* Poison wins before bootstrap sends YardInit. */
                        drop_init_cut(cut_index)
                    :: else -> skip
                    fi
                :: target_vdisk_epoch != vdisk_epoch ->
                    printf("[VDisk] ignore stale Poison epoch=%d current=%d\n",
                        target_vdisk_epoch, vdisk_epoch)
                fi
            :: message == YARD_INIT_OK ->
                if
                :: target_vdisk_epoch == vdisk_epoch &&
                        vdisk_state == VDISK_STARTING ->
                    vdisk_state = VDISK_RUNNING;
                    printf("[VDisk] YardInit completed; actor epoch=%d RUNNING\n",
                        target_vdisk_epoch)
                :: else ->
                    printf("[VDisk] ignore YardInit result for stopped/stale epoch=%d\n",
                        target_vdisk_epoch)
                fi
            :: else -> assert(false)
            fi
        }

    /* HandleGone starts the requested replacement only after old actor stop.
     * Allocation, not YardInit delivery, creates the exact owner-round cut. */
    :: atomic {
        vdisk_state == VDISK_STOPPED && vdisk_start_requested &&
            nw_local_vdisk && !nw_slay_inflight &&
            !nw_pdisk_restart_inflight && !init_cut_active;
        vdisk_start_requested = false;
        vdisk_epoch++;
        vdisk_state = VDISK_STARTING;
        init_generation = nw_current_generation;
        init_vdisk_epoch = vdisk_epoch;
        allocate_init_cut(cut_index);
        printf("[VDisk] start epoch=%d generation=%d; allocate InitOwnerRound\n",
            vdisk_epoch, init_generation)
    }

    /* Explicit bound check: the model stores one InitOwnerRound cut.  Do not
     * silently serialize a production start which would allocate a second
     * round while the previous YardInit is still live. */
    :: atomic {
        vdisk_state == VDISK_STOPPED && vdisk_start_requested &&
            nw_local_vdisk && !nw_slay_inflight &&
            !nw_pdisk_restart_inflight && init_cut_active;
        printf("[MODEL SCOPE] second concurrent YardInit is required\n");
        assert(false)
    }

    /* This actor step competes with a newly queued Poison.  If it wins, the
     * already sent YardInit remains live after the VDisk actor stops. */
    :: atomic {
        vdisk_state == VDISK_STARTING && init_cut_active && !init_sent &&
            init_vdisk_epoch == vdisk_epoch && nfull(pdisk_actor_mailbox);
        init_sent = true;
        init_target_pdisk_epoch = pdisk_epoch;
        pdisk_actor_mailbox!YARD_INIT(init_generation, init_vdisk_epoch,
            init_target_pdisk_epoch, 0);
        printf("[VDisk] send YardInit generation=%d vdiskEpoch=%d pdiskEpoch=%d\n",
            init_generation, init_vdisk_epoch, init_target_pdisk_epoch)
    }
    od
}

/* Each fixed implementation timer is a distinct production Schedule call.
 * Delivery transfers its token reference into the NodeWarden mailbox. */
proctype RetryTimer() {
    byte round;
    byte target_epoch;
    byte generation;

end_retry_timer:
    do
    :: atomic {
        issue_timer_armed && nfull(nw_mailbox);
        round = issue_timer_round;
        target_epoch = issue_timer_nw_epoch;
        generation = issue_timer_generation;
        issue_timer_armed = false;
        issue_timer_round = NO_ROUND;
        nw_mailbox!RETRY_SLAY(round, target_epoch, generation, 0)
    }
    :: atomic {
        notready_timer_armed && nfull(nw_mailbox);
        round = notready_timer_round;
        target_epoch = notready_timer_nw_epoch;
        generation = notready_timer_generation;
        notready_timer_armed = false;
        notready_timer_round = NO_ROUND;
        nw_mailbox!RETRY_SLAY(round, target_epoch, generation, 0)
    }
    od
}


proctype PDiskActor() {
    mtype message;
    byte first;
    byte second;
    byte target_pdisk_epoch;
    byte target_nw_epoch;
    byte cut_index;

end_pdisk_actor:
    do
    /* All receives from the PDisk actor FIFO stay adjacent. */
    :: atomic {
        pdisk_state == PDISK_RUNNING &&
            pdisk_actor_mailbox?[SLAY, _, _, eval(pdisk_epoch), _] &&
            nfull(pdisk_input_queue);
        pdisk_actor_mailbox?SLAY(first, second, target_pdisk_epoch, target_nw_epoch);
        pdisk_input_queue!SLAY(first, second, pdisk_epoch, target_nw_epoch);
        printf("[PDisk actor] accept Slay round=%d into InputQueue epoch=%d\n",
            first, pdisk_epoch)
    }

    :: atomic {
        (pdisk_state == PDISK_RUNNING || pdisk_state == PDISK_STARTING) &&
            pdisk_actor_mailbox?[YARD_INIT, _, _, eval(pdisk_epoch), _] &&
            nfull(pdisk_input_queue);
        pdisk_actor_mailbox?YARD_INIT(first, second,
            target_pdisk_epoch, target_nw_epoch);
        pdisk_input_queue!YARD_INIT(first, second, pdisk_epoch, 0);
        printf("[PDisk actor] accept YardInit generation=%d vdiskEpoch=%d epoch=%d\n",
            first, second, pdisk_epoch)
    }

    /* Restart authorization is ordered with Slay in this same FIFO. */
    :: atomic {
        pdisk_state == PDISK_RUNNING &&
            pdisk_actor_mailbox?[REQUEST_PDISK_RESTART, _, _, eval(pdisk_epoch), _];
        pdisk_actor_mailbox?REQUEST_PDISK_RESTART(first, second,
            target_pdisk_epoch, target_nw_epoch);
        pdisk_state = PDISK_RESTARTING;
        pdisk_restart_phase = RESTART_STOPPING;
        pdisk_restart_reply_nw_epoch = target_nw_epoch;
        printf("[PDisk actor] enter observable RESTARTING epoch=%d\n", pdisk_epoch)
    }

    /* StateInit replies NOTREADY until the replacement actor is online. */
    :: atomic {
        pdisk_state == PDISK_STARTING &&
            pdisk_actor_mailbox?[SLAY, _, _, eval(pdisk_epoch), _] &&
            nfull(nw_mailbox);
        pdisk_actor_mailbox?SLAY(first, second, target_pdisk_epoch, target_nw_epoch);
        printf("[PDisk actor] StateInit returns NOTREADY round=%d epoch=%d\n",
            first, pdisk_epoch);
        if
        :: target_nw_epoch == nw_epoch && nw_slay_inflight &&
                first == nw_round ->
            assert(nw_current_attempt_outstanding);
            nw_current_attempt_outstanding = false;
            nw_current_result_available = true
        :: else -> skip
        fi;
        nw_mailbox!SLAY_NOTREADY(first, second, target_nw_epoch, pdisk_epoch)
    }

    /* Stop destroys the old actor mailbox. */
    :: atomic {
        pdisk_state == PDISK_RESTARTING && len(pdisk_actor_mailbox) > 0;
        pdisk_actor_mailbox?message(first, second,
            target_pdisk_epoch, target_nw_epoch);
        if
        :: message == SLAY ->
            if
            :: target_nw_epoch == nw_epoch && first == nw_round ->
                nw_current_attempt_outstanding = false
            :: else -> skip
            fi;
            release_round(first)
        :: message == YARD_INIT ->
            if
            :: init_cut_active && init_vdisk_epoch == second ->
                drop_init_cut(cut_index)
            :: else -> skip
            fi
        :: else -> skip
        fi;
        printf("[PDisk restart] drop mailbox message=");
        printm(message);
        printf(" round=%d targetEpoch=%d\n", first, target_pdisk_epoch)
    }

    /* A delayed event addressed to a dead incarnation cannot enter the new
     * actor.  This transition also prevents a stale FIFO head from blocking. */
    :: atomic {
        pdisk_state != PDISK_RESTARTING && len(pdisk_actor_mailbox) > 0 &&
            !pdisk_actor_mailbox?[SLAY, _, _, eval(pdisk_epoch), _] &&
            !pdisk_actor_mailbox?[YARD_INIT, _, _, eval(pdisk_epoch), _] &&
            !pdisk_actor_mailbox?[REQUEST_PDISK_RESTART, _, _, eval(pdisk_epoch), _];
        pdisk_actor_mailbox?message(first, second,
            target_pdisk_epoch, target_nw_epoch);
        if
        :: message == SLAY ->
            if
            :: target_nw_epoch == nw_epoch && first == nw_round ->
                nw_current_attempt_outstanding = false
            :: else -> skip
            fi;
            release_round(first)
        :: message == YARD_INIT ->
            if
            :: init_cut_active && init_vdisk_epoch == second ->
                drop_init_cut(cut_index)
            :: else -> skip
            fi
        :: else -> skip
        fi;
        printf("[PDisk actor] drop message for stale epoch=%d current=%d\n",
            target_pdisk_epoch, pdisk_epoch)
    }
    od
}

/* Stop drains every incarnation-owned stage without synthesizing a result. */
proctype PDiskStopper() {
    mtype kind;
    mtype result;
    byte round;
    byte generation;
    byte epoch;
    byte target_nw_epoch;
    byte cut_index;

end_pdisk_stopper:
    do
    :: atomic {
        pdisk_state == PDISK_RESTARTING &&
            pdisk_restart_phase == RESTART_DRAINING &&
            pdisk_yard_init_pending;
        assert(init_cut_active && init_sent);
        drop_init_cut(cut_index);
        pdisk_yard_init_pending = false;
        pdisk_yard_init_generation = 0;
        pdisk_yard_init_vdisk_epoch = 0;
        pdisk_yard_init_epoch = 0;
        printf("[PDisk restart] abort PendingYardInit\n")
    }

    :: atomic {
        pdisk_state == PDISK_RESTARTING &&
            pdisk_restart_phase == RESTART_DRAINING &&
            len(pdisk_input_queue) > 0;
        pdisk_input_queue?kind(round, generation, epoch, target_nw_epoch);
        if
        :: kind == SLAY ->
#ifdef REQUIRE_INPUT_QUEUE_LOSS
            trace_input_queue_loss = true;
#endif
            if
            :: target_nw_epoch == nw_epoch && round == nw_round ->
                nw_current_attempt_outstanding = false
            :: else -> skip
            fi;
            release_round(round)
        :: kind == YARD_INIT ->
            if
            :: init_cut_active && init_vdisk_epoch == generation ->
                drop_init_cut(cut_index)
            :: else -> skip
            fi
        fi;
        printf("[PDisk restart] abort InputQueue kind=");
        printm(kind);
        printf(" first=%d epoch=%d\n", round, epoch)
    }

    :: atomic {
        pdisk_state == PDISK_RESTARTING &&
            pdisk_restart_phase == RESTART_DRAINING &&
            pdisk_exec_phase != EXEC_EMPTY;
        if
        :: pdisk_exec_kind == SLAY ->
            if
            :: pdisk_exec_nw_epoch == nw_epoch && pdisk_exec_round == nw_round ->
                nw_current_attempt_outstanding = false
            :: else -> skip
            fi;
            release_round(pdisk_exec_round)
        :: pdisk_exec_kind == YARD_INIT ->
            if
            :: init_cut_active && init_vdisk_epoch == pdisk_exec_generation ->
                drop_init_cut(cut_index)
            :: else -> skip
            fi
        fi;
        printf("[PDisk restart] abort executing phase=");
        printm(pdisk_exec_phase);
        printf(" round=%d\n", pdisk_exec_round);
        pdisk_exec_phase = EXEC_EMPTY;
        pdisk_exec_result = RESULT_NONE
    }

    :: atomic {
        pdisk_state == PDISK_RESTARTING &&
            pdisk_restart_phase == RESTART_DRAINING &&
            len(pdisk_completion_queue) > 0;
        pdisk_completion_queue?result(round, generation, epoch, target_nw_epoch);
        if
        :: target_nw_epoch == nw_epoch && round == nw_round ->
            nw_current_attempt_outstanding = false
        :: else -> skip
        fi;
        release_round(round);
        printf("[PDisk restart] drop committed result=");
        printm(result);
        printf(" round=%d\n", round)
    }
    od
}

/* Restart phases are separate transitions and remain externally observable. */
proctype PDiskRestarter() {
    byte round_index;

end_pdisk_restarter:
    do
    :: atomic {
        pdisk_state == PDISK_RESTARTING &&
            pdisk_restart_phase == RESTART_STOPPING;
        pdisk_restart_phase = RESTART_DRAINING;
        printf("[PDisk restart] worker stopped; drain old incarnation\n")
    }

    :: atomic {
        pdisk_state == PDISK_RESTARTING &&
            pdisk_restart_phase == RESTART_DRAINING &&
            len(pdisk_actor_mailbox) == 0 &&
            len(pdisk_input_queue) == 0 &&
            pdisk_exec_phase == EXEC_EMPTY &&
            !pdisk_yard_init_pending &&
            len(pdisk_completion_queue) == 0;
        pdisk_restart_phase = RESTART_CREATING;
        printf("[PDisk restart] old incarnation fully drained\n")
    }

    :: atomic {
        pdisk_state == PDISK_RESTARTING &&
            pdisk_restart_phase == RESTART_CREATING;
        pdisk_epoch++;
        reset_owner_relation_after_pdisk_restart(round_index);
        pdisk_state = PDISK_STARTING;
        pdisk_callback_pending = true;
        pdisk_callback_epoch = pdisk_epoch;
        pdisk_callback_target_nw_epoch = pdisk_restart_reply_nw_epoch;
        printf("[PDisk actor] create epoch=%d in STARTING; callback pending\n",
            pdisk_epoch);
    }
    od
}

/* Recreating PDisk never waits for space in NodeWarden's mailbox.  Delivery
 * of the already produced callback is a separate transport step. */
proctype PDiskCallbackOutbox() {
end_pdisk_callback_outbox:
    do
    :: atomic {
        pdisk_callback_pending &&
            pdisk_callback_target_nw_epoch != nw_epoch;
        printf("[PDisk callback] drop stale target nwEpoch=%d current=%d\n",
            pdisk_callback_target_nw_epoch, nw_epoch);
        pdisk_callback_pending = false
    }
    :: atomic {
        pdisk_callback_pending &&
            pdisk_callback_target_nw_epoch == nw_epoch &&
            nw_actor_state == ACTOR_UP && nfull(nw_mailbox);
        nw_mailbox!PDISK_RESTARTED(pdisk_callback_epoch,
            pdisk_callback_target_nw_epoch, 0, 0);
        pdisk_callback_pending = false
    }
    od
}

proctype PDiskWorker() {
    mtype kind;
    byte round;
    byte generation;
    byte epoch;
    byte target_nw_epoch;

end_pdisk_worker:
    do
    /* Pop and execute are separate: restart may abort accepted work. */
    :: atomic {
        pdisk_state == PDISK_RUNNING &&
            pdisk_exec_phase == EXEC_EMPTY && len(pdisk_input_queue) > 0;
        pdisk_input_queue?kind(round, generation, epoch, target_nw_epoch);
        assert(epoch == pdisk_epoch);
        pdisk_exec_kind = kind;
        pdisk_exec_round = round;
        pdisk_exec_generation = generation;
        pdisk_exec_epoch = epoch;
        pdisk_exec_nw_epoch = target_nw_epoch;
        pdisk_exec_result = RESULT_NONE;
        pdisk_exec_phase = EXEC_ACCEPTED;
        printf("[PDisk worker] take kind=");
        printm(kind);
        printf(" first=%d epoch=%d\n", round, epoch)
    }

    /* YardInitStart installs PendingYardInits before ordinary owner work. */
    :: atomic {
        pdisk_state == PDISK_RUNNING &&
            pdisk_exec_phase == EXEC_ACCEPTED &&
            pdisk_exec_kind == YARD_INIT && !pdisk_yard_init_pending;
        assert(pdisk_exec_epoch == pdisk_epoch);
        assert(init_cut_active);
        assert(init_vdisk_epoch == pdisk_exec_generation);
        pdisk_yard_init_pending = true;
        pdisk_yard_init_generation = pdisk_exec_round;
        pdisk_yard_init_vdisk_epoch = pdisk_exec_generation;
        pdisk_yard_init_epoch = pdisk_exec_epoch;
        pdisk_exec_phase = EXEC_EMPTY;
        pdisk_exec_result = RESULT_NONE;
        printf("[PDisk worker] YardInitStart generation=%d vdiskEpoch=%d\n",
            pdisk_yard_init_generation, pdisk_yard_init_vdisk_epoch)
    }

    /* Exact production priority: PendingYardInit, absent owner, owner-round
     * race, then physical removal. */
    :: atomic {
        pdisk_state == PDISK_RUNNING && pdisk_exec_phase == EXEC_ACCEPTED &&
            pdisk_exec_kind == SLAY;
        assert(pdisk_exec_epoch == pdisk_epoch);
        if
        :: pdisk_yard_init_pending ->
            pdisk_exec_result = RESULT_NOTREADY;
            printf("[PDisk worker] pending YardInit; prepare NOTREADY roundToken=%d\n",
                pdisk_exec_round)
        :: !pdisk_yard_init_pending && !pdisk_owner_present ->
            pdisk_exec_result = RESULT_ALREADY;
            printf("[PDisk worker] owner absent; prepare ALREADY roundToken=%d\n",
                pdisk_exec_round)
        :: !pdisk_yard_init_pending && pdisk_owner_present &&
                !round_after_owner[pdisk_exec_round] ->
            pdisk_exec_result = RESULT_RACE;
            printf("[PDisk worker] stale owner round; prepare RACE roundToken=%d\n",
                pdisk_exec_round)
        :: !pdisk_yard_init_pending && pdisk_owner_present &&
                round_after_owner[pdisk_exec_round] ->
            pdisk_owner_present = false;
            pdisk_exec_result = RESULT_OK;
            printf("[PDisk worker] commit owner removal roundToken=%d generation=%d\n",
                pdisk_exec_round, pdisk_exec_generation)
        fi;
        pdisk_exec_phase = EXEC_RESULT_READY
    }

    /* Commit and insertion into completion queue are independently losable. */
    :: atomic {
        pdisk_state == PDISK_RUNNING &&
            pdisk_exec_phase == EXEC_RESULT_READY &&
            nfull(pdisk_completion_queue);
        if
        :: pdisk_exec_result == RESULT_OK ->
            pdisk_completion_queue!SLAY_OK(pdisk_exec_round,
                pdisk_exec_generation, pdisk_exec_epoch, pdisk_exec_nw_epoch)
        :: pdisk_exec_result == RESULT_ALREADY ->
            pdisk_completion_queue!SLAY_ALREADY(pdisk_exec_round,
                pdisk_exec_generation, pdisk_exec_epoch, pdisk_exec_nw_epoch)
        :: pdisk_exec_result == RESULT_NOTREADY ->
            pdisk_completion_queue!SLAY_NOTREADY(pdisk_exec_round,
                pdisk_exec_generation, pdisk_exec_epoch, pdisk_exec_nw_epoch)
        :: pdisk_exec_result == RESULT_RACE ->
            pdisk_completion_queue!SLAY_RACE(pdisk_exec_round,
                pdisk_exec_generation, pdisk_exec_epoch, pdisk_exec_nw_epoch)
        fi;
        pdisk_exec_phase = EXEC_EMPTY;
        pdisk_exec_result = RESULT_NONE
    }
    od
}

/* PendingYardInits owns a sent request independently of the VDisk actor.
 * Commit publishes the allocated OwnerRound, then reports to that actor
 * incarnation; a preceding Poison can make the report stale, not the commit. */
proctype PDiskYardInit() {
    byte target_vdisk_epoch;
    byte cut_index;

end_pdisk_yard_init:
    do
    :: atomic {
        pdisk_state == PDISK_RUNNING && pdisk_yard_init_pending &&
            !vdisk_init_result_pending;
        assert(pdisk_yard_init_epoch == pdisk_epoch);
        assert(init_cut_active && init_sent);
        assert(init_vdisk_epoch == pdisk_yard_init_vdisk_epoch);
        target_vdisk_epoch = pdisk_yard_init_vdisk_epoch;
        pdisk_owner_present = true;
        /* Same invariant as the LTL claim, placed at the commit point so a
         * guided/random run reports the exact late YardInit transition. */
        assert(bsc_vslot_allocated);
        commit_init_cut(cut_index);
        pdisk_yard_init_pending = false;
        pdisk_yard_init_generation = 0;
        pdisk_yard_init_vdisk_epoch = 0;
        pdisk_yard_init_epoch = 0;
        vdisk_init_result_pending = true;
        vdisk_init_result_epoch = target_vdisk_epoch;
        printf("[PDisk worker] commit YardInit owner for vdiskEpoch=%d\n",
            target_vdisk_epoch)
    }

    :: atomic {
        pdisk_state == PDISK_RUNNING && pdisk_yard_init_pending &&
            vdisk_init_result_pending;
        printf("[MODEL SCOPE] second YardInit result outbox slot is required\n");
        assert(false)
    }
    od
}

proctype VDiskInitResultOutbox() {
end_vdisk_init_result_outbox:
    do
    :: atomic {
        vdisk_init_result_pending && nfull(vdisk_mailbox);
        vdisk_mailbox!YARD_INIT_OK(vdisk_init_result_epoch);
        vdisk_init_result_pending = false
    }
    od
}

proctype PDiskCompletion() {
    mtype result;
    byte round;
    byte generation;
    byte epoch;
    byte target_nw_epoch;

end_pdisk_completion:
    do
    /* A matching result remains in the PDisk-owned completion queue until
     * transport can atomically place it in the current NodeWarden mailbox.
     * Therefore the recovery ghost never describes a result stranded only in
     * this proctype's locals. */
    :: atomic {
        pdisk_state == PDISK_RUNNING && nw_slay_inflight &&
            pdisk_completion_queue?[_, eval(nw_round), _,
                eval(pdisk_epoch), eval(nw_epoch)] &&
            nfull(nw_mailbox);
        pdisk_completion_queue?result(round, generation, epoch, target_nw_epoch);
        assert(epoch == pdisk_epoch);
        assert(target_nw_epoch == nw_epoch && round == nw_round);
        assert(nw_current_attempt_outstanding);
        nw_current_attempt_outstanding = false;
        nw_current_result_available = true;
        printf("[PDisk completion] deliver result=");
        printm(result);
        printf(" round=%d to nwEpoch=%d\n", round, target_nw_epoch);
        nw_mailbox!result(round, generation, target_nw_epoch, epoch)
    }

    :: atomic {
        pdisk_state == PDISK_RUNNING && len(pdisk_completion_queue) > 0 &&
            !(nw_slay_inflight &&
                pdisk_completion_queue?[_, eval(nw_round), _,
                    eval(pdisk_epoch), eval(nw_epoch)]);
        pdisk_completion_queue?result(round, generation, epoch, target_nw_epoch);
        /* A result for an obsolete owner round or dead NodeWarden actor has
         * no production side effect. */
        printf("[PDisk completion] discard stale result=");
        printm(result);
        printf(" round=%d nwEpoch=%d\n", round, target_nw_epoch);
        release_round(round)
    }
    od
}

/* The new actor notifies NodeWarden before StateInit finishes.  A separate
 * process is essential: weak process fairness then guarantees initialization
 * even if retries keep producing NOTREADY replies. */
proctype PDiskStarter() {
end_pdisk_starter:
    do
    :: atomic {
        pdisk_state == PDISK_STARTING;
        pdisk_state = PDISK_RUNNING;
        printf("[PDisk actor] initialization complete; epoch=%d RUNNING\n", pdisk_epoch)
    }
    od
}

/* Operations retain only their required order.  No component waits for a
 * scenario phase marker, so every actor and fault may interleave between the
 * two sends. */
proctype OperationEnvironment() {
#if OPERATION_MODE == 1
    atomic {
        nfull(bsc_mailbox);
        printf("[Environment] direct DESTROY\n");
        bsc_mailbox!START_DESTROY(2, 0, 0)
    }
#elif OPERATION_MODE == 2
    atomic {
        nfull(bsc_mailbox);
        printf("[Environment] WIPE generation=1\n");
        bsc_mailbox!START_WIPE(1, 0, 0)
    };
#ifdef REQUIRE_WIPE_COMPLETION
    /* Trace-selection only: choose the legal schedule in which WIPED reaches
     * BSC before the superseding DESTROY is injected. */
    bsc_wipe_completed;
#endif
    nfull(bsc_mailbox);
    printf("[Environment] superseding DESTROY generation=2\n");
    bsc_mailbox!START_DESTROY(2, 0, 0)
#else
    if
    :: atomic {
        nfull(bsc_mailbox);
        printf("[Environment] direct DESTROY\n");
        bsc_mailbox!START_DESTROY(2, 0, 0)
    }
    :: atomic {
        nfull(bsc_mailbox);
        printf("[Environment] WIPE generation=1\n");
        bsc_mailbox!START_WIPE(1, 0, 0)
    };
#ifdef REQUIRE_WIPE_COMPLETION
       bsc_wipe_completed;
#endif
       nfull(bsc_mailbox);
       printf("[Environment] superseding DESTROY generation=2\n");
       bsc_mailbox!START_DESTROY(2, 0, 0)
    fi;
#endif
    skip
}

/* One optional PDisk restart, with origin selected independently of timing. */
proctype PDiskRestartEnvironment() {
end_pdisk_restart_environment:
#if PDISK_RESTART_MODE == 1
    skip
#elif PDISK_RESTART_MODE == 2
#ifdef REQUIRE_SLAY_IN_FLIGHT_RESTART
    nw_slay_inflight;
#endif
    atomic {
        nfull(bsc_mailbox);
        printf("[Environment] BSC-originated PDisk restart\n");
        bsc_mailbox!REQUEST_PDISK_RESTART(0, 0, 0)
    }
#elif PDISK_RESTART_MODE == 3
#ifdef REQUIRE_SLAY_IN_FLIGHT_RESTART
    nw_slay_inflight;
#endif
    atomic {
        nfull(nw_mailbox);
        printf("[Environment] PDisk asks NodeWarden for restart\n");
        nw_mailbox!ASK_WARDEN_RESTART(0, nw_epoch, 0, 0)
    }
#else
    if
    :: skip
    ::
#ifdef REQUIRE_SLAY_IN_FLIGHT_RESTART
       nw_slay_inflight;
#endif
       atomic {
        nfull(bsc_mailbox);
        printf("[Environment] BSC-originated PDisk restart\n");
        bsc_mailbox!REQUEST_PDISK_RESTART(0, 0, 0)
    }
    ::
#ifdef REQUIRE_SLAY_IN_FLIGHT_RESTART
       nw_slay_inflight;
#endif
       atomic {
        nfull(nw_mailbox);
        printf("[Environment] PDisk asks NodeWarden for restart\n");
        nw_mailbox!ASK_WARDEN_RESTART(0, nw_epoch, 0, 0)
    }
    fi;
#endif
    skip
}

/* Lifecycle supervisors model crashes, not ordinary mailbox messages.  Their
 * first transition may occur at any point selected by Spin. */
proctype BSCRestartEnvironment() {
    atomic {
        bsc_actor_state == ACTOR_UP && !bsc_processing_event;
        bsc_actor_state = ACTOR_DOWN;
        bsc_epoch++;
        bsc_registered_nw_epoch = 0;
        printf("[BSC lifecycle] DOWN; persistent intent survives, epoch=%d\n",
            bsc_epoch)
    };
    atomic {
        bsc_actor_state == ACTOR_DOWN;
        bsc_actor_state = ACTOR_STARTING;
        printf("[BSC lifecycle] STARTING epoch=%d\n", bsc_epoch)
    };
    atomic {
        bsc_actor_state == ACTOR_STARTING;
        bsc_actor_state = ACTOR_UP;
        bsc_reconnect_pending = true;
        printf("[BSC lifecycle] UP epoch=%d; reconnect pending\n", bsc_epoch)
    };
    skip
}

proctype NodeWardenRestartEnvironment() {
    atomic {
        nw_actor_state == ACTOR_UP && !nw_processing_event;
        nw_actor_state = ACTOR_DOWN;
        nw_epoch++;
        printf("[NodeWarden lifecycle] DOWN; clear volatile state, epoch=%d\n",
            nw_epoch);
        nw_local_vdisk = false;
        nw_current_generation = 0;
        nw_slay_inflight = false;
        nw_slay_generation = 0;
        nw_slay_action = ACTION_NONE;
        if
        :: nw_round != NO_ROUND -> release_round(nw_round)
        :: else -> skip
        fi;
        nw_round = NO_ROUND;
        nw_last_issue_pdisk_epoch = 0;
        nw_pdisk_restart_inflight = false;
        nw_current_result_available = false;
        nw_current_attempt_outstanding = false;
        /* This profile replaces the NodeWarden session, so private scheduled
         * events owned by that session disappear.  Transport messages already
         * queued outside it retain their round references and become stale. */
        if
        :: issue_timer_armed ->
            release_round(issue_timer_round);
            issue_timer_armed = false;
            issue_timer_round = NO_ROUND
        :: else -> skip
        fi;
        if
        :: notready_timer_armed ->
            release_round(notready_timer_round);
            notready_timer_armed = false;
            notready_timer_round = NO_ROUND
        :: else -> skip
        fi;
        nw_register_pending = false
    };
    atomic {
        nw_actor_state == ACTOR_DOWN;
        nw_actor_state = ACTOR_STARTING;
        printf("[NodeWarden lifecycle] STARTING epoch=%d\n", nw_epoch)
    };
    atomic {
        nw_actor_state == ACTOR_STARTING;
        nw_actor_state = ACTOR_UP;
        nw_known_bsc_epoch = bsc_epoch;
        nw_register_pending = true;
        nw_register_source_epoch = nw_epoch;
        nw_register_target_bsc_epoch = bsc_epoch;
        printf("[NodeWarden lifecycle] UP epoch=%d; RegisterNode pending\n",
            nw_epoch)
    };
    skip
}

/* Crash/epoch changes above are unconditional.  Reconnect and RegisterNode
 * wait for transport capacity in independent processes. */
proctype BSCReconnectOutbox() {
end_bsc_reconnect_outbox:
    do
    :: atomic {
        bsc_reconnect_pending && bsc_actor_state == ACTOR_UP &&
            nw_actor_state == ACTOR_UP && nfull(nw_mailbox);
        nw_mailbox!BSC_RECONNECTED(bsc_epoch, nw_epoch, 0, 0);
        bsc_reconnect_pending = false
    }
    od
}

proctype RegisterNodeOutbox() {
end_register_node_outbox:
    do
    :: atomic {
        nw_register_pending && nw_register_source_epoch != nw_epoch;
        nw_register_pending = false
    }
    :: atomic {
        nw_register_pending && nw_register_source_epoch == nw_epoch &&
            nw_actor_state == ACTOR_UP && bsc_actor_state == ACTOR_UP &&
            nfull(bsc_mailbox);
        bsc_mailbox!REGISTER_NODE(0, nw_register_source_epoch,
            nw_register_target_bsc_epoch);
        nw_register_pending = false
    }
    od
}

init {
    atomic {
        run BSC();
        run NodeWarden();
        run VDisk();
        run RetryTimer();
        run PDiskActor();
        run PDiskStopper();
        run PDiskRestarter();
        run PDiskCallbackOutbox();
        run PDiskWorker();
        run PDiskYardInit();
        run VDiskInitResultOutbox();
        run PDiskCompletion();
        run PDiskStarter();
        run OperationEnvironment();
        run PDiskRestartEnvironment();
        run BSCReconnectOutbox();
        run RegisterNodeOutbox();
#if ENABLE_BSC_RESTART
        run BSCRestartEnvironment();
#endif
#if ENABLE_NW_RESTART
        run NodeWardenRestartEnvironment();
#endif
    }
}

/* BSC never frees the stable VSlotId while its physical PDisk owner exists. */
ltl safety_freed_vslot_has_no_owner {
    [] (!bsc_vslot_allocated -> !pdisk_owner_present)
}

/* A not-yet-committed YardInit can recreate the physical owner after BSC has
 * observed DESTROYED, so freeing the VSlot also requires no live init cut. */
ltl safety_freed_vslot_has_no_live_init {
    [] (!bsc_vslot_allocated -> !init_cut_active)
}

/* With finitely many injected restarts and a non-failing PDisk, pending DELETE
 * eventually releases the BSC-side VSlotId.  Run this claim with weak process
 * fairness so finite environment actions and continuously enabled actor work
 * cannot be starved forever by the scheduler. */
ltl live_delete_eventually_frees_vslot {
    [] (bsc_delete_pending -> <> !bsc_vslot_allocated)
}
