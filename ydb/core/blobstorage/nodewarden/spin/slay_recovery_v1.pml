/*
 * Minimal model of WIPE/DESTROY recovery for one VSlot.
 * Version v1 models NodeWarden with action-aware SlayInFlight recovery.
 *
 * BSC owns persistent desired state and the VSlot allocation. NodeWarden
 * applies ServiceSet updates and tracks one TEvSlay. PDisk owns the physical
 * slot contents. ScenarioDriver only injects external operations and failures;
 * it never decides whether PDisk processes or loses a message.
 *
 * The model explores:
 *   - a direct DESTROY;
 *   - WIPE followed by DESTROY while WIPE may still be in flight;
 *   - a PDisk restart initiated by BSC or requested by PDisk through BSC;
 *   - the race between processing TEvSlay and destroying the old PDisk actor
 *     mailbox during restart;
 *   - optional BSC/NodeWarden restarts after the PDisk restart.
 *
 * After the one injected PDisk restart, the environment is reliable. A WIPE
 * keeps the VSlot allocated; only a matching DESTROYED report frees it.
 */
mtype = {
    SERVICE_SET_WIPE,
    SERVICE_SET_DESTROY,
    POISON,
    SLAY,
    SLAY_OK,
    WIPED,
    DESTROYED,
    REGISTER_NODE,
    START_WIPE,
    START_DESTROY,
    REQUEST_SELF_RESTART,
    ASK_WARDEN_RESTART,
    ASK_BSC_RESTART,
    REQUEST_PDISK_RESTART,
    PDISK_RESTARTED,
    RESTART_BSC,
    RESTART_NODE,
    MOOD_NORMAL,
    MOOD_WIPE,
    MOOD_DELETE,
    ACTION_NONE,
    ACTION_WIPE,
    ACTION_DESTROY,
    PDISK_RUNNING,
    PDISK_RESTARTING
};
/* ServiceSet/report channels carry a VDisk generation abstraction. */
chan bsc_to_nw = [3] of { mtype, byte };
chan nw_to_bsc = [3] of { mtype, byte };
chan nw_to_vdisk = [1] of { mtype };
/* A PDisk actor mailbox may contain both an old WIPE and a newer DESTROY. */
chan nw_to_pdisk = [2] of { mtype, byte, byte };
chan pdisk_to_nw = [2] of { mtype, byte, byte };
chan restart_pdisk = [1] of { mtype };
chan scenario_to_bsc = [1] of { mtype, byte };
chan scenario_to_nw = [1] of { mtype };
chan scenario_to_pdisk = [1] of { mtype };
/* BSC persistent state. */
mtype bsc_mood = MOOD_NORMAL;
byte bsc_vdisk_id = 0;
bool bsc_wipe_pending = false;
bool bsc_delete_pending = false;
bool bsc_vslot_allocated = true;
bool bsc_reported_wiped = false;
bool bsc_reported_destroyed = false;
/* NodeWarden volatile state for one TVSlotId. */
bool nw_local_vdisk = false;
bool nw_slay_inflight = false;
byte nw_current_vdisk_id = 0;
byte nw_slay_vdisk_id = 0;
mtype nw_slay_action = ACTION_NONE;
byte nw_slay_target_epoch = 0;
bool nw_pdisk_restart_inflight = false;
byte nw_round = 0;
/* Physical/runtime state. */
mtype pdisk_state = PDISK_RUNNING;
byte pdisk_epoch = 1;
byte pdisk_last_processed_epoch = 0;
byte pdisk_last_processed_round = 0;
bool vdisk_actor_running = false;
bool pdisk_owner_present = true;
/* Scenario markers are observations only; actor behavior does not depend on
 * them, except that the driver waits for externally visible completion. */
bool scenario_wipe_then_destroy = false;
bool first_slay_sent = false;
bool pdisk_restart_finished = false;
bool bsc_restart_finished = false;
bool node_restart_finished = false;
bool scenario_finished = false;
inline send_slay(vdisk_id) {
    nw_round++;
    nw_slay_target_epoch = pdisk_epoch;
    printf("[NodeWarden] send TEvSlay round=%d vdisk=%d pdiskEpoch=%d\n",
        nw_round, vdisk_id, nw_slay_target_epoch);
    nw_to_pdisk!SLAY(nw_round, vdisk_id);
    first_slay_sent = true
}
proctype BSC() {
    byte report_id;
    byte command_id;
    do
    :: scenario_to_bsc?START_WIPE(command_id) ->
        atomic {
            assert(bsc_vslot_allocated);
            bsc_mood = MOOD_WIPE;
            bsc_vdisk_id = command_id;
            bsc_wipe_pending = true;
            printf("[BSC] persist WIPE for allocated VSlot, vdisk=%d\n", command_id);
            bsc_to_nw!SERVICE_SET_WIPE(command_id);
        }
    :: scenario_to_bsc?START_DESTROY(command_id) ->
        atomic {
            assert(bsc_vslot_allocated);
            bsc_mood = MOOD_DELETE;
            bsc_vdisk_id = command_id;
            bsc_wipe_pending = false;
            bsc_delete_pending = true;
            printf("[BSC] persist DELETE for VSlot, vdisk=%d\n", command_id);
            bsc_to_nw!SERVICE_SET_DESTROY(command_id);
        }
    :: scenario_to_bsc?REQUEST_PDISK_RESTART(command_id) ->
        printf("[BSC] initiate PDisk restart and notify NodeWarden\n");
        bsc_to_nw!REQUEST_PDISK_RESTART(0)
    :: scenario_to_bsc?RESTART_BSC(command_id) ->
        printf("[BSC] restart; persistent VSlot operation survives\n");
        if
        :: bsc_vslot_allocated && bsc_delete_pending ->
            printf("[BSC] resync ServiceSet(DESTROY) after restart\n");
            bsc_to_nw!SERVICE_SET_DESTROY(bsc_vdisk_id)
        :: bsc_vslot_allocated && bsc_wipe_pending ->
            printf("[BSC] resync ServiceSet(WIPE) after restart\n");
            bsc_to_nw!SERVICE_SET_WIPE(bsc_vdisk_id)
        :: else -> skip
        fi;
        bsc_restart_finished = true

    :: nw_to_bsc?WIPED(report_id) ->
        atomic {
            assert(bsc_vslot_allocated); /* WIPE never frees a VSlotId. */
            if
            :: report_id != bsc_vdisk_id ->
                printf("[BSC] ignore obsolete WIPED identity vdisk=%d expected=%d\n",
                    report_id, bsc_vdisk_id)
            :: bsc_mood == MOOD_WIPE ->
                bsc_mood = MOOD_NORMAL;
                bsc_wipe_pending = false;
                bsc_reported_wiped = true;
                printf("[BSC] accept WIPED vdisk=%d; VSlot stays allocated\n", report_id)
            :: else ->
                /* DELETE superseded this WIPE. WIPED cannot complete DELETE. */
                printf("[BSC] ignore WIPED vdisk=%d while VSlot is deleting\n", report_id)
            fi
        }
    :: nw_to_bsc?DESTROYED(report_id) ->
        atomic {
            if
            :: report_id != bsc_vdisk_id ->
                printf("[BSC] ignore obsolete DESTROYED identity vdisk=%d expected=%d\n",
                    report_id, bsc_vdisk_id)
            :: bsc_mood == MOOD_DELETE && bsc_delete_pending ->
                assert(!pdisk_owner_present);
                bsc_delete_pending = false;
                bsc_vslot_allocated = false;
                bsc_reported_destroyed = true;
                printf("[BSC] accept DESTROYED vdisk=%d and free VSlotId\n", report_id)
            :: else ->
                printf("[BSC] ignore obsolete DESTROYED vdisk=%d\n", report_id)
            fi
        }
    :: nw_to_bsc?REGISTER_NODE(report_id) ->
        printf("[BSC] receive RegisterNode and replay persistent ServiceSet\n");
        if
        :: bsc_vslot_allocated && bsc_delete_pending ->
            bsc_to_nw!SERVICE_SET_DESTROY(bsc_vdisk_id)
        :: bsc_vslot_allocated && bsc_wipe_pending ->
            bsc_to_nw!SERVICE_SET_WIPE(bsc_vdisk_id)
        :: else -> skip
        fi
    :: nw_to_bsc?ASK_BSC_RESTART(report_id) ->
        printf("[BSC] authorize PDisk-requested restart and send ServiceSet(RESTART)\n");
        bsc_to_nw!REQUEST_PDISK_RESTART(0)
    od
}
proctype NodeWarden() {
    byte request_id;
    byte reply_round;
    byte reply_id;
    do
    :: bsc_to_nw?SERVICE_SET_WIPE(request_id) ->
        atomic {
            printf("[NodeWarden] receive ServiceSet(WIPE) vdisk=%d\n", request_id);
            nw_local_vdisk = true;
            nw_current_vdisk_id = request_id;
            if
            :: !nw_slay_inflight ->
                printf("[NodeWarden] poison VDisk for WIPE\n");
                nw_to_vdisk!POISON;
                nw_slay_inflight = true;
                nw_slay_action = ACTION_WIPE;
                nw_slay_vdisk_id = request_id;
                send_slay(request_id)
            :: else ->
                /* Repeated WIPE refreshes identity but never downgrades DELETE. */
                if
                :: nw_slay_action == ACTION_WIPE -> nw_slay_vdisk_id = request_id
                :: else -> skip
                fi;
                printf("[NodeWarden] merge WIPE with existing SlayInFlight\n")
            fi
        }
    :: bsc_to_nw?SERVICE_SET_DESTROY(request_id) ->
        atomic {
            printf("[NodeWarden] receive ServiceSet(DESTROY) vdisk=%d\n", request_id);
            nw_current_vdisk_id = request_id;
            if
            :: !nw_slay_inflight ->
                printf("[NodeWarden] poison VDisk and erase it from LocalVDisks\n");
                nw_to_vdisk!POISON;
                nw_slay_inflight = true;
                nw_slay_action = ACTION_DESTROY;
                nw_slay_vdisk_id = request_id;
                send_slay(request_id)
            :: else ->
                if
                :: nw_slay_action == ACTION_WIPE ->
                    /* DELETE supersedes WIPE: keep the new identity/action and
                     * issue a new round so the WIPE result becomes stale. */
                    nw_slay_action = ACTION_DESTROY;
                    nw_slay_vdisk_id = request_id;
                    printf("[NodeWarden] upgrade in-flight WIPE to DESTROY\n");
                    send_slay(nw_slay_vdisk_id)
                :: nw_slay_action == ACTION_DESTROY ->
                    nw_slay_vdisk_id = request_id;
                    printf("[NodeWarden] merge duplicate DESTROY\n")
                fi
            fi;
            nw_local_vdisk = false;
            vdisk_actor_running = false;
        }
    :: bsc_to_nw?REQUEST_PDISK_RESTART(request_id) ->
        atomic {
            printf("[NodeWarden] apply BSC-authorized ServiceSet(RESTART)\n");
            nw_pdisk_restart_inflight = true;
            restart_pdisk!REQUEST_PDISK_RESTART;
        }

    :: pdisk_to_nw?ASK_WARDEN_RESTART(reply_round, reply_id) ->
        printf("[NodeWarden] receive AskWardenRestartPDisk and ask BSC\n");
        nw_to_bsc!ASK_BSC_RESTART(0)
    :: atomic {
        pdisk_to_nw?PDISK_RESTARTED(reply_round, reply_id);
        if
        :: nw_pdisk_restart_inflight ->
            nw_pdisk_restart_inflight = false;
            if
            :: nw_slay_inflight && nw_slay_action != ACTION_NONE ->
                printf("[NodeWarden] PDisk restarted; replay action-aware TEvSlay\n");
                send_slay(nw_slay_vdisk_id)
            :: else ->
                printf("[NodeWarden] PDisk restarted; no slay needs replay\n")
            fi
            /* The request may already be processed and its result queued, so
             * mailbox length is not an invariant. An in-flight operation must
             * either have been processed by its target actor or be replayed to
             * the current PDisk incarnation. */
            assert(!nw_slay_inflight ||
                (pdisk_last_processed_epoch == nw_slay_target_epoch &&
                    pdisk_last_processed_round == nw_round) ||
                nw_slay_target_epoch == pdisk_epoch)
        :: else ->
            printf("[NodeWarden] ignore stale PDisk restart completion\n")
        fi
    }
    :: pdisk_to_nw?SLAY_OK(reply_round, reply_id) ->
        if
        :: nw_slay_inflight && reply_round == nw_round ->
            atomic {
                if
                :: nw_slay_action == ACTION_DESTROY ->
                    printf("[NodeWarden] accept TEvSlayResult round=%d and report DESTROYED\n", reply_round);
                    nw_to_bsc!DESTROYED(nw_slay_vdisk_id)
                :: nw_slay_action == ACTION_WIPE ->
                    printf("[NodeWarden] accept TEvSlayResult round=%d and report WIPED\n", reply_round);
                    nw_to_bsc!WIPED(nw_slay_vdisk_id);
                    if
                    :: nw_local_vdisk ->
                        vdisk_actor_running = true;
                        pdisk_owner_present = true
                    :: else -> skip
                    fi
                fi;
                nw_slay_action = ACTION_NONE;
                nw_slay_vdisk_id = 0;
                nw_slay_target_epoch = 0;
                nw_slay_inflight = false;
            }
        :: else ->
            printf("[NodeWarden] ignore stale TEvSlayResult round=%d expected=%d\n", reply_round, nw_round)
        fi

    :: scenario_to_nw?RESTART_NODE ->
        atomic {
            printf("[NodeWarden] full node restart; lose volatile state and register again\n");
            nw_local_vdisk = false;
            nw_current_vdisk_id = 0;
            nw_slay_inflight = false;
            nw_slay_vdisk_id = 0;
            nw_slay_action = ACTION_NONE;
            nw_slay_target_epoch = 0;
            nw_pdisk_restart_inflight = false;
            nw_round = 0;
            vdisk_actor_running = false;
            nw_to_bsc!REGISTER_NODE(0);
            node_restart_finished = true;
        }
    od
}
proctype VDisk() {
    do
    :: nw_to_vdisk?POISON ->
        printf("[VDisk] receive Poison and stop actor\n");
        vdisk_actor_running = false
    od
}
proctype PDisk() {
    byte request_round;
    byte request_id;
    do
    :: scenario_to_pdisk?REQUEST_SELF_RESTART ->
        printf("[PDisk] request restart from NodeWarden\n");
        pdisk_to_nw!ASK_WARDEN_RESTART(0, 0)
    :: restart_pdisk?REQUEST_PDISK_RESTART ->
        atomic {
            pdisk_state = PDISK_RESTARTING;
            printf("[PDisk] enter RESTARTING; destroy old actor mailbox\n");
            /* Every message already routed to this actor incarnation is lost.
             * Messages sent after the atomic restart target the new actor. */
            do
            :: len(nw_to_pdisk) > 0 ->
                nw_to_pdisk?SLAY(request_round, request_id);
                printf("[PDisk] drop old-incarnation TEvSlay round=%d vdisk=%d\n",
                    request_round, request_id)
            :: else -> break
            od;
            pdisk_epoch++;
            pdisk_state = PDISK_RUNNING;
            pdisk_restart_finished = true;
            printf("[PDisk] new actor is RUNNING; notify NodeWarden\n");
            pdisk_to_nw!PDISK_RESTARTED(0, 0);
        }
    :: atomic {
        pdisk_state == PDISK_RUNNING && len(nw_to_pdisk) > 0;
        nw_to_pdisk?SLAY(request_round, request_id);
        pdisk_last_processed_epoch = pdisk_epoch;
        pdisk_last_processed_round = request_round;
        if
        :: pdisk_owner_present ->
            printf("[PDisk] process TEvSlay round=%d vdisk=%d and remove owner\n",
                request_round, request_id);
            pdisk_owner_present = false
        :: !pdisk_owner_present ->
            printf("[PDisk] process TEvSlay round=%d vdisk=%d: owner already absent\n",
                request_round, request_id)
        fi;
        pdisk_to_nw!SLAY_OK(request_round, request_id);
    }
    od
}
proctype ScenarioDriver() {
    /* Both branches finish with DELETE, but one passes through WIPE first. */
    if
    :: scenario_wipe_then_destroy = false;
       printf("[Scenario] start direct DESTROY\n");
       scenario_to_bsc!START_DESTROY(2)
    :: scenario_wipe_then_destroy = true;
       printf("[Scenario] start WIPE before DESTROY\n");
       scenario_to_bsc!START_WIPE(1)
    fi;
    first_slay_sent;
    if
    :: scenario_wipe_then_destroy ->
       printf("[Scenario] supersede WIPE with DESTROY\n");
       scenario_to_bsc!START_DESTROY(2)
    :: !scenario_wipe_then_destroy -> skip
    fi;
    /* Scenario injects a restart request but does not choose whether the
     * running PDisk processes TEvSlay before that request wins the race. */
    if
    :: printf("[Scenario] BSC initiates ServiceSet(RESTART)\n");
       scenario_to_bsc!REQUEST_PDISK_RESTART(0)
    :: printf("[Scenario] PDisk requests restart through NodeWarden and BSC\n");
       scenario_to_pdisk!REQUEST_SELF_RESTART
    fi;
    pdisk_restart_finished;
    /* Explore no control-plane restart, either restart, and both orders. */
    if
    :: printf("[Scenario] no control-plane restart after PDisk restart\n")
    :: printf("[Scenario] restart BSC\n");
       scenario_to_bsc!RESTART_BSC(0);
       bsc_restart_finished
    :: printf("[Scenario] restart the whole NodeWarden node\n");
       scenario_to_nw!RESTART_NODE;
       node_restart_finished
    :: printf("[Scenario] restart BSC, then NodeWarden\n");
       scenario_to_bsc!RESTART_BSC(0);
       bsc_restart_finished;
       scenario_to_nw!RESTART_NODE;
       node_restart_finished
    :: printf("[Scenario] restart NodeWarden, then BSC\n");
       scenario_to_nw!RESTART_NODE;
       node_restart_finished;
       scenario_to_bsc!RESTART_BSC(0);
       bsc_restart_finished
    fi;
    scenario_finished = true;
    printf("[Scenario] injected failures are over\n");
}
init {
    atomic {
        run BSC();
        run NodeWarden();
        run VDisk();
        run PDisk();
        run ScenarioDriver();
    }
}
/* A pending DELETE must eventually release the BSC-side VSlotId. */
ltl live_delete_eventually_frees_vslot {
    [] (bsc_delete_pending -> <> !bsc_vslot_allocated)
}
