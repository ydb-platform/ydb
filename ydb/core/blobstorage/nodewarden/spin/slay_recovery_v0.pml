/*
 * Minimal model of one DESTROY operation for a VSlot.
 *
 * Components kept in the model:
 *   BSC        -- owns the persistent desired state and waits for DESTROYED;
 *   NodeWarden -- applies ServiceSet, poisons/removes VDisk, sends TEvSlay;
 *   VDisk      -- represents the local actor stopped by Poison;
 *   PDisk      -- consumes TEvSlay and sends TEvSlayResult;
 *   Scenario   -- injects one PDisk restart and optional control-plane restarts.
 *
 * The first TEvSlay is deliberately lost by a PDisk restart.  The restart is
 * chosen nondeterministically from the two paths present in production:
 *
 *   BSC -> ServiceSet(RESTART) -> NodeWarden -> PDisk
 *   PDisk -> AskWardenRestartPDisk -> NodeWarden -> RestartPDisk request to BSC
 *         -> ServiceSet(RESTART) -> NodeWarden -> PDisk
 *
 * After the loss, the model may also restart BSC, NodeWarden, both in either
 * order, or neither.  The environment is then reliable: every replayed slay
 * completes.  This is the weakest useful assumption for checking recovery.
 *
 * Version v0: old NodeWarden. Replay still depends on LocalVDisks.
 */
mtype = {
    SERVICE_SET_DESTROY,
    POISON,
    SLAY,
    SLAY_OK,
    DESTROYED,
    REGISTER_NODE,
    REQUEST_SELF_RESTART,
    ASK_WARDEN_RESTART,
    ASK_BSC_RESTART,
    REQUEST_PDISK_RESTART,
    PDISK_RESTARTED,
    RESTART_BSC,
    RESTART_NODE
};
/* Actor mailboxes are bounded because the model has one VSlot and one op. */
chan bsc_to_nw = [2] of { mtype };
chan nw_to_bsc = [2] of { mtype };
chan nw_to_vdisk = [1] of { mtype };
chan nw_to_pdisk = [1] of { mtype, byte };
chan pdisk_to_nw = [1] of { mtype, byte };
chan restart_pdisk = [1] of { mtype };
chan scenario_to_bsc = [1] of { mtype };
chan scenario_to_nw = [1] of { mtype };
chan scenario_to_pdisk = [1] of { mtype };
/* BSC database state survives a BSC process restart. */
bool bsc_desires_destroy = false;
bool bsc_pending = false;
bool bsc_reported_destroyed = false;
/* NodeWarden state for one TVSlotId. */
bool nw_local_vdisk = true;
bool nw_slay_inflight = false;
bool nw_has_slay_identity = false;
bool nw_pdisk_restart_inflight = false;
byte nw_round = 0;
/* Physical/runtime state. */
bool vdisk_actor_running = true;
bool pdisk_owner_present = true;
/* Scenario markers make the counterexample readable. */
bool first_slay_sent = false;
bool pdisk_restart_finished = false;
bool bsc_restart_finished = false;
bool node_restart_finished = false;
bool scenario_finished = false;
proctype BSC() {
    atomic {
        bsc_desires_destroy = true;
        bsc_pending = true;
        printf("[BSC] persist desired DESTROY and send ServiceSet(DESTROY)\n");
        bsc_to_nw!SERVICE_SET_DESTROY;
    }
    do
    :: nw_to_bsc?DESTROYED ->
        atomic {
            /* A successful report is impossible before the PDisk owner is
             * physically gone.  This is a local safety invariant. */
            assert(!pdisk_owner_present);
            bsc_desires_destroy = false;
            bsc_pending = false;
            bsc_reported_destroyed = true;
            printf("[BSC] receive DESTROYED, operation complete\n");
        }
    :: nw_to_bsc?REGISTER_NODE ->
        /* RegisterNode returns the persistent desired ServiceSet. */
        printf("[BSC] receive RegisterNode and replay desired ServiceSet\n");
        if
        :: bsc_desires_destroy -> bsc_to_nw!SERVICE_SET_DESTROY
        :: else -> skip
        fi
    :: nw_to_bsc?ASK_BSC_RESTART ->
        /* This models AskBSCToRestartPDisk/TRestartPDisk. */
        printf("[BSC] authorize PDisk-requested restart and send ServiceSet(RESTART)\n");
        bsc_to_nw!REQUEST_PDISK_RESTART
    :: scenario_to_bsc?REQUEST_PDISK_RESTART ->
        /* BSC may also initiate the same ServiceSet(RESTART) directly. */
        printf("[BSC] initiate PDisk restart and notify NodeWarden\n");
        bsc_to_nw!REQUEST_PDISK_RESTART
    :: scenario_to_bsc?RESTART_BSC ->
        /* Desired state is persistent; reconnect causes a resync. */
        printf("[BSC] restart; persistent desired DESTROY survives\n");
        if
        :: bsc_desires_destroy ->
            printf("[BSC] resync ServiceSet(DESTROY) after restart\n");
            bsc_to_nw!SERVICE_SET_DESTROY
        :: else -> skip
        fi;
        bsc_restart_finished = true
    od
}
proctype NodeWarden() {
    byte reply_round;
    do
    :: bsc_to_nw?SERVICE_SET_DESTROY ->
        printf("[NodeWarden] receive ServiceSet(DESTROY)\n");
        if
        :: !nw_slay_inflight ->
            atomic {
                printf("[NodeWarden] poison VDisk and erase it from LocalVDisks\n");
                nw_to_vdisk!POISON;
                nw_local_vdisk = false; /* old DESTROY erases LocalVDisks */
                nw_slay_inflight = true;
                nw_round++;
                /* Old SlayInFlight retains only owner round. */
                nw_has_slay_identity = false;
                printf("[NodeWarden] send TEvSlay round=%d\n", nw_round);
                nw_to_pdisk!SLAY(nw_round);
                first_slay_sent = true;
            }
        :: else ->
            /* Old Slay() and the fixed deduplication both avoid a duplicate
             * while the same operation is already considered in flight. */
            printf("[NodeWarden] ignore duplicate DESTROY because SlayInFlight exists\n");
            skip
        fi
    :: bsc_to_nw?REQUEST_PDISK_RESTART ->
        atomic {
            printf("[NodeWarden] apply BSC-authorized ServiceSet(RESTART)\n");
            nw_pdisk_restart_inflight = true;
            restart_pdisk!REQUEST_PDISK_RESTART;
        }
    :: pdisk_to_nw?ASK_WARDEN_RESTART(reply_round) ->
        /* A local PDisk request is forwarded to BSC; NodeWarden does not
         * bypass the controller and restart this PDisk on its own. */
        printf("[NodeWarden] receive AskWardenRestartPDisk and ask BSC\n");
        nw_to_bsc!ASK_BSC_RESTART
    :: atomic {
        pdisk_to_nw?PDISK_RESTARTED(reply_round);
        if
        :: nw_pdisk_restart_inflight ->
            nw_pdisk_restart_inflight = false;
            /* Old OnPDiskRestartFinished iterates LocalVDisks first.  A DESTROY
             * has already erased this record, so it cannot replay the slay. */
            if
            :: nw_local_vdisk && nw_slay_inflight ->
                nw_round++;
                printf("[NodeWarden] PDisk restarted; replay TEvSlay through LocalVDisks round=%d\n", nw_round);
                nw_to_pdisk!SLAY(nw_round)
            :: else ->
                printf("[ERROR] PDisk restarted, but DESTROY slay is orphaned: LocalVDisks=0 SlayInFlight=1 identity=0\n");
                skip
            fi
            /* Exact recovery invariant: after a successful restart callback,
             * every still in-flight slay must already have a new request in
             * the PDisk mailbox.  The old implementation violates this here. */
            assert(!nw_slay_inflight || len(nw_to_pdisk) > 0)
        :: else ->
            /* A full NodeWarden restart forgets the local restart request;
             * its delayed completion is stale and must not trigger replay. */
            printf("[NodeWarden] ignore PDisk restart completion without an in-flight request\n");
            skip
        fi
    }
    :: pdisk_to_nw?SLAY_OK(reply_round) ->
        if
        :: nw_slay_inflight && reply_round == nw_round ->
            atomic {
                printf("[NodeWarden] receive TEvSlayResult(OK) round=%d and report DESTROYED\n", reply_round);
                nw_slay_inflight = false;
                nw_has_slay_identity = false;
                nw_to_bsc!DESTROYED;
            }
        :: else ->
            printf("[NodeWarden] ignore stale TEvSlayResult round=%d expected=%d\n", reply_round, nw_round);
            skip
        fi
    :: scenario_to_nw?RESTART_NODE ->
        atomic {
            /* A full NodeWarden restart loses volatile SlayInFlight and the
             * in-flight local restart callback.  RegisterNode makes BSC
             * replay the still-persistent desired state. */
            printf("[NodeWarden] full node restart; lose volatile state and register again\n");
            nw_slay_inflight = false;
            nw_has_slay_identity = false;
            nw_pdisk_restart_inflight = false;
            nw_local_vdisk = false;
            nw_round = 0;
            nw_to_bsc!REGISTER_NODE;
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
    do
    :: scenario_to_pdisk?REQUEST_SELF_RESTART ->
        /* Production path: TEvAskWardenRestartPDisk, then controller auth. */
        printf("[PDisk] request restart from NodeWarden\n");
        pdisk_to_nw!ASK_WARDEN_RESTART(0)
    :: restart_pdisk?REQUEST_PDISK_RESTART ->
        /* The actor restart overtakes and drops its queued first TEvSlay. */
        nw_to_pdisk?SLAY(request_round);
        atomic {
            printf("[PDisk] restart drops queued TEvSlay round=%d\n", request_round);
            pdisk_restart_finished = true;
            printf("[PDisk] notify NodeWarden that restart finished\n");
            pdisk_to_nw!PDISK_RESTARTED(0);
        }
    :: pdisk_restart_finished && len(nw_to_pdisk) > 0 ->
        nw_to_pdisk?SLAY(request_round);
        atomic {
            printf("[PDisk] process TEvSlay round=%d and remove owner\n", request_round);
            pdisk_owner_present = false;
            printf("[PDisk] send TEvSlayResult(OK) round=%d\n", request_round);
            pdisk_to_nw!SLAY_OK(request_round);
        }
    od
}
proctype ScenarioDriver() {
    first_slay_sent;
    /* Explore both production restart origins. */
    if
    :: printf("[Scenario] BSC initiates ServiceSet(RESTART)\n");
       scenario_to_bsc!REQUEST_PDISK_RESTART
    :: printf("[Scenario] PDisk requests restart through NodeWarden and BSC\n");
       scenario_to_pdisk!REQUEST_SELF_RESTART
    fi;
    pdisk_restart_finished;
    /* Explore no control-plane restart, either restart, and both orders. */
    if
    :: printf("[Scenario] no control-plane restart after the loss\n")
    :: printf("[Scenario] restart BSC after the lost TEvSlay\n");
       scenario_to_bsc!RESTART_BSC;
       bsc_restart_finished
    :: printf("[Scenario] restart the whole NodeWarden node after the loss\n");
       scenario_to_nw!RESTART_NODE;
       node_restart_finished
    :: printf("[Scenario] restart BSC, then the whole NodeWarden node\n");
       scenario_to_bsc!RESTART_BSC;
       bsc_restart_finished;
       scenario_to_nw!RESTART_NODE;
       node_restart_finished
    :: printf("[Scenario] restart the whole NodeWarden node, then BSC\n");
       scenario_to_nw!RESTART_NODE;
       node_restart_finished;
       scenario_to_bsc!RESTART_BSC;
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
/* Eventual completion is a liveness property and cannot be reduced to a
 * point-in-time assert: it needs the fair infinite execution suffix. */
ltl live_destroy_eventually_reported {
    [] (scenario_finished && bsc_pending -> <> bsc_reported_destroyed)
}
