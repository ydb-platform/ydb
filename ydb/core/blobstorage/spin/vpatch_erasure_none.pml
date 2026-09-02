/*
 * Failure-aware ErasureNone VPatch model for one DIFF request.
 *
 * START/FOUND has already completed with the only part found. DSProxy, the
 * client BSQueue, remote SkeletonFront, and the VDisk application remain
 * separate. NeverTag, queue-watchdog, and connection events race with the
 * normal result path.
 *
 * SkeletonFront opens its request window, receives the local VDisk completion,
 * closes the window, and only then publishes the result. A queue failure may
 * terminalize BSQueue first; the VDisk may still write while the old result is
 * stale or physically lost. Therefore ERROR does not imply no write.
 *
 * Local completion is reliable while its concrete SkeletonFront incarnation
 * is running. If PDiskError or restart is handled first, it retires the request
 * without closing the old private accounting; the late completion becomes
 * irrelevant. If completion is handled first, a later lifecycle failure
 * still resets the connection but does not undo accounting. FRONT_RESTART
 * summarizes destruction of the old Front and creation of a clean incarnation.
 * Payload bytes, retries, unrelated queue traffic, and fallback internals are
 * outside the model. A full quiet watchdog interval eventually elapses.
 * The one-item BSQueue projection hides events after item termination. Local
 * work and fallback terminate under weak process fairness. The post-completion
 * VPatchDyingRequest handshake is out of scope.
 */

#define CHANNEL_CAPACITY 1
#define FRONT_EVENT_CAPACITY 2
#define QUEUE_EVENT_CAPACITY 4
#define PROXY_EVENT_CAPACITY 2
#define FRONT_EVENTS_PER_REQUEST 2
#define REQUEST_EPOCH 0
#define RESTART_EPOCH 1
#define NO_EPOCH 2

mtype = {
    NO_RESULT,
    CLIENT_OK,
    CLIENT_ERROR,
    DIFF_REQUEST,
    VPATCH_OK,
    VPATCH_ERROR,
    NETWORK_DISCONNECTED,
    FRONT_DISCONNECTED,
    QUEUE_WATCHDOG,
    QUEUE_OK,
    QUEUE_ERROR,
    NEVER_WAKEUP,
    PDISK_ERROR,
    FRONT_RESTART,
    FRONT_STABLE,
    FRONT_RUNNING,
    FRONT_DB_ERROR,
    ARM_NEVER_WAKEUP,
    ARM_CONNECTION,
    ARM_QUEUE_WATCHDOG,
    ARM_FRONT_LIFECYCLE
};

/* Each point-to-point channel has the writer and reader in its name. */
chan proxy_to_queue = [CHANNEL_CAPACITY] of { mtype };
chan queue_to_front = [CHANNEL_CAPACITY] of { mtype };
chan front_to_app = [CHANNEL_CAPACITY] of { mtype, byte };
chan front_to_transport = [CHANNEL_CAPACITY] of { mtype };
chan front_to_connection = [CHANNEL_CAPACITY] of { mtype };
chan front_to_watchdog = [CHANNEL_CAPACITY] of { mtype };
chan front_to_lifecycle = [CHANNEL_CAPACITY] of { mtype };
chan proxy_to_never_wakeup = [CHANNEL_CAPACITY] of { mtype };

/* VDiskApp and FrontLifecycle race in the SkeletonFront mailbox. */
chan front_events = [FRONT_EVENT_CAPACITY] of { mtype, byte };

/* RemoteTransport, ConnectionFailure, QueueWatchdog, and SkeletonFront write once. */
chan queue_events = [QUEUE_EVENT_CAPACITY] of { mtype };

/* ClientBSQueue and VPatchNeverWakeup race in the DSProxy mailbox. */
chan proxy_events = [PROXY_EVENT_CAPACITY] of { mtype };

/* DSProxy owns the client lifecycle and fallback state. */
mtype client_result = NO_RESULT;
byte client_reply_count;
bool vpatch_succeeded;
bool fallback_started;

/* ClientBSQueue owns its one request item and one terminal result. */
bool queue_started;
bool queue_item_outstanding;
mtype queue_result = NO_RESULT;
byte queue_reply_count;
bool queue_drained_by_reset;
bool item_settled;

/* SkeletonFront owns epoch-0 history, immutable after request resolution. */
mtype front_state = FRONT_RUNNING;
byte front_epoch = REQUEST_EPOCH;
byte accounted_epoch = NO_EPOCH;
byte published_epoch = NO_EPOCH;
byte request_accept_history;
byte request_complete_history;
byte request_outstanding_history;
bool current_front_has_request;
bool front_result_published;
bool request_retired;
bool front_restarted;
bool old_completion_ignored;

/* FrontLifecycle is the sole writer. */
bool stable_front_selected;

/* VDiskApp owns app_done; it and fallback may write the same target. */
bool app_done;
bool target_written;

/* ConnectionFailure is the sole writer. */
bool disconnect_injected;

/* RemoteTransport is the sole writer. */
bool transport_result_dropped;

#define CLIENT_REPLIED (client_result != NO_RESULT)
#define ITEM_TERMINAL (queue_result != NO_RESULT)
#define REQUEST_ACCEPTED (request_accept_history == 1)
#define REQUEST_ACCOUNTED (request_complete_history == 1)
#define REQUEST_RETIRED (request_retired)
#define REQUEST_RESOLVED (REQUEST_ACCOUNTED || REQUEST_RETIRED)
#define LOCAL_COMPLETION_SETTLED \
    (REQUEST_ACCOUNTED || old_completion_ignored)

inline finish_client_with_fallback() {
    atomic {
        assert(!CLIENT_REPLIED);
        assert(client_reply_count == 0);
        fallback_started = true;
        if
        :: target_written = true;
            client_result = CLIENT_OK
        :: client_result = CLIENT_ERROR
        fi;
        client_reply_count++;
        assert(client_reply_count == 1)
    }
}

inline finish_queue(result) {
    atomic {
        assert(!ITEM_TERMINAL);
        assert(queue_item_outstanding);
        assert(queue_reply_count == 0);
        queue_result = result;
        queue_item_outstanding = false;
        queue_reply_count++;
        assert(queue_reply_count == 1)
    };

    if
    :: result == QUEUE_OK ->
        proxy_events!QUEUE_OK
    :: result == QUEUE_ERROR ->
        proxy_events!QUEUE_ERROR
    fi
}

inline finish_queue_after_reset() {
    atomic {
        assert(REQUEST_ACCEPTED);
        assert(!ITEM_TERMINAL);
        assert(queue_item_outstanding);
        assert(queue_reply_count == 0);
        queue_drained_by_reset = true;
        queue_result = QUEUE_ERROR;
        queue_item_outstanding = false;
        queue_reply_count++;
        assert(queue_reply_count == 1)
    };
    proxy_events!QUEUE_ERROR
}

inline handle_proxy_event(event) {
    if
    :: event == NEVER_WAKEUP ->
        finish_client_with_fallback()

    :: event == QUEUE_OK ->
        atomic {
            assert(queue_result == QUEUE_OK);
            assert(target_written);
            assert(REQUEST_ACCOUNTED);
            assert(client_reply_count == 0);
            vpatch_succeeded = true;
            client_result = CLIENT_OK;
            client_reply_count++;
            assert(client_reply_count == 1)
        }

    :: event == QUEUE_ERROR ->
        finish_client_with_fallback()
    fi
}

inline account_local_completion(completion, completion_epoch) {
    atomic {
        assert(completion == VPATCH_OK || completion == VPATCH_ERROR);
        assert(app_done);
        assert(front_state == FRONT_RUNNING);
        assert(completion_epoch == front_epoch);
        assert(completion_epoch == REQUEST_EPOCH);
        assert(!REQUEST_RETIRED);
        assert(request_complete_history == 0);
        assert(request_outstanding_history == 1);
        assert(current_front_has_request);
        request_complete_history++;
        request_outstanding_history--;
        current_front_has_request = false;
        accounted_epoch = completion_epoch;
        published_epoch = completion_epoch;
        front_result_published = true;
        front_to_transport!completion
    }
}

inline handle_front_failure(failure) {
    atomic {
        assert(failure == PDISK_ERROR || failure == FRONT_RESTART);
        assert(front_state == FRONT_RUNNING);
        assert(front_epoch == REQUEST_EPOCH);
        assert(REQUEST_ACCEPTED);
        assert(!REQUEST_RETIRED);

        if
        :: REQUEST_ACCOUNTED ->
            assert(request_outstanding_history == 0);
            assert(!current_front_has_request)
        :: else ->
            assert(request_outstanding_history == 1);
            assert(current_front_has_request);
            request_retired = true
        fi;

        if
        :: failure == PDISK_ERROR ->
            front_state = FRONT_DB_ERROR
        :: failure == FRONT_RESTART ->
            front_epoch = RESTART_EPOCH;
            current_front_has_request = false;
            front_restarted = true
        fi
    };

    /* DropConnection is asynchronous and may itself be lost in transport. */
    if
    :: queue_events!FRONT_DISCONNECTED
    :: skip
    fi
}

inline ignore_old_completion(completion, completion_epoch) {
    atomic {
        assert(completion == VPATCH_OK || completion == VPATCH_ERROR);
        assert(app_done);
        assert(REQUEST_RETIRED);
        assert(!REQUEST_ACCOUNTED);
        assert(completion_epoch == REQUEST_EPOCH);
        assert(request_outstanding_history == 1);
        assert(front_state == FRONT_DB_ERROR ||
            completion_epoch != front_epoch);
        if
        :: front_state == FRONT_DB_ERROR ->
            assert(current_front_has_request)
        :: completion_epoch != front_epoch ->
            assert(!current_front_has_request)
        fi;
        old_completion_ignored = true
    }
}

inline handle_local_completion(completion, completion_epoch) {
    if
    :: front_state == FRONT_RUNNING && completion_epoch == front_epoch &&
            !REQUEST_RETIRED ->
        account_local_completion(completion, completion_epoch)
    :: front_state == FRONT_DB_ERROR || completion_epoch != front_epoch ->
        ignore_old_completion(completion, completion_epoch)
    fi
}

proctype VDiskApp() {
    mtype request;
    mtype completion;
    byte request_epoch;

    /* Pre-accept rejection legitimately leaves the application idle. */
end_wait_for_diff:
    front_to_app?request, request_epoch;
    assert(request == DIFF_REQUEST);
    assert(request_epoch == REQUEST_EPOCH);

    /* ERROR is intentionally ambiguous about whether the write happened. */
    if
    :: atomic {
            target_written = true;
            app_done = true;
            completion = VPATCH_OK
        }
    :: atomic {
            app_done = true;
            completion = VPATCH_ERROR
        }
    :: atomic {
            target_written = true;
            app_done = true;
            completion = VPATCH_ERROR
        }
    fi;
    front_events!completion, request_epoch
}

proctype SkeletonFront() {
    mtype request;
    mtype event;
    byte event_epoch;
    byte events_handled;

    /* Pre-accept rejection legitimately leaves SkeletonFront idle. */
end_wait_for_diff:
    queue_to_front?request;
    assert(request == DIFF_REQUEST);

    atomic {
        assert(front_state == FRONT_RUNNING);
        assert(front_epoch == REQUEST_EPOCH);
        assert(request_accept_history == 0);
        assert(request_outstanding_history == 0);
        assert(!current_front_has_request);
        request_accept_history++;
        request_outstanding_history++;
        current_front_has_request = true;
        assert(request_outstanding_history == 1)
    };

    front_to_connection!ARM_CONNECTION;
    front_to_watchdog!ARM_QUEUE_WATCHDOG;
    front_to_app!DIFF_REQUEST, REQUEST_EPOCH;
    front_to_lifecycle!ARM_FRONT_LIFECYCLE;

    /* Completion and lifecycle changes race in the same Front mailbox. */
    do
    :: events_handled < FRONT_EVENTS_PER_REQUEST ->
        front_events?event, event_epoch;
        if
        :: event == VPATCH_OK || event == VPATCH_ERROR ->
            handle_local_completion(event, event_epoch)
        :: event == PDISK_ERROR || event == FRONT_RESTART ->
            assert(event_epoch == REQUEST_EPOCH);
            handle_front_failure(event)
        :: event == FRONT_STABLE ->
            assert(event_epoch == REQUEST_EPOCH)
        fi;
        assert(events_handled < FRONT_EVENTS_PER_REQUEST);
        events_handled++
    :: else ->
        break
    od
}

proctype FrontLifecycle() {
    mtype arm;

    /* A pre-accept rejection has no Front lifecycle event for this request. */
end_wait_for_acceptance:
    front_to_lifecycle?arm;
    assert(arm == ARM_FRONT_LIFECYCLE);

    if
    :: atomic {
            stable_front_selected = true
        };
        front_events!FRONT_STABLE, REQUEST_EPOCH
    :: front_events!PDISK_ERROR, REQUEST_EPOCH
    :: front_events!FRONT_RESTART, REQUEST_EPOCH
    fi
}

proctype RemoteTransport() {
    mtype result;

    /* Rejection or lifecycle-first retirement publishes no remote result. */
end_wait_for_result:
    front_to_transport?result;
    assert(result == VPATCH_OK || result == VPATCH_ERROR);

    atomic {
        assert(REQUEST_ACCOUNTED);
        assert(front_result_published);
        if
        :: disconnect_injected ->
            if
            :: transport_result_dropped = true
            /* An old-session result may arrive as a stale dead letter. */
            :: queue_events!result
            fi
        :: else ->
            queue_events!result
        fi
    }
}

proctype ConnectionFailure() {
    mtype arm;

    /* A pre-accept rejection has no post-accept connection failure. */
end_wait_for_acceptance:
    front_to_connection?arm;
    assert(arm == ARM_CONNECTION);

    if
    :: skip
    :: atomic {
            disconnect_injected = true;
            queue_events!NETWORK_DISCONNECTED
        }
    fi
}

proctype QueueWatchdog() {
    mtype arm;

    /* A pre-accept rejection has no in-flight item for this watchdog. */
end_wait_for_acceptance:
    front_to_watchdog?arm;
    assert(arm == ARM_QUEUE_WATCHDOG);
    queue_events!QUEUE_WATCHDOG
}

proctype ClientBSQueue() {
    mtype request;
    mtype event;

    proxy_to_queue?request;
    assert(request == DIFF_REQUEST);
    atomic {
        queue_started = true;
        queue_item_outstanding = true
    };

    if
    /* Includes queue-side failure before remote acceptance. */
    :: finish_queue(QUEUE_ERROR)

    /* The first remote result, disconnect, or watchdog event terminalizes. */
    :: queue_to_front!DIFF_REQUEST;
        queue_events?event;
        if
        :: event == VPATCH_OK ->
            finish_queue(QUEUE_OK)
        :: event == VPATCH_ERROR ->
            finish_queue(QUEUE_ERROR)
        :: event == NETWORK_DISCONNECTED || event == FRONT_DISCONNECTED ||
                event == QUEUE_WATCHDOG ->
            finish_queue_after_reset()
        fi
    fi;
    item_settled = true
}

proctype VPatchNeverWakeup() {
    mtype arm;

    proxy_to_never_wakeup?arm;
    assert(arm == ARM_NEVER_WAKEUP);
    proxy_events!NEVER_WAKEUP
}

proctype DSProxy() {
    mtype event;

    proxy_to_queue!DIFF_REQUEST;
    proxy_to_never_wakeup!ARM_NEVER_WAKEUP;

    /* The first queue result or NeverTag transition determines the strategy. */
    proxy_events?event;
    handle_proxy_event(event);
    assert(CLIENT_REPLIED)
}

ltl safe_client_ok_requires_target {
    [] (client_result == CLIENT_OK -> target_written)
}

ltl safe_vpatch_success_requires_accounted_write {
    [] (vpatch_succeeded ->
        (queue_result == QUEUE_OK && target_written &&
            REQUEST_ACCOUNTED && !fallback_started))
}

ltl safe_request_accounting_history_exact {
    [] (request_accept_history <= 1 &&
        request_complete_history <= request_accept_history &&
        request_outstanding_history <= 1 &&
        request_accept_history ==
            request_complete_history + request_outstanding_history)
}

ltl safe_front_publishes_after_accounting {
    [] (front_result_published ->
        (REQUEST_ACCOUNTED && !REQUEST_RETIRED &&
            request_outstanding_history == 0 &&
            accounted_epoch == REQUEST_EPOCH &&
            published_epoch == accounted_epoch))
}

ltl safe_retirement_is_not_completion {
    [] (REQUEST_RETIRED ->
        (!REQUEST_ACCOUNTED &&
            request_outstanding_history == 1 &&
            !front_result_published))
}

ltl safe_retired_completion_not_accounted {
    [] (old_completion_ignored ->
        (REQUEST_RETIRED && !REQUEST_ACCOUNTED &&
            request_outstanding_history == 1 && !front_result_published))
}

ltl safe_failure_has_exact_request_fate {
    [] ((front_state == FRONT_DB_ERROR || front_restarted) ->
        ((REQUEST_ACCOUNTED && !REQUEST_RETIRED &&
                request_outstanding_history == 0) ||
            (!REQUEST_ACCOUNTED && REQUEST_RETIRED &&
                request_outstanding_history == 1)))
}

ltl safe_completion_has_single_fate {
    [] !(REQUEST_ACCOUNTED && old_completion_ignored)
}

ltl safe_current_front_accounting_matches_lifecycle {
    [] ((REQUEST_ACCOUNTED -> !current_front_has_request) &&
        ((front_state == FRONT_DB_ERROR && REQUEST_RETIRED) ->
            current_front_has_request) &&
        (front_restarted ->
            (front_epoch == RESTART_EPOCH && !current_front_has_request)))
}

ltl safe_terminal_item_not_outstanding {
    [] (ITEM_TERMINAL -> !queue_item_outstanding)
}

ltl safe_reset_item_stays_terminal_error {
    [] (queue_drained_by_reset ->
        (queue_result == QUEUE_ERROR && !queue_item_outstanding))
}

ltl safe_transport_drop_not_forwarded {
    [] (transport_result_dropped ->
        (disconnect_injected && REQUEST_ACCOUNTED && !vpatch_succeeded))
}

ltl safe_single_request_replies {
    [] (client_reply_count <= 1 && queue_reply_count <= 1)
}

/* Liveness assumes weak process fairness and terminating local work/fallback. */
ltl live_eventual_client_reply {
    <> CLIENT_REPLIED
}

ltl live_accepted_request_resolves_or_retires {
    [] (REQUEST_ACCEPTED -> <> REQUEST_RESOLVED)
}

ltl live_stable_front_accounts_request {
    [] ((REQUEST_ACCEPTED && stable_front_selected) ->
        <> REQUEST_ACCOUNTED)
}

ltl live_local_completion_is_settled {
    [] (app_done -> <> LOCAL_COMPLETION_SETTLED)
}

ltl live_old_completion_becomes_irrelevant {
    [] (REQUEST_RETIRED -> <> old_completion_ignored)
}

ltl live_item_eventual_terminal {
    [] (queue_started -> <> ITEM_TERMINAL)
}

ltl live_retired_request_terminalizes_item {
    [] (REQUEST_RETIRED -> <> ITEM_TERMINAL)
}

ltl live_eventual_client_item_settlement {
    <> (CLIENT_REPLIED && ITEM_TERMINAL && item_settled &&
        (!REQUEST_ACCEPTED ||
            (REQUEST_RESOLVED && LOCAL_COMPLETION_SETTLED)))
}

init {
    /* Publish the complete actor set without startup interleaving. */
    atomic {
        run DSProxy();
        run ClientBSQueue();
        run SkeletonFront();
        run VDiskApp();
        run FrontLifecycle();
        run RemoteTransport();
        run ConnectionFailure();
        run QueueWatchdog();
        run VPatchNeverWakeup()
    }
}
