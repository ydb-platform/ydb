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
 * Local actor delivery and a healthy SkeletonFront incarnation are assumed.
 * INJECT_LOCAL_COMPLETION_LOSS deliberately violates that contract and must
 * expose the accounting hang. Payload bytes, retries, node failure,
 * incarnation loss, unrelated queue traffic, and fallback internals are
 * outside the model. A full quiet watchdog interval eventually elapses.
 * Terminal actors do not wait for dead-letter events. Local work and fallback
 * terminate under weak process fairness.
 */

#define CHANNEL_CAPACITY 1
#define QUEUE_EVENT_CAPACITY 3
#define PROXY_EVENT_CAPACITY 2

mtype = {
    NO_RESULT,
    CLIENT_OK,
    CLIENT_ERROR,
    DIFF_REQUEST,
    VPATCH_OK,
    VPATCH_ERROR,
    CONNECTION_DROPPED,
    QUEUE_WATCHDOG,
    QUEUE_OK,
    QUEUE_ERROR,
    NEVER_WAKEUP,
    ARM_NEVER_WAKEUP,
    ARM_CONNECTION,
    ARM_QUEUE_WATCHDOG
};

/* Each point-to-point channel has the writer and reader in its name. */
chan proxy_to_queue = [CHANNEL_CAPACITY] of { mtype };
chan queue_to_front = [CHANNEL_CAPACITY] of { mtype };
chan front_to_app = [CHANNEL_CAPACITY] of { mtype };
chan app_to_front = [CHANNEL_CAPACITY] of { mtype };
chan front_to_transport = [CHANNEL_CAPACITY] of { mtype };
chan front_to_connection = [CHANNEL_CAPACITY] of { mtype };
chan front_to_watchdog = [CHANNEL_CAPACITY] of { mtype };
chan proxy_to_never_wakeup = [CHANNEL_CAPACITY] of { mtype };

/* One slot per writer keeps every atomic mailbox send nonblocking. */
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
bool post_accept_disconnect;
bool queue_settled;

/* SkeletonFront owns the remote request-window accounting. */
byte front_accept_count;
byte front_complete_count;
byte front_outstanding;
bool front_result_published;

/* VDiskApp owns app_done; it and fallback may write the same target. */
bool app_done;
bool target_written;

/* ConnectionFailure is the sole writer. */
bool disconnect_injected;

/* RemoteTransport is the sole writer. */
bool transport_result_dropped;

#define CLIENT_REPLIED (client_result != NO_RESULT)
#define QUEUE_TERMINAL (queue_result != NO_RESULT)
#define FRONT_ACCEPTED (front_accept_count == 1)
#define FRONT_ACCOUNTED (front_complete_count == 1)

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
        assert(!QUEUE_TERMINAL);
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

inline finish_queue_after_disconnect() {
    atomic {
        assert(FRONT_ACCEPTED);
        assert(!QUEUE_TERMINAL);
        assert(queue_item_outstanding);
        assert(queue_reply_count == 0);
        post_accept_disconnect = true;
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
            assert(FRONT_ACCOUNTED);
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

proctype VDiskApp() {
    mtype request;
    mtype completion;

    /* Pre-accept rejection legitimately leaves the application idle. */
end_wait_for_diff:
    front_to_app?request;
    assert(request == DIFF_REQUEST);

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

#ifdef INJECT_LOCAL_COMPLETION_LOSS
    if
    :: app_to_front!completion
    :: skip
    fi
#else
    app_to_front!completion
#endif
}

proctype SkeletonFront() {
    mtype request;
    mtype completion;

    /* Pre-accept rejection legitimately leaves SkeletonFront idle. */
end_wait_for_diff:
    queue_to_front?request;
    assert(request == DIFF_REQUEST);

    atomic {
        assert(front_accept_count == 0);
        assert(front_outstanding == 0);
        front_accept_count++;
        front_outstanding++;
        assert(front_outstanding == 1)
    };

    front_to_connection!ARM_CONNECTION;
    front_to_watchdog!ARM_QUEUE_WATCHDOG;
    front_to_app!DIFF_REQUEST;

    app_to_front?completion;
    atomic {
        assert(app_done);
        assert(front_complete_count == 0);
        assert(front_outstanding == 1);
        front_complete_count++;
        front_outstanding--;
        front_result_published = true
    };
    front_to_transport!completion
}

proctype RemoteTransport() {
    mtype result;

    /* Pre-accept rejection legitimately leaves the transport idle. */
end_wait_for_result:
    front_to_transport?result;
    assert(result == VPATCH_OK || result == VPATCH_ERROR);

    atomic {
        assert(FRONT_ACCOUNTED);
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
            queue_events!CONNECTION_DROPPED
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
    /* Pre-accept rejection or finite retries ending in a terminal ERROR. */
    :: finish_queue(QUEUE_ERROR)

    /* The first remote result, disconnect, or watchdog event terminalizes. */
    :: queue_to_front!DIFF_REQUEST;
        queue_events?event;
        if
        :: event == VPATCH_OK ->
            finish_queue(QUEUE_OK)
        :: event == VPATCH_ERROR || event == QUEUE_WATCHDOG ->
            finish_queue(QUEUE_ERROR)
        :: event == CONNECTION_DROPPED ->
            finish_queue_after_disconnect()
        fi
    fi;
    queue_settled = true
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
            FRONT_ACCOUNTED && !fallback_started))
}

ltl safe_front_accounting_exact {
    [] (front_accept_count <= 1 &&
        front_complete_count <= front_accept_count &&
        front_outstanding <= 1 &&
        front_accept_count == front_complete_count + front_outstanding)
}

ltl safe_front_publishes_after_accounting {
    [] (front_result_published ->
        (FRONT_ACCOUNTED && front_outstanding == 0))
}

ltl safe_queue_terminal_clears_item {
    [] (QUEUE_TERMINAL -> !queue_item_outstanding)
}

ltl safe_disconnect_stays_terminal_error {
    [] (post_accept_disconnect ->
        (queue_result == QUEUE_ERROR && !queue_item_outstanding))
}

ltl safe_transport_drop_not_forwarded {
    [] (transport_result_dropped ->
        (disconnect_injected && FRONT_ACCOUNTED && !vpatch_succeeded))
}

ltl safe_single_replies {
    [] (client_reply_count <= 1 && queue_reply_count <= 1)
}

/* Liveness assumes weak process fairness and terminating local work/fallback. */
ltl live_eventual_client_reply {
    <> CLIENT_REPLIED
}

ltl live_accepted_request_accounted {
    [] (FRONT_ACCEPTED -> <> FRONT_ACCOUNTED)
}

ltl live_queue_eventual_terminal {
    [] (queue_started -> <> QUEUE_TERMINAL)
}

ltl live_eventual_cleanup {
    <> (CLIENT_REPLIED && QUEUE_TERMINAL && queue_settled &&
        (!FRONT_ACCEPTED || FRONT_ACCOUNTED))
}

init {
    /* Publish the complete actor set without startup interleaving. */
    atomic {
        run DSProxy();
        run ClientBSQueue();
        run SkeletonFront();
        run VDiskApp();
        run RemoteTransport();
        run ConnectionFailure();
        run QueueWatchdog();
        run VPatchNeverWakeup()
    }
}
