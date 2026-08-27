/*
 * Focused ForceEnd/BSQueue lifecycle model.
 *
 * The model keeps the two pieces of request accounting separate:
 *
 *   client BSQueue -> VDisk -> TEvVDiskRequestCompleted -> SkeletonFront
 *       -> interconnect response -> client BSQueue -> DSProxy
 *
 * SkeletonFront always closes the server-side request window.  The wrapper
 * controls whether it also forwards TEvVPatchResult to the client BSQueue.
 *
 * FORWARD_FORCE_END_RESULT=0 models SetForceEndResponse()/DoNotResend.
 * FORWARD_FORCE_END_RESULT=1 models forwarding the result while DSProxy still
 * ignores it semantically through ForceStopFlags.
 *
 * QueueEnvironment independently chooses whether another request is waiting
 * in the same queue.  Once SkeletonFront has completed the ForceEnd request,
 * it may produce successful unrelated progress (which moves the real
 * watchdog barrier) or a whole quiet watchdog interval.  The abstraction
 * assumes a forwarded response crosses the local queue before a fresh
 * watchdog interval can expire; only a suppressed response becomes a ghost.
 */

#ifndef FORWARD_FORCE_END_RESULT
#error "wrapper must define FORWARD_FORCE_END_RESULT"
#endif

mtype = {
    FORCE_END_REQUEST,
    FORCE_END_OK,
    FORCE_END_ERROR,
    VDISK_REQUEST_COMPLETED,
    QUEUE_PROGRESS,
    QUIET_WATCHDOG_INTERVAL,
    RESET_QUEUE
};

chan dsproxy_to_queue = [1] of { mtype };
chan queue_to_vdisk = [1] of { mtype };
chan vdisk_to_skeleton_front = [1] of { mtype };
chan skeleton_front_to_queue = [1] of { mtype };
/* Rendezvous prevents a progress event from being queued after watchdog exit. */
chan watchdog_events = [0] of { mtype };
chan watchdog_ack = [0] of { mtype };
chan watchdog_to_queue = [1] of { mtype };
chan queue_to_dsproxy = [1] of { mtype };

bool force_stop_flag;
bool force_end_sent;
bool force_end_inflight;
bool remote_actor_stopped;
bool server_window_completed;
bool response_forwarded;
bool response_received_by_queue;
bool response_suppressed;
bool force_end_terminal;
bool force_end_error;
bool watchdog_fired;
bool watchdog_reset;
bool environment_ready;
bool unrelated_request_waiting;
bool unrelated_request_completed;
bool unrelated_request_failed;
bool patch_request_replied;
bool transport_result_observed_by_dsproxy;
bool force_stop_result_ignored;
byte progress_epoch;
byte semantic_patch_results;

proctype DSProxy()
{
    /* SendStopDiffs sets ForceStopFlags before sending through BSQueue. */
    force_stop_flag = true;
    force_end_sent = true;
    dsproxy_to_queue!FORCE_END_REQUEST;

    /* Patch completion does not wait for a ForceEnd transport result. */
    patch_request_replied = true
}

proctype DSProxyResultHandler()
{
    mtype result;

end_wait_for_transport_result:
    queue_to_dsproxy?result;
    atomic {
        assert(result == FORCE_END_OK || result == FORCE_END_ERROR);
        if
        :: force_stop_flag ->
            /* dsproxy_patch.cpp returns before quorum/result accounting. */
            force_stop_result_ignored = true
        :: else ->
            semantic_patch_results++
        fi;
        transport_result_observed_by_dsproxy = true;
        assert(semantic_patch_results == 0)
    }
}

proctype ClientBSQueue()
{
    mtype request;
    mtype result;

    dsproxy_to_queue?request;
    assert(request == FORCE_END_REQUEST);
    force_end_inflight = true;
    queue_to_vdisk!request;

    if
    :: skeleton_front_to_queue?result ->
        assert(result == FORCE_END_OK);
        assert(server_window_completed);
        response_received_by_queue = true;
        force_end_terminal = true;
        force_end_inflight = false;
        queue_to_dsproxy!result

    :: watchdog_to_queue?RESET_QUEUE ->
        atomic {
            watchdog_reset = true;
            force_end_error = true;
            force_end_terminal = true;
            force_end_inflight = false;
            if
            :: unrelated_request_waiting ->
                unrelated_request_waiting = false;
                unrelated_request_failed = true
            :: else ->
                skip
            fi
        };
        queue_to_dsproxy!FORCE_END_ERROR
    fi
}

proctype VDisk()
{
    mtype request;

    queue_to_vdisk?request;
    assert(request == FORCE_END_REQUEST);
    remote_actor_stopped = true;

    /* FinalizeAndSend produces TEvVDiskRequestCompleted for SkeletonFront. */
    vdisk_to_skeleton_front!VDISK_REQUEST_COMPLETED
}

proctype SkeletonFront()
{
    mtype completion;

    vdisk_to_skeleton_front?completion;
    assert(completion == VDISK_REQUEST_COMPLETED);

    /* extQueue.Completed()/intQueue.Completed() happen in both variants. */
    server_window_completed = true;

#if FORWARD_FORCE_END_RESULT
    response_forwarded = true;
    skeleton_front_to_queue!FORCE_END_OK
#else
    /* DoNotResend consumes the event after server-side accounting. */
    response_suppressed = true
#endif
}

proctype QueueEnvironment()
{
    do
    :: force_end_inflight -> break
    :: force_end_terminal -> goto done
    od;

    /* An independent request may already share this queue. */
    if
    :: unrelated_request_waiting = true
    :: skip
    fi;
    environment_ready = true;

    (server_window_completed);
    do
    :: !force_end_inflight || watchdog_fired ->
        break
    :: response_suppressed && force_end_inflight && !watchdog_fired ->
        if
        :: watchdog_events!QUEUE_PROGRESS;
           watchdog_ack?QUEUE_PROGRESS
        :: watchdog_events!QUIET_WATCHDOG_INTERVAL;
           watchdog_ack?QUIET_WATCHDOG_INTERVAL
        fi
    od

done:
    skip
}

proctype QueueWatchdog()
{
    mtype event;

    do
    :: server_window_completed && environment_ready -> break
    :: force_end_terminal -> goto done
    od;
    do
    :: !force_end_inflight ->
        break
    :: watchdog_events?event ->
        if
        :: event == QUEUE_PROGRESS ->
            /* Any successful queue progress moves the watchdog barrier. */
            if
            :: progress_epoch == 0 -> progress_epoch = 1
            :: progress_epoch == 1 -> progress_epoch = 0
            fi;
            watchdog_ack!QUEUE_PROGRESS
        :: event == QUIET_WATCHDOG_INTERVAL ->
            watchdog_fired = true;
            watchdog_ack!QUIET_WATCHDOG_INTERVAL;
            watchdog_to_queue!RESET_QUEUE;
            break
        fi
    od

done:
    skip
}

proctype UnrelatedRequest()
{
    do
    :: environment_ready -> break
    :: force_end_terminal -> goto done
    od;
    if
    :: unrelated_request_waiting ->
        do
        :: unrelated_request_failed ->
            break
        :: !watchdog_fired ->
            unrelated_request_waiting = false;
            unrelated_request_completed = true;
            break
        od
    :: else ->
        skip
    fi

done:
    skip
}

init
{
    atomic {
        run DSProxy();
        run DSProxyResultHandler();
        run ClientBSQueue();
        run VDisk();
        run SkeletonFront();
        run QueueEnvironment();
        run QueueWatchdog();
        run UnrelatedRequest()
    }
}

ltl safe_force_end_stops_remote_actor {
    [] (server_window_completed -> remote_actor_stopped)
}

ltl safe_force_end_not_counted_for_quorum {
    [] (transport_result_observed_by_dsproxy ->
        (force_stop_result_ignored && semantic_patch_results == 0))
}

ltl safe_no_completed_request_left_inflight {
    [] (!(server_window_completed && response_suppressed
        && force_end_inflight))
}

ltl safe_no_collateral_queue_error {
    [] (!unrelated_request_failed)
}

ltl live_force_end_cleanly_releases_queue {
    [] (force_end_sent -> <>
        (force_end_terminal && response_received_by_queue
            && !watchdog_reset && !force_end_error))
}
