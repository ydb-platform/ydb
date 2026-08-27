/*
 * Reduced Block-2+1 VPatch model with the BSQueue failure contract.
 *
 * One logical queue request has the following outcomes:
 *
 *   WAITING -- reject/disconnect-before-accept --> terminal ERROR
 *   WAITING -- TRYLATER -----------------------> WAITING
 *   WAITING -- accept + response --------------> terminal OK/ERROR
 *   WAITING -- accept + disconnect ------------> terminal ERROR
 *
 * In the last case the VDisk operation may still finish.  The accepted
 * request is never retried, and its late response belongs to the stale queue
 * session.  This is the important at-most-once/ambiguous-outcome behaviour of
 * BSQueue's ResetConnection() + Drain().
 *
 * Blob bytes are abstracted to target_written[].  Both VPatch and fallback
 * write the same target value, so a late VPatch VPut after fallback is safe.
 * A finite parity timeout and a terminating fallback are explicit assumptions.
 */

#define DATA_PARTS 2
#define PARITY_PART 2
#define TOTAL_PARTS 3

/* Start/Found completed successfully; it uses the same BSQueue lifecycle. */
#define DIFF_D0  0
#define DIFF_D1  1
#define DIFF_P   2
#define XOR_D0   3
#define XOR_D1   4
#define REQUEST_COUNT 5

#define Q_IDLE      0
#define Q_WAITING   1
#define Q_ACCEPTED  2
#define Q_TERMINAL  3

#define R_NONE   0
#define R_OK     1
#define R_ERROR  2

bool request_started;
bool client_replied;
bool client_ok;
bool vpatch_succeeded;
bool fallback_started;
bool fallback_succeeded;
byte client_reply_count;

bool sent[REQUEST_COUNT];
bool delivered[REQUEST_COUNT];
bool app_done[REQUEST_COUNT];
bool connection_kept[REQUEST_COUNT];
bool post_accept_disconnect[REQUEST_COUNT];
bool stale_result_dropped[REQUEST_COUNT];
byte q_state[REQUEST_COUNT];
byte q_result[REQUEST_COUNT];
byte app_result[REQUEST_COUNT];
byte accept_count[REQUEST_COUNT];

bool target_written[TOTAL_PARTS];
bool vpatch_data_written[DATA_PARTS];
bool vpatch_parity_written;

bool parity_diff_received;
bool parity_dead;
bool xor_seen[DATA_PARTS];
byte xor_count;

#define all_diff_terminal \
    (q_state[DIFF_D0] == Q_TERMINAL && \
     q_state[DIFF_D1] == Q_TERMINAL && \
     q_state[DIFF_P] == Q_TERMINAL)

#define all_diff_ok \
    (q_result[DIFF_D0] == R_OK && \
     q_result[DIFF_D1] == R_OK && \
     q_result[DIFF_P] == R_OK)

#define any_diff_error \
    ((q_state[DIFF_D0] == Q_TERMINAL && q_result[DIFF_D0] == R_ERROR) || \
     (q_state[DIFF_D1] == Q_TERMINAL && q_result[DIFF_D1] == R_ERROR) || \
     (q_state[DIFF_P] == Q_TERMINAL && q_result[DIFF_P] == R_ERROR))

#define all_target_written \
    (target_written[0] && target_written[1] && target_written[2])

#define all_sent_terminal \
    ((!sent[0] || q_state[0] == Q_TERMINAL) && \
     (!sent[1] || q_state[1] == Q_TERMINAL) && \
     (!sent[2] || q_state[2] == Q_TERMINAL) && \
     (!sent[3] || q_state[3] == Q_TERMINAL) && \
     (!sent[4] || q_state[4] == Q_TERMINAL))

#define every_accept_at_most_once \
    (accept_count[0] <= 1 && accept_count[1] <= 1 && \
     accept_count[2] <= 1 && accept_count[3] <= 1 && \
     accept_count[4] <= 1)

#define stale_sessions_stay_terminal_error \
    ((!post_accept_disconnect[0] || \
        (q_state[0] == Q_TERMINAL && q_result[0] == R_ERROR)) && \
     (!post_accept_disconnect[1] || \
        (q_state[1] == Q_TERMINAL && q_result[1] == R_ERROR)) && \
     (!post_accept_disconnect[2] || \
        (q_state[2] == Q_TERMINAL && q_result[2] == R_ERROR)) && \
     (!post_accept_disconnect[3] || \
        (q_state[3] == Q_TERMINAL && q_result[3] == R_ERROR)) && \
     (!post_accept_disconnect[4] || \
        (q_state[4] == Q_TERMINAL && q_result[4] == R_ERROR)))

#define completed_stale_results_are_dropped \
    ((!post_accept_disconnect[0] || !app_done[0] || stale_result_dropped[0]) && \
     (!post_accept_disconnect[1] || !app_done[1] || stale_result_dropped[1]) && \
     (!post_accept_disconnect[2] || !app_done[2] || stale_result_dropped[2]) && \
     (!post_accept_disconnect[3] || !app_done[3] || stale_result_dropped[3]) && \
     (!post_accept_disconnect[4] || !app_done[4] || stale_result_dropped[4]))

inline finish_with_fallback()
{
    atomic {
        assert(!client_replied);
        fallback_started = true;
        if
        :: fallback_succeeded = true;
           target_written[0] = true;
           target_written[1] = true;
           target_written[2] = true;
           client_ok = true
        :: fallback_succeeded = false;
           client_ok = false
        fi;
        client_reply_count++;
        assert(client_reply_count == 1);
        client_replied = true
    }
}

inline maybe_finish_parity()
{
    atomic {
        if
        :: (!parity_dead && parity_diff_received
                && xor_count == DATA_PARTS && !app_done[DIFF_P]) ->
            assert(xor_seen[0] && xor_seen[1]);
            if
            :: target_written[PARITY_PART] = true;
               vpatch_parity_written = true;
               if
               :: connection_kept[DIFF_P] ->
                   app_result[DIFF_P] = R_OK;
                   app_done[DIFF_P] = true;
                   q_result[DIFF_P] = R_OK;
                   q_state[DIFF_P] = Q_TERMINAL
               :: post_accept_disconnect[DIFF_P] ->
                   app_result[DIFF_P] = R_OK;
                   stale_result_dropped[DIFF_P] = true;
                   app_done[DIFF_P] = true
               fi
            :: if
               :: connection_kept[DIFF_P] ->
                   app_result[DIFF_P] = R_ERROR;
                   app_done[DIFF_P] = true;
                   q_result[DIFF_P] = R_ERROR;
                   q_state[DIFF_P] = Q_TERMINAL
               :: post_accept_disconnect[DIFF_P] ->
                   app_result[DIFF_P] = R_ERROR;
                   stale_result_dropped[DIFF_P] = true;
                   app_done[DIFF_P] = true
               fi
            fi
        :: else ->
            skip
        fi
    }
}

/*
 * Submit one logical BSQueue item. TRYLATER is a pre-accept stutter step, so a
 * finite sequence of TRYLATER replies collapses to the final choice below.
 */
inline send_request(id)
{
    atomic {
        assert(!sent[id]);
        sent[id] = true;
        q_state[id] = Q_WAITING;
        if
        :: /* NOTREADY, deadline, undelivered, or disconnect before acceptance. */
           q_result[id] = R_ERROR;
           q_state[id] = Q_TERMINAL

        :: q_state[id] = Q_ACCEPTED;
           assert(accept_count[id] == 0);
           accept_count[id]++;
           delivered[id] = true;
           if
           :: connection_kept[id] = true
           :: /* Drain returns ERROR; the accepted operation keeps running. */
              post_accept_disconnect[id] = true;
              q_result[id] = R_ERROR;
              q_state[id] = Q_TERMINAL
           fi
        fi;
        assert(accept_count[id] <= 1)
    }
}

/* Deliver or discard the application result according to the queue session. */
inline complete_request(id, status)
{
    if
    :: connection_kept[id] ->
        assert(q_state[id] == Q_ACCEPTED);
        app_result[id] = status;
        app_done[id] = true;
        q_result[id] = status;
        q_state[id] = Q_TERMINAL
    :: post_accept_disconnect[id] ->
        assert(q_state[id] == Q_TERMINAL);
        assert(q_result[id] == R_ERROR);
        app_result[id] = status;
        stale_result_dropped[id] = true;
        app_done[id] = true
    fi
}

proctype DataDiffApp(byte id; byte xor_id; byte part)
{
end_wait_for_delivery:
    (delivered[id]);

    /* Data actors submit XOR before their own VPut result is known. */
    send_request(xor_id);
    if
    :: atomic {
           target_written[part] = true;
           vpatch_data_written[part] = true;
           complete_request(id, R_OK)
       }
    :: atomic {
           complete_request(id, R_ERROR)
       }
    fi;

done:
    skip
}

proctype ParityDiffApp()
{
end_wait_for_delivery:
    (delivered[DIFF_P]);

    parity_diff_received = true;
    maybe_finish_parity();

done:
    skip
}

proctype XorApp(byte id; byte source)
{
end_wait_for_delivery:
    (delivered[id]);

    atomic {
        if
        :: parity_dead ->
            complete_request(id, R_ERROR)
        :: else ->
            assert(!xor_seen[source]);
            xor_seen[source] = true;
            xor_count++;
            assert(xor_count <= DATA_PARTS);
            complete_request(id, R_OK)
        fi
    };
    maybe_finish_parity();

done:
    skip
}

/* A finite VPatch actor deadline: a parity actor cannot wait for XOR forever. */
proctype ParityTimeout()
{
end_wait_for_diff:
    (parity_diff_received);

    atomic {
        if
        :: !app_done[DIFF_P] ->
            parity_dead = true;
            complete_request(DIFF_P, R_ERROR)
        :: app_done[DIFF_P] ->
            skip
        fi
    };

done:
    skip
}

proctype DSProxy()
{
    request_started = true;
    send_request(DIFF_D0);
    send_request(DIFF_D1);
    send_request(DIFF_P);

    do
    :: all_diff_terminal && all_diff_ok ->
        atomic {
            assert(all_target_written);
            assert(!client_replied);
            vpatch_succeeded = true;
            client_ok = true;
            client_reply_count++;
            assert(client_reply_count == 1);
            client_replied = true
        };
        break
    :: any_diff_error ->
        /* Production starts fallback without stopping other VPatch actors. */
        finish_with_fallback();
        break
    od;

done:
    skip
}

init
{
    atomic {
        run DSProxy();

        run DataDiffApp(DIFF_D0, XOR_D0, 0);
        run DataDiffApp(DIFF_D1, XOR_D1, 1);
        run ParityDiffApp();
        run XorApp(XOR_D0, 0);
        run XorApp(XOR_D1, 1);
        run ParityTimeout()
    }
}

ltl safe_ok_requires_correct_target {
    [] (client_ok -> all_target_written)
}

ltl safe_vpatch_ok_requires_all_writes {
    [] (vpatch_succeeded ->
        (vpatch_data_written[0] && vpatch_data_written[1]
            && vpatch_parity_written))
}

ltl safe_parity_requires_unique_xors {
    [] (vpatch_parity_written ->
        (parity_diff_received && xor_seen[0] && xor_seen[1]
            && xor_count == DATA_PARTS))
}

ltl safe_single_client_reply {
    [] (client_reply_count <= 1)
}

ltl safe_queue_accepts_at_most_once {
    [] (every_accept_at_most_once)
}

ltl safe_stale_results_not_forwarded {
    [] (stale_sessions_stay_terminal_error
        && completed_stale_results_are_dropped)
}

ltl live_client_eventual_reply {
    [] (request_started -> <> client_replied)
}

ltl live_sent_requests_terminal {
    [] (request_started -> <> (client_replied && all_sent_terminal))
}
