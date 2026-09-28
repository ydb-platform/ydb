#define STEP_COUNT 3

mtype = {
    PENDING, RUNNING, PASSED, FAILED, UNSUPPORTED, CANCELLED, LOST,
    ACTIVE, CANCELLING, COMPLETED, RUN_FAILED, RUN_CANCELLED, INFRA_FAILED,
    OUTCOME_NONE, OUTCOME_OK, OUTCOME_ERROR, OUTCOME_BAD_METRICS
};

mtype step_state[STEP_COUNT];
mtype run_state = ACTIVE;
mtype outcome[STEP_COUNT];

bool supported[STEP_COUNT];
bool process_live[STEP_COUNT];
bool completion_ready[STEP_COUNT];
bool metrics_valid[STEP_COUNT];
bool artifacts_durable[STEP_COUNT];
bool manifest_references_artifacts[STEP_COUNT];

byte attempt[STEP_COUNT];
byte completion_attempt[STEP_COUNT];
byte starts[STEP_COUNT];
byte total_starts = 0;
byte starts_when_cancelled = 0;

bool controller_alive = true;
bool crash_used = false;
bool cancel_requested = false;
bool cancel_durable = false;
bool storage_failed = false;
bool stale_completion_used = false;
bool continue_on_error;
byte current_step = 0;

#define RUN_TERMINAL (run_state == COMPLETED || run_state == RUN_FAILED || run_state == RUN_CANCELLED || run_state == INFRA_FAILED)
#define STEP_TERMINAL(s) (step_state[s] == PASSED || step_state[s] == FAILED || step_state[s] == UNSUPPORTED || step_state[s] == CANCELLED || step_state[s] == LOST)
#define CONTROLLER_HAS_WORK (current_step == STEP_COUNT || STEP_TERMINAL(current_step) || step_state[current_step] == PENDING || (step_state[current_step] == RUNNING && completion_ready[current_step]))

inline check_safety() {
    assert(starts[0] <= 1 && starts[1] <= 1 && starts[2] <= 1);
    assert(!cancel_durable || (starts[0] <= 1 && starts[1] <= 1 && starts[2] <= 1));
    assert(step_state[0] != PASSED || (metrics_valid[0] && artifacts_durable[0]));
    assert(step_state[1] != PASSED || (metrics_valid[1] && artifacts_durable[1]));
    assert(step_state[2] != PASSED || (metrics_valid[2] && artifacts_durable[2]));
    assert(!manifest_references_artifacts[0] || artifacts_durable[0]);
    assert(!manifest_references_artifacts[1] || artifacts_durable[1]);
    assert(!manifest_references_artifacts[2] || artifacts_durable[2]);
    assert(step_state[0] != UNSUPPORTED || starts[0] == 0);
    assert(step_state[1] != UNSUPPORTED || starts[1] == 0);
    assert(step_state[2] != UNSUPPORTED || starts[2] == 0);
    assert(!RUN_TERMINAL || (!process_live[0] && !process_live[1] && !process_live[2]));
}

inline advance_terminal_prefix() {
    do
    :: current_step < STEP_COUNT && STEP_TERMINAL(current_step) -> current_step++
    :: else -> break
    od
}

inline finish_remaining_as_cancelled() {
    byte i = 0;
    do
    :: i < STEP_COUNT ->
        if
        :: step_state[i] == PENDING -> step_state[i] = CANCELLED
        :: else -> skip
        fi;
        i++
    :: else -> break
    od
}

inline finish_run_if_possible() {
    advance_terminal_prefix();
    if
    :: current_step == STEP_COUNT ->
        if
        :: cancel_durable -> run_state = RUN_CANCELLED
        :: storage_failed -> run_state = INFRA_FAILED
        :: step_state[0] == LOST || step_state[1] == LOST || step_state[2] == LOST -> run_state = INFRA_FAILED
        :: step_state[0] == FAILED || step_state[1] == FAILED || step_state[2] == FAILED -> run_state = RUN_FAILED
        :: else -> run_state = COMPLETED
        fi
    :: else -> skip
    fi
}

active proctype Lifecycle() {
    byte s;

    step_state[0] = PENDING;
    step_state[1] = PENDING;
    step_state[2] = PENDING;
    outcome[0] = OUTCOME_NONE;
    outcome[1] = OUTCOME_NONE;
    outcome[2] = OUTCOME_NONE;
    supported[0] = true;
    supported[1] = true;
    supported[2] = false;

    if
    :: continue_on_error = false
    :: continue_on_error = true
    fi;

    do
    :: RUN_TERMINAL ->
        check_safety();
        break

    :: run_state == ACTIVE && controller_alive && !storage_failed && !cancel_durable && CONTROLLER_HAS_WORK ->
        advance_terminal_prefix();
        if
        :: current_step == STEP_COUNT -> finish_run_if_possible()
        :: else ->
            s = current_step;
            if
            :: step_state[s] == PENDING && !supported[s] ->
                step_state[s] = UNSUPPORTED
            :: step_state[s] == PENDING && supported[s] ->
                attempt[s]++;
                starts[s]++;
                total_starts++;
                step_state[s] = RUNNING;
                process_live[s] = true
            :: step_state[s] == RUNNING && completion_ready[s] ->
                if
                :: completion_attempt[s] != attempt[s] ->
                    completion_ready[s] = false;
                    assert(step_state[s] == RUNNING && process_live[s])
                :: cancel_durable ->
                    completion_ready[s] = false;
                    step_state[s] = CANCELLED
                :: completion_attempt[s] == attempt[s] && outcome[s] == OUTCOME_OK ->
                    metrics_valid[s] = true;
                    if
                    :: storage_failed = true
                    :: else ->
                        artifacts_durable[s] = true;
                        manifest_references_artifacts[s] = true;
                        step_state[s] = PASSED;
                        completion_ready[s] = false
                    fi
                :: completion_attempt[s] == attempt[s] && outcome[s] == OUTCOME_BAD_METRICS ->
                    step_state[s] = FAILED;
                    completion_ready[s] = false
                :: completion_attempt[s] == attempt[s] && outcome[s] == OUTCOME_ERROR ->
                    step_state[s] = FAILED;
                    completion_ready[s] = false
                fi;
                if
                :: step_state[s] == FAILED && !continue_on_error ->
                    finish_remaining_as_cancelled();
                    current_step = STEP_COUNT;
                    run_state = RUN_FAILED
                :: else -> skip
                fi
            :: else -> skip
            fi
        fi;
        check_safety()

    :: run_state == ACTIVE && current_step < STEP_COUNT && process_live[current_step] ->
        s = current_step;
        atomic {
            if
            :: outcome[s] = OUTCOME_OK
            :: outcome[s] = OUTCOME_ERROR
            :: outcome[s] = OUTCOME_BAD_METRICS
            fi;
            completion_attempt[s] = attempt[s];
            process_live[s] = false;
            completion_ready[s] = true
        };
        check_safety()

    :: run_state == ACTIVE && current_step < STEP_COUNT && process_live[current_step] &&
       !completion_ready[current_step] && !stale_completion_used ->
        stale_completion_used = true;
        completion_attempt[current_step] = 0;
        outcome[current_step] = OUTCOME_ERROR;
        completion_ready[current_step] = true

    :: run_state == ACTIVE && !crash_used && controller_alive ->
        crash_used = true;
        controller_alive = false;
        check_safety()

    :: run_state == ACTIVE && !controller_alive ->
        controller_alive = true;
        if
        :: current_step < STEP_COUNT && step_state[current_step] == RUNNING && process_live[current_step] ->
            skip
        :: current_step < STEP_COUNT && step_state[current_step] == RUNNING && completion_ready[current_step] ->
            skip
        :: current_step < STEP_COUNT && step_state[current_step] == RUNNING &&
           !process_live[current_step] && !completion_ready[current_step] ->
            step_state[current_step] = LOST
        :: else -> skip
        fi;
        check_safety()

    :: run_state == ACTIVE && !cancel_requested ->
        cancel_requested = true

    :: run_state == ACTIVE && cancel_requested && controller_alive && !cancel_durable && !storage_failed ->
        atomic {
            starts_when_cancelled = total_starts;
            cancel_durable = true;
            run_state = CANCELLING
        };
        check_safety()

    :: run_state == CANCELLING ->
        if
        :: current_step < STEP_COUNT && process_live[current_step] ->
            process_live[current_step] = false;
            completion_ready[current_step] = false;
            step_state[current_step] = CANCELLED
        :: current_step < STEP_COUNT && step_state[current_step] == RUNNING &&
           !process_live[current_step] ->
            completion_ready[current_step] = false;
            step_state[current_step] = CANCELLED
        :: else -> skip
        fi;
        finish_remaining_as_cancelled();
        current_step = STEP_COUNT;
        run_state = RUN_CANCELLED;
        check_safety()

    :: run_state == ACTIVE && !storage_failed ->
        storage_failed = true;

    :: run_state == ACTIVE && storage_failed ->
        if
        :: current_step < STEP_COUNT && process_live[current_step] ->
            process_live[current_step] = false;
            step_state[current_step] = LOST
        :: current_step < STEP_COUNT && step_state[current_step] == RUNNING &&
           !process_live[current_step] ->
            completion_ready[current_step] = false;
            step_state[current_step] = LOST
        :: else -> skip
        fi;
        finish_remaining_as_cancelled();
        current_step = STEP_COUNT;
        run_state = INFRA_FAILED;
        check_safety()
    od
}

ltl no_pass_without_durable_results {
    [] ((step_state[0] == PASSED -> metrics_valid[0] && artifacts_durable[0]) &&
        (step_state[1] == PASSED -> metrics_valid[1] && artifacts_durable[1]) &&
        (step_state[2] == PASSED -> metrics_valid[2] && artifacts_durable[2]))
}

ltl no_manifest_before_artifact {
    [] ((!manifest_references_artifacts[0] || artifacts_durable[0]) &&
        (!manifest_references_artifacts[1] || artifacts_durable[1]) &&
        (!manifest_references_artifacts[2] || artifacts_durable[2]))
}

ltl no_duplicate_start {
    [] (starts[0] <= 1 && starts[1] <= 1 && starts[2] <= 1)
}

ltl terminal_has_no_process {
    [] (RUN_TERMINAL -> (!process_live[0] && !process_live[1] && !process_live[2]))
}

ltl no_start_after_cancel {
    [] (cancel_durable -> total_starts == starts_when_cancelled)
}

ltl unsupported_never_started {
    [] ((step_state[0] == UNSUPPORTED -> starts[0] == 0) &&
        (step_state[1] == UNSUPPORTED -> starts[1] == 0) &&
        (step_state[2] == UNSUPPORTED -> starts[2] == 0))
}

ltl completed_is_clean {
    [] (run_state == COMPLETED ->
        step_state[0] != FAILED && step_state[0] != LOST &&
        step_state[1] != FAILED && step_state[1] != LOST &&
        step_state[2] != FAILED && step_state[2] != LOST)
}

ltl eventually_terminal {
    <> RUN_TERMINAL
}
