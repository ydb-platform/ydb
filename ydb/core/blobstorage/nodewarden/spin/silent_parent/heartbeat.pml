#ifndef CASE
#define CASE 1
#endif
#ifndef BUMP_COOKIE
#define BUMP_COOKIE 1
#endif
#ifndef CHECK_BINDING
#define CHECK_BINDING 1
#endif
#ifndef CHECK_RESPONSE_COOKIE
#define CHECK_RESPONSE_COOKIE 1
#endif
#ifndef CHECK_RESPONSE_ORIGIN
#define CHECK_RESPONSE_ORIGIN 1
#endif
#ifndef REQUIRE_SCEPTER
#define REQUIRE_SCEPTER 0
#endif

byte bindingNode = 1;
byte bindingCookie = 7;
byte bindingSession = 1;
byte capturedNode = 1;
byte capturedCookie = 7;
byte capturedSession = 1;
byte probeCookie = 1;
byte misses;
bool activeMonitor = true;
bool waiting = true;
bool poisoned;
byte poisonTarget;
bool aliveReplyAccepted;
bool ignoredReply;
bool ignoredDeadline;
bool stoppedStaleActor;
bool replyEventDone;
bool deadlineEventDone;
bool done;
byte requestsIssued = 1;

inline check_current() {
#if CHECK_BINDING
    if
    :: capturedNode != bindingNode || capturedCookie != bindingCookie || capturedSession != bindingSession ->
        activeMonitor = false;
        stoppedStaleActor = true
    :: else -> skip
    fi
#else
    skip
#endif
}

inline deadline(cookie) {
    check_current();
    if
    :: activeMonitor && cookie == probeCookie ->
        if
        :: waiting -> misses++
        :: else -> skip
        fi;
        if
        :: misses == 3 ->
            poisoned = true;
            poisonTarget = capturedSession;
            /* Poison only the still-current binding session, at miss three. */
            assert(poisonTarget == bindingSession);
            assert(capturedNode == bindingNode && capturedCookie == bindingCookie);
            activeMonitor = false
        :: else ->
            probeCookie++;
            requestsIssued++;
            waiting = true
        fi
    :: else -> ignoredDeadline = true
    fi
}

inline ok_reply(cookie, node, session, hasScepter) {
    check_current();
    if
    :: (activeMonitor && waiting
#if CHECK_RESPONSE_COOKIE
        && cookie == probeCookie
#endif
#if CHECK_RESPONSE_ORIGIN
        && node == capturedNode && session == capturedSession
#endif
#if REQUIRE_SCEPTER
        && hasScepter
#endif
        ) ->
        misses = 0;
        waiting = false;
        aliveReplyAccepted = true;
#if BUMP_COOKIE
        probeCookie++;
#endif
    :: else -> ignoredReply = true
    fi
}

proctype ReplyRace() {
    atomic {
        ok_reply(3, 1, 1, false);
        replyEventDone = true
    }
}

proctype DeadlineRace() {
    atomic {
        deadline(3);
        if
        :: aliveReplyAccepted -> assert(!poisoned && misses == 0)
        :: else -> skip
        fi;
        deadlineEventDone = true
    }
}

init {
#if CASE == 1
    /* A keeper disabled forever, while IC remains connected. */
    deadline(1);
    assert(misses == 1 && !poisoned);
    deadline(2);
    assert(misses == 2 && !poisoned);
    deadline(3);
    assert(misses == 3 && poisoned);
    done = true
#elif CASE == 2
    /* The success reset invalidates the old timer during the next interval. */
    misses = 2;
    ok_reply(1, 1, 1, false);
    assert(aliveReplyAccepted && misses == 0);
    deadline(1);
    assert(ignoredDeadline && misses == 0 && !poisoned && requestsIssued == 1);
    done = true
#elif CASE == 3
    misses = 2;
    bindingSession = 2;
    deadline(1);
    assert(stoppedStaleActor && !poisoned);
    done = true
#elif CASE == 4
    misses = 2;
    probeCookie = 3;
    ok_reply(1, 1, 1, false);
    assert(ignoredReply && misses == 2);
    deadline(3);
    assert(poisoned);
    done = true
#elif CASE == 5
    /* A working non-root parent is alive without a scepter. */
    misses = 2;
    ok_reply(1, 1, 1, false);
    assert(aliveReplyAccepted && misses == 0 && activeMonitor);
    done = true
#elif CASE == 6
    deadline(1);
    deadline(2);
    ok_reply(3, 1, 1, false);
    assert(aliveReplyAccepted && misses == 0 && !poisoned);
    deadline(3);
    assert(ignoredDeadline && !poisoned && misses == 0 && requestsIssued == 3);
    done = true
#elif CASE == 7
    misses = 2;
    ok_reply(1, 2, 1, false);
    assert(ignoredReply && misses == 2 && !aliveReplyAccepted);
    done = true
#elif CASE == 8
    misses = 2;
    ok_reply(1, 1, 2, false);
    assert(ignoredReply && misses == 2 && !aliveReplyAccepted);
    done = true
#elif CASE == 9
    misses = 2;
    bindingCookie = 8;
    deadline(1);
    assert(stoppedStaleActor && !poisoned);
    done = true
#elif CASE == 10
    atomic {
        misses = 2;
        probeCookie = 3;
        run ReplyRace();
        run DeadlineRace()
    };
    replyEventDone && deadlineEventDone;
    assert(!aliveReplyAccepted || !poisoned);
    done = true
#endif
}

ltl finishes { <> done }
ltl reached_poison { [] !poisoned }
ltl reached_alive_without_scepter { [] !aliveReplyAccepted }
