LIBRARY()

OWNER( 
    ddoarn 
    g:kikimr 
) 

SRCS(
    affinity.cpp
    affinity.h
    cpumask.h
    datetime.h
    defs.h
    funnel_queue.h
    futex.h
    intrinsics.h
    local_process_key.h
    named_tuple.h
    queue_chunk.h
    queue_oneone_inplace.h
    recentwnd.h
    rope.h
    should_continue.cpp
    should_continue.h
    thread.h
    threadparkpad.cpp
    threadparkpad.h
    ticket_lock.h
    timerfd.h
    unordered_cache.h
)

PEERDIR(
    util
)

END()
