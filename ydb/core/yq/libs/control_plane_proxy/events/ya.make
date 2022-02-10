OWNER(g:yq)
 
LIBRARY() 
 
SRCS( 
    events.cpp 
) 
 
PEERDIR( 
    library/cpp/actors/core 
    library/cpp/actors/interconnect 
    ydb/core/yq/libs/control_plane_storage/events
) 
 
END() 
