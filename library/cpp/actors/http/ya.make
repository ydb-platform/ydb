RECURSE_FOR_TESTS(ut) 
 
LIBRARY() 
 
OWNER(xenoxeno g:kikimr) 
 
SRCS( 
    http_cache.cpp 
    http_cache.h 
    http_config.h 
    http_proxy_acceptor.cpp 
    http_proxy_incoming.cpp 
    http_proxy_outgoing.cpp 
    http_proxy_sock_impl.h 
    http_proxy_ssl.h 
    http_proxy.cpp 
    http_proxy.h 
    http_static.cpp 
    http_static.h 
    http.cpp 
    http.h 
) 
 
PEERDIR( 
    contrib/libs/openssl 
    library/cpp/actors/core
    library/cpp/actors/interconnect
    library/cpp/dns
    library/cpp/monlib/metrics
    library/cpp/string_utils/quote
) 
 
END() 
