LIBRARY()

OWNER(g:messagebus)

SRCS(
    html_output.cpp
    www.cpp
)

ARCHIVE(
    NAME www_static.inc
    messagebus.js
    bus-ico.png
)

PEERDIR(
    library/cpp/archive 
    library/cpp/cgiparam
    library/cpp/html/pcdata 
    library/cpp/http/fetch 
    library/cpp/http/server 
    library/cpp/json/writer 
    library/cpp/messagebus 
    library/cpp/messagebus/oldmodule 
    library/cpp/monlib/deprecated/json
    library/cpp/uri 
)

END()
