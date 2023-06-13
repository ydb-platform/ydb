LIBRARY()

SRCS(
    html_output.cpp
    www.cpp
)

RESOURCE(
    messagebus.js /messagebus.js
    bus-ico.png /bus-ico.png
)

PEERDIR(
    library/cpp/resource
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
