LIBRARY()


SRCS(
    css_mon_page.h
    fonts_mon_page.h
    js_mon_page.h
)

RESOURCE(
    static/css/bootstrap.min.css static/css/bootstrap.min.css
    static/fonts/glyphicons-halflings-regular.eot static/fonts/glyphicons-halflings-regular.eot
    static/fonts/glyphicons-halflings-regular.svg static/fonts/glyphicons-halflings-regular.svg
    static/fonts/glyphicons-halflings-regular.ttf static/fonts/glyphicons-halflings-regular.ttf
    static/fonts/glyphicons-halflings-regular.woff static/fonts/glyphicons-halflings-regular.woff
    static/js/bootstrap.min.js static/js/bootstrap.min.js
    static/js/jquery.min.js static/js/jquery.min.js
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
)

END()
