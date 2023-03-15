LIBRARY()

RESOURCE(
    static/common.css lwtrace/mon/static/common.css
    static/common.js lwtrace/mon/static/common.js
    static/css/bootstrap.min.css lwtrace/mon/static/css/bootstrap.min.css
    static/css/d3-gantt.css lwtrace/mon/static/css/d3-gantt.css
    static/css/jquery.treegrid.css lwtrace/mon/static/css/jquery.treegrid.css
    static/analytics.css lwtrace/mon/static/analytics.css
    static/analytics.flot.html lwtrace/mon/static/analytics.flot.html
    static/analytics.gantt.html lwtrace/mon/static/analytics.gantt.html
    static/analytics.header.html lwtrace/mon/static/analytics.header.html
    static/analytics.js lwtrace/mon/static/analytics.js
    static/fonts/glyphicons-halflings-regular.eot lwtrace/mon/static/fonts/glyphicons-halflings-regular.eot
    static/fonts/glyphicons-halflings-regular.svg lwtrace/mon/static/fonts/glyphicons-halflings-regular.svg
    static/fonts/glyphicons-halflings-regular.ttf lwtrace/mon/static/fonts/glyphicons-halflings-regular.ttf
    static/fonts/glyphicons-halflings-regular.woff2 lwtrace/mon/static/fonts/glyphicons-halflings-regular.woff2
    static/fonts/glyphicons-halflings-regular.woff lwtrace/mon/static/fonts/glyphicons-halflings-regular.woff
    static/footer.html lwtrace/mon/static/footer.html
    static/header.html lwtrace/mon/static/header.html
    static/img/collapse.png lwtrace/mon/static/img/collapse.png
    static/img/expand.png lwtrace/mon/static/img/expand.png
    static/img/file.png lwtrace/mon/static/img/file.png
    static/img/folder.png lwtrace/mon/static/img/folder.png
    static/js/bootstrap.min.js lwtrace/mon/static/js/bootstrap.min.js
    static/js/d3.v4.min.js lwtrace/mon/static/js/d3.v4.min.js
    static/js/d3-gantt.js lwtrace/mon/static/js/d3-gantt.js
    static/js/d3-tip-0.8.0-alpha.1.js lwtrace/mon/static/js/d3-tip-0.8.0-alpha.1.js
    static/js/filesaver.min.js lwtrace/mon/static/js/filesaver.min.js
    static/js/jquery.flot.extents.js lwtrace/mon/static/js/jquery.flot.extents.js
    static/js/jquery.flot.min.js lwtrace/mon/static/js/jquery.flot.min.js
    static/js/jquery.flot.navigate.min.js lwtrace/mon/static/js/jquery.flot.navigate.min.js
    static/js/jquery.flot.selection.min.js lwtrace/mon/static/js/jquery.flot.selection.min.js
    static/js/jquery.min.js lwtrace/mon/static/js/jquery.min.js
    static/js/jquery.treegrid.bootstrap3.js lwtrace/mon/static/js/jquery.treegrid.bootstrap3.js
    static/js/jquery.treegrid.min.js lwtrace/mon/static/js/jquery.treegrid.min.js
    static/js/jquery.url.min.js lwtrace/mon/static/js/jquery.url.min.js
)

SRCS(
    mon_lwtrace.cpp
)

PEERDIR(
    library/cpp/html/pcdata
    library/cpp/lwtrace
    library/cpp/lwtrace/mon/analytics
    library/cpp/monlib/dynamic_counters
    library/cpp/resource
    library/cpp/string_utils/base64
)

END()
