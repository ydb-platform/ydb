LIBRARY()

RESOURCE(
    static/css/bootstrap.min.css schlab/css/bootstrap.min.css
    static/css/scharm.css schlab/css/scharm.css
    static/css/schviz.css schlab/css/schviz.css
    static/js/jquery.min.js schlab/js/jquery.min.js
    static/js/bootstrap.min.js schlab/js/bootstrap.min.js
    static/js/d3-tip-0.8.0-alpha.1.js schlab/js/d3-tip-0.8.0-alpha.1.js
    static/js/d3.v4.min.js schlab/js/d3.v4.min.js
    static/js/scharm.js schlab/js/scharm.js
    static/js/schviz.js schlab/js/schviz.js
    static/js/scharm-test0.js schlab/js/scharm-test0.js
    static/scharm.html schlab/scharm.html
)

SRCS(
    mon.cpp
)

PEERDIR(
    library/cpp/html/pcdata
    library/cpp/monlib/dynamic_counters
    library/cpp/resource
    ydb/library/schlab/schemu
)

END()

RECURSE(
    static
    static/css
    static/js
    test
)
