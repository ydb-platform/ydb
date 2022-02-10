/*
 * This code is executed from document ready function
 * Also note that some variable are set from C++ code
 */

var x1 = $.url("?x1");
var x2 = $.url("?x2");
var y1 = $.url("?y1");
var y2 = $.url("?y2");

var gantt = $.url("?gantt");
var out = $.url("?out");

var linesfill =    $.url("?linesfill")  == "y";
var linessteps =   $.url("?linessteps") == "y";
var linesshow =    $.url("?linesshow")  != "n";
var pointsshow =   $.url("?pointsshow") != "n";
var autoscale =    $.url("?autoscale")  == "y";
var legendshow =   $.url("?legendshow") != "n";
var cutts =        $.url("?cutts")      == "y";
var fullscreen =   $.url("?fullscreen") == "y";
var realnames =    $.url("?realnames")  == "y";
var xzoomoff  =    $.url("?xzoomoff")   == "y";
var yzoomoff  =    $.url("?yzoomoff")   == "y";

function inIframe() { try { return window.self !== window.top; } catch (e) { return true; } }
function inDashboard() { return fullscreen && inIframe(); }

// URL management
function makeUrl(url, queryFunc, hashFunc) {
    var query = $.url("?", url);
    var hash = $.url("#", url);
    if (paused) {
        query.paused = "y";
    }

    if (queryFunc) { queryFunc(query); }
    if (hashFunc) { hashFunc(hash); }
    var queryStr = "";
    var first = true;
    for (var k in query) {
        queryStr += (first? "?": "&") + k + "=" + encodeURIComponent(query[k]);
        first = false;
    }
    var hashStr = "";
    first = true;
    for (k in hash) {
        hashStr += (first? "#": "&") + k + "=" + encodeURIComponent(hash[k]);
        first = false;
    }
    return queryStr + hashStr;
}

function dataUrl() {
    return makeUrl(dataurl, function(query) {
        query.error = "text";
    });
}

// Error message popup
$("<div id='errmsg'></div>").css({
    position: "absolute",
    display: "none",
    border: "1px solid #faa",
    padding: "2px",
    "background-color": "#fcc",
    opacity: 0.80
}).appendTo("body");

if (out == "gantt") {
    $("#gantt-apply").click(function() {
        try {
            let val = $("#textareaGantt").val().replace(/\s/g, "");
            JSON.parse(val);
            window.location.replace(makeUrl($.url(), function(query) {
                query.gantt = val;
            }));
        } catch(e) {
            $("#errmsg").text("Not valid JSON: " + e)
            .css({bottom: "5px", left: "25%", width: "50%"})
            .fadeIn(200);
        }
    });

    if (gantt) {
        // Data fetching and auto-refreshing
        var fetchCounter = 0;
        function fetchData() {
            function onDataReceived(json, textStatus, xhr) {
                $("#errmsg").hide();
                logs = json;
                chart(config, logs, true);
            }

            function onDataError(xhr, error) {
                console.log(arguments);
                $("#errmsg").text("Fetch data error: " + error + (xhr.status == 200? xhr.responseText: ""))
                .css({bottom: "5px", left: "25%", width: "50%"})
                .fadeIn(200);
            }

            if (dataurl) {
                $.ajax({
                    url: dataUrl(),
                    type: "GET",
                    dataType: "json",
                    success: function (json, textStatus, xhr) { onDataReceived(json, textStatus, xhr); },
                    error: onDataError
                });
            } else {
                onDataReceived(datajson, "textStatus", "xhr");
            }

            // if (fetchPeriod > 0) {
            //     if (fetchCounter == 0) {
            //         fetchCounter++;
            //         setTimeout(function() {
            //             fetchCounter--;
            //             if (!paused) {
            //                 fetchData();
            //             }
            //         }, $("#errmsg").is(":visible")? errorPeriod: fetchPeriod);
            //     }
            // }
        }

        try {
            var config = JSON.parse(gantt);
            $("#textareaGantt").val(JSON.stringify(config, "\n", " "));
            let formHeight = 220;
            var chart = d3.gantt()
                .height(window.innerHeight - $("#placeholder")[0].getBoundingClientRect().y - window.scrollY - formHeight)
                .selector("#placeholder");
            var logs = null;
            fetchData();
        } catch(e) {
            $("#textareaGantt").val(gantt);
            alert("Not valid JSON: " + e);
        }
    }
} else { // flot
    // Special options adjustment for fullscreen charts in iframe (solomon dashboards)
    if (fullscreen) {
        navigate = false; // Disable navigation to avoid scrolling problems
        legendshow = false; // Show legend only on hover
    }

    // Adjust zooming options
    var xZoomRange = null;
    var yZoomRange = null;
    if (xzoomoff) {
        xZoomRange = false;
    }
    if (yzoomoff) {
        yZoomRange = false;
    }

    var placeholder =  $("#placeholder");
    var playback =     $("#playback");

    var data = null;
    var loaded_data = [];
    var imported_data = [];
    var fetchPeriod = refreshPeriod;
    var errorPeriod = 5000;
    var playbackPeriod = 500;
    var abstimestep = 1.0;

    var paused = $.url("?paused")  == "y";
    var timestep = abstimestep;

    var seriesDescription = {
        _time: "time",
        _thread: "thread"
    }

    function seriesDesc(name) {
        if (seriesDescription.hasOwnProperty(name)) {
            return (realnames? "[" + name + "] ": "") + seriesDescription[name];
        } else {
            return name;
        }
    }

    playback.show();

    var options = {
        series: {
            lines: { show: linesshow, fill: linesfill, steps: linessteps},
            points: { show: pointsshow },
            shadowSize: 0
        },
        xaxis: { zoomRange: xZoomRange },
        yaxis: { zoomRange: yZoomRange },
        grid: { clickable: true, hoverable: true },
        zoom: { interactive: navigate },
        pan: { interactive: navigate },
        legend: {
            show: legendshow,
            labelFormatter: function(label, series) { return seriesDesc(label) + '(' + seriesDesc(xn) + ')'; }
        }
    };

    if (fullscreen) {
        $("body").attr("class","body-fullscreen");
        $("#container").attr("class","container-fullscreen");
        $("#toolbar").attr("class","toolbar-fullscreen");
        $("#selectors-container").attr("class","toolbar-fullscreen");
        options.grid.margin = 0;
    }

    if (x1) { options.xaxis.min = x1; }
    if (x2) { options.xaxis.max = x2; }
    if (y1) { options.yaxis.min = y1; }
    if (y2) { options.yaxis.max = y2; }

    $("<div id='tooltip'></div>").css({
        position: "absolute",
        display: "none",
        border: "1px solid #fdd",
        padding: "2px",
        "background-color": "#fee",
        opacity: 0.80
    }).appendTo("body");

    // Helper to hide tooltip
    var lastShow = new Date();
    var hideCounter = 0;
    function hideTooltip() {
        if (hideCounter == 0) {
            hideCounter++;
            setTimeout(function() {
                hideCounter--;
                if (new Date().getTime() - lastShow.getTime() > 1000) {
                    $("#tooltip").fadeOut(200);
                } else if ($("#tooltip").is(":visible")) {
                    hideTooltip();
                }
            }, 200);
        }
    }

    // Helper to hide legend
    var legendLastShow = new Date();
    var legendHideCounter = 0;
    function hideLegend() {
        if (legendHideCounter == 0) {
            legendHideCounter++;
            setTimeout(function() {
                legendHideCounter--;
                if (new Date().getTime() - legendLastShow.getTime() > 1000) {
                    options.legend.show = false;
                    $.plot(placeholder, data, options);
                } else {
                    hideLegend();
                }
            }, 200);
        }
    }

    function onPlotClick(event, pos, item) {
        // Leave fullscreen on click
        var nonFullscreenUrl = makeUrl($.url(), function(query) {
            delete query.fullscreen;
        });
        if (inDashboard()) {
            window.open(nonFullscreenUrl, "_blank");
        } else if (fullscreen) {
            window.location.href = nonFullscreenUrl;
        }
    }

    function onPlotHover(event, pos, item) {
        var redraw = false;
        // Show legend on hover
        if (fullscreen) {
            legendLastShow = new Date();
            if (!options.legend.show) {
                redraw = true;
            }
            options.legend.show = true;
            hideLegend();
        }

        // Show names on hover
        var left = placeholder.position().left;
        var top = placeholder.position().top;
        var ttmargin = 10;
        var ttwidth = $("#tooltip").width();
        var ttheight = $("#tooltip").height();
        if (linessteps) {
            var pts = data[0].data;
            var idx = 0;
            for (var i = 0; i < pts.length - 1; i++) {
                var x1 = pts[i][0];
                var x2 = pts[i+1][0];
                if (pos.x >= x1 && (x2 == null || pos.x < x2)) {
                    idx = i;
                }
            }
            var n = pts[idx][2];
            var html = (n? "<u><b><center>" + n + "</center></b></u>": "") + "<table><tr><td align=\"right\">" + seriesDesc(xn) + "</td><td>: </td><td>" + pts[idx][3] + "</td></tr>";
            for (var sid = 0; sid < data.length; sid++) {
                var series = data[sid];
                var y = series.data[idx][4];
                html += "<tr><td align=\"right\">" + seriesDesc(series.label) + "</td><td>: </td><td>" + y + "</td></tr>";
            }
            html += "</table>";

            lastShow = new Date();
            if (pos.pageX < left + placeholder.width() && pos.pageX > left &&
                pos.pageY < top + placeholder.height() && pos.pageY > top) {
                $("#tooltip").html(html)
                .css({top: Math.max(top + ttmargin + 10, pos.pageY - ttheight - 20),
                    left: Math.max(left + ttmargin + 10, pos.pageX - ttwidth - 20)})
                .fadeIn(200, "swing", hideTooltip());
            }
        } else {
            if (item) {
                var idx = item.dataIndex;
                var n = item.series.data[idx][2];
                var x = item.series.data[idx][3]; //item.datapoint[0];
                var y = item.series.data[idx][4]; //item.datapoint[1];
                $("#tooltip").html((n? n + ": ": "") + seriesDesc(item.series.label) + " = " + y + ", " + seriesDesc(xn) + " = " + x)
                .css({top: Math.max(top + ttmargin + 10, item.pageY - ttheight - 20),
                    left: Math.max(left + ttmargin + 10, item.pageX - ttwidth - 20)})
                .fadeIn(200);
            } else {
                $("#tooltip").hide();
            }
        }

        // Redraw if required
        if (redraw) {
            $.plot(placeholder, data, options);
        }
    }

    // Add some more interactivity
    function onZoomOrPan(event, plot) {
        var axes = plot.getAxes();
        options.xaxis.min = axes.xaxis.min;
        options.xaxis.max = axes.xaxis.max;
        options.yaxis.min = axes.yaxis.min;
        options.yaxis.max = axes.yaxis.max;
    }

    // Bind to events
    placeholder.bind("plotclick", onPlotClick);
    placeholder.bind("plothover", onPlotHover);
    placeholder.bind("plotpan", onZoomOrPan);
    placeholder.bind("plotzoom", onZoomOrPan);

    // Data fetching and auto-refreshing
    var fetchCounter = 0;
    function fetchData() {
        function onDataReceived(json, textStatus, xhr) {
            $("#errmsg").hide();
            if (paused && logs) {
                return;
            }

            // Plot fetched data
            loaded_data = json;
            data = loaded_data.concat(imported_data);

            if (autoscale) {
                var xmin = null;
                var xmax = null;
                var ymin = null;
                var ymax = null;
                for (var sid = 0; sid < data.length; sid++) {
                    var pts = data[sid].data;
                    for (var i = 0; i < pts.length; i++) {
                        var x = pts[i][0];
                        var y = pts[i][1];
                        if (x != null && y != null) {
                            if (xmin == null || x < xmin) { xmin = x; }
                            if (xmax == null || x > xmax) { xmax = x; }
                            if (ymin == null || y < ymin) { ymin = y; }
                            if (ymax == null || y > ymax) { ymax = y; }
                        }
                    }
                }
                if (xmin != null) { options.xaxis.min = xmin; }
                if (xmax != null) { options.xaxis.max = xmax; }
                if (ymin != null) { options.yaxis.min = ymin; }
                if (ymax != null) { options.yaxis.max = ymax; }
            }
            $.plot(placeholder, data, options);
        }

        function onDataError(xhr, error) {
            console.log(arguments);
            $("#errmsg").text("Fetch data error: " + error + (xhr.status == 200? xhr.responseText: ""))
            .css({bottom: "5px", left: "25%", width: "50%"})
            .fadeIn(200);
        }

        if (dataurl) {
            $.ajax({
                url: dataUrl(),
                type: "GET",
                dataType: "json",
                success: function (json, textStatus, xhr) { onDataReceived(json, textStatus, xhr); },
                error: onDataError
            });
        } else {
            onDataReceived(datajson, "textStatus", "xhr");
        }

        if (fetchPeriod > 0) {
            if (fetchCounter == 0) {
                fetchCounter++;
                setTimeout(function() {
                    fetchCounter--;
                    if (!paused) {
                        fetchData();
                    }
                }, $("#errmsg").is(":visible")? errorPeriod: fetchPeriod);
            }
        }
    }

    // Playback control
    var pb_pause = $("#pb-pause");

    function setActive(btn, active) {
        if (active) {
            btn.addClass("btn-primary");
            btn.removeClass("btn-default");
        } else {
            btn.addClass("btn-default");
            btn.removeClass("btn-primary");
        }
    }

    function onPlaybackControl() {
        setActive(pb_pause, paused);
        fetchPeriod = refreshPeriod;
        fetchData();
    }

    function clickPause(val) {
        paused = val;
        onPlaybackControl();
    }

    pb_pause.click(function() { clickPause(!paused); });

    function addOptionButton(opt, btn, defOn) {
        setActive(btn, defOn ? $.url("?" + opt) != "n" : $.url("?" + opt) == "y");
        btn.click(function() {
            window.location.replace(makeUrl($.url(), function(query) {
                if (defOn) {
                    if ($.url("?" + opt) != "n") {
                        query[opt] = "n";
                    } else {
                        delete query[opt];
                    }
                } else {
                    if ($.url("?" + opt) == "y") {
                        delete query[opt];
                    } else {
                        query[opt] = "y";
                    }
                }
            }));
        });
    }

    // Options
    addOptionButton("linesfill", $("#pb-linesfill"), false);
    addOptionButton("linessteps", $("#pb-linessteps"), false);
    addOptionButton("linesshow", $("#pb-linesshow"), true);
    addOptionButton("pointsshow", $("#pb-pointsshow"), true);
    addOptionButton("legendshow", $("#pb-legendshow"), true);
    addOptionButton("cutts", $("#pb-cutts"), false);
    addOptionButton("autoscale", $("#pb-autoscale"), false);

    var pb_fullscreen = $("#pb-fullscreen");
    setActive(pb_fullscreen, fullscreen);
    pb_fullscreen.click(function(){
        window.location.href = makeUrl($.url(), function(query) {
            if (fullscreen) {
                delete query.fullscreen;
            } else {
                query.fullscreen = "y";
            }
        });
    });

    // Embeded mode (in case there is other stuff on page)
    function embededMode() {
        $("#pb-fullscreen").hide();
        $("#pb-autoscale").hide();
        $("#pb-legendshow").hide();
        $("#pb-pause").hide();
    }

    function enableSelection() {
        // Redraw chart with selection enabled
        options.selection = {mode: "x"};
        var plot = $.plot(placeholder, data, options);
        if ($.url("?sel_x1") && $.url("?sel_x2")) {
            plot.setSelection({
                xaxis: {
                    from: $.url("?sel_x1") ,
                    to: $.url("?sel_x2")
                }
            });
        }

        // Create selection hooks
        placeholder.bind("plotselected", function (event, ranges) {
            window.location.replace(makeUrl($.url(), function(query) {
                query.sel_x1 = ranges.xaxis.from;
                query.sel_x2 = ranges.xaxis.to;
            }));
        });
        placeholder.bind("plotunselected", function (event) {
            window.location.replace(makeUrl($.url(), function(query) {
                delete query.sel_x1;
                delete query.sel_x2;
            }));
        });

        // Init and show unselect button
        $("#pb-unselect").click(function () {
            plot.clearSelection();
        });
        $("#pb-unselect").removeClass("hidden");
    }

    // Export data
    var pb_export = $("#pb-export");
    pb_export.click(function(){
        var blob = new Blob([JSON.stringify(data)], {type: "application/json;charset=utf-8"});
        saveAs(blob, "exported.json");
    });

    // Import data
    $("#btnDoImport").click(function(){
        var files = document.getElementById('importFiles').files;
        if (files.length <= 0) {
        return false;
        }

        var fr = new FileReader();
        fr.onload = function(e) {
            imported_data = imported_data.concat(JSON.parse(e.target.result));
            data = loaded_data.concat(imported_data);
            $.plot(placeholder, data, options);
            $('#importModal').modal('hide');
        }
        fr.readAsText(files.item(0));
    });

    // Initialize stuff and fetch data
    onPlaybackControl();
}
