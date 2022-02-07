d3.scharm = function() {
    // main scharm object
    function scharm(newCfg) {
        createAll();

        // configure stuff using given config
        cfg = newCfg;

        var data = [];
        for (var i = 0; i < cfg.length; i++) {
            var row = [];
            for (var a = 0; a < attributes.length; a++) {
                if (attributes[a].type) {
                    row.push(cfg[i][attributes[a].name]);
                } else {
                    row.push(i); // save row index for remove operation
                }
            }
            data.push(row);
        }

        var tr = table.select("tbody").selectAll("tr.scharm-table-row")
          .data(data)
        ;
        tr.exit().remove();
        var trEnterUpd = tr.enter().append("tr")
            .attr("class", "scharm-table-row")
          .merge(tr) // enter + update
        ;

        var td = trEnterUpd.selectAll("td")
          .data(function(d) { return d; })
        ;
        td.enter().append("td")
          .merge(td) // enter + update
            .attr("contenteditable", function(d, i) { return attributes[i].type? "true": null; })
            .text(function(d, i) { return attributes[i].type? d: null; })
            .on('keypress', onKeyPress)
            .attr("class", function(d, i) {
                return "scharm-cell scharm-cell-" +
                    (attributes[i].type? attributes[i].type: attributes[i].class)
                ;
            })
        ;

        table.select("tbody").selectAll("td.scharm-cell-btn-remove")
            .each(function(d, i) {
                d3.select(this)
                    .html("") // clear
                  .append("button")
                    .html("&times;")
                    .attr("type", "button")
                    .attr("class", "btn btn-default btn-xs")
                    .on("click", function() {
                        scharm.removeRow(d);
                    })
                ;
                //d3.select(this).html('<button class="btn btn-default btn-xs">&times;</button>');
            })
        ;

        drawSeries();

        return scharm;
    }

    // private:

    var dataToCfg = function() {
        var curCfg = [];
        table.select("tbody").selectAll("tr.scharm-table-row")
            .each(function() {
                var row = {};
                d3.select(this).selectAll("td")
                    .each(function(d, i) {
                        if (attributes[i].type) {
                            if (isNumberType(attributes[i].type)) {
                                row[attributes[i].name] = +this.textContent;
                            } else {
                                row[attributes[i].name] = this.textContent;
                            }
                        }
                    })
                ;
                curCfg.push(row);
            })
        ;
        return curCfg;
    }

    var createAll = function() {
        if (table) {
            return; // ensure to create once
        }

        // create empty generators table
        table = d3.select(tableSelector).append("table")
            .attr("class", "table scharm-table")
        ;

        table.append("thead").append("tr").attr("class", "scharm-table-head")
          .selectAll("td")
          .data(attributes)
          .enter().append("th")
            .text(function(d) { return d.name; })
        ;

        table.append("tbody");

        // initialize modal dialog for raw editing
        $('#RawModal').on('shown.bs.modal', function (e) {
            scharm(dataToCfg());
            $('#cfg-text').val(JSON.stringify(cfg, null, " "));
        });
        $('#cfg-save').on('click', function (e) {
            $('#RawModal').modal('hide');
            var newCfg = JSON.parse($('#cfg-text').val());
            scharm(newCfg);
        });

        // initialize throughput editor
        d3.select("input#throughput").on('keypress', onKeyPress);

        // init axis
        x = d3.scaleLinear()
            .domain([timeDomainStartMs, timeDomainEndMs])
            .range([0, width]);
            //.clamp(true); // dosn't work with zoom/pan
        xZoomed = x;
        y = d3.scaleLinear()
            .domain([bytesDomainMax, 0])
            .range([0, height - margin.top - margin.bottom]);
        xAxis = d3.axisBottom()
            .scale(x)
            //.tickSubdivide(true)
            .tickSize(8)
            .tickPadding(8);
        yAxis = d3.axisLeft()
            .scale(y)
            .tickSize(8)
            .tickPadding(8);

        // create svg element
        svg = d3.select(chartSelector)
          .append("svg")
            .attr("class", "chart")
            .attr("width", width + margin.left + margin.right)
            .attr("height",height + margin.top + margin.bottom)
        ;

        // disable default page scrolling with mouse wheel
        // because wheel is used for zooming
        //window.onwheel = function(){ return false; }

        zoom = d3.zoom()
            .scaleExtent([1/10, 100])
            //.translateExtent([0, 0], [1000,0])
            .on("zoom", function() {
                var tr = d3.event.transform;
                xZoomed = tr.rescaleX(x);
                svg.select("g.x.axis").call(xAxis.scale(xZoomed));

                zoomContainer1.attr("transform", "translate(" + tr.x + ",0) scale(" + tr.k + ",1)");
                zoomContainer2.attr("transform", "translate(" + tr.x + ",0) scale(" + tr.k + ",1)");

                drawSeries();

                //tr(x);
            })
        ;

        svgChartContainer = svg.append('g')
            .attr("transform", "translate(" + margin.left + ", " + margin.top + ")")
        ;
        svgChart = svgChartContainer.append("svg")
            .attr("top", 0)
            .attr("left", 0)
            .attr("width", width)
            .attr("height", height)
            .attr("viewBox", "0 0 " + width + " " + height)
        ;

        zoomContainer1 = svgChart.append("g")
            .attr("width", width)
            .attr("height", height)
        ;

        zoomPanel = svgChart.append("rect")
            .attr("class", "zoom-panel")
            .attr("width", width)
            .attr("height", height)
            .call(zoom)
        ;

        zoomContainer2 = svgChart.append("g")
            .attr("width", width)
            .attr("height", height)
        ;

        // draw chart
        seriesSvg = zoomContainer1.append("g")
            .attr("class", "gantt-chart")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
        ;
        seriesSvg.append("path");

        // container for non-zoomable elements
        fixedContainer = svg.append("g")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
            .attr("transform", "translate(" + margin.left + ", " + margin.top + ")")
        ;

        // create x axis
        fixedContainer.append("g")
            .attr("class", "x axis")
            .attr("transform", "translate(0, " + (height - margin.top - margin.bottom) + ")")
            .call(xAxis)
          .append("text")
            .attr("fill", "#000")
            .attr("transform", "translate(0,10)")
            .attr("x", width / 2)
            .attr("y", 32)
            .attr("dy", "0.71em")
            .attr("text-anchor", "middle")
            .text("time (ms)");
        ;

        // create y axis
        fixedContainer.append("g")
            .attr("class", "y axis")
            .call(yAxis)
          .append("text")
            .attr("fill", "#000")
            .attr("y", -14)
            .attr("x", 10)
            .attr("dy", "0.71em")
            .attr("text-anchor", "end")
            .text("queue size (bytes)");
        ;

        // right margin
        var rmargin = fixedContainer.append("g")
            .attr("id", "right-margin")
            .attr("transform", "translate(" + width + ", 0)")
        ;
        rmargin.append("rect")
            .attr("x", 0)
            .attr("y", 0)
            .attr("width", 1)
            .attr("height", height - margin.top - margin.bottom)
        ;

        // top margin
        var tmargin = fixedContainer.append("g")
            .attr("id", "top-margin")
            .attr("transform", "translate(0, 0)")
        ;
        tmargin.append("rect")
            .attr("x", 0)
            .attr("y", 0)
            .attr("width", width)
            .attr("height", 1)
        ;

    }

    var drawSeries = function() {
        // generate series data
        var seriesEvents = [];
        var tLeft = xZoomed.invert(0);
        var tRight = xZoomed.invert(width);
        for (var i = 0; i < cfg.length; i++) {
            var gen = cfg[i];
            var period2 = Math.ceil((tRight - gen.startTime * timeUnit) / (gen.period * timeUnit)) + 1;
            var pe = Math.min(period2,
                gen.periodCount? gen.periodCount: period2);
            for (var p = 0; p < pe; p++) {
                for (var j = 0; j < gen.reqCount; j++) {
                    var t = gen.startTime * timeUnit
                        + p * gen.period * timeUnit
                        + j * gen.reqInterval * timeUnit;
                    var item = [t, gen];
                    seriesEvents.push(item);
                }
            }
        }
        seriesEvents.sort(function(a, b) {
            return a[0] - b[0];
        });

        var seriesData = [];
        var throughput = $("input#throughput").val() * 1024 * 1024 / 1000;
        var t = 0;
        var queueBytes = 0;
        var queueBytesMax = 0;

        var lastPoint = null;
        function addPoint(x, y) {
            if (x >= tLeft) {
                if (seriesData.length == 0 && lastPoint != null) {
                    seriesData.push(lastPoint);
                }
                seriesData.push([x, y]);
            } else {
                lastPoint = [x, y];
            }
        }

        for (var i = 0; i < seriesEvents.length; i++) {
            var e = seriesEvents[i];

            var t0 = t + queueBytes / throughput;
            var t1 = e[0];
            if (t0 < t1) {
                queueBytes = 0;
                t = t0;
                addPoint(t, queueBytes);
            }

            var dt = t1 - t;
            queueBytes = Math.max(0, queueBytes - throughput * dt);
            t = t1;
            addPoint(t, queueBytes);

            queueBytes += e[1].reqSizeBytes;
            addPoint(t, queueBytes);

            if (t > tLeft && queueBytesMax < queueBytes) {
                queueBytesMax = queueBytes;
            }
        }

        // add final point with zero bytes queued
        if (queueBytes > 0) {
            var t0 = t + queueBytes / throughput;
            queueBytes = 0;
            t = t0;
            addPoint(t, queueBytes);
        }

        // update y scale
        y.domain([queueBytesMax? queueBytesMax: bytesDomainMax,  0]);
        svg.select("g.y.axis").call(yAxis);

        var area = d3.area()
            .x(function(d) { return x(d[0]); })
            .y1(function(d) { return y(d[1]); })
            .y0(y(0));

        seriesSvg.select("path")
            .datum(seriesData)
            .attr("fill", "steelblue")
            .attr("d", area);
    }

    var onKeyPress = function() {
        if (d3.event.keyCode == 13) {
            d3.event.preventDefault();
            scharm(dataToCfg());
        }
    }

    // public:

    scharm.margin = function(value) {
        if (!arguments.length)
            return margin;
        margin = value;
        return scharm;
    }

    scharm.timeDomain = function(value) {
        if (!arguments.length)
            return [timeDomainStartMs, timeDomainEndMs];
        timeDomainStartMs = value[0];
        timeDomainEndMs = value[1];
        return scharm;
    }

    scharm.width = function(value) {
        if (!arguments.length)
            return width;
        width = +value;
        return scharm;
    }

    scharm.height = function(value) {
        if (!arguments.length)
            return height;
        height = +value;
        return scharm;
    }

    scharm.tableSelector = function(value) {
        if (!arguments.length)
            return tableSelector;
        tableSelector = value;
        return scharm;
    }

    scharm.chartSelector = function(value) {
        if (!arguments.length)
            return chartSelector;
        chartSelector = value;
        return scharm;
    }

    scharm.applyUrl = function(value) {
        if (!arguments.length)
            return applyUrl;
        applyUrl = value;
        return scharm;
    }

    scharm.addRow = function() {
        var newCfg = dataToCfg();
        var row = new Object(emptyRow);
        row.label = "gen" + newCfg.length;
        newCfg.push(row);
        scharm(newCfg);
    }

    scharm.removeRow = function(i) {
        var newCfg = dataToCfg();
        newCfg.splice(i, 1);
        scharm(newCfg);
    }

    // should be called to regenerate chart on buttons clicks/etc
    scharm.draw = function() {
        scharm(dataToCfg());
    }

    // should be called to POST current config
    scharm.sendCfg = function(callback) {
        scharm(dataToCfg());
        d3.json(applyUrl, callback).post(JSON.stringify(cfg));
    }
    // should be called to apply the current config
    scharm.applyCfg = function() {
        scharm(dataToCfg());
    }

    // constructor
    var attributes = [
        { name: "label",        type: "string" },
        { name: "startTime",    type: "time" },
        { name: "period",       type: "time" },
        { name: "periodCount",  type: "integer" },
        { name: "reqSizeBytes", type: "bytes" },
        { name: "reqCount",     type: "integer" },
        { name: "reqInterval",  type: "time" },
        { name: "user",         type: "string" },
        { name: "desc",         type: "string" },

        // Remove button
        { name: "",             class: "btn-remove" }
    ];

    var isNumberType = function(t) {
        return t === 'time' || t === 'integer' || t === 'bytes';
    }

    var margin = {
        top: 20,
        right: 40,
        bottom: 20,
        left: 80,
        footer: 100,
    };
    var emptyRow = {
        label: "label",
        startTime: 0,
        period: 100,
        periodCount: 0, // infinite generation
        reqSizeBytes: 1024,
        reqCount: 1,
        reqInterval: 10,
        user: "vdisk0",
        desc: "write"
    };
    var tableSelector = 'body', chartSelector = 'body', applyUrl = '?mode=setconfig',
        height = document.body.clientHeight - margin.top - margin.bottom - 5,
        width = document.body.clientWidth - margin.right - margin.left - 5,
        timeDomainStartMs = 0, timeDomainEndMs = 1000, timeUnit = 1,
        bytesDomainMax = 10e6,
        cfg,
        table,
        x, y, xAxis, yAxis, xZoomed,
        svg, svgChartContainer, svgChart, seriesSvg,
        zoom, zoomContainer1, zoomPanel, zoomContainer2, fixedContainer
    ;

    return scharm;
}
