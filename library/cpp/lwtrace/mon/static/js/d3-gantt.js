d3.gantt = function() {
    function gantt(config, logs, autoscale) {
        parseLogs(config, logs);

        if (autoscale) {
            gantt.timeDomain([minT, maxT]);
        }

        initAxis();

        // create svg element
        svg = d3.select(selector)
          .append("svg")
            .attr("class", "chart")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
        ;

        // create arrowhead marker
        defs = svg.append("defs");
        defs.append("marker")
            .attr("id", "arrow")
            .attr("viewBox", "0 -5 10 10")
            .attr("refX", 5)
            .attr("refY", 0)
            .attr("markerWidth", 4)
            .attr("markerHeight", 4)
            .attr("orient", "auto")
          .append("path")
            .attr("d", "M0,-5L10,0L0,5")
            .attr("class","arrowHead")
        ;

        zoom = d3.zoom()
            .scaleExtent([0.1, 1000])
            //.translateExtent([0, 0], [1000,0])
            .on("zoom", function() {
                if (tipShown != null) {
                    tip.hide(tipShown);
                }
                var tr = d3.event.transform;
                xZoomed = tr.rescaleX(x);
                svg.select("g.x.axis").call(xAxis.scale(xZoomed));

                var dy = d3.event.sourceEvent.screenY - zoom.startScreenY;
                var newScrollTop = documentBodyScrollTop() - dy;
                window.scrollTo(documentBodyScrollLeft(), newScrollTop);
                documentBodyScrollTop(newScrollTop);
                zoom.startScreenY = d3.event.sourceEvent.screenY;

                zoomContainer1.attr("transform", "translate(" + tr.x + ",0) scale(" + tr.k + ",1)");
                zoomContainer2.attr("transform", "translate(" + tr.x + ",0) scale(" + tr.k + ",1)");

                render();
            })
            .on("start", function() {
                zoom.startScreenY = d3.event.sourceEvent.screenY;
            })
            .on("end", function() {
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

        zoomContainer1 = svgChart.append("g");

        zoomPanel = svgChart.append("rect")
            .attr("class", "zoom-panel")
            .attr("width", width)
            .attr("height", height)
            .call(zoom)
        ;

        zoomContainer2 = svgChart.append("g");
        bandsSvg = zoomContainer2.append("g");

        // tooltips for bands
        var maxTipHeight = 130;
        const tipDirection = d => y(d.band) - maxTipHeight < documentBodyScrollTop()? 's': 'n';
        tip = d3.tip()
            .attr("class", "d3-tip")
            .offset(function(d) {
                // compute x to return tip in chart region
                var t0 = (d.t1 + d.t2) / 2;
                var t1 = Math.min(Math.max(t0, xZoomed.invert(0)), xZoomed.invert(width));
                var dir = tipDirection(d);
                return [dir === 'n'? -10 : 10, xZoomed(t1) - xZoomed(t0)];
            })
            .direction(tipDirection)
            .html(function(d) {
                let text = '';
                for (let item of d.record) {
                    text += probes[item[PROBEID]].provider + "." + probes[item[PROBEID]].name + "(";
                    let first = true;
                    for (let [param, value] of Object.entries(item[PARAMS])) {
                        text += (first? "": ", ") + param + "='" + value + "'";
                        first = false;
                    }
                    text += ")\n";
                }
                return "<pre>" + text + "</pre>";
            })
        ;

        bandsSvg.call(tip);

        render();

        // container for non-zoomable elements
        fixedContainer = svg.append("g")
            .attr("transform", "translate(" + margin.left + ", " + margin.top + ")")
        ;

        // create x axis
        fixedContainer.append("g")
            .attr("class", "x axis")
            .attr("transform", "translate(0, " + (height - margin.top - margin.bottom) + ")")
          .transition()
            .call(xAxis)
        ;

        // create y axis
        fixedContainer.append("g")
            .attr("class", "y axis")
          .transition()
            .call(yAxis)
        ;

        // make y axis ticks draggable
        var ytickdrag = d3.drag()
            .on("drag", function(d) {
                var ypos = d3.event.y - margin.top;
                var index = Math.floor((ypos / y.step()));
                index = Math.min(Math.max(index, 0), this.initDomain.length - 1);
                if (index != this.curIndex) {
                    var newDomain = [];
                    for (var i = 0; i < this.initDomain.length; ++i) {
                        newDomain.push(this.initDomain[i]);
                    }
                    newDomain.splice(this.initIndex, 1);
                    newDomain.splice(index, 0, this.initDomain[this.initIndex]);

                    this.curIndex = index;
                    this.curDomain = newDomain;
                    y.domain(newDomain);

                    // rearange y scale and axis
                    svg.select("g.y.axis").transition().call(yAxis);

                    // rearange other stuff
                    render(-1, true);
                }
            })
            .on("start", function(d) {
                var ypos = d3.event.y - margin.top;
                this.initIndex = Math.floor((ypos / y.step()));
                this.initDomain = y.domain();
            })
            .on("end", function(d) {
                svg.select("g.y.axis").call(yAxis);
            })
        ;
        svg.selectAll("g.y.axis .tick")
            .call(ytickdrag)
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

        // ruler
        ruler = fixedContainer.append("g")
            .attr("id", "ruler")
            .attr("transform", "translate(0, 0)")
        ;
        ruler.append("rect")
            .attr("id", "ruler-line")
            .attr("x", 0)
            .attr("y", 0)
            .attr("width", "1")
            .attr("height", height - margin.top - margin.bottom + 8)
        ;
        ruler.append("rect")
            .attr("id", "bgrect")
            .attr("x", 0)
            .attr("y", 0)
            .attr("width", 0)
            .attr("height", 0)
            .style("fill", "white")
        ;
        ruler.append("text")
            .attr("x", 0)
            .attr("y", height - margin.top - margin.bottom + 16)
            .attr("dy", "0.71em")
            .text("0")
        ;

        svg.on('mousemove', function() {
            positionRuler(d3.event.pageX);
        });

        // scroll handling
        window.onscroll = function myFunction() {
            documentBodyScrollLeft(document.body.scrollLeft);
            documentBodyScrollTop(document.body.scrollTop);
            var scroll = scrollParams();

            svgChartContainer
                .attr("transform", "translate(" + margin.left
                                         + ", " + (margin.top + scroll.y1) + ")");
            svgChart
                .attr("viewBox", "0 " + scroll.y1 + " " + width + " " + scroll.h)
                .attr("height", scroll.h);
            tmargin
                .attr("transform", "translate(0," + scroll.y1 + ")");
            fixedContainer.select(".x.axis")
                .attr("transform", "translate(0," + scroll.y2 + ")");
            rmargin.select("rect")
                .attr("y", scroll.y1)
                .attr("height", scroll.h);
            ruler.select("#ruler-line")
                .attr("y", scroll.y1)
                .attr("height", scroll.h);

            positionRuler();
        }

        // render axis
        svg.select("g.x.axis").call(xAxis);
        svg.select("g.y.axis").call(yAxis);

        // update to initiale state
        window.onscroll(0);

        return gantt;
    }

// private:

    var keyFunction = function(d) {
        return d.t1.toString() + d.t2.toString() + d.band.toString();
    }

    var bandTransform = function(d) {
        return "translate(" + x(d.t1) + "," + y(d.band) + ")";
    }

    var xPixel = function(d) {
        return xZoomed.invert(1) - xZoomed.invert(0);
    }

    var render = function(t0, smooth) {
        // Save/restore last t0 value
        if (!arguments.length || t0 == -1) {
            t0 = render.t0;
        }
        render.t0 = t0;
        smooth = smooth || false;

        // Create rectangles for bands
        bands = bandsSvg.selectAll("rect.bar")
          .data(data, keyFunction);
        bands.exit().remove();
        bands.enter().append("rect")
            .attr("class", "bar")
            .attr("vector-effect", "non-scaling-stroke")
            .style("fill", d => d.color)
            .on('click', function(d) {
                if (tipShown != d) {
                    tipShown = d;
                    tip.show(d);
                } else {
                    tipShown = null;
                    tip.hide(d);
                }
            })
          .merge(bands)
          .transition().duration(smooth? 250: 0)
            .attr("y", 0)
            .attr("transform", bandTransform)
            .attr("height", y.bandwidth())
            .attr("width", d => Math.max(1*xPixel(), x(d.t2) - x(d.t1)))
        ;

        var emptyMarker = bandsSvg.selectAll("text")
          .data(data.length == 0? ["no data to show"]: []);
        emptyMarker.exit().remove();
        emptyMarker.enter().append("text")
            .text(d => d)
        ;
    }

    function initAxis() {
        x = d3.scaleLinear()
            .domain([timeDomainStart, timeDomainEnd])
            .range([0, width])
            //.clamp(true); // dosn't work with zoom/pan
        xZoomed = x;
        y = d3.scaleBand()
            .domain(Object.values(data).map(d => d.band).sort())
            .rangeRound([0, height - margin.top - margin.bottom])
            .padding(0.5);
        xAxis = d3.axisBottom()
            .scale(x)
            //.tickSubdivide(true)
            .tickSize(8)
            .tickPadding(8);
        yAxis = d3.axisLeft()
            .scale(y)
            .tickSize(0);
    }

    // slow function wrapper
    var documentBodyScrollLeft = function(value) {
        if (!arguments.length) {
            if (documentBodyScrollLeft.value === undefined) {
                documentBodyScrollLeft.value = document.body.scrollLeft;
            }
            return documentBodyScrollLeft.value;
        } else {
            documentBodyScrollLeft.value = value;
        }
    }

    // slow function wrapper
    var documentBodyScrollTop = function(value) {
        if (!arguments.length) {
            if (!documentBodyScrollTop.value === undefined) {
                documentBodyScrollTop.value = document.body.scrollTop;
            }
            return documentBodyScrollTop.value;
        } else {
            documentBodyScrollTop.value = value;
        }
    }

    var scrollParams = function() {
        var y1 = documentBodyScrollTop();
        var y2 = y1 + window.innerHeight - margin.footer;
        y2 = Math.min(y2, height - margin.top - margin.bottom);
        var h = y2 - y1;
        return {
            y1: y1,
            y2: y2,
            h: h
        };
    }

    var posTextFormat = d3.format(".1f");

    var positionRuler = function(pageX) {
        if (!arguments.length) {
            pageX = positionRuler.pageX || 0;
        } else {
            positionRuler.pageX = pageX;
        }

        // x-coordinate
        if (!positionRuler.svgLeft) {
            positionRuler.svgLeft = svg.node().getBoundingClientRect().x;
        }

        var xpos = pageX - margin.left + 1 - positionRuler.svgLeft;
        var tpos = xZoomed.invert(xpos);
        tpos = Math.min(Math.max(tpos, xZoomed.invert(0)), xZoomed.invert(width));
        ruler.attr("transform", "translate(" + xZoomed(tpos) + ", 0)");
        var posText = posTextFormat(tpos);

        // scroll-related
        var scroll = scrollParams();

        var text = ruler.select("text")
            .attr("y", scroll.y2 + 16)
        ;

        // getBBox() is very slow, so compute symbol width once
        var xpadding = 5;
        var ypadding = 5;
        if (!positionRuler.bbox) {
            positionRuler.bbox = text.node().getBBox();
        }

        text.text(posText);
        var textWidth = 10 * posText.length;
        ruler.select("#bgrect")
            .attr("x", -textWidth/2 - xpadding)
            .attr("y", positionRuler.bbox.y - ypadding)
            .attr("width", textWidth + (xpadding*2))
            .attr("height", positionRuler.bbox.height + (ypadding*2))
        ;

        render(tpos);
    }

    /*
     * Log Query Language:
     * Data expressions:
     *  1) myparam[0], myparam[-1] // the first and the last myparam in any probe/provider
     *  2) myparam // the first (the same as [0])
     *  3) PROVIDER..myparam // any probe with myparam in PROVIDER
     *  4) MyProbe._elapsedMs // Ms since track begin for the first MyProbe event
     *  5) PROVIDER.MyProbe._sliceUs // Us since previous event in track for the first PROVIDER.MyProbe event
     */
    function compile(query) {
        query = query.replace(/\s/g, "");
        let [compiled, rest] = sum(query);
        if (rest.length != 0) {
            throw "parse error: unexpected expression starting at: '" + query + "'";
        }
        return record => {
            try {
                return compiled(record);
            } catch (e) {
                return null;
            }
        };

        function sum(query) {
            let term0;
            if (query[0] == '-') {
                let [term, rest] = product(query.substr(1));
                query = rest;
                term0 = x => -term(x);
            } else {
                let [term, rest] = product(query);
                query = rest;
                term0 = term;
            }
            let terms = [];
            while (query.length > 0) {
                let negate;
                if (query[0] == '+') {
                    negate = false;
                } else if (query[0] == '-') {
                    negate = true;
                } else {
                    break;
                }
                let [term, rest] = product(query.substr(1));
                query = rest;
                terms.push(negate? x => -term(x): term);
            }
            const cast = x => (isNaN(+x)? x: +x);
            return [terms.reduce((a, f) => x => cast(a(x)) + cast(f(x)), term0), query];
        }

        function product(query) {
            let [factor0, rest] = parentheses(query);
            query = rest;
            let factors = [];
            while (query.length > 0) {
                let invert;
                if (query[0] == '*') {
                    invert = false;
                } else if (query[0] == '/') {
                    invert = true;
                } else {
                    break;
                }
                let [factor, rest] = parentheses(query.substr(1));
                query = rest;
                factors.push(invert? x => 1 / factor(x): factor);
            }
            return [factors.reduce((a, f) => x => a(x) * f(x), factor0), query];
        }

        function parentheses(query) {
            if (query[0] == "(") {
                let [expr, rest] = sum(query.substr(1));
                if (rest[0] != ")") {
                    throw "parse error: missing ')' before '" + rest + "'";
                }
                return [expr, rest.substr(1)];
            } else {
                return atom(query);
            }
        }

        function atom(query) {
            specialParam = {
                _thrNTime: item => (item[US] - thrNTimeZero) * 1e-6,
                _thrRTime: item => (item[US] - thrRTimeZero) * 1e-6,
                _thrTime: item => item[US] * 1e-6,
                _thrNTimeMs: item => (item[US] - thrNTimeZero) * 1e-3,
                _thrRTimeMs: item => (item[US] - thrRTimeZero) * 1e-3,
                _thrTimeMs: item => item[US] * 1e-3,
                _thrNTimeUs: item => item[US] - thrNTimeZero,
                _thrRTimeUs: item => item[US] - thrRTimeZero,
                _thrTimeUs: item => item[US],
                _thrNTimeNs: item => (item[US] - thrNTimeZero) * 1e+3,
                _thrRTimeNs: item => (item[US] - thrRTimeZero) * 1e+3,
                _thrTimeNs: item => item[US] * 1e+3,
                _thread: item => item[THREAD],
            };
            var match;
            if (match = query.match(/^\d+(\.\d+)?/)) { // number literals
                let literal = match[0];
                return [record => literal, query.substr(match[0].length)];
            } else if (match = query.match(/^#[0-9a-fA-F]+/)) { // color literals
                let literal = match[0];
                return [record => literal, query.substr(match[0].length)];
            } else if (match = query.match(/^'(.*)'/)) { // string literal
                let literal = match[1].replace(/\\'/, "'").replace(/\\\\/, "\\");
                return [record => literal, query.substr(match[0].length)];
            } else if (match = query.match(/^(?:(?:(\w+)\.)?(\w*)\.)?(\w+)(?:\[(-?\d+)\])?/)) {
                let provider = match[1] || "";
                let probe = match[2] || "";
                let param = match[3];
                let index = +(match[4] || 0);
                let probeId = new Set();
                for (let id = 0; id < probes.length; id++) {
                    if ((!probe || probes[id].name == probe) &&
                        (!provider || probes[id].provider == provider))
                    {
                        probeId.add(id);
                    }
                }
                let isSpecial = specialParam.hasOwnProperty(param);
                return [record => {
                    let end = index >= 0? record.length: -1;
                    let step = index >= 0? 1: -1;
                    let skip = index >= 0? index: -index - 1;
                    for (let i = index >= 0? 0: record.length - 1; i != end; i += step) {
                        let item = record[i];
                        if (probeId.has(item[PROBEID]) && (isSpecial || item[PARAMS].hasOwnProperty(param))) {
                            if (skip == 0) {
                                return isSpecial? specialParam[param](item): item[PARAMS][param];
                            } else {
                                skip--;
                            }
                        }
                    }
                    throw "no data";
                }, query.substr(match[0].length)];
            } else {
                throw "parse error: invalid expression starting at '" + query + "'";
            }
        }
    }

    // ex1: "linear().domain([-1, 0, 1]).range(['red', 'white', 'green'])",
    // ex2: "ordinal().domain(['a1', 'a2']).range(['blue', 'yellow'])"
    function parseScale(query) {
        query = query.replaceAll("'","\"");
        var match, scale;
        if (match = query.match(/^linear\(\)/)) {
            scale = d3.scaleLinear();
        } else if (match = query.match(/^pow\(\)/)) {
            scale = d3.scalePow();
        } else if (match = query.match(/^log\(\)/)) {
            scale = d3.scaleLog();
        } else if (match = query.match(/^identity\(\)/)) {
            scale = d3.scaleIdentity();
        } else if (match = query.match(/^time\(\)/)) {
            scale = d3.scaleTime();
        } else if (match = query.match(/^threshold\(\)/)) {
            scale = d3.scaleThreshold();
        } else if (match = query.match(/^ordinal\(\)/)) {
            scale = d3.scaleOrdinal();
        } else {
            throw "Unable to parse scale: " + query;
        }
        if (match = query.match(/\.domain\(([^\)]+)\)/)) {
            scale.domain(JSON.parse(match[1]));
        }
        if (match = query.match(/\.range\(([^\)]+)\)/)) {
            scale.range(JSON.parse(match[1]));
        }
        if (match = query.match(/\.unknown\(([^\)]+)\)/)) {
            scale.unknown(JSON.parse(match[1]));
        }
        return scale;
    }

    function parseLogs(config, logs) {
        data = [];
        probes = logs.probes;
        if (config.hasOwnProperty('scales'))
        for (let [name, text] of Object.entries(config.scales)) {
            scales[name] = parseScale(text);
        }
        // Compute aggregates
        let minUs = new Map();
        for (record of logs.depot) {
            if (record.length > 0) {
                let item = record[0];
                let us = item[US];
                let thread = item[US];
                if (!minUs.has(thread)) {
                    minUs.set(thread, us);
                } else {
                    minUs.set(thread, Math.min(us, minUs.get(thread)));
                }
            }
        }
        thrNTimeZero = Number.MAX_VALUE;
        thrRTimeZero = 0;
        for (let [thread, us] of minUs) {
            thrNTimeZero = Math.min(thrNTimeZero, us);
            thrRTimeZero = Math.max(thrRTimeZero, us);
        }
        // Comple data for bands
        for (let bandCfg of config.bands) {
            function applyScale(func, scaleName) {
                if (scaleName) {
                    let scale = scales[scaleName];
                    return (record) => {
                        let value = func(record);
                        if (value != null) {
                            value = scale(value);
                        }
                        return value;
                    };
                } else {
                    return func;
                }
            }
            let t1f = applyScale(compile(bandCfg.t1), bandCfg.t1Scale);
            let t2f = applyScale(compile(bandCfg.t2), bandCfg.t2Scale);
            let bandf = applyScale(compile(bandCfg.band), bandCfg.bandScale);
            let colorf = applyScale(compile(bandCfg.color), bandCfg.colorScale);
            let minTime = Number.MAX_VALUE;
            let maxTime = -Number.MAX_VALUE;
            for (record of logs.depot) {
                let t1 = t1f(record),
                    t2 = t2f(record),
                    band = bandf(record),
                    color = colorf(record)
                ;
                if (t1 != null && t2 != null && band != null && color != null) {
                    data.push({t1, t2, band, color, record});
                    minTime = Math.min(minTime, t1);
                    maxTime = Math.max(maxTime, t2);
                }
            }
            if (minTime != Number.MAX_VALUE) {
                minT = minTime;
                maxT = maxTime;
            }
        }
    }

// public:

    gantt.width = function(value) {
        if (!arguments.length)
            return width;
        width = +value;
        return gantt;
    }

    gantt.height = function(value) {
        if (!arguments.length)
            return height;
        height = +value;
        return gantt;
    }

    gantt.selector = function(value) {
        if (!arguments.length)
            return selector;
        selector = value;
        return gantt;
    }

    gantt.timeDomain = function(value) {
        if (!arguments.length)
            return [timeDomainStart, timeDomainEnd];
        timeDomainStart = value[0];
        timeDomainEnd = value[1];
        return gantt;
    }

    gantt.data = function() {
        return data;
    }

    // constructor

    // Log Format
    const
        THREAD = 0,
        US = 1,
        PROBEID = 2,
        PARAMS = 3
    ;

    // Config
    var margin = { top: 20, right: 40, bottom: 20, left: 100, footer: 100 },
        height = document.body.clientHeight - margin.top - margin.bottom - 5,
        width = document.body.clientWidth - margin.right - margin.left - 5,
        selector = 'body',
        timeDomainStart = 0,
        timeDomainEnd = 1000,
        scales = {};
    ;

    // View
    var x = null,
        xZoomed = null,
        y = null,
        xAxis = null,
        yAxis = null,
        svg = null,
        defs = null,
        svgChartContainer = null,
        svgChart = null,
        zoomPanel = null,
        zoomContainer1 = null,
        zoomContainer2 = null,
        fixedContainer = null,
        zoom = null,
        bandsSvg = null,
        bands = null,
        tip = null,
        tipShown = null,
        ruler = null
    ;

    // Model
    var data = null,
        probes = null,
        thrRTimeZero = 0,
        thrNTimeZero = 0,
        minT,
        maxT
    ;

    return gantt;
}