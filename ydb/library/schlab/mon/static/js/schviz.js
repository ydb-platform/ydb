d3.schviz = function() {
    // main schviz object
    function schviz(hist) {
        data = parseHistory(hist);

        initAxis();

        // create svg element
        svg = d3.select(selector)
          .append("svg")
            .attr("class", "chart")
            .attr("width", width + margin.left + margin.right)
            .attr("height",height + margin.top + margin.bottom)
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

        // disable default page scrolling with mouse wheel
        // because wheel is used for zooming
        window.onwheel = function(){ return false; }

        zoom = d3.zoom()
            .scaleExtent([0.1, 1000])
            //.translateExtent([0, 0], [1000,0])
            .on("zoom", function() {
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

                drawCbsStates();
                drawReqBands();
                drawReqArrivals();

                //tr(x);
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

        cbsSvg = zoomContainer1.append("g")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
        ;

        reqSvg = zoomContainer2.append("g")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
        ;

        // tooltips for requests
        var maxTipHeight = 130;
        function tipDirection(d) {
            var y0 = y(d[0].get('cbsName'));
            return (y0 - maxTipHeight < documentBodyScrollTop()? 's': 'n');
        }
        tip = d3.tip()
            .attr("class", "d3-tip")
            .offset(function(d) {
                // compute x to return tip in chart region
                var t0 = (d[0].get('ts') + d[1].get('ts')) / 2;
                var t1 = Math.min(Math.max(t0, xZoomed.invert(0)), xZoomed.invert(width));
                var dir = tipDirection(d);
                return [dir === 'n'? -10 : 10, xZoomed(t1) - xZoomed(t0)];
            })
            .direction(tipDirection)
            .html(function(d) {
                return "<table><tr><td class='tip-heading' colspan='2'>"
                    + "<u>" + d[0].get('kind') + " #" + d[0].get('id') + ":</u></td></tr>"
                        + "<tr><td class='tip-key'>part:</td><td class='tip-value'>"
                        + getPartNo(d) + "/" + getPartCount(d)
                        + "</td></tr>"
                        + "<tr><td class='tip-key'>est. cost:</td><td class='tip-value'>"
                        + costFmt(getEstCost(d)) + "/" + costFmt(getEstTotalCost(d))
                        + "</td></tr>"
                        + "<tr><td class='tip-key'>real cost:</td><td class='tip-value'>"
                        + costFmt(getRealCost(d)) + "/" + costFmt(getRealTotalCost(d))
                        + "</td></tr>"
                        + "<tr><td class='tip-key'>arrive:</td><td class='tip-value'>"
                        + timeFmt(d[0].get('arrived')) + "&nbsp;ms"
                        + "</td></tr>"
                        + "<tr><td class='tip-key'>begin:</td><td class='tip-value'>"
                        + timeFmt(d[0].get('ts')) + "&nbsp;ms"
                        + "</td></tr>"
                        + "<tr><td class='tip-key'>end:</td><td class='tip-value'>"
                        + timeFmt(d[1].get('ts')) + "&nbsp;ms"
                        + "</td></tr>"
                        + "<tr><td class='tip-key'>seqno:</td><td class='tip-value'>"
                        + d[0].get('seqno')
                        + "</td></tr>"
                    + "</table>"
                ;
            })
        ;

        reqSvg.call(tip);

        // create rectangles for cbs bands
        cbsbands = cbsSvg.selectAll(".chart")
          .data(data.cbsband, cbsKeyFunction)
          .enter()
          .append("rect")
            .attr("class", function(d) {
                return cbsStateClass[d[0].get('state')] || "bar";
            })
            .attr("y", 0)
            .attr("transform", cbsRectTransform)
            .attr("height", y.bandwidth())
            .attr("width", function(d) {
                return Math.max(1*xPixel(), (x(d[1].get('ts')) - x(d[0].get('ts'))));
            })
        ;

        cbsstates = zoomContainer1.append("g");
        drawCbsStates(0);

        drawReqBands();

        drawReqArrivals();

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
                    cbsbands.transition().attr("transform", cbsRectTransform);
                    drawReqBands();
                    drawReqArrivals(true);
                    drawCbsStates(-1, true);
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

        // draw chart
        schviz.draw();

       // update to initiale state
        window.onscroll(0);

        return schviz;
    }

// private:

    var cbsKeyFunction = function(d) {
        return "cbs:" +
                d[0].get('ts').toString() +
                d[0].get('idx').toString() +
                d[1].get('ts').toString();
    }

    var cbsStateKeyFunction = function(d) {
        return "cbsState:" + d.get('idx').toString();
    }

    var reqKeyFunction = function(d) {
        return "req:" +
                d[0].get('ts').toString() +
                d[0].get('id').toString() +
                d[1].get('ts').toString();
    }

    var reqStateKeyFunction = function(d) {
        return "reqState:" + d[0].get('id').toString();
    }

    var cbsRectTransform = function(d) {
        return "translate(" + x(d[0].get('ts')) + "," + y(d[0].get('name')) + ")";
    }

    var xPixel = function(d) {
        return xZoomed.invert(1) - xZoomed.invert(0);
    }

    var getEstCost = function(d) {
        return (d[1].get('state') == "DONE"?
            d[0].get('cost') :
            d[0].get('cost') - d[1].get('cost'));
    }

    var getRealCost = function(d) {
        return d[1].get('ts') - d[0].get('ts');
    }

    var getEstTotalCost = function(d) {
        return d[0].get('initCost');
    }

    var getRealTotalCost = function(d) {
        var realtotalcost = 0;
        var id = d[0].get('id');
        data.reqband.filter(function(dd) {
            return dd[0].get('id') == id && dd[0].get('state') == "RUNNING";
        }).forEach(function(dd) {
            realtotalcost += getRealCost(dd);
        });
        return realtotalcost;
    }

    var getPartNo = function(d) {
        return d[0].get('partNo');
    }

    var getPartCount = function(d) {
        var partCount = 0;
        var id = d[0].get('id');
        data.reqband.filter(function(dd) {
            return dd[0].get('id') == id && dd[0].get('state') == "RUNNING";
        }).forEach(function(dd) {
            partCount++;
        });
        return partCount;
    }

    var initAxis = function() {
        x = d3.scaleLinear()
            .domain([timeDomainStartMs, timeDomainEndMs])
            .range([0, width])
            //.clamp(true); // dosn't work with zoom/pan
        xZoomed = x;
        y = d3.scaleBand()
            .domain(Object.values(data.cbs).map(x => x.name).sort())
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

    var drawCbsStates = function(t0, smoothIn) {
        // Save/restore last t0 value
        if (!arguments.length || t0 == -1) {
            t0 = drawCbsStates.t0;
        }
        drawCbsStates.t0 = t0;

        var smooth = (arguments.length > 1? smoothIn: false);

        var pxl = xPixel();

        // find cbsbands containing given instant t0 and return left state
        function findCbsState(data) {
            return data.cbsband.filter(function(d) {
                return d[0].get('ts') <= t0 && t0 < d[1].get('ts');
            }).map(function(d) {
                return d[0];
            });
        }
        var cbsStateData = findCbsState(data);

        // create/update/remove bands for cbs max_budget
        var updCurBudget = cbsstates.selectAll("rect.cbs-active")
          .data(cbsStateData, cbsStateKeyFunction);
        updCurBudget.exit().remove();
        updCurBudget.enter().append("rect")
            .attr("class", "cbs-active")
          .merge(updCurBudget) // enter + update
          .transition().duration(smooth ? transDuration : 0)
            .attr("transform", function(d) {
                return "translate(" + x(t0) + "," + y(d.get('name')) + ")";
            })
            .attr("y", y.bandwidth() * (1+1/8) + 4)
            .attr("height", y.bandwidth() / 8)
            .attr("width", function(d) {
                var budget = d.get('max_budget');
                return Math.max(0, x(budget) - x(0));
            })
        ;

        // create/update/remove bands for cbs cur_budget
        var updCurBudget = cbsstates.selectAll("rect.cbs-running")
          .data(cbsStateData, cbsStateKeyFunction);
        updCurBudget.exit().remove();
        updCurBudget.enter().append("rect")
            .attr("class", "cbs-running")
          .merge(updCurBudget) // enter + update
          .transition().duration(smooth ? transDuration : 0)
            .attr("transform", function(d) {
                return "translate(" + x(t0) + "," + y(d.get('name')) + ")";
            })
            .attr("y", y.bandwidth() * (1+1/8) + 4)
            .attr("height", y.bandwidth() / 8)
            .attr("width", function(d) {
                var budget = d.get('cur_budget');
                if (d.get('state') == "RUNNING") {
                    budget -= t0 - d.get('ts');
                }
                return Math.max(0, x(budget) - x(0));
            })
        ;

        // create/update/remove bars for cbs deadline
        var updDeadline = cbsstates.selectAll("line")
          .data(cbsStateData, cbsStateKeyFunction);
        updDeadline.exit().remove();
        updDeadline.enter().append("line")
            .attr("class", "arrow-deadline")
            //.attr("marker-end", "url(#arrow)")
            .attr("transform", function(d) {
                return "translate("
                    + x(d.get('deadline')) + ","
                    + y(d.get('name')) + ")scale(" + pxl + ",1)";
            })
          .merge(updDeadline) // enter + update
          .transition().duration(smooth ? transDuration : 0)
            .attr("x1", 0)
            .attr("x2", 0)
            .attr("y1", 0)
            .attr("y2", y.bandwidth() * 1.4)
            .attr("transform", function(d) {
                return "translate("
                    + x(d.get('deadline')) + ","
                    + y(d.get('name')) + ")scale(" + pxl + ",1)";
            })
        ;

        // find reqbands containing given instant t0 and return left state
        function findReqState(data) {
            // find cbsbands containing given instant t0 and return left state
            return data.reqband.filter(function(d) {
                return d[0].get('ts') <= t0 && t0 < d[1].get('ts');
            }).map(function(d) {
                var cbsIdx = d[0].get('cbsIdx');
                var curCbsState = cbsStateData.find(function(x) {return x.get('idx') == cbsIdx;});
                var realcost = curCbsState.get('band')[0].get('ts') - curCbsState.get('band')[1].get('ts');
                var estcost = curCbsState.get('band')[0].get('costOut') - curCbsState.get('band')[1].get('costOut');
                var estToRealCoef = estcost / realcost;
                return [d[0], curCbsState, estToRealCoef];
            });
        }
        var reqStateData = findReqState(data);

        // create/update/remove bands for req cost
        var updReqCost = cbsstates.selectAll("rect.req-real")
          .data(reqStateData, reqStateKeyFunction);
        updReqCost.exit().remove();
        updReqCost.enter().append("rect")
            .attr("class", "req-real")
          .merge(updReqCost) // enter + update
          .transition().duration(smooth ? transDuration : 0)
            .attr("transform", function(d) {
                var c = d[1];
                var costOut = c.get('costOut');
                if (c.get('hasRunningRequest')) {
                    costOut += d[2] * (t0 - c.get('ts'));
                }
                return "translate(" + x(t0 + d[0].get('queuePos') - costOut)
                              + "," + y(c.get('name')) + ")";
            })
            .attr("x", pxl)
            .attr("y", y.bandwidth() * (1) + 2)
            .attr("height", y.bandwidth() * (1/8))
            .attr("width", function(d) {
                var initCost = d[0].get('initCost');
                return Math.max(1*pxl, x(initCost) - x(0) - 2*pxl);
            })
        ;
    }

    var drawReqBands = function() {
        var pxl = xPixel();

        function reqbandTransform(d) {
            return "translate(" + x(d[0].get('ts')) + "," + y(d[0].get('cbsName')) + ")";
        }

        function cutPoints(x, dx, y, dy) {
            return      (x     ) + "," + (y + 0.20 * dy)
                + " " + (x - dx) + "," + (y + 0.35 * dy)
                + " " + (x + dx) + "," + (y + 0.65 * dy)
                + " " + (x     ) + "," + (y + 0.80 * dy)
            ;
        }

        function cutHalfPoints(x, dx, y, dy) {
            return      (x + dx) + "," + (y + 0.30 * dy)
                + " " + (x     ) + "," + (y + 0.60 * dy)
            ;
        }

        var h = y.bandwidth() * 0.6;
        var y0 = y.bandwidth() * 0.2;
        var dx = 5*pxl;
        function reqbandPoints(d) {
            var w = Math.max(1*pxl, (x(d[1].get('ts')) - x(d[0].get('ts'))) - 2*pxl);
            var x1 = pxl;
            var x2 = x1 + w;
            var y1 = y0;
            var y2 = y1 + h;
            var leftCut = (d[0].get('cost') != d[0].get('initCost'));
            var rightCut = (d[1].get('state') != "DONE");

            return      x1 + "," + y1
                + " " + (leftCut? cutPoints(x1, dx, y1, y2-y1) : "")
                + " " + x1 + "," + y2
                + " " + x2 + "," + y2
                + " " + (rightCut? cutPoints(x2, -dx, y2, y1-y2) : "")
                + " " + x2 + "," + y1
                ;
        }

        function reqband2Points(d) {
            var w = Math.max(1*pxl, (x(d[1].get('ts')) - x(d[0].get('ts'))) - 2*pxl);
            var x1 = pxl;
            var x2 = x1 + w;
            var y1 = y0;
            var y2 = y1 + h;
            var leftCut = (d[0].get('cost') != d[0].get('initCost'));
            var rightCut = (d[1].get('state') != "DONE");

            var estcost = getEstCost(d);
            var realcost = getRealCost(d);

            var cutFunc = cutPoints;
            var delta = estcost - realcost;
            if (delta === 0) {
                return ""; // do not draw anything
            } else if (delta > 0) { // overestimation
                x2 = x1 + delta / estcost * w;
                y1 += h / 2;
                rightCut = false;
                cutFunc = cutHalfPoints;
            } else { // overrun
                x1 = x2 + delta / realcost * w;
                leftCut = false;
            }

            return      x1 + "," + y1
                + " " + (leftCut? cutFunc(x1, dx, y1, y2-y1) : "")
                + " " + x1 + "," + y2
                + " " + x2 + "," + y2
                + " " + (rightCut? cutFunc(x2, -dx, y2, y1-y2) : "")
                + " " + x2 + "," + y1
                ;
        }

        function reqbandKindPoints(d) {
            var w = Math.max(1*pxl, Math.min(x(d[1].get('ts')) - x(d[0].get('ts')), 10*pxl));
            var x1 = pxl;
            var x2 = x1 + w;
            var y1 = y0;
            var y2 = y1 + h;

            if (d[0].get('kind') == "WRITE") {
                var cx = (x1 + x2) / 2;
                var cy = (y1 * 3/4 + y2 * 1/4);
                var dx = (cx - x1) * 0.8;
                var dy = (cy - y1) * 0.8;
                return      (cx - dx) + "," + (cy     )
                    + " " + (cx     ) + "," + (cy - dy)
                    + " " + (cx + dx) + "," + (cy     )
                    + " " + (cx     ) + "," + (cy + dy)
                    ;
            } else {
                return "";
            }
        }

        // create polygons for req bands (real part)
        var reqbandsData = data.reqband.filter(function(d) {
            return d[0].get('state') == "RUNNING";}
        );
        var reqbandsUpd = reqSvg.selectAll("polygon.req-real")
          .data(reqbandsData, reqKeyFunction);
        reqbandsUpd.exit().remove();
        reqbandsUpd.enter().append("polygon")
            .attr("class", function(d) {
                return "req-real reqband-" + d[0].get('id');
            })
            .on('mouseover', function(d) {
                tip.show(d);
                reqSvg.selectAll(".reqband-" + d[0].get('id'))
                    .classed("req-real-hover", true);
                reqSvg.selectAll(".reqband2-" + d[0].get('id'))
                    .classed("req-est-hover", true);
                reqSvg.selectAll(".reqarrival-" + d[0].get('id'))
                    .classed("arrow-arrival-hover", true)
                    .raise();
            })
            .on('mouseout', function(d) {
                tip.hide(d);
                reqSvg.selectAll(".reqband-" + d[0].get('id'))
                    .classed("req-real-hover", false);
                reqSvg.selectAll(".reqband2-" + d[0].get('id'))
                    .classed("req-est-hover", false);
                reqSvg.selectAll(".reqarrival-" + d[0].get('id'))
                    .classed("arrow-arrival-hover", false);
            })
            .attr("transform", reqbandTransform)
          .merge(reqbandsUpd) // enter + update
            .attr("points", reqbandPoints)
          .transition()
            .attr("transform", reqbandTransform)
        ;

        // create polygons for req bands (real/est diff)
        var reqbands2Upd = reqSvg.selectAll("polygon.req-est")
          .data(reqbandsData, reqKeyFunction);
        reqbands2Upd.exit().remove();
        reqbands2Upd.enter().append("polygon")
            .attr("class", function(d) {
                return "req-est reqband2-" + d[0].get('id');
            })
            .attr("transform", reqbandTransform)
          .merge(reqbands2Upd) // enter + update
            .attr("points", reqband2Points)
          .transition()
            .attr("transform", reqbandTransform)
        ;

        // create polygons for req kind
        var reqbandsKindUpd = reqSvg.selectAll("polygon.req-kind")
          .data(reqbandsData, reqKeyFunction);
        reqbandsKindUpd.exit().remove();
        reqbandsKindUpd.enter().append("polygon")
            .attr("class", function(d) {
                return "req-kind reqbandkind-" + d[0].get('id');
            })
            .attr("transform", reqbandTransform)
          .merge(reqbandsKindUpd) // enter + update
            .attr("points", reqbandKindPoints)
          .transition()
            .attr("transform", reqbandTransform)
        ;
    }

    var drawReqArrivals = function(smoothIn) {
        var smooth = (arguments.length > 0? smoothIn: false);

        // create/update/remove bars for req arrivals
        var updArrivals = reqSvg.selectAll("line.arrow-arrival")
          .data(data.reqband.filter(function(d) {
              return d[0].get('arrived') === d[0].get('ts');
          }), reqKeyFunction);
        updArrivals.exit().remove();
        updArrivals.enter().append("line")
            .attr("class", function(d) {
                return "arrow-arrival reqarrival-" + d[0].get('id');
            })
            //.attr("marker-end", "url(#arrow)")
            .attr("x1", 0)
            .attr("x2", 0)
            .attr("y1", y.bandwidth() * 0.5)
            .attr("y2", -y.bandwidth() * 0.4)
            .attr("transform", function(d) {
                return "translate("
                    + x(d[0].get('arrived')) + ","
                    + y(d[0].get('cbsName')) + ")scale(" + xPixel() + ",1)";
            })
          .merge(updArrivals) // enter + update
          .transition().duration(smooth ? transDuration : 0)
            .attr("transform", function(d) {
                return "translate("
                    + x(d[0].get('arrived')) + ","
                    + y(d[0].get('cbsName')) + ")scale(" + xPixel() + ",1)";
            })
        ;
    }

    // slow function wrapper
    var documentBodyScrollLeft = function(value) {
        // return document.body.scrollLeft;
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
        // return document.body.scrollTop;
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
        var xpos = pageX - margin.left + 1;
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

        drawCbsStates(tpos);
    }

    var beginParseCbs = function(data, frame, cbs) {
        if (cbs.idx === undefined) {
            alert("'idx' is not set for cbs in frame:\n" + JSON.stringify(frame));
        }

        // create cbs if required
        var idx = +cbs.idx;
        if (!data.cbs.hasOwnProperty(idx)) {
            var c = {};

            // initialize new cbs info
            c.idx = idx;
            c.band = null;
            c.ts = +frame.ts * timeUnit;
            c.name = cbs.name || ("cbs" + frame.idx);
            c.max_budget = (+cbs.max_budget || 0) * timeUnit;
            c.period = (+cbs.period || 0) * timeUnit;
            c.cur_budget = (+cbs.cur_budget || 0) * timeUnit;
            c.deadline = (+cbs.deadline || 0) * timeUnit;
            c.state = cbs.state || "IDLE";
            c.reqIn = 0;
            c.reqOut = 0;
            c.costIn = 0;
            c.costOut = 0;
            c.runningTime = 0;
            c.idleTime = 0;
            c.activeTime = 0;
            c.depletedTime = 0;
            c.hasRunningRequest = false;

            data.cbs[idx] = c;
        } else {
            // update current req info
            var c = data.cbs[idx];

            // deep copy current request state as the first band's element
            c.band = [];
            c.band.push(d3.map(c));

            // update current req info
            c.ts = +frame.ts * timeUnit;
            c.hasRunningRequest = false;
            if (cbs.hasOwnProperty('name')) { c.name = cbs.name; }
            if (cbs.hasOwnProperty('max_budget')) { c.max_budget = +cbs.max_budget * timeUnit; }
            if (cbs.hasOwnProperty('period')) { c.period = +cbs.period * timeUnit; }
            if (cbs.hasOwnProperty('cur_budget')) { c.cur_budget = +cbs.cur_budget * timeUnit; }
            if (cbs.hasOwnProperty('deadline')) { c.deadline = +cbs.deadline * timeUnit; }
            if (cbs.hasOwnProperty('state')) { c.state = cbs.state; }
        }
    }

    var endParseCbs = function(data, frame, cbs) {
        // create cbs if required
        var idx = +cbs.idx;
        var c = data.cbs[idx];
        if (c.band !== null) {
            // deep copy new request state as the second band's element
            c.band.push(d3.map(c));
            if (c.band[0].get('ts') < c.band[1].get('ts')) {
                data.cbsband.push(c.band);
            }
        }
    }

    var beginParseCbsReq = function(data, frame, cbs, req) {
        if (req.id === undefined) {
            alert("'id' is not set for req in frame's cbs:\n" + JSON.stringify(cbs));
        }

        var idx = +cbs.idx;
        var c = data.cbs[idx];

        var id = +req.id;
        if (!data.req.hasOwnProperty(id)) {
            var r = {};

            // initialize new req info
            r.id = id;
            r.band = null;
            r.cbsIdx = idx;
            r.cbsName = c.name;
            r.ts = +frame.ts * timeUnit;
            r.cost = (+req.cost || 0) * timeUnit;
            r.initCost = r.cost;
            r.seqno = +req.seqno || 0;
            r.state = req.state || "PENDING";
            r.kind = req.kind || "REQUEST";
            r.queuePos = c.costIn;
            r.arrived = r.ts;
            r.partNo = 0;

            // update cbs statistics
            c.reqIn++;
            c.costIn += r.cost;
            if (r.state == "RUNNING") {
                c.hasRunningRequest = true;
            }

            data.req[id] = r;
        } else {
            // update current req info
            var r = data.req[id];
            if (r.state == "PENDING" && (
                (req.hasOwnProperty('state') && req.state == "PENDING") || !req.hasOwnProperty('state'))) {
                // process the cost update ONLY while still in PENDING state and the update is stateless or still PENDING
                if (req.hasOwnProperty('cost')) {
                    c.costIn -= r.cost;
                    r.cost = +req.cost * timeUnit;
                    r.initCost = r.cost;
                    c.costIn += r.cost;
                 }
            } else {
                r.band = [];

                // deep copy current request state as the first band's element
                r.band.push(d3.map(r));

                // update current req info
                r.ts = +frame.ts * timeUnit;


                if (req.hasOwnProperty('cost')) { r.cost = +req.cost * timeUnit; }
                if (req.hasOwnProperty('seqno')) { r.seqno = +req.seqno; }
                if (req.hasOwnProperty('state')) { r.state = req.state; }

                // update cbs/req statistics
                if (r.state == "RUNNING") {
                    r.partNo++;
                }
                if (r.state == "DONE") {
                    c.reqOut++;
                    c.costOut += r.band[0].get('cost');
                } else if (r.band[0].get('state') == "RUNNING") {
                    c.costOut += r.band[0].get('cost') - r.cost;
                }
                if (r.state == "RUNNING") {
                    c.hasRunningRequest = true;
                }
            }
        }
    }

    var endParseCbsReq = function(data, frame, cbs, req) {
        var idx = +cbs.idx;
        var c = data.cbs[idx];

        var id = +req.id;
        var r = data.req[id];

        if (r.band !== null) {
            // deep copy new request state as the second band's element
            r.band.push(d3.map(r));
            if (r.band[0].get('ts') < r.band[1].get('ts')) {
                data.reqband.push(r.band);
            }
        }
    }

    var finalizeCbs = function(data, ts, c) {
        var band = [];

        // deep copy current request state as the first band's element
        band.push(d3.map(c));

        // update current req info
        c.ts = ts * timeUnit;

        // deep copy new request state as the second band's element
        band.push(d3.map(c));
        if (band[0].get('ts') < band[1].get('ts')) {
            data.cbsband.push(band);
        }
    }

    var finalizeCbsReq = function(data, ts, r) {
        var band = [];

        // deep copy current request state as the first band's element
        band.push(d3.map(r));

        // update current req info
        r.ts = ts * timeUnit;

        // deep copy new request state as the second band's element
        band.push(d3.map(r));
        if (band[0].get('ts') < band[1].get('ts')) {
            data.reqband.push(band);
        }
    }

    var finalize = function(data, ts) {
        for (var idx in data.cbs) {
            finalizeCbs(data, ts, data.cbs[idx]);
        }
        for (var id in data.req) {
            finalizeCbsReq(data, ts, data.req[id]);
        }
    }

    var parseFrame = function(data, frame) {
        var i, j;
        for (i = 0; i < frame.cbs.length; ++i) {
            var cbs = frame.cbs[i];
            beginParseCbs(data, frame, cbs);
            if (cbs.hasOwnProperty('req')) {
                for (j = 0; j < cbs.req.length; ++j) {
                    var req = cbs.req[j];
                    beginParseCbsReq(data, frame, cbs, req);
                }
                for (j = 0; j < cbs.req.length; ++j) {
                    var req = cbs.req[j];
                    endParseCbsReq(data, frame, cbs, req);
                }
            }
            endParseCbs(data, frame, cbs);
        }
    }

    var parseKeyframe = function(data, frame) {
        // current state
        data.cbs = {};
        data.req = {};

        // data for vizualization
        data.cbsband = [];
        data.reqband = [];

        // regular frame parse
        parseFrame(data, frame);
    }

    var parseHistory = function(hist) {
        var i, data = {},
            keyframeFound = false;
        for (i = 0; i < hist.length; ++i) {
            var frame = hist[i];
            if (!keyframeFound) {
                if (frame.keyframe) {
                    keyframeFound = true;
                    parseKeyframe(data, frame);
                } else {
                    // skip frames before keyframe
                }
            } else {
                if (!frame.keyframe) {
                    parseFrame(data, frame);
                } else {
                    // skip non-first keyframes
                }
            }
        }
        if (hist.length > 1) {
            finalize(data, hist[hist.length-1].ts);
        }

        // Erase every cbs that has no request
        data.cbsband = data.cbsband.filter(x => data.cbs[x[0].get('idx')].band !== null);
        Object.keys(data.cbs).forEach(function(key) {
            if (data.cbs[key].band === null) {
                delete data.cbs[key];
            }
        });
        console.log(data);
        return data;
    }

// public:

    schviz.draw = function(cbs) {
        svg.select("g.x.axis").call(xAxis);
        svg.select("g.y.axis").call(yAxis);
        return schviz;
    }

    schviz.update = function(cbs) {

        var svg = d3.select(".chart");
        var schvizChartGroup = svg.select(".gantt-chart");

        // update task data attached to rect elements
        var rect = schvizChartGroup.selectAll("rect")
            .data(cbs, cbsKeyFunction);

        // attach new cbs state
        rect.enter()
            .insert("rect", ":first-child")
            .attr("rx", 5)
            .attr("ry", 5)
            .attr("class", function(d) {
                if (taskStatus[d.status] == null) {
                    return "bar";
                }
                return taskStatus[d.status];
            })
          .transition()
            .attr("y", 0)
            .attr("transform", rectTransform)
            .attr("height", function(d) {
                return y.bandwidth();
            })
            .attr("width", function(d) {
                return Math.max(1*xPixel, (x(d.endDate) - x(d.startDate)));
            });

        // rearrange current cbs states
        rect.transition()
            .attr("transform", rectTransform)
            .attr("height", function(d) {
                return y.bandwidth();
            })
            .attr("width", function(d) {
                return Math.max(1*xPixel(), (x(d.endDate) - x(d.startDate)));
            });

        // remove deleted cbs states
        rect.exit().remove();

        // update axes
        svg.select(".x").transition().call(xAxis);
        svg.select(".y").transition().call(yAxis);

        return schviz;
    }

    schviz.margin = function(value) {
        if (!arguments.length)
            return margin;
        margin = value;
        return schviz;
    }

    schviz.timeDomain = function(value) {
        if (!arguments.length)
            return [timeDomainStartMs, timeDomainEndMs];
        timeDomainStartMs = value[0];
        timeDomainEndMs = value[1];
        return schviz;
    }

    schviz.width = function(value) {
        if (!arguments.length)
            return width;
        width = +value;
        return schviz;
    }

    schviz.height = function(value) {
        if (!arguments.length)
            return height;
        height = +value;
        return schviz;
    }

    schviz.selector = function(value) {
        if (!arguments.length)
            return selector;
        selector = value;
        return schviz;
    }

// constructor

    var margin = {
        top: 20,
        right: 40,
        bottom: 20,
        left: 200,
        footer: 100,
    };
    var height = document.body.clientHeight - margin.top - margin.bottom - 5;
    var width = document.body.clientWidth - margin.right - margin.left - 5;

    var selector = 'body';
    var timeDomainStartMs = 0;
    var timeDomainEndMs = 1000;
    var timeUnit = 1e-6;
    var costFmt = d3.format('.4s');
    var timeFmt = d3.format(',.6r');
    var cbsStateClass = {
        "IDLE": "cbs-idle",
        "ACTIVE": "cbs-active",
        "RUNNING": "cbs-running",
        "DEPLETED": "cbs-depleted",
        "DEAD": "cbs-dead"
    };

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
        cbsSvg = null,
        reqSvg = null,
        cbsbands = null,
        cbsstates = null,
        tip = null,
        ruler = null,
        data = { "bands": d3.map(), "states": d3.map() },
        transDuration = 250
    ;

    return schviz;
}
