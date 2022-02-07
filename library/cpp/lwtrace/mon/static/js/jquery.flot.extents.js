/*
 * Copyright 2012, Serge V. Izmaylov
 * Released under GPL Version 2 license.
 */

(function ($) {
    var options = {
        series: {
            extents: {
                show: false,
                lineWidth: 1,
                barHeight: 17,
                color: "rgba(192, 192, 192, 1.0)",
                showConnections: true,
                connectionColor: "rgba(0, 192, 128, 0.8)",
                fill: true,
                fillColor: "rgba(64, 192, 255, 0.5)",
                showLabels: true,
                rowHeight: 20,
                rows: 7,
                barVAlign: "top",
                labelHAlign: "left"
            }
        }
    };

    function processRawData(plot, series, data, datapoints) {
        if (!series.extents || !series.extents.show)
            return;

        // Fool Flot with fake datapoints
        datapoints.format = [ // Fake format
            { x: true, number: true, required: true },
            { y: true, number: true, required: true },
        ];
        datapoints.points = []; // Empty data
        datapoints.pointsize = 2; // Fake size

        // Check if we have extents data
        if (series.extentdata == null)
            return;

        // Process our real data
        var row = 0;
        for (i = 0; i < series.extentdata.length; i++) {
            // Skip bad extents
            if ((series.extentdata[i].start == null) || (series.extentdata[i].end == null))
                continue;

            if (series.extentdata[i].end < series.extentdata[i].start) {
                var t = series.extentdata[i].end;
                series.extentdata[i].end = series.extentdata[i].start;
                series.extentdata[i].start = t;
            }
            if ((series.extentdata[i].labelHAlign != "left") && (series.extentdata[i].labelHAlign != "right"))
                series.extentdata[i].labelHAlign = series.extents.labelHAlign;
            if (series.extentdata[i].row == null) {
                series.extentdata[i].row = row;
                row = (row+1) % series.extents.rows;
            } else {
                row = (series.extentdata[i].row+1) % series.extents.rows;
            }
            if (series.extentdata[i].color == null)
                series.extentdata[i].color = series.extents.color;
            if (series.extentdata[i].fillColor == null)
                series.extentdata[i].fillColor = series.extents.fillColor;
        }
        
    };

    function drawSingleExtent(ctx, width, height, xfrom, xto, series, extent) {
        if (xfrom < 0) xfrom = 0;
        if (xto > width) xto = width;
        var bw = xto-xfrom;

        var yfrom;
        if (series.extents.barVAlign == "top")
            yfrom = 4 + series.extents.rowHeight*extent.row;
        else
            yfrom = height - 4 - series.extents.rowHeight*(extent.row) - series.extents.barHeight;

        if (series.extents.fill) {
            ctx.fillStyle = extent.fillColor;
            ctx.fillRect(xfrom, yfrom, bw, series.extents.barHeight);
        }

        ctx.strokeStyle = extent.color;
        ctx.strokeRect(xfrom, yfrom, bw, series.extents.barHeight);
    }

    function drawSingleConnection(ctx, width, height, xfrom, xto, rfrom, rto, series) {
        if (xfrom < 0) xfrom = 0;
        if (xto > width) xto = width;

        var yfrom, yto;
        if (series.extents.barVAlign == "top") {
            yfrom = 4 + Math.round(series.extents.rowHeight*rfrom) + Math.round(series.extents.barHeight*0.5);
            yto = 4 + Math.round(series.extents.rowHeight*rto) + Math.round(series.extents.barHeight*0.5);
        } else {
            yfrom = height - 4 - Math.round(series.extents.rowHeight*rfrom) - Math.round(series.extents.barHeight*0.5);
            yto = height - 4 - Math.round(series.extents.rowHeight*rto) - Math.round(series.extents.barHeight*0.5);
        }

        ctx.beginPath();
        ctx.moveTo(xfrom, yfrom);
        ctx.lineTo(xfrom+10, yfrom);
        ctx.lineTo(xto-10, yto);
        ctx.lineTo(xto, yto);
        ctx.lineTo(xto-6, yto-3);
        ctx.lineTo(xto-6, yto+3);
        ctx.lineTo(xto, yto);
        ctx.stroke();
    }

    function addExtentLabel(placeholder, plotOffset, width, xfrom, xto, series, extent) {
        var styles = [];
        if (series.extents.barVAlign == "top")
            styles.push("top:"+Math.round((plotOffset.top+series.extents.rowHeight*extent.row+4))+"px");
        else
            styles.push("bottom:"+Math.round((plotOffset.bottom+series.extents.rowHeight*extent.row+4))+"px");
        if (extent.labelHAlign == "left")
            styles.push("left:"+Math.round((plotOffset.left+xfrom+3))+"px");
        else
            styles.push("right:"+Math.round((plotOffset.right+(width-xto)+3))+"px");
        styles.push("");

        placeholder.append('<div '+((extent.id !=null)?('id="'+extent.id+'" '):'')+'class="extentLabel" style="font-size:smaller;position:absolute;'+(styles.join(';'))+'">'+extent.label+'</div>');
    }

    function drawSeries(plot, ctx, series) {
        if (!series.extents || !series.extents.show || !series.extentdata)
            return;

        var placeholder = plot.getPlaceholder();
        placeholder.find(".extentLabel").remove();

        ctx.save();

        var plotOffset = plot.getPlotOffset();
        var axes = plot.getAxes();
        var yf = axes.yaxis.p2c(axes.yaxis.min);
        var yt = axes.yaxis.p2c(axes.yaxis.max);
        var ytop = (yf>yt)?yt:yf;
        var ybot = (yf>yt)?yf:yt;
        var width = plot.width();
        var height = plot.height();

        ctx.translate(plotOffset.left, plotOffset.top);
        ctx.lineJoin = "round";

        for (var i = 0; i < series.extentdata.length; i++) {
            var xfrom, xto;
            if ((series.extentdata[i].start == null) || (series.extentdata[i].end == null))
                continue;
            if ((series.extentdata[i].start < axes.xaxis.max) && (series.extentdata[i].end > axes.xaxis.min)) {
                xfrom = axes.xaxis.p2c((series.extentdata[i].start<axes.xaxis.min)?axes.xaxis.min:series.extentdata[i].start);
                xto = axes.xaxis.p2c((series.extentdata[i].end>axes.xaxis.max)?axes.xaxis.max:series.extentdata[i].end);
                drawSingleExtent(ctx, width, height, xfrom, xto, series, series.extentdata[i]);

                if (series.extents.showConnections && (series.extentdata[i].start > axes.xaxis.min) && (series.extentdata[i].start < axes.xaxis.max))
                    if ((series.extentdata[i].depends != null) && (series.extentdata[i].depends.length != null) && (series.extentdata[i].depends.length > 0))
                        for (var j=0; j<series.extentdata[i].depends.length; j++) {
                            var k = series.extentdata[i].depends[j];
                            if ((k < 0) || (k >= series.extentdata.length))
                                continue;
                            if ((series.extentdata[k].start == null) || (series.extentdata[k].end == null))
                                continue;
                            var cxto = xfrom;
                            var cxfrom = series.extentdata[k].end;
                            if (cxfrom < axes.xaxis.min) cxfrom = axes.xaxis.min;
                            if (cxfrom > axes.xaxis.max) cxfrom = axes.xaxis.max;
                            cxfrom = axes.xaxis.p2c(cxfrom);
                            ctx.strokeStyle = series.extents.connectionColor;
                            drawSingleConnection(ctx, width, height, cxfrom, cxto, series.extentdata[k].row, series.extentdata[i].row, series);
                        }

                if (series.extents.showLabels && (series.extentdata[i].label != null))
                    addExtentLabel(placeholder, plotOffset, width, xfrom, xto, series, series.extentdata[i]);
            }
        }

        ctx.restore();
    };

    function init(plot) {
        plot.hooks.processRawData.push(processRawData);
        plot.hooks.drawSeries.push(drawSeries);
    };
    
    $.plot.plugins.push({
        init: init,
        name: "extents",
        options: options,
        version: "0.3"
    });
})(jQuery);
