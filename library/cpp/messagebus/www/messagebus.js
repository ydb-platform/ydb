function logTransform(v) {
    return Math.log(v + 1);
}

function plotHist(where, hist) {
    var max = hist.map(function(x) {return x[1]}).reduce(function(x, y) {return Math.max(x, y)});

    var ticks = [];
    for (var t = 1; ; t *= 10) {
        if (t > max) {
            break;
        }
        ticks.push(t);
    }

    $.plot(where, [hist],
        {
            data: hist,
            series: {
                bars: {
                    show: true,
                    barWidth: 0.9
                }
            },
            xaxis: {
                mode: 'categories',
                tickLength: 0
            },
            yaxis: {
                ticks: ticks,
                transform: logTransform
            }
        }
    );
}

function plotQueueSize(where, data, ticks) {
    $.plot(where, [data],
        {
            xaxis: {
                ticks: ticks,
            },
            yaxis: {
                //transform: logTransform
            }
        }
    );
}
