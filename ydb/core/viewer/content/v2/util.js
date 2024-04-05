var grey = "grey";
var green = "lightgreen";
var blue = "#6495ED"; // #4169E1 #4682B4
var lightblue = "lightblue";
var yellow = "yellow";
var orange = "orange";
var red = "red";
var black = "black";
var lightgrey = "#F7F7F7";

function pad2(val) {
    if (val < 10) {
        return "0" + val;
    } else {
        return val;
    }
}

function pad4(val) {
    var len = String(val).length;
    for (var i = len; i < 4; i++) {
        val = "0" + val;
    }
    return val;
}

function pad9(val) {
    var len = String(val).length;
    for (var i = len; i < 9; i++) {
        val = "0" + val;
    }
    return val;
}

function upTimeToString(upTime) {
    var seconds = Math.floor(upTime / 1000);
    if (seconds < 60) {
        return seconds + "s";
    } else {
        var minutes = Math.floor(seconds / 60);
        seconds = seconds % 60;
        if (minutes < 60) {
            return minutes + ":" + pad2(seconds);
        } else {
            var hours = Math.floor(minutes / 60);
            minutes = minutes % 60;
            if (hours < 24) {
                return hours + ":" + pad2(minutes) + ":" + pad2(seconds);
            } else {
                var days = Math.floor(hours / 24);
                hours = hours % 24;
                return days + "d " + pad2(hours) + ":" + pad2(minutes) + ":" + pad2(seconds);
            }
        }
    }
}


var sizes = ["", "K", "M", "G", "T", "P", "E"];
var base = 1000;

function bytesToSize(bytes) {
    if (isNaN(bytes)) {
        return "";
    }
    if (bytes < base)
        return String(bytes);
    var i = parseInt(Math.floor(Math.log(bytes) / Math.log(base)));
    if (i >= sizes.length) {
        i = sizes.length - 1;
    }
    var val = bytes / Math.pow(base, i);
    if (val > 999) {
        val = val / base;
        i++;
    }
    return val.toPrecision(3) + sizes[i];
}

function bytesToSpeed(bytes) {
    return bytesToMB(bytes) + '/s';
}

function bytesToGB(bytes) {
    if (isNaN(bytes)) {
        return "";
    }
    var val = bytes / 1000000000;
    if (val < 10) {
        return val.toFixed(2) + sizes[3];
    } else if (val < 100) {
        return val.toFixed(1) + sizes[3];
    } else {
        return val.toFixed() + sizes[3];
    }
}

function bytesToGB3(bytes) {
    if (isNaN(bytes)) {
        return "";
    }
    var val = bytes / 1000000000;
    if (val < 10) {
        return val.toFixed(1) + sizes[3];
    } else {
        return val.toFixed() + sizes[3];
    }
}

function bytesToGB0(bytes) {
    if (isNaN(bytes)) {
        return "";
    }
    var val = bytes / 1000000000;
    return val.toFixed() + sizes[3];
}

function bytesToMB(bytes) {
    if (isNaN(bytes)) {
        return "";
    }
    var val = bytes / 1000000;
    if (val < 10) {
        return val.toFixed(2) + sizes[2];
    } else if (val < 100) {
        return val.toFixed(1) + sizes[2];
    } else {
        return val.toFixed() + sizes[2];
    }
}

function valueToNumber(value) {
    return Number(value).toLocaleString();
}
function asNumber(num) {
    return (num === undefined || num === null) ? 0 : Number(num);
}

function getTime() {
    return new Date().getTime();
}

function updateObject(target, source) {
    return Object.assign(target, source);
}

function getPDiskId(info) {
    return info.NodeId + '-' + info.PDiskId;
}

function getVDiskId(info) {
    var vDiskId = info.VDiskId;
    return vDiskId.GroupID + '-' + vDiskId.GroupGeneration + '-' + vDiskId.Ring + '-' + vDiskId.Domain + '-' + vDiskId.VDisk;
}

function getNodeIdFromPeerName(peerName) {
    return Number(peerName.split(':', 1)[0]);
}

function getPortFromPeerName(peerName) {
    return Number(peerName.split(':', 3)[2]);
}

function getPartValuesText(part, total) {
    if (part === total) {
        return total;
    } else {
        return part + ' / ' + total;
    }
}

function flagToColor(flag) {
    switch (flag) {
    case 'Green':
        return green;
    case 'Blue':
        return blue;
    case 'Yellow':
        return yellow;
    case 'Orange':
        return orange;
    case 'Red':
        return red;
    default:
        return grey;
    }
}

function isBastionProxy(hostname) {
    return hostname === 'ydb.bastion.cloud.yandex-team.ru';
}

function isViewerProxy(hostname) {
    return hostname === 'viewer.ydb.yandex-team.ru';
}

function getBaseUrlForHost(host) {
    var hostname = window.location.hostname;
    if (isBastionProxy(hostname)) {
        return 'https://ydb.bastion.cloud.yandex-team.ru/' + host + '/';
    } else if (isViewerProxy(hostname)) {
        return 'https://viewer.ydb.yandex-team.ru/' + host + '/';
    } else {
        return 'http://' + host + '/';
    }
}

function mapValue(min, max, value) {
    return Math.floor(min + (max - min + 1) * value);
}

function mapColor(minColor, maxColor, value) {
    var minR = parseInt(minColor.substr(1, 2), 16);
    var minG = parseInt(minColor.substr(3, 2), 16);
    var minB = parseInt(minColor.substr(5, 2), 16);
    var maxR = parseInt(maxColor.substr(1, 2), 16);
    var maxG = parseInt(maxColor.substr(3, 2), 16);
    var maxB = parseInt(maxColor.substr(5, 2), 16);
    return '#'
            + pad2(Number(mapValue(minR, maxR, value)).toString(16))
            + pad2(Number(mapValue(minG, maxG, value)).toString(16))
            + pad2(Number(mapValue(minB, maxB, value)).toString(16));
}

function PoolColors(colors) {
    if (colors === undefined) {
        this.colors = [
                 {usage: 0.00, R: 144, G: 238, B: 144},
                 {usage: 0.75, R: 255, G: 255, B: 0},
                 {usage: 1.00, R: 255, G: 0, B: 0}
             ];
    } else {
        this.colors = colors;
    }
}

PoolColors.prototype.getPoolColor = function(usage) {
    usage = Number(usage);
    var idx = 1;
    var len = this.colors.length - 1;
    for (idx = 1; idx < len && usage > this.colors[idx].usage; idx++);
    var a = this.colors[idx - 1];
    var b = this.colors[idx];
    usage = Math.max(usage, a.usage);
    usage = Math.min(usage, b.usage);
    var R = a.R + ((usage - a.usage) / (b.usage - a.usage)) * (b.R - a.R);
    var G = a.G + ((usage - a.usage) / (b.usage - a.usage)) * (b.G - a.G);
    var B = a.B + ((usage - a.usage) / (b.usage - a.usage)) * (b.B - a.B);
    return 'rgb(' + R.toFixed() + ',' + G.toFixed() + ',' + B.toFixed() + ')';
};

//code.iamkate.com
function Queue(){var a=[],b=0;this.getLength=function(){return a.length-b};this.isEmpty=function(){return 0==a.length};this.enqueue=function(b){a.push(b)};this.dequeue=function(){if(0!=a.length){var c=a[b];2*++b>=a.length&&(a=a.slice(b),b=0);return c}};this.peek=function(){return 0<a.length?a[b]:void 0}};

function UpdateQueue(options) {
    Object.assign(this, {}, options);
    this.updateQueue = new Queue();
    this.updateDelay = 0;
    this.runUpdateQueue();
}

UpdateQueue.prototype.enqueue = function(item) {
    if (this.updateDelay === 0) {
        this.onUpdate(item);
    } else {
        this.updateQueue.enqueue(item);
    }
};

UpdateQueue.prototype.isEmpty = function() {
    return this.updateQueue.isEmpty();
};

UpdateQueue.prototype.getLength = function() {
    return this.updateQueue.getLength();
};

UpdateQueue.prototype.runUpdateQueue = function() {
    if (!this.updateQueue.isEmpty()) {
        var update = this.updateQueue.dequeue();
        this.onUpdate(update);
        if (this.updateQueue.isEmpty()) {
            console.log('done updateQueue');
        } else {
            setTimeout(this.runUpdateQueue.bind(this), this.updateDelay);
            return;
        }
    }
    setTimeout(this.runUpdateQueue.bind(this), 100);
};

UpdateQueue.prototype.flushUpdateQueue = function() {
    while (!this.updateQueue.isEmpty()) {
        this.onUpdate(this.updateQueue.dequeue());
    }
};

(function() {
    // Workaround for Safari. Always attach tooltip to <body>
    var originalTooltipFn = $.fn.tooltip;
    $.fn.tooltip = function() {
        var arg0 = arguments[0];
        if ('object' == typeof arg0) {
            arg0['container'] = 'body';
        }
        return originalTooltipFn.apply(this, arguments);
    };
})();
