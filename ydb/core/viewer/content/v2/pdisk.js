function PDisk(update, options) {
    Object.assign(this, update, options);
    this.Id = getPDiskId(this);
    this.buildDomElement();
}

PDisk.prototype.getPDiskUrl = function() {
    var url;
    if (this.diskCell && this.diskCell.diskMap && this.diskCell.diskMap.node) {
        url = this.diskCell.diskMap.node.getBaseUrl() + 'actors/pdisks/pdisk' + pad9(this.PDiskId);
    } else if (this.Host) {
        url = getBaseUrlForHost(this.Host + ':8765') + 'actors/pdisks/pdisk' + pad9(this.PDiskId);
    }
    return url;
}

PDisk.prototype.onClick = function() {
    var url = this.getPDiskUrl();
    if (url) {
        window.open(url);
    }
}

PDisk.prototype.buildDomElement = function() {
    var pDiskElement = $('<div>', {
                             class: 'progress pdisk ' + this.class
                         });
    var progressBar = $('<div>', {
                            'class': 'progress-bar pdisk_bar',
                            'aria-valuemin': 0,
                            'aria-valuemax': 100
                        });
    progressBar.append($('<span>', {class: 'pdisk_bar_value'}));
    pDiskElement.append(progressBar);
    pDiskElement.append($('<div>', {class: 'pdisk_icons'}));
    pDiskElement.tooltip({html: true}).on('show.bs.tooltip', function() {
        this.diskCell.diskMap.onShowPDiskToolTip(this);
    }.bind(this)).on('hide.bs.tooltip', function() {
        this.diskCell.diskMap.onHidePDiskToolTip(this);
    }.bind(this)).click(this.onClick.bind(this));

    this.domElement = pDiskElement[0];
    this.updatePDiskInfo();
}

PDisk.prototype.getIcon = function(icon, color) {
    return "<span class='glyphicon glyphicon-" + icon + " pdisk_icon' style='color:" + color + "'></span>";
}

PDisk.prototype.getDeviceIcon = function(color) {
    return this.getIcon('save', color);
}

PDisk.prototype.getRealtimeIcon = function(color) {
    return this.getIcon('fire', color);
}

PDisk.prototype.getUsage = function() {
    if (this.TotalSize && this.AvailableSize) {
        return 1 - (this.AvailableSize / this.TotalSize);
    } else {
        return 0;
    }
}

PDisk.prototype.updatePDiskInfo = function(update) {
    if (update) {
        if (update.TotalSize) {
            update.TotalSize = Number(update.TotalSize);
        }
        if (update.AvailableSize) {
            update.AvailableSize = Number(update.AvailableSize);
        }
        Object.assign(this, update);
        if (!update.State) {
            delete this.State;
        }
    }
    var pDisk = $(this.domElement);
    var state = '';
    var html = '';
    var progressBar = pDisk.find('.progress-bar');
    var progressBarValue = progressBar.find('.pdisk_bar_value');
    state = '<table class="tooltip-table"><tr><td>PDisk</td><td>' + this.Id + '</td></tr>';
    if (this.State) {
        state += '<tr><td>State</td><td>' + this.State + '</td></tr>';
    } else {
        state += '<tr><td>State</td><td>not available</td></tr>';
    }
    if (this.Host) {
        state += '<tr><td>Host</td><td style="white-space: nowrap">' + this.Host + '</td></tr>';
    }
    if (this.Path) {
        state += '<tr><td>Path</td><td style="white-space: nowrap">' + this.Path + '</td></tr>';
    }
    if (this.State === 'Normal') {
        pDisk.css('backgroundColor', 'lightblue');
        this.color = green;
        var total = this.TotalSize;
        var avail = this.AvailableSize;
        if (total !== undefined && avail !== undefined) {
            var percent = Math.round((total - avail) * 100 / total);
            var progressClass = '';
            if (percent >= 92) {
                progressClass = 'progress-bar-danger';
                this.color = orange;
            } else if (percent >= 85) {
                progressClass = 'progress-bar-warning';
                this.color = yellow;
            } else {
                progressClass = 'progress-bar-success';
            }
            progressBar.removeClass('progress-bar-danger progress-bar-warning progress-bar-success').addClass(progressClass);
            progressBar.attr('aria-valuenow', percent);
            progressBar.css('width', percent + '%');
            progressBarValue.html(percent + '%');
            if (percent < 8 || percent >= 85) {
                progressBarValue.css('color', '#303030');
            } else {
                progressBarValue.css('color', 'white');
            }
            state += '<tr><td>Available</td><td>' + bytesToGB(avail) + ' of ' + bytesToGB(total) + '</td></tr>';
            if (this.Realtime !== undefined) {
                switch (this.Realtime) {
                case 'Yellow':
                    html += this.getRealtimeIcon('yellow');
                    state += '<tr><td>Realtime</td><td>Yellow</td></tr>';
                    break;
                case 'Orange':
                    html += this.getRealtimeIcon('orange');
                    state += '<tr><td>Realtime</td><td>Orange</td></tr>';
                    break;
                case 'Red':
                    html += this.getRealtimeIcon('red');
                    state += '<tr><td>Realtime</td><td>Red</td></tr>';
                    break;
                }
            }

            if (this.Device !== undefined) {
                switch (this.Device) {
                case 'Yellow':
                    html += this.getDeviceIcon('yellow');
                    state += '<tr><td>Device</td><td>Yellow</td></tr>';
                    break;
                case 'Orange':
                    html += this.getDeviceIcon('orange');
                    state += '<tr><td>Device</td><td>Orange</td></tr>';
                    break;
                case 'Red':
                    html += this.getDeviceIcon('red');
                    state += '<tr><td>Device</td><td>Red</td></tr>';
                    break;
                }
            }
            //progressBar.append(html);
        }
    }
    switch (this.State) {
    case 'Initial':
        pDisk.css('background-color', grey);
        this.color = grey;
        break;
    case 'Normal':
        pDisk[0].style.backgroundColor = 'lightblue';
        this.color = green;
        // wtf?
        break;
    case 'InitialFormatRead':
    case 'InitialSysLogRead':
    case 'InitialCommonLogRead':
        pDisk.css('background-color', yellow);
        this.color = yellow;
        break;
    case 'InitialFormatReadError':
    case 'InitialSysLogReadError':
    case 'InitialSysLogParseError':
    case 'InitialCommonLogReadError':
    case 'InitialCommonLogParseError':
    case 'CommonLoggerInitError':
    case 'OpenFileError':
    case 'ChunkQuotaError':
    case 'DeviceIoError':
    case 'Stopped':
        pDisk.css('background-color', red);
        this.color = red;
        break;
    case undefined:
        pDisk.css('background-color', red);
        this.color = red;
        break;
    }

    state += '</table>';
    var pDiskIcons = pDisk.find('.pdisk_icons');
    pDiskIcons.html(html);
    pDisk.attr('data-original-title', state);
}

PDisk.prototype.restoreTooltips = function() {
    $(this.domElement).tooltip({html: true}).on('show.bs.tooltip', function() {
        this.diskCell.diskMap.onShowPDiskToolTip(this);
    }.bind(this)).on('hide.bs.tooltip', function() {
        this.diskCell.diskMap.onHidePDiskToolTip(this);
    }.bind(this));
}
