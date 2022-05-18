function VDisk(update, options) {
    Object.assign(this, update, options);
    this.Id = getVDiskId(this);
    this.buildDomElement();
}

VDisk.prototype.getVDiskUrl = function() {
    var url;
    if (this.diskCell && this.diskCell.diskMap && this.diskCell.diskMap.node) {
        url = this.diskCell.diskMap.node.getBaseUrl() + 'actors/vdisks/vdisk' + pad9(this.PDiskId) + "_" + pad9(this.VDiskSlotId);
    } else if (this.PDisk && this.PDisk.Host) {
        url = getBaseUrlForHost(this.PDisk.Host + ':8765') + 'actors/vdisks/vdisk' + pad9(this.PDisk.PDiskId) + "_" + pad9(this.VDiskSlotId);
    }
    return url;
}

VDisk.prototype.onClick = function() {
    var url = this.getVDiskUrl();
    if (url) {
        window.open(url);
    }
}

VDisk.prototype.buildDomElement = function() {
    var vDiskElement = $('<div>', {class: 'vdisk ' + this.class});
    vDiskElement.tooltip({html: true}).on('show.bs.tooltip', function() {
        this.diskCell.diskMap.onShowVDiskToolTip(this);
    }.bind(this)).on('hide.bs.tooltip', function() {
        this.diskCell.diskMap.onHideVDiskToolTip(this);
    }.bind(this)).click(this.onClick.bind(this));
    this.domElement = vDiskElement[0];
    this.updateVDiskInfo();
}

VDisk.prototype.stateSeverity = {
    Initial: 3,
    LocalRecoveryError: 5,
    SyncGuidRecoveryError: 5,
    SyncGuidRecovery: 3,
    PDiskError: 5,
    OK: 1
};

VDisk.prototype.colorSeverity = {
    Grey: 0,
    Green: 1,
    Blue: 2,
    Yellow: 3,
    Orange: 4,
    Red: 5
};

VDisk.prototype.getStateSeverity = function() {
    var sev = this.stateSeverity[this.VDiskState];
    if (sev === undefined) {
        sev = 0;
    }
    return sev;
}

VDisk.prototype.isErrorState = function() {
    return this.getStateSeverity() === 5;
}

VDisk.prototype.getColorSeverity = function(color) {
    var sev = this.colorSeverity[color];
    if (sev === undefined) {
        sev = 0;
    }
    return sev;
}

VDisk.prototype.updateVDiskInfo = function(update) {
    if (update) {
        if (update.AllocatedSize) {
            update.AllocatedSize = Number(update.AllocatedSize);
        }
        if (update.ReadThroughput) {
            update.ReadThroughput = Number(update.ReadThroughput);
        }
        if (update.WriteThroughput) {
            update.WriteThroughput = Number(update.WriteThroughput);
        }
        Object.assign(this, update);
        if (!update.VDiskState) {
            delete this.VDiskState;
        }
    }
    var vDisk = $(this.domElement);
    var state;
    var severity = 0;

    state = '<table class="tooltip-table"><tr><td>VDisk</td><td>' + this.Id + '</td></tr>';
    if (this.VDiskState) {
        state += '<tr><td>State</td><td>' + this.VDiskState + '</td></tr>';
        severity = this.getStateSeverity();
    } else {
        state += '<tr><td>State</td><td>not available</td></tr>';
    }

    if (this.StoragePoolName) {
        state += '<tr><td>StoragePool</td><td style="white-space: nowrap">' + this.StoragePoolName + '</td></tr>';
    }
    var rank = this.SatisfactionRank;
    if (rank) {
        if (rank.FreshRank) {
            if (rank.FreshRank.Flag !== 'Green') {
                state += '<tr><td>Fresh</td><td>' + rank.FreshRank.Flag + '</td></tr>';
                severity = Math.max(severity, Math.min(4, this.getColorSeverity(rank.FreshRank.Flag)));
            }
        }
        if (rank.LevelRank) {
            if (rank.LevelRank.Flag !== 'Green') {
                state += '<tr><td>Level</td><td>' + rank.LevelRank.Flag + '</td></tr>';
                severity = Math.max(severity, Math.min(4, this.getColorSeverity(rank.LevelRank.Flag)));
            }
        }
        if (rank.FreshRank.RankPercent) {
            state += '<tr><td>Fresh</td><td>' + rank.FreshRank.RankPercent + '%</td></tr>';
        }
        if (rank.LevelRank.RankPercent) {
            state += '<tr><td>Level</td><td>' + rank.LevelRank.RankPercent + '%</td></tr>';
        }
    }
    if (this.DiskSpace && this.DiskSpace !== 'Green') {
        state += '<tr><td>Space</td><td>' + this.DiskSpace + '</td></tr>';
        severity = Math.max(severity, this.getColorSeverity(this.DiskSpace));
    }
    if (this.FrontQueues && this.FrontQueues !== 'Green') {
        state += '<tr><td>FronQueues</td><td>' + this.FrontQueues + '</td></tr>';
        severity = Math.max(severity, Math.min(4, this.getColorSeverity(this.FrontQueues)));
    }
    isDonor = false;
    if (this.DonorMode === true) {
        state += '<tr><td>Donor</td><td>YES</td></tr>';
        isDonor = true;
    } else if (!this.Replicated) {
        state += '<tr><td>Replicated</td><td>NO</td></tr>';
        if (severity === 1) {
            severity = 2;
        }
    }
    hasDonors = false;
    if (this.Donors) {
        hasDonors = true;
        for (var d = 0; d < this.Donors.length; ++d) {
            var donor = this.Donors[d];
            if (d === 0) {
                state += '<tr><td>Donors</td><td>';
            } else {
                state += '<tr><td></td><td>';
            }
            state += donor.NodeId;
            state += '-';
            state += donor.PDiskId;
            state += ' vslot ';
            state += donor.VSlotId;
            state += '</td></tr>';
        }
    }

    var color = grey;
    switch (severity) {
    case 0:
        color = grey;
        break;
    case 1:
        color = green;
        break;
    case 2:
        color = blue;
        break;
    case 3:
        color = yellow;
        break;
    case 4:
        color = orange;
        break;
    case 5:
        color = red;
        break;
    }

    if (this.UnsyncedVDisks > 0) {
        state += '<tr><td>UnsyncVDisks</td><td>' + this.UnsyncedVDisks + '</td></tr>';
    }
    if (this.AllocatedSize) {
        state += '<tr><td>Allocated</td><td>' + bytesToGB(this.AllocatedSize) + '</td></tr>';
    }
    if (this.ReadThroughput) {
        state += '<tr><td>Read</td><td>' + bytesToSpeed(this.ReadThroughput) + '</td></tr>';
    }
    if (this.WriteThroughput) {
        state += '<tr><td>Write</td><td>' + bytesToSpeed(this.WriteThroughput) + '</td></tr>';
    }
    state += '</table>';
    if (isDonor) {
        background = 'linear-gradient(45deg, forestgreen, forestgreen 8px, transparent 8px, transparent 100%)';
        vDisk.css('background', background);
    } else if (hasDonors) {
        background = 'linear-gradient(45deg, darkmagenta, darkmagenta 8px, transparent 8px, transparent 100%)';
        vDisk.css('background', background);
    }
    vDisk.css('background-color', color);
    vDisk.attr('data-original-title', state);
    this.color = color;
}

VDisk.prototype.restoreTooltips = function() {
    $(this.domElement).tooltip({html: true}).on('show.bs.tooltip', function() {
        this.diskCell.diskMap.onShowVDiskToolTip(this);
    }.bind(this)).on('hide.bs.tooltip', function() {
        this.diskCell.diskMap.onHideVDiskToolTip(this);
    }.bind(this));
}
