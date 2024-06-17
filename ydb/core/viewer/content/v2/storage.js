function Storage(options) {
    Object.assign(this, {
                      Id: options.GroupID,
                      sysInfo: {},
                      nodeInfo: {},
                      poolMap: null,
                      nodeMap: null,
                      diskMap: null
                  },
                  options);
    this.buildDomElement();
}

Storage.prototype.buildDomElement = function() {
    var row = $('<tr>');
    row.data('storage', this);
    var id = $('<span>', {class: 'storage_group_id', text: this.Id});
    this.storageGroupIdDomElement = id[0];
    id.tooltip();
    row.append($('<td>').append(id));

    /*

    row.append($('<td>', {class: 'storage_units', text: this.AcquiredUnits}));
    row.append($('<td>', {class: 'storage_erasure', text: this.ErasureSpecies}));
    row.append($('<td>', {class: 'storage_latency', text: this.Latency}));

    var vdisks = new DiskMap(this.Id + '-vdisks');
    var pdisks = new DiskMap(this.Id + '-pdisks');

    for (var numVDisk in this.VDisks) {
        var vDisk = this.VDisks[numVDisk];
        vDisk.CellId = this.Id;
        vdisks.updateVDiskInfo(vDisk);
    }

    for (var numPDisk in this.VDisks) {
        var vDisk = this.VDisks[numPDisk];
        var pDisk = vDisk.PDisk;
        if (pDisk) {
            pdisks.updatePDiskInfo(pDisk);
        }
    }

    vdisks.resizeVDisks();

    var disks = $('<td>', {class: 'storage_vdisks'});
    disks.append(vdisks.domElement);
    row.append(disks);

    disks = $('<td>', {class: 'storage_pdisks'});
    disks.append(pdisks.domElement);
    row.append(disks);

    */

    this.domElement = row[0];

    /*var host = $('<td>');
    var hostName = $('<div>');
    var hostLink = $('<a>', {
                         'href': this.getViewerUrl(),
                         'target': '_blank',
                         'data-original-title': this.Address
                     });
    hostLink.html(this.getHostName());
    var hostRoles = $('<span>', {class: 'node_roles'});
    hostName.append(hostLink);
    hostName.append(hostRoles);
    hostLink.tooltip({html: true});
    host.append(hostName);
    var state = $('<div>', {class: 'statecontainer'});
    var loadAverage = $('<div>', {
                            'class': 'usageblock progress',
                            'data-original-title': 'Load average'
                        });
    var loadAverageBar = $('<div>', {
                               class: 'progress-bar',
                               role: 'progressbar'
                           });
    loadAverage.append(loadAverageBar);
    loadAverage.tooltip({html: true});
    state.append(loadAverage);
    host.append(state);
    row.append(host);
    var uptime = $('<td>', {class: 'uptime_place'});
    row.append(uptime);
    var cpu = $('<td>', {class: 'cpu_place'});
    row.append(cpu);
    var interconnect = $('<td>', {class: 'interconnect_place'});
    row.append(interconnect);
    var disks = $('<td>', {class: 'disk_place'});
    row.append(disks);
    this.domElement = row[0];
    this.domElementNodeIcons = nodeIcons[0];
    this.domElementHostLink = hostLink[0];
    this.domElementUpTime = uptime[0];
    this.domElementRoles = hostRoles[0];
    this.domElementLoadAverageBar = loadAverageBar[0];*/
}

Storage.prototype.imageManGreen = $('<img src="man-green.png" style="height:26px" data-original-title="<500ms">');
Storage.prototype.imageManYellow = $('<img src="man-yellow.png" style="height:30px" data-original-title="500ms - 1000ms">');
Storage.prototype.imageManOrange = $('<img src="man-orange.png" style="height:28px" data-original-title="1000ms - 2000ms">');
Storage.prototype.imageManRed = $('<img src="man-red.png" style="height:28px" data-original-title=">2000ms">');
Storage.prototype.spanNone = $('<span>-</span>');

Storage.prototype.getImageFromLatency = function() {
    /*switch (Math.floor(Math.random()*4)) {
    case 0:
        return this.imageManGreen.clone().tooltip();
    case 1:
        return this.imageManYellow.clone().tooltip();
    case 2:
        return this.imageManOrange.clone().tooltip();
    case 3:
        return this.imageManRed.clone().tooltip();
    }*/
    switch (this.Latency) {
    case 'Green':
        return this.imageManGreen.clone().tooltip();
    case 'Yellow':
        return this.imageManYellow.clone().tooltip();
    case 'Orange':
        return this.imageManOrange.clone().tooltip();
    case 'Red':
        return this.imageManRed.clone().tooltip();
    }
    return this.spanNone.clone();
}

Storage.prototype.appear = function() {
    if (!this.visible) {
        var row = $(this.domElement);

        var erasureElem;

        if (this.BlobDepotId === undefined) {
            erasureElem = $('<td>', {class: 'storage_erasure', text: this.ErasureSpecies});
        } else if (!this.BlobDepotId) {
            erasureElem = $('<td>', {text: 'BlobDepot (error)'});
        } else {
            label = 'BlobDepot';
            if (this.BlobDepotOnlineTime === undefined || getTime() - this.BlobDepotOnlineTime >= 15000) {
                label = 'BlobDepot (error)';
            }
            erasureElem = $('<td>');
            var link = $('<a>', {
                'href': '../../../tablets/app?TabletID=' + this.BlobDepotId,
                'text': label,
                'title': this.BlobDepotId
            });
            erasureElem.html(link);
        }

        row.append(erasureElem);

        row.append(this.storageUnits = $('<td>', {class: 'storage_units'}));
        row.append(this.allocatedSize = $('<td>'));
        row.append(this.availableSize = $('<td>'));
        row.append(this.readSpeed = $('<td>'));
        row.append(this.writeSpeed = $('<td>'));

        var pDiskOptions = {
            onShowPDiskToolTip: function(pDisk) {
                if (this.enableEvents) {
                    var vDisk = this.vDisksByPDisks[pDisk.Id];
                    if (vDisk) {
                        this.enableEvents = false;
                        $(vDisk.domElement).tooltip('show');
                        this.enableEvents = true;
                    }
                }
            }.bind(this),
            onHidePDiskToolTip: function(pDisk) {
                if (this.enableEvents) {
                    var vDisk = this.vDisksByPDisks[pDisk.Id];
                    if (vDisk) {
                        this.enableEvents = false;
                        $(vDisk.domElement).tooltip('hide');
                        this.enableEvents = true;
                    }
                }
            }.bind(this),
            sort: function() {}
        };

        var vDiskOptions = {
            onShowVDiskToolTip: function(vDisk) {
                if (this.enableEvents) {
                    var pDisk = this.pDisksByVDisks[vDisk.Id];
                    if (pDisk) {
                        this.enableEvents = false;
                        $(pDisk.domElement).tooltip('show');
                        this.enableEvents = true;
                    }
                }
            }.bind(this),
            onHideVDiskToolTip: function(vDisk) {
                if (this.enableEvents) {
                    var pDisk = this.pDisksByVDisks[vDisk.Id];
                    if (pDisk) {
                        this.enableEvents = false;
                        $(pDisk.domElement).tooltip('hide');
                        this.enableEvents = true;
                    }
                }
            }.bind(this),
            sort: function() {}
        };

        this.enableEvents = true;

        this.vDiskMap = new DiskMap(vDiskOptions);
        this.pDiskMap = new DiskMap(pDiskOptions);

        this.vDisksByPDisks = {};
        this.pDisksByVDisks = {};

        row.append(this.storageLatency = $('<td>', {class: 'storage_latency'}));

        var vdisks = $('<td>', {class: 'storage_vdisks'});
        vdisks.append(this.vDiskMap.domElement);
        row.append(vdisks);

        var pdisks = $('<td>', {class: 'storage_pdisks'});
        pdisks.append(this.pDiskMap.domElement);

        this.update();

        row.append(pdisks);
        this.visible = true;
    }
}

Storage.prototype.disappear = function() {
    if (this.visible) {
        $(this.domElement).find('td').slice(1).remove();
        delete this.vDiskMap;
        delete this.pDiskMap;
        delete this.vDisksByPDisks;
        delete this.pDisksByVDisks;
        this.visible = false;
    }
}

Storage.prototype.update = function() {
    //var row = $(this.domElement);
    if (this.AcquiredUnits > 0) {
        this.storageUnits.text(this.AcquiredUnits);
    } else {
        this.storageUnits.text('-');
    }
    this.storageLatency.empty().append(this.getImageFromLatency());

    var nodes = this.group.view.nodes;
    var allocatedSize;
    var availableSize;
    var readSpeed = 0;
    var writeSpeed = 0;

    for (var numDisk in this.VDisks) {
        var vDisk = this.VDisks[numDisk];
        var pDisk = vDisk.PDisk;
        vDisk.CellId = this.Id;
        vDisk.StoragePoolName = this.StoragePool.Name;
        vDisk = this.vDiskMap.updateVDiskInfo(vDisk);
        if (vDisk.AllocatedSize !== undefined) {
            if (allocatedSize === undefined) {
                allocatedSize = 0;
            }
            allocatedSize += vDisk.AllocatedSize;
        }
        if (vDisk.AvailableSize !== undefined) {
            if (availableSize === undefined) {
                availableSize = 0;
            }
            availableSize += vDisk.AvailableSize;
        }
        if (vDisk.ReadThroughput) {
            readSpeed += vDisk.ReadThroughput;
        }
        if (vDisk.WriteThroughput) {
            writeSpeed += vDisk.WriteThroughput;
        }

        if (!pDisk) {
            pDisk = {NodeId: '?', PDiskId: '?', Id: 'unknown-' + numDisk};
        }
        var node = nodes[pDisk.NodeId];
        if (node) {
            pDisk.Host = node.Host;
        }
        pDisk = this.pDiskMap.updatePDiskInfo(pDisk);
        this.vDisksByPDisks[pDisk.Id] = vDisk;
        this.pDisksByVDisks[vDisk.Id] = pDisk;
    }

    if (this.AllocatedSize !== undefined) {
        allocatedSize = Number(this.AllocatedSize);
    }
    if (this.AvailableSize !== undefined) {
        availableSize = Number(this.AvailableSize);
    }
    if (this.ReadThroughput !== undefined) {
        readSpeed = Number(this.ReadThroughput);
    }
    if (this.WriteThroughput !== undefined) {
        writeSpeed = Number(this.WriteThroughput);
    }

    this.vDiskMap.resizeVDisks();

    if (allocatedSize === undefined) {
        this.allocatedSize.text('-');
    } else {
        this.allocatedSize.text(bytesToGB(allocatedSize));
    }
    if (availableSize === undefined) {
        this.availableSize.text('-');
    } else {
        this.availableSize.text(bytesToGB(availableSize));
    }
    if (readSpeed) {
        this.readSpeed.text(bytesToSpeed(readSpeed));
    } else {
        this.readSpeed.text('-');
    }
    if (writeSpeed) {
        this.writeSpeed.text(bytesToSpeed(writeSpeed));
    } else {
        this.writeSpeed.text('-');
    }
}

function isVDiskInErrorState(state) {
    if (!state) {
        return false;
    }

    switch (state) {
    case "LocalRecoveryError":
    case "SyncGuidRecoveryError":
    case "PDiskError":
        return true;
    default:
        return false;
    }
}

Storage.prototype.updateFromStorage = function(update) {
    if (this.GroupGeneration < update.GroupGeneration && this.visible) {
        this.disappear();
        this.VDisks = [];
        this.appear();
    }
    var storage = updateObject(this, update);
    $(this.storageGroupIdDomElement).attr('data-original-title', storage.StoragePool.Name);
    switch (this.Overall) {
    case 'Green':
        this.color = green;
        break;
    case 'Blue':
        this.color = blue;
        break;
    case 'Yellow':
        this.color = yellow;
        //console.log(this);
        break;
    case 'Orange':
        this.color = orange;
        //console.log(this);
        break;
    case 'Red':
        this.color = red;
        //console.log(this);
        break;
    }

    var blobDepotError = false;
    if (this.BlobDepotId !== undefined && (!this.BlobDepotId || this.BlobDepotOnlineTime === undefined ||
            getTime() - this.BlobDepotOnlineTime >= 15000)) {
        this.color = red; // blob depot is ought to be working, but it does not
        blobDepotError = true;
    }

    var usage = 0;
    var missingDisks = 0;
    for (var numDisk in this.VDisks) {
        var vDisk = this.VDisks[numDisk];
        if (vDisk) {
            if (vDisk.AllocatedSize) {
                vDisk.AllocatedSize = Number(vDisk.AllocatedSize);
            }
            if (vDisk.AvailableSize) {
                vDisk.AvailableSize = Number(vDisk.AvailableSize);
            }
            // meaingful only if hard disk space division is enabled
            //usage = Math.max(usage, vDisk.AllocatedSize / (vDisk.AllocatedSize + vDisk.AvailableSize));
            if (vDisk.ReadThroughput) {
                vDisk.ReadThroughput = Number(vDisk.ReadThroughput);
            }
            if (vDisk.WriteThroughput) {
                vDisk.WriteThroughput = Number(vDisk.WriteThroughput);
            }
        }

        var pDisk = vDisk.PDisk;
        if (pDisk) {
            if (pDisk.TotalSize) {
                pDisk.TotalSize = Number(pDisk.TotalSize);
            }
            if (pDisk.AvailableSize) {
                pDisk.AvailableSize = Number(pDisk.AvailableSize);
            }
        }
        if (pDisk && pDisk.AvailableSize && pDisk.TotalSize) {
            usage = Math.max(usage, 1 - pDisk.AvailableSize / pDisk.TotalSize);
            if (!pDisk.State || (vDisk.Replicated === false && !vDisk.DonorMode) || isVDiskInErrorState(vDisk.VDiskState) === true) {
                missingDisks++;
            }
        } else {
            missingDisks++;
        }
    }
    this.usage = usage;
    if (blobDepotError) {
        missingDisks = -1;
    }
    this.missingDisks = missingDisks;
    if (this.visible) {
        this.update();
    }
}

Storage.prototype.getColor = function() {
    switch (this.color) {
    case grey:
        return "Grey";
    case green:
        return "Green";
    case blue:
        return "Blue";
    case yellow:
        return "Yellow";
    case orange:
        return "Orange";
    case red:
        return "Red";
    }
}

Storage.prototype.getTooltipText = function() {
    var text = '#' + this.Id;
    return text;
}

Storage.prototype.getStatus = function() {
    if (this.color) {
        if (this.color === green) {
            return 'Good';
        } else {
            return 'Bad';
        }
    }
    return 'Unknown';
}

Storage.prototype.getUsage = function() {
    return this.usage;
}

Storage.prototype.getMissingDisks = function() {
    return this.missingDisks;
}
