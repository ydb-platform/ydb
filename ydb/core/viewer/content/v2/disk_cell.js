function DiskCell(id, options) {
    Object.assign(this, {
                      Id: id,
                      vDisks: {},
                      pDiskClass: DiskCell.prototype.pDiskClass,
                      vDiskClass: DiskCell.prototype.vDiskClass,
                      vDisksCount: 0
                  },
                  options);
    this.buildDomElement();
}

DiskCell.prototype.buildDomElement = function() {
    var cell = $('<div>', {class: 'disk_cell'});
    this.domElement = cell[0];
    this.domElement.object = this;
}

DiskCell.prototype.buildVDiskPlace = function() {
    var cell = $(this.domElement);
    var vDiskPlace = $('<div>', {class: 'vdisk_place'});
    if (this.pDiskDomElement) {
        vDiskPlace.css('margin-bottom', '3px');
    }
    this.vDiskDomElement = vDiskPlace[0];
    cell.prepend(vDiskPlace);
}

DiskCell.prototype.buildPDiskPlace = function() {
    var cell = $(this.domElement);
    var pDiskPlace = $('<div>', {class: 'pdisk_place'});
    if (this.vDiskDomElement) {
        pDiskPlace.css('margin-top', '3px');
    }
    this.pDiskDomElement = pDiskPlace[0];
    cell.append(pDiskPlace);
}

DiskCell.prototype.vDiskClass = 'vdisk-wide';
DiskCell.prototype.vDiskClasses = 'vdisk-wide vdisk-normal vdisk-narrow';
DiskCell.prototype.pDiskClasses = 'pdisk-wide pdisk-normal pdisk-narrow';
DiskCell.prototype.pDiskClass = 'pdisk-narrow';
DiskCell.prototype.maxVDisks = 0;
DiskCell.prototype.maxWideVDisks = 5;
DiskCell.prototype.maxNormalVDisks = 6;
DiskCell.prototype.maxNarrowPDisks = 10;
DiskCell.prototype.maxNormalPDisks = 15;
DiskCell.prototype.totalWidth = 100;

DiskCell.prototype.updatePDiskClasses = function() {
    if (this.pDisk) {
        var pDisk = this.pDisk;
        $(pDisk.domElement).removeClass(this.pDiskClasses).addClass(DiskCell.prototype.pDiskClass);
    }
}

DiskCell.prototype.updateVDiskClasses = function() {
    for (var vDiskId in this.vDisks) {
        var vDisk = this.vDisks[vDiskId];
        $(vDisk.domElement).removeClass(this.vDiskClasses).addClass(DiskCell.prototype.vDiskClass);
    }
}

DiskCell.prototype.checkDisksClass = function(vDisks) {
    if (vDisks > DiskCell.prototype.maxVDisks) {
        var oldVDiskClass = DiskCell.prototype.vDiskClass;
        var oldPDiskClass = DiskCell.prototype.pDiskClass;

        DiskCell.prototype.maxVDisks = vDisks;
        if (DiskCell.prototype.maxVDisks > this.maxWideVDisks) {
            DiskCell.prototype.vDiskClass = 'vdisk-normal';
        }
        if (DiskCell.prototype.maxVDisks > this.maxNormalVDisks) {
            DiskCell.prototype.vDiskClass = 'vdisk-narrow';
        }
        if (DiskCell.prototype.maxVDisks > this.maxNarrowPDisks) {
            DiskCell.prototype.pDiskClass = 'pdisk-normal';
            DiskCell.prototype.totalWidth = 160;
        }
        if (DiskCell.prototype.maxVDisks > this.maxNormalPDisks) {
            DiskCell.prototype.pDiskClass = 'pdisk-wide';
            DiskCell.prototype.totalWidth = 260;
        }
        if (oldVDiskClass !== DiskCell.prototype.vDiskClass || oldPDiskClass !== DiskCell.prototype.pDiskClass) {
            $('.' + oldVDiskClass).removeClass(oldVDiskClass).addClass(DiskCell.prototype.vDiskClass);
            $('.' + oldPDiskClass).removeClass(oldPDiskClass).addClass(DiskCell.prototype.pDiskClass);
        }
    }
    if (this.pDiskClass !== DiskCell.prototype.pDiskClass) {
        if (this.diskMap) {
            this.diskMap.updatePDiskClasses();
        }
        this.pDiskClass = DiskCell.prototype.pDiskClass;
    }
    if (this.vDiskClass !== DiskCell.prototype.vDiskClass) {
        if (this.diskMap) {
            this.diskMap.updateVDiskClasses();
        }
        this.vDiskClass = DiskCell.prototype.vDiskClass;
    }
}

DiskCell.prototype.updatePDiskInfo = function(update) {
    var id = getPDiskId(update);
    var pDisk = this.pDisk;
    if (pDisk === undefined) {
        if (!this.pDiskDomElement) {
            this.buildPDiskPlace();
        }
        pDisk = this.pDisk = new PDisk(update, {class: DiskCell.prototype.pDiskClass, diskCell: this});
        this.pDiskDomElement.appendChild(pDisk.domElement);
    }
    //this.allocatedSize = Number(update.TotalSize) - Number(update.AvailableSize);
    pDisk.updatePDiskInfo(update);
    return pDisk;
}

DiskCell.prototype.updateVDiskInfo = function(update) {
    var id = getVDiskId(update);
    var vDisk = this.vDisks[id];
    if (vDisk === undefined) {
        if (!this.vDiskDomElement) {
            this.buildVDiskPlace();
        }
        vDisk = this.vDisks[id] = new VDisk(update, {class: DiskCell.prototype.vDiskClass, diskCell: this});
        this.vDiskDomElement.appendChild(vDisk.domElement);
        this.vDisksCount = this.vDiskDomElement.childElementCount;
        this.checkDisksClass(this.vDisksCount);
    }
    vDisk.updateVDiskInfo(update);
    return vDisk;
}

DiskCell.prototype.resizeVDisks = function() {
    if (this.vDisksCount) {
        var minWidth = 5;
        var availWidth = this.totalWidth - minWidth * this.vDisksCount;
        var normalWidth = availWidth / this.vDisksCount;
        if (availWidth > 0) {
            var allocatedSize = 0;
            var known = 0;
            var total = 0;
            var averageSize = 0;
            for (var id in this.vDisks) {
                var vDisk = this.vDisks[id];
                if (vDisk.AllocatedSize) {
                    allocatedSize = allocatedSize + Number(vDisk.AllocatedSize);
                    known++;
                }
                total++;
            }
            if (known < total && known > 0) {
                averageSize = allocatedSize / known;
                allocatedSize = averageSize * total;
            }

            for (var id in this.vDisks) {
                var vDisk = this.vDisks[id];
                var partSize;
                if (vDisk.AllocatedSize) {
                    partSize = vDisk.AllocatedSize / (allocatedSize / this.vDisksCount);
                } else {
                    if (allocatedSize > 0) {
                        partSize = averageSize / (allocatedSize / this.vDisksCount);
                    } else {
                        partSize = 1;
                    }
                }
                var width = Math.floor(normalWidth * partSize + minWidth);
                if (vDisk.width != width) {
                    $(vDisk.domElement).css('width', width + 'px');
                }
                vDisk.width = width;
            }
        }
    }
}

DiskCell.prototype.getUsage = function() {
    if (this.pDisk) {
        return this.pDisk.getUsage();
    } else {
        return 0;
    }
}

DiskCell.prototype.restoreTooltips = function() {
    for (var id in this.vDisks) {
        var vDisk = this.vDisks[id];
        vDisk.restoreTooltips();
    }
    if (this.pDisk) {
        this.pDisk.restoreTooltips();
    }
}
