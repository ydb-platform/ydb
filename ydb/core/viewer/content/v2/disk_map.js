function DiskMap(options) {
    Object.assign(this, {
                      cells: {},
                      vDisksCount: 0
                  },
                  options);
    this.buildDomElement();
}

DiskMap.prototype.buildDomElement = function() {
    var diskRow = $('<div>', {class: 'disk_map'});
    this.domElement = diskRow[0];
    return diskRow[0];
}

DiskMap.prototype.getDiskCell = function(id, update) {
    var cell = this.cells[id];
    if (cell === undefined) {
        cell = this.cells[id] = new DiskCell(id, {diskMap: this});
        this.domElement.appendChild(cell.domElement);
    }
    if (!cell.path && update.Path) {
        cell.path = update.Path;
        this.sort();
    }
    return cell;
}

DiskMap.prototype.sort = function() {
    var domElement = this.domElement;
    var children = [];
    for (var i = 0; i < domElement.children.length; ++i) {
        children.push(domElement.children[i]);
    }
    children.sort(function(a, b) {
        return (a.object.path && b.object.path) ? a.object.path.localeCompare(b.object.path) : 0;
    });
    children.forEach(function(obj) {
        domElement.appendChild(obj);
    });
}

DiskMap.prototype.updatePDiskInfo = function(update) {
    var id;
    if (update.NodeId && update.PDiskId !== undefined) {
        id = getPDiskId(update);
    } else if (update.CellId !== undefined) {
        id = update.CellId;
    }
    if (id !== undefined) {
        var cell = this.getDiskCell(id, update);
        return cell.updatePDiskInfo(update);
    }
}

DiskMap.prototype.updateVDiskInfo = function(update) {
    var id;
    if (update.NodeId && update.PDiskId !== undefined) {
        id = getPDiskId(update);
    } else if (update.CellId !== undefined) {
        id = update.CellId;
    }
    if (id !== undefined) {
        var cell = this.getDiskCell(id, update);
        var count = cell.vDisksCount;
        var vDisk = cell.updateVDiskInfo(update);
        this.vDisksCount += cell.vDisksCount - count;
        return vDisk;
    }
}

DiskMap.prototype.updatePDiskClasses = function() {
    for (var cellId in this.cells) {
        var cell = this.cells[cellId];
        cell.updatePDiskClasses();
    }
}

DiskMap.prototype.updateVDiskClasses = function() {
    for (var cellId in this.cells) {
        var cell = this.cells[cellId];
        cell.updateVDiskClasses();
    }
}

DiskMap.prototype.resizeVDisks = function() {
    for (var cellId in this.cells) {
        var cell = this.cells[cellId];
        cell.resizeVDisks();
    }
}

DiskMap.prototype.getUsage = function() {
    var usage = 0;
    for (var cellId in this.cells) {
        var cell = this.cells[cellId];
        if (cell.pDisk) {
            usage = Math.max(usage, cell.pDisk.getUsage());
        }
    }
    return usage;
}

DiskMap.prototype.restoreTooltips = function() {
    for (var cellId in this.cells) {
        var cell = this.cells[cellId];
        cell.restoreTooltips();
    }
}

DiskMap.prototype.onShowPDiskToolTip = function(pDisk) {}
DiskMap.prototype.onHidePDiskToolTip = function(pDisk) {}
DiskMap.prototype.onShowVDiskToolTip = function(vDisk) {}
DiskMap.prototype.onHideVDiskToolTip = function(vDisk) {}
