function TabletMap(options) {
    Object.assign(this, {
                      cells: {}
                  },
                  options);
    this.buildDomElement();
}

TabletMap.prototype.buildDomElement = function() {
    var tabletRow = $('<div>', {class: 'tablet_map'});
    this.domElement = tabletRow[0];
    return tabletRow[0];
}

TabletMap.prototype.getCell = function(type, up) {
    var cell = this.cells[type];
    if (!cell) {
        cell = this.cells[type] = new TabletCell(up);
        $(this.domElement).append(cell.domElement);
    }
    return cell;
}

TabletMap.prototype.updateTabletInfo = function(update) {
    var seenTypes = {};
    for (var idx in update) {
        var up = update[idx];
        var type = up.Type + '-' + up.Leader;
        seenTypes[type] = true;
        var cell = this.getCell(type, up);
        cell.setCount(up.Count);
    }
    for (var t in this.cells) {
        if (!seenTypes[t]) {
            $(this.cells[t].domElement).remove();
            delete this.cells[t];
        }
    }
}

TabletMap.prototype.restoreTooltips = function() {
    for (var t in this.cells) {
        this.cells[t].restoreTooltips();
    }
}
