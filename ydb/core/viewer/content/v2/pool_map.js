function PoolMap() {
    this.buildDomElement();
}

PoolMap.prototype.poolColors = new PoolColors();

PoolMap.prototype.buildDomElement = function() {
    this.domElement = $('<div>', {class: 'pool_row'})[0];
}

PoolMap.prototype.setPoolMap = function(pools) {
    var poolsrow = $(this.domElement);
    var children = poolsrow.find('.pool_cell');
    for (var idx = 0; idx < pools.length; idx++) {
        var cell;
        var pool;
        var inner;
        if (idx >= children.length) {
            cell = $('<div>', {class: 'pool_cell'});
            var pooldiv = $('<div>', {class: 'pool_block'});
            var innerdiv = $('<div>', {style: 'width:100%;height:50%;background-color:' + lightgrey});
            pooldiv.append(innerdiv);
            cell.append(pooldiv);
            poolsrow.append(cell);
            pool = pooldiv[0];
            inner = innerdiv[0];
            cell.tooltip();
            cell = cell[0];
        } else {
            cell = children[idx];
            pool = cell.firstChild;
            inner = pool.firstChild;
        }
        var usage = (pools[idx].Usage * 100).toFixed();
        if (pools[idx].Name !== '') {
            cell.setAttribute('data-original-title', pools[idx].Name + ' ' + usage + '%');
        } else {
            cell.setAttribute('data-original-title', usage + '%');
        }
        if (usage < 0) {
            usage = 0;
        }
        if (usage > 100) {
            usage = 100;
        }
        var height = 100 - usage;
        pool.style.backgroundColor = this.poolColors.getPoolColor(pools[idx].Usage);
        inner.style.height = height + '%';
    }
}

PoolMap.prototype.restoreTooltips = function() {
    $(this.domElement).find('.pool_cell').tooltip('enable');
}
