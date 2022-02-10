function NodeMap(options) {
    Object.assign(this, {height: 40, minPlaceSize: 3, maxPlaceSize: 10, maxWidth: 700, class: 'node_map node_map_shadow'}, options);
    if (this.placeSize === undefined) {
        this.placeSize = Math.max(this.minPlaceSize, Math.min(this.maxPlaceSize, this.getMaxPlaceSize()));
    }
    if (this.boxSize === undefined) {
        if (this.boxSpace === undefined) {
            if (this.placeSize > 2) {
                this.boxSpace = 1;
            } else {
                this.boxSpace = 0;
            }
        }
        this.boxSize = this.placeSize - this.boxSpace;
    } else {
        this.boxSpace = this.placeSize - this.boxSize;
    }
    this.map = {};
    this.buildDomElement();
}

NodeMap.prototype.getMaxPlaceSize = function() {
    return Math.floor(Math.sqrt(this.height * this.maxWidth / this.nodes));
}

NodeMap.prototype.getWidth = function() {
    var countPerColumn = Math.floor(this.height / this.placeSize);
    return Math.ceil(this.nodes / countPerColumn) * placeSize - this.boxSpace;
}

NodeMap.prototype.buildDomElement = function() {
    var nodeRow = $('<span>', {class: this.class});
    nodeRow.tooltip({
        html: true,
        placement: 'top',
        offset: '40px'
    });
    var canvas = document.createElement('canvas');
    this.countPerColumn = Math.max(Math.floor(this.height / this.placeSize), 1);
    this.height = canvas.height = this.countPerColumn * this.placeSize - this.boxSpace;
    this.width = canvas.width = Math.ceil(this.nodes / this.countPerColumn) * this.placeSize - this.boxSpace;
    nodeRow.append(canvas);
    var realNodes = Math.ceil(canvas.height / this.placeSize) * Math.ceil(canvas.width / this.placeSize);
    this.domElement = nodeRow[0];
    this.canvas = canvas;
    this.context = canvas.getContext('2d');
    canvas.addEventListener('click', this.onClick.bind(this));
    canvas.addEventListener('mousemove', this.onMouseMove.bind(this));
    var i;
    /*for (i = 0; i < this.nodes; ++i) {
        this.setNodeMap(i, '#e0e0e0');
    }*/
    for (i = this.nodes; i < realNodes; ++i) {
        this.setNodeMap(i, '#c0c0c0');
    }
}

NodeMap.prototype.setNodeTitle = function(position, title) {
    var p = this.map[position];
    if (p === undefined) {
        p = this.map[position] = {};
    }
    p.title = title;
}

NodeMap.prototype.setNodeMap = function(position, color, title) {
    var p = this.map[position];
    if (p === undefined) {
        p = this.map[position] = {};
    }
    p.color = color;
    if (title) {
        p.title = title;
    }
    var x = Math.floor(position / this.countPerColumn) * this.placeSize;
    var y = (position % this.countPerColumn) * this.placeSize;
    this.context.fillStyle = color;
    this.context.fillRect(x, y, this.boxSize, this.boxSize);
}

NodeMap.prototype.getNodeMap = function(position) {
    return this.map[position];
}

NodeMap.prototype.onClick = function(event) {
    var x = Math.floor((event.offsetX - 1) / this.placeSize);
    var y = Math.floor((event.offsetY - 1) / this.placeSize);
    var position = x * this.countPerColumn + y;
    console.log('Click on node_map position ' + position);
    if (this.onNodeClick) {
        this.onNodeClick({position: position});
    }
}

NodeMap.prototype.onMouseMove = function(event) {
    var x = Math.floor((event.offsetX - 1) / this.placeSize);
    var y = Math.floor((event.offsetY - 1) / this.placeSize);
    var position = x * this.countPerColumn + y;
    var n = this.map[position];
    if (n !== undefined && n.title) {
        this.domElement.setAttribute('data-original-title', n.title);
        $(this.domElement).tooltip('show');
        var tooltip = this.tooltip;
        if (tooltip === undefined) {
            tooltip = this.tooltip = $('.tooltip');
        }
        var ofs_x = event.offsetX - this.width / 2;
        var ofs_y = event.offsetY;
        tooltip.css('top', parseInt(tooltip.css('top')) + ofs_y + 'px');
        tooltip.css('left', parseInt(tooltip.css('left')) + ofs_x + 'px');
        //console.log('MouseMove on node_map position ' + position);
    }
}
