function Overview(options) {
    Object.assign(this, {

                  }, options);
    this.nodes = {};
    this.nodesCount = 0;
    this.statusMap = null;
    this.ageMap = null;
    this.loadMap = null;
    this.poolColors = new PoolColors();
    this.nodeMapSettings = {
        maxWidth: 1400,
        maxPlaceSize: 40,
        height: 85
    };
    this.updateQueue = new UpdateQueue({
        onUpdate: this.runSysUpdate.bind(this)
    });
}

Overview.prototype.addNode = function(node) {
    node.position = this.nodesCount;
    this.nodes[node.Id] = node;
    this.nodesCount++;
}

Overview.prototype.rebuild = function() {
    if (!this.statusMap) {
        this.nodeMapSettings.nodes = this.nodesCount;
        this.statusMap = new NodeMap(this.nodeMapSettings);
        this.ageMap = new NodeMap(this.nodeMapSettings);
        this.loadMap = new NodeMap(this.nodeMapSettings);
        var container = $(this.domElement);
        container.append($('<div>', {class: 'map_overview_header'}).text('Status'));
        container.append(this.statusMap.domElement);
        container.append($('<div>', {class: 'map_overview_header'}).text('Age'));
        container.append(this.ageMap.domElement);
        container.append($('<div>', {class: 'map_overview_header'}).text('Load Average'));
        container.append(this.loadMap.domElement);
        for (var nodeId in this.nodes) {
            var node = this.nodes[nodeId];
            var title = node.getTooltipText(); 
            this.statusMap.setNodeTitle(node.position, title);
            this.ageMap.setNodeTitle(node.position, title);
            this.loadMap.setNodeTitle(node.position, title);
        }
    }
}

Overview.prototype.runSysUpdate = function(update) {
    var nodeId = update.NodeId;
    if (nodeId) {
        var node = this.nodes[nodeId];
        if (node) {
            node.updateSysInfoStatsOnly(update);
        }
        var statusColor = node.getStatusColor();
        this.statusMap.setNodeMap(node.position, statusColor);
        var ageColor = node.getAgeColor();
        this.ageMap.setNodeMap(node.position, ageColor);
        var loadAverage = node.getLoadAverage();
        if (loadAverage) {
            var loadColor = this.poolColors.getPoolColor(loadAverage);
            this.loadMap.setNodeMap(node.position, loadColor);
        }
    }
}

Overview.prototype.onSysInfo = function(data) {
    if (data.SystemStateInfo !== undefined) {
        var deadNodes = {};
        if (!data.since) {
            for (var i in this.nodes) {
                deadNodes[i] = true;
            }
            this.updateQueue.flushUpdateQueue();
        } else {
            data.SystemStateInfo.sort(function(a, b) {
                return Number(b.ChangeTime) - Number(a.ChangeTime);
            });
        }
        for (var idx = 0; idx < data.SystemStateInfo.length; ++idx) {
            var update = data.SystemStateInfo[idx];
            if (!data.since) {
                delete deadNodes[update.NodeId];
            }
            this.updateQueue.enqueue(update);
        }
        for (var i in deadNodes) {
            var node = this.nodes[i];
            if (node) {
                this.statusMap.setNodeMap(node.position, 'black');
                this.ageMap.setNodeMap(node.position, 'black');
                this.loadMap.setNodeMap(node.position, 'black');
            }
        }
    }
    if (!this.updateQueue.isEmpty()) {
        this.updateQueue.updateDelay = Math.floor(data.refreshTimeout / this.updateQueue.getLength());
        console.log('Overview.updateQueue length = ' + this.updateQueue.getLength() + ' updateDelay = ' + this.updateQueue.updateDelay);
    } else {
        this.updateQueue.updateDelay = 50;
    }
}

Overview.prototype.onDisconnect = function() {

}
 
