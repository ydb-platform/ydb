function CpuView(options) {
    Object.assign(this, {

                  }, options);
    this.nodes = {};
    this.nodesCount = 0;
    this.poolMaps = {};
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

CpuView.prototype.addNode = function(node) {
    node.position = this.nodesCount;
    this.nodes[node.Id] = node;
    this.nodesCount++;
}

CpuView.prototype.rebuild = function() {
    this.nodeMapSettings.nodes = this.nodesCount;
}

CpuView.prototype.runSysUpdate = function(update) {
    var nodeId = update.NodeId;
    if (nodeId) {
        var node = this.nodes[nodeId];
        if (node) {
            node.updateSysInfoStatsOnly(update);
        }
        if (node.sysInfo && node.sysInfo.PoolStats) {
            for (var p = 0; p < node.sysInfo.PoolStats.length; ++p) {
                var poolStat = node.sysInfo.PoolStats[p];
                var poolMap = this.poolMaps[poolStat.Name];
                if (poolMap === undefined) {
                    poolMap = new NodeMap(this.nodeMapSettings);
                    var container = $(this.domElement);
                    container.append($('<div>', {class: 'map_overview_header'}).text(poolStat.Name));
                    container.append(poolMap.domElement);
                    this.poolMaps[poolStat.Name] = poolMap;
                    for (var thisNodeId in this.nodes) {
                        var thisNode = this.nodes[thisNodeId];
                        var title = thisNode.getTooltipText(); 
                        poolMap.setNodeTitle(thisNode.position, title);
                    }
                }
                var poolColor = this.poolColors.getPoolColor(poolStat.Usage);
                poolMap.setNodeMap(node.position, poolColor);
            }
        }
    }
}

CpuView.prototype.onSysInfo = function(data) {
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
                for (var p in this.poolMaps) {
                    this.poolMaps[p].setNodeMap(node.position, 'black');
                }
            }
        }
    }
    if (!this.updateQueue.isEmpty()) {
        this.updateQueue.updateDelay = Math.floor(data.refreshTimeout / this.updateQueue.getLength());
        console.log('CpuView.updateQueue length = ' + this.updateQueue.getLength() + ' updateDelay = ' + this.updateQueue.updateDelay);
    } else {
        this.updateQueue.updateDelay = 50;
    }
}

CpuView.prototype.onDisconnect = function() {

}
