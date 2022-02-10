function NetView(options) {
    Object.assign(this, {}, options);
    this.nodes = {};
    this.nodesCount = 0;
    this.sourceMap = null;
    this.targetMap = null;
    this.poolColors = new PoolColors([
        {usage: 0.00, R: 144, G: 144, B: 144},
        {usage: 1.00, R: 144, G: 238, B: 144},
        {usage: 2.00, R: 255, G: 255, B: 0},
        {usage: 4.00, R: 255, G: 0, B: 0}
    ]);
    this.nodeMapSettings = {
        maxWidth: 1400,
        maxPlaceSize: 40,
        height: 85
    };
    this.updateQueue = new UpdateQueue({
        onUpdate: this.runNetUpdate.bind(this)
    });
}

NetView.prototype.addNode = function(node) {
    node.position = this.nodesCount;
    this.nodes[node.Id] = node;
    this.nodesCount++;
}

NetView.prototype.rebuild = function() {
    if (!this.sourceMap) {
        this.nodeMapSettings.nodes = this.nodesCount;
        this.sourceMap = new NodeMap(this.nodeMapSettings);
        this.targetMap = new NodeMap(this.nodeMapSettings);
        var container = $(this.domElement);
        container.append($('<div>', {class: 'map_overview_header'}).text('Source'));
        container.append(this.sourceMap.domElement);
        container.append($('<div>', {class: 'map_overview_header'}).text('Target'));
        container.append(this.targetMap.domElement);
        for (var nodeId in this.nodes) {
            var node = this.nodes[nodeId];
            var title = node.getTooltipText(); 
            this.sourceMap.setNodeTitle(node.position, title);
            this.targetMap.setNodeTitle(node.position, title);
        }
    }
}

NetView.prototype.runNetUpdate = function(update) {
    var nodeId = update.nodeId;
    if (nodeId) {
        var node = this.nodes[nodeId];
        if (update.sourceStatus && this.sourceMap) {
            this.sourceMap.setNodeMap(node.position, this.poolColors.getPoolColor(update.sourceStatus));
        }
        if (update.targetStatus && this.targetMap) {
            this.targetMap.setNodeMap(node.position, this.poolColors.getPoolColor(update.targetStatus));    
        }
    }
}

NetView.prototype.onSysInfo = function(data) {
}

NetView.prototype.onNodeInfo = function(data) {
    if (data !== undefined) {
        var deadNodes = {};
        var nodes = {};
        if (!data.since) {
            for (var i in this.nodes) {
                deadNodes[i] = true;
            }
            this.updateQueue.flushUpdateQueue();
        }
        for (var sourceNodeId in data) {
            var node = this.nodes[sourceNodeId];
            if (node === undefined) {
                continue;
            }
            var src = nodes[Number(sourceNodeId)] = {
                srcConnectStatus: 0,
                srcConnectCount: 0,
                trgConnectStatus: 0,
                trgConnectCount: 0
            };
            var nodeInfo = data[sourceNodeId];
            if (nodeInfo === null) {
                continue;
            }
            if (nodeInfo.NodeStateInfo) {
                var nodeStateInfo = nodeInfo.NodeStateInfo;
                for (var idx = 0; idx < nodeStateInfo.length; ++idx) {
                    var update = nodeStateInfo[idx];
                    var targetNodeId = getNodeIdFromPeerName(update.PeerName);
                    var node = this.nodes[targetNodeId];
                    if (node !== undefined/* && update.Connected*/) {
                        var trg = nodes[targetNodeId];
                        if (trg === undefined) {
                            trg = nodes[targetNodeId] = {
                                srcConnectStatus: 0,
                                srcConnectCount: 0,
                                trgConnectStatus: 0,
                                trgConnectCount: 0
                            };
                        }
                        src.srcConnectStatus += update.ConnectStatus;
                        src.srcConnectCount++;
                        trg.trgConnectStatus += update.ConnectStatus;
                        trg.trgConnectCount++;
                    }
                }
            }
            if (!data.since) {
                delete deadNodes[sourceNodeId];
            }
        }
        for (var id in nodes) {
            var n = nodes[id];
            var o = {
                nodeId: id
            };
            if (n.srcConnectCount > 0) {
                o.sourceStatus = n.srcConnectStatus / n.srcConnectCount;
            }
            if (n.trgConnectCount > 0) {
                o.targetStatus = n.trgConnectStatus / n.trgConnectCount;
            }
            this.updateQueue.enqueue(o);
        }
        for (var i in deadNodes) {
            var node = this.nodes[i];
            if (node && this.sourceMap) {
                this.sourceMap.setNodeMap(node.position, 'black');
                //this.targetMap.setNodeMap(node.position, 'black');
            }
        }
    }
    if (this.sourceMap) {
        if (!this.updateQueue.isEmpty()) {
            this.updateQueue.updateDelay = Math.floor(data.refreshTimeout / this.updateQueue.getLength());
            console.log('NetView.updateQueue length = ' + this.updateQueue.getLength() + ' updateDelay = ' + this.updateQueue.updateDelay);
        } else {
            this.updateQueue.updateDelay = 50;
        }
    }
}

NetView.prototype.onDisconnect = function() {

}
