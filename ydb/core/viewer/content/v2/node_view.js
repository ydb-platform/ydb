function NodeView(options) {
    Object.assign(this, {
                      nodes: {},
                      groups: {},
                      visibleNodes: {},
                      visibleRefreshTimeout: 1000
                  }, options);
    this.nodesCount = Object.keys(this.nodes).length;
    this.refreshViewTimer = null;
    this.isNodeGood = function(node) { return node.sysInfo.SystemState === 'Green'; }
    this.getNodeGroupName = function(node) { return node.getIdGroup(); }
    this.groupOrder = function(prev, next) { return Number(prev.split('..')[0]) < Number(next.split('..')[0]); }
    this.observer = new IntersectionObserver(this.onVisibilityChange.bind(this));
    this.refreshViewInterval = setInterval(this.refreshView.bind(this), this.visibleRefreshTimeout);
}

NodeView.prototype.groupOrder = function(prev, next) { return prev < next; }

NodeView.prototype.getNodeGroup = function(name) {
    var group = this.groups[name];
    if (group === undefined) {
        group = this.groups[name] = new NodeGroup({name: name, view: this});
        var point = this.domElement.lastChild;
        if (point === null || this.groupOrder($(point).data('group').name, name)) {
            this.domElement.appendChild(group.domElement);
        } else {
            for(;;) {
                var prev = point.previousSibling;
                if (prev === null) {
                    break;
                }
                if (this.groupOrder($(prev).data('group').name, name)) {
                    break;
                }
                point = prev;
            }
            this.domElement.insertBefore(group.domElement, point);
        }
    }
    return group;
}

NodeView.prototype.addNode = function(node) {
    node.nodeView = this;
    this.nodes[node.Id] = node;
    this.nodesCount++;
    if (this.observer) {
        this.observer.observe(node.domElement);
    }
}

NodeView.prototype.removeGroup = function(group) {
    this.domElement.removeChild(group.domElement);
    delete this.groups[group.name];
}

NodeView.prototype.rebuild = function() {
    for (var i in this.nodes) {
        var node = this.nodes[i];
        var groupName = this.getNodeGroupName(node);
        var group = this.getNodeGroup(groupName);
        group.addNode(node);
    }
    this.rebuildGroupHeaders(true);
    //delete this.lastRefreshOverall;
}

NodeView.prototype.updateGroup = function(node) {
    var groupName = this.getNodeGroupName(node);
    if (!node.group || node.group.name !== groupName) {
        var oldGroup = node.group;
        var newGroup = this.getNodeGroup(groupName);
        newGroup.addNode(node);
        if (oldGroup.isEmpty()) {
            this.removeGroup(oldGroup);
        } else {
            oldGroup.updateHeader(true);
        }
        newGroup.updateHeader(true);
    }
}

NodeView.prototype.onVisibilityChange = function(entries) {
    var now = getTime();
    for (var idx = 0; idx < entries.length; ++idx) {
        var entry = entries[idx];
        var node = $(entry.target).data('node');
        if (entry.isIntersecting) {
            if (!node.visible) {
                node.appear();
                this.visibleNodes[node.Id] = true;
            }
        } else {
            node.disappear();
            delete this.visibleNodes[node.Id];
        }
    }
    if (this.refreshViewTimer === null) {
        //this.refreshViewTimer = setTimeout(this.refreshView.bind(this), 10);
        this.refreshView();
    }
}

NodeView.prototype.onNodeInfo = function(data) {
    if (data.NodeStateInfo !== undefined) {
        for (var i in data.NodeStateInfo) {
            var update = data.NodeStateInfo[i];
            var nodeId = getNodeIdFromPeerName(update.PeerName);
            var node = this.nodes[nodeId];
            if (node && node.sysInfo && !node.sysInfo.Roles) {
                var port = getPortFromPeerName(update.PeerName);
                var role = this.PortToRoles[port];
                if (role) {
                    node.sysInfo.Roles = [role];
                }
                if (!node.Port) {
                    node.Port = port;
                }
            }
        }
    }
}

NodeView.prototype.onSysInfo = function(data) {
    if (data.SystemStateInfo !== undefined) {
        var deadNodes = {};
        for (var i in this.nodes) {
            deadNodes[i] = true;
        }
        for (var i in data.SystemStateInfo) {
            var update = data.SystemStateInfo[i];
            if (!this.sysInfoChangeTime || update.ChangeTime < this.sysInfoChangeTime) {
                this.sysInfoChangeTime = update.ChangeTime;
            }
            var nodeId = update.NodeId;
            if (nodeId) {
                var node = this.nodes[nodeId];
                if (node) {
                    node.updateSysInfoStatsOnly(update);
                }
                delete deadNodes[nodeId];
            }
        }
        for(var i in deadNodes) {
            var node = this.nodes[i];
            if (node) {
                node.onDisconnect();
            }
        }

        if (data.ResponseTime != 0) {
            this.sysInfoChangeTime = data.ResponseTime;
        }
    }
}

NodeView.prototype.onPDiskInfo = function(data) {
    for (var nodeId in data) {
        var update = data[nodeId];
        if (update.PDiskStateInfo) {
            var node = this.nodes[nodeId];
            if (node) {
                node.onPDiskInfo(update);
            }
        }
    }
}

NodeView.prototype.onVDiskInfo = function(data) {
    for (var nodeId in data) {
        var update = data[nodeId];
        if (update.VDiskStateInfo) {
            var node = this.nodes[nodeId];
            if (node) {
                node.onVDiskInfo(update);
            }
        }
    }
}

NodeView.prototype.onDisconnect = function() {
    for (var nodeId in this.nodes) {
        this.nodes[nodeId].onDisconnect();
    }
}

NodeView.prototype.refreshView = function() {
    this.refreshViewTimer = null;
    var now = getTime();
    var keys = Object.keys(this.visibleNodes);
    if (keys.length > 0) {
        console.log('refreshView: ' + keys);
        var nodeView = this;
        keys.forEach(function(nodeId) {
            var node = nodeView.nodes[nodeId];
            if (!node.visible) {
                delete nodeView.visibleNodes[nodeId];
            } else {
                nodeView.refreshNode(node, now);
            }
        });
    }
}

NodeView.prototype.isTimeToUpdateNode = function(node, now) {
    return node.visible && (!node.lastUpdate || (node.lastUpdate <= (now - this.visibleRefreshTimeout)));
    //return node.visible;
}

NodeView.prototype.refreshNode = function(node, now) {
    if (node.visible) {
        if (this.isTimeToUpdateNode(node, now)) {
            node.refresh();
            node.lastUpdate = now;
        }
    }
}

NodeView.prototype.rebuildGroupHeaders = function(checkAlive) {
    for (var name in this.groups) {
        var group = this.groups[name];
        if (group.isEmpty()) {
            this.removeGroup(group);
        } else {
            group.updateHeader(checkAlive);
        }
    }
}

NodeView.prototype.getReferenceNode = function() {
    for (var i in this.nodes) {
        return this.nodes[i];
    }
    return null;
}

NodeView.prototype.getConfig = function() {
    return this.config;
}

NodeView.prototype.getRootPath = function() {
    var config = this.getConfig();
    if (config) {
        return '/' + config.DomainsConfig.Domain[0].Name;
    }
}

NodeView.prototype.getTenantName = function(sysInfo) {
    if (sysInfo.Tenants !== undefined && sysInfo.Tenants.length > 0) {
        return sysInfo.Tenants.join(',');
    }
    return this.getRootPath();
}

NodeView.prototype.scrollToNodePosition = function(position) {
    for (var nodeId in this.nodes) {
        var node = this.nodes[nodeId];
        if (node.position === position) {
            console.log('scroll to node ' + node.Id);
            node.domElement.scrollIntoView();
        }
    }
}
