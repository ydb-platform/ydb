function NodeGroup(options) {
    Object.assign(this, options);
    this.nodes = {};
    this.nodeState = {};
    this.nodesTotal = 0;
    this.nodesGood = 0;
    this.buildDomElement();
    this.nodeMap = null;
}

NodeGroup.prototype.disableDoubleClick = function(e) {
    if (e.detail > 1) {
        e.preventDefault();
    }
}

NodeGroup.prototype.buildDomElement = function() {
    var nodeBlock = $('<div>', {class: 'node_group_block'});
    var nodeGroupHeader = $('<div>', {class: 'node_group_header'});
    var nodeGroupName = $('<div>', {class: 'node_group_header_name'});
    var nodeGroupInfo = $('<div>', {class: 'node_group_header_info'});
    var nodeGroupNodes = $('<div>', {class: 'node_group_header_nodes'});
    nodeGroupName.html(this.name);
    nodeGroupHeader.mousedown(this.disableDoubleClick.bind(this));
    nodeGroupHeader.click(this.toggleContainer.bind(this));
    nodeGroupInfo.html("<img src='throbber.gif'></img>");
    nodeGroupHeader.append(nodeGroupName);
    nodeGroupHeader.append(nodeGroupNodes);
    nodeGroupHeader.append(nodeGroupInfo);
    nodeBlock.append(nodeGroupHeader);
    var nodeGroupContainer = $('<table>', {class: 'nodelist node_group_container'});
    var nodeGroupHead = $('<thead>');
    var nodeGroupRow = $('<tr>');
    nodeGroupRow.append('<th>#</th>');
    nodeGroupRow.append('<th>Node</th>');
    nodeGroupRow.append('<th>Uptime</th>');
    nodeGroupRow.append('<th>CPU</th>');
    nodeGroupRow.append('<th>RAM</th>');
    /*if (nodesCount <= 32) {
        nodeGroupRow.append('<th>Net</th>');
    } else {
        nodeGroupRow.append('<th>Network</th>');
    }*/
    /*nodeGroupRow.append('<th>Net</th>');*/
    nodeGroupRow.append('<th>Disks & Tablets</th>');
    nodeGroupHead.append(nodeGroupRow);
    var nodeGroupBody = $('<tbody>');
    nodeGroupContainer.append(nodeGroupHead);
    nodeGroupContainer.append(nodeGroupBody);
    nodeBlock.append(nodeGroupContainer);
    this.domElement = nodeBlock[0];
    this.domElementGroupHeader = nodeGroupHeader[0];
    this.domElementGroupHeaderNodes = nodeGroupNodes[0];
    this.domElementGroupInfo = nodeGroupInfo[0];
    this.domElementGroupContainer = nodeGroupContainer[0];
    this.domElementGroupContainerBody = nodeGroupBody[0];
    this.domElementGroupContainer.style.display = 'none';
    nodeBlock.data('group', this);
}

NodeGroup.prototype.toggleContainer = function() {
    var domElement = this.domElementGroupContainer;
    if (domElement.style.display === 'none') {
        domElement.style.display = 'table';

        $(this.domElementGroupHeader).addClass('node_group_header_pressed');
    } else {
        $(this.domElementGroupHeader).removeClass('node_group_header_pressed');
        domElement.style.display = 'none';
    }
}

NodeGroup.prototype.isEmpty = function() {
    return this.nodesTotal === 0;
}

NodeGroup.prototype.addNode = function(node) {
    if (node.group !== undefined) {
        node.group.removeNode(node);
    }

    var point = this.domElementGroupContainerBody.lastChild;
    if (point === null || $(point).data('node').Id < node.Id) {
        this.domElementGroupContainerBody.appendChild(node.domElement);
    } else {
        for(;;) {
            var prev = point.previousSibling;
            if (prev === null) {
                break;
            }
            if ($(prev).data('node').Id < node.Id) {
                break;
            }
            point = prev;
        }
        this.domElementGroupContainerBody.insertBefore(node.domElement, point);
    }

    node.group = this;
    this.nodes[node.Id] = node;
    ++this.nodesTotal;
}

NodeGroup.prototype.removeNode = function(node) {
    this.domElementGroupContainerBody.removeChild(node.domElement);
    var color = node.color;
    if (color === green) {
        --this.nodesGood;
    }
    delete node.group;
    delete this.nodeState[node.groupPosition];
    delete node.groupPosition;
    delete this.nodes[node.Id];
    --this.nodesTotal;
}

NodeGroup.prototype.updateNodeStateMap = function(node) {
    var color = flagToColor(node.sysInfo.SystemState);
    var position = node.groupPosition;
    if (position !== undefined) {
        var prevColor = this.nodeState[position];
        if (prevColor !== color) {
            this.nodeState[position] = color;
            this.nodeMap.setNodeMap(position, color, node.getTooltipText());
            var n = node.domElement;
            switch (color) {
            case red:
                n.style.backgroundColor = '#ffe0e0';//red;
                break;
            case orange:
                n.style.backgroundColor = '#ffe0d0';//orange;
                break;
            case yellow:
                n.style.backgroundColor = '#ffffe0';//yellow;
                break;
            case green:
            case grey:
                n.style.backgroundColor = null;
                break;
            }
            var updateInfo = false;
            if (prevColor === undefined || prevColor === grey) {
                if (color === green) {
                    this.nodesGood++;
                }
                updateInfo = true;
            } else {
                if (color !== prevColor) {
                    if (color === green) {
                        this.nodesGood++;
                    } else if (prevColor === green) {
                        this.nodesGood--;
                    }
                    updateInfo = true;
                }
            }
            return true;
        }
    }
    return false;
}

NodeGroup.prototype.updateNodeStateInfo = function() {
    this.domElementGroupInfo.innerHTML = this.nodesTotal + ' nodes';
}

NodeGroup.prototype.updateNodeState = function(node) {
    var updateInfo = this.updateNodeStateMap(node);
    if (updateInfo) {
        this.updateNodeStateInfo();
    }
}

NodeGroup.prototype.updateHeader = function(checkAlive) {
    var nodeGroupBlock = $(this.domElement);
    var nodeGroupHeaderNodes = $(this.domElementGroupHeaderNodes);
    var nodes = 0;
    var nodeGroupContainer = $(this.domElementGroupContainerBody).children().each(function () {
        var node = $(this).data('node');
        if (node !== undefined) {
            node.groupPosition = nodes;
            ++nodes;
        }
    });
    if (this.nodeMap === null || this.nodeMap.nodes !== nodes) {
        var size = 5;
        if (nodes <= 16) {
            size = 20;
        } else if (nodes <= 72) {
            size = 10;
        }
        var height = 22;
        if (nodes > 512) {
            if (nodes > 1000) {
                size = 3;
            }
            height += Math.floor((nodes - 512) / 512) * size;
        }
        this.nodeMap = new NodeMap({nodes: nodes, height: height, placeSize: size, class: 'node_map'});
        nodeGroupHeaderNodes.empty();
        nodeGroupHeaderNodes.append(this.nodeMap.domElement);
    }
    if (checkAlive) {
        this.nodesGood = 0;
        this.nodeState = {};
        for (var nodeId in this.nodes) {
            var node = this.nodes[nodeId];
            if (node !== undefined) {
                if (node.sysInfo && node.sysInfo.SystemState) {
                    this.updateNodeStateMap(node);
                }
            }
        }
        this.updateNodeStateInfo();
    }
}

