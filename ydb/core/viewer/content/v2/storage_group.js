function StorageGroup(options) {
    Object.assign(this, options);
    this.storage = {};
    this.storageState = {};
    this.storageTotal = 0;
    this.storageGood = 0;
    this.storageAllocatedBytes = 0;
    this.buildDomElement();
    this.nodeMap = null;
}

StorageGroup.prototype.disableDoubleClick = function(e) {
    if (e.detail > 1) {
        e.preventDefault();
    }
}

StorageGroup.prototype.buildDomElement = function() {
    var storageBlock = $('<div>', {class: 'storage_group_block'});
    var StorageGroupHeader = $('<div>', {class: 'storage_group_header'});
    var StorageGroupName = $('<div>', {class: 'storage_group_header_name'});
    var StorageGroupInfo = $('<div>', {class: 'storage_group_header_info'});
    var StorageGroupNodes = $('<div>', {class: 'storage_group_header_nodes'});
    StorageGroupName.html(this.name);
    StorageGroupHeader.mousedown(this.disableDoubleClick.bind(this));
    StorageGroupHeader.click(this.toggleContainer.bind(this));
    StorageGroupInfo.html("<img src='throbber.gif'></img>");
    StorageGroupHeader.append(StorageGroupName);
    StorageGroupHeader.append(StorageGroupNodes);
    StorageGroupHeader.append(StorageGroupInfo);
    storageBlock.append(StorageGroupHeader);
    var StorageGroupContainer = $('<table>', {class: 'storagelist storage_group_container'});
    var StorageGroupHead = $('<thead>');
    var StorageGroupRow = $('<tr>');
    StorageGroupRow.append('<th>Group Id</th>');
    StorageGroupRow.append('<th>Erasure</th>');
    StorageGroupRow.append('<th>Units</th>');
    StorageGroupRow.append('<th>Allocated</th>');
    StorageGroupRow.append('<th>Available</th>');
    StorageGroupRow.append('<th>Read</th>');
    StorageGroupRow.append('<th>Write</th>');
    StorageGroupRow.append('<th>Latency</th>');
    StorageGroupRow.append('<th>VDisks</th>');
    StorageGroupRow.append('<th>PDisks</th>');
    StorageGroupHead.append(StorageGroupRow);
    var StorageGroupBody = $('<tbody>');
    StorageGroupContainer.append(StorageGroupHead);
    StorageGroupContainer.append(StorageGroupBody);
    storageBlock.append(StorageGroupContainer);
    this.domElement = storageBlock[0];
    this.domElementGroupHeader = StorageGroupHeader[0];
    this.domElementGroupHeaderNodes = StorageGroupNodes[0];
    this.domElementGroupInfo = StorageGroupInfo[0];
    this.domElementGroupContainer = StorageGroupContainer[0];
    this.domElementGroupContainerBody = StorageGroupBody[0];
    this.domElementGroupContainer.style.display = 'none';
    storageBlock.data('group', this);
}

StorageGroup.prototype.toggleContainer = function() {
    var domElement = this.domElementGroupContainer;
    if (domElement.style.display === 'none') {
        domElement.style.display = 'table';
        $(this.domElementGroupHeader).addClass('storage_group_header_pressed');
        this.pressed = true;
    } else {
        $(this.domElementGroupHeader).removeClass('storage_group_header_pressed');
        domElement.style.display = 'none';
        this.pressed = false;
        if (this.isEmpty()) {
            this.view.removeGroup(this);
        }
    }
}

StorageGroup.prototype.isEmpty = function() {
    return this.storageTotal === 0;
}

StorageGroup.prototype.addStorage = function(storage) {
    if (storage.group !== undefined) {
        storage.group.removeStorage(storage);
    }

    var point = this.domElementGroupContainerBody.lastChild;
    if (point === null || $(point).data('storage').Id < storage.Id) {
        this.domElementGroupContainerBody.appendChild(storage.domElement);
    } else {
        for(;;) {
            var prev = point.previousSibling;
            if (prev === null) {
                break;
            }
            if ($(prev).data('storage').Id < storage.Id) {
                break;
            }
            point = prev;
        }
        this.domElementGroupContainerBody.insertBefore(storage.domElement, point);
    }

    storage.group = this;
    this.storage[storage.Id] = storage;
    ++this.storageTotal;
}

StorageGroup.prototype.removeStorage = function(storage) {
    this.domElementGroupContainerBody.removeChild(storage.domElement);
    var color = storage.color;
    if (color === green) {
        --this.storageGood;
    }
    delete storage.group;
    delete this.storageState[storage.groupPosition];
    delete storage.groupPosition;
    delete this.storage[storage.Id];
    --this.storageTotal;
}

StorageGroup.prototype.updateStorageStateMap = function(storage) {
    var color = storage.color;
    var position = storage.groupPosition;
    if (position !== undefined) {
        var prevColor = this.storageState[position];
        if (prevColor !== color) {
            this.storageState[position] = color;
            this.nodeMap.setNodeMap(position, color, storage.getTooltipText());
            var s = storage.domElement;
            switch (color) {
            case red:
                s.style.backgroundColor = '#ffe0e0';//red;
                break;
            case orange:
                s.style.backgroundColor = '#ffe0d0';//orange;
                break;
            case yellow:
                s.style.backgroundColor = '#ffffe0';//yellow;
                break;
            case blue:
                s.style.backgroundColor = '#e0f0ff';//blue;
                break;
            case green:
            case grey:
                s.style.backgroundColor = null;
                break;
            }
            var updateInfo = false;
            if (prevColor === undefined || prevColor === grey) {
                if (color === green) {
                    this.storageGood++;
                }
                updateInfo = true;
            } else {
                if (color !== prevColor) {
                    if (color === green) {
                        this.storageGood++;
                    } else if (prevColor === green) {
                        this.storageGood--;
                    }
                    updateInfo = true;
                }
            }
            return true;
        }
    }
    return false;
}

StorageGroup.prototype.updateStorageStateInfo = function() {
    this.domElementGroupInfo.innerHTML = this.view.getStorageGroupHeader(this);
}

StorageGroup.prototype.updateStorageState = function(storage) {
    var updateInfo = this.updateStorageStateMap(storage);
    if (updateInfo) {
        this.updateStorageStateInfo();
    }
}

StorageGroup.prototype.updateHeader = function(checkAlive) {
    var StorageGroupBlock = $(this.domElement);
    var StorageGroupHeaderNodes = $(this.domElementGroupHeaderNodes);
    var items = 0;
    var StorageGroupContainer = $(this.domElementGroupContainerBody).children().each(function () {
        var storage = $(this).data('storage');
        if (storage !== undefined) {
            storage.groupPosition = items;
            ++items;
        }
    });
    if (this.nodeMap === null || this.nodeMap.nodes !== items) {
        var size = 5;
        if (items <= 16) {
            size = 20;
        } else if (items <= 72) {
            size = 10;
        }
        var height = 22;
        if (items > 512) {
            if (items > 1000) {
                size = 3;
            }
            height += Math.floor((items - 512) / 512) * size;
        }
        this.storageState = {};
        StorageGroupHeaderNodes.empty();
        if (items > 0) {
            this.nodeMap = new NodeMap({nodes: items, height: height, placeSize: size, class: 'node_map'});
            StorageGroupHeaderNodes.append(this.nodeMap.domElement);
        } else {
            this.nodeMap = null;
        }
    }

    //this.storageGood = 0;

    this.allocatedSizeBytes = 0;

    for (var storageId in this.storage) {
        var storage = this.storage[storageId];
        if (storage) {
            this.updateStorageStateMap(storage);
            this.allocatedSizeBytes += storage.allocatedSizeBytes;
        }
    }

    this.updateStorageStateInfo();


    /*if (checkAlive) {
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
    }*/
}

