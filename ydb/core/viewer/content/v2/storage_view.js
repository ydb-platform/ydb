function StorageView(options) {
    Object.assign(this, {
                      nodes: {},
                      groups: {}
                  }, options);
    this.storage = {};
    this.storagePools = {};
    this.observer = new IntersectionObserver(this.onVisibilityChange.bind(this), {rootMargin: '50%'});
    this.getStorageGroupName = function(storage) { return storage.StoragePool.Name; }
    this.getStorageGroupHeader = function(storageGroup) { return bytesToGB0(storageGroup.allocatedSizeBytes) + ' / ' + storageGroup.storageTotal + ' groups'; }
    this.groupOrder = function(prev, next) { return prev < next; }

    DiskCell.prototype.maxWideVDisks = 99;
    DiskCell.prototype.maxNormalVDisks = 99;
    DiskCell.prototype.maxNarrowPDisks = 99;
    DiskCell.prototype.maxNormalPDisks = 99;
    DiskCell.prototype.totalWidth = 200;
    DiskCell.prototype.pDiskClass = 'pdisk-extra-narrow';

    this.stats = new Stats();
    this.statPools = this.stats.addStat('Pools');
    this.statGroups = this.stats.addStat('Groups');
    this.statVDisks = this.stats.addStat('VDisks');
    this.statPDisks = this.stats.addStat('PDisks');
    this.statTotalSize = this.stats.addStat('Total');
    this.statAllocatedSize = this.stats.addStat('Used');
    $('#stats_placement').append(this.stats.domElement);
    //$(this.stats.domElement).insertBefore(this.domElement);
}

StorageView.prototype.getStorageGroup = function(name) {
    var group = this.groups[name];
    if (group === undefined) {
        group = this.groups[name] = new StorageGroup({name: name, view: this});
        var point = this.domElement.lastElementChild;
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

StorageView.prototype.addNode = function(node) {
    this.nodes[node.Id] = node;
}

StorageView.prototype.removeGroup = function(group) {
    this.domElement.removeChild(group.domElement);
    delete this.groups[group.name];
}

StorageView.prototype.rebuild = function() {
    for (var i in this.storage) {
        var storage = this.storage[i];
        var groupName = this.getStorageGroupName(storage);
        var group = this.getStorageGroup(groupName);
        group.addStorage(storage);
    }
    this.rebuildGroupHeaders(true);
    //delete this.lastRefreshOverall;
}

StorageView.prototype.updateGroup = function(storage) {
    var groupName = this.getStorageGroupName(storage);
    if (!storage.group || storage.group.name !== groupName) {
        var newGroup = this.getStorageGroup(groupName);
        var oldGroup = storage.group;
        newGroup.addStorage(storage);
        if (oldGroup) {
            if (oldGroup.isEmpty()) {
                if (oldGroup.pressed) {
                    oldGroup.updateHeader(true);
                } else {
                    this.removeGroup(oldGroup);
                }
            } else {
                //oldGroup.updateHeader(true);
            }
            oldGroup.updateStorageState(storage);
        }
        //newGroup.updateHeader(true);
    } else {
        storage.group.updateStorageState(storage);
    }
}

StorageView.prototype.removeFromGroup = function(storage) {
    var oldGroup = storage.group;
    if (oldGroup) {
        oldGroup.removeStorage(storage);
        if (oldGroup.isEmpty()) {
            if (oldGroup.pressed) {
                oldGroup.updateHeader(true);
            } else {
                this.removeGroup(oldGroup);
            }
        } else {
            //oldGroup.updateHeader(true);
        }
    }
}

StorageView.prototype.onDisconnect = function() {
// TODO
}

StorageView.prototype.rebuildGroupHeaders = function(checkAlive) {
    for (var name in this.groups) {
        var group = this.groups[name];
        if (group.isEmpty()) {
            this.removeGroup(group);
        } else {
            group.updateHeader(checkAlive);
        }
    }
}

var FilterVDisk = null;

StorageView.prototype.onChangeFilter = function() {
    var newFilter = $('#vdisk_filter').val();
    try {
        if (newFilter != '') {
            FilterVDisk = Function('return(function(vdisk){with(vdisk){return (' + newFilter + ');}})');
        } else {
            FilterVDisk = null;
        }
        viewer.manuallyRefreshOverall();
    } catch (error) {
        FilterVDisk = null;
    }
}

StorageView.prototype.onChangeFilterButton = function() {}

StorageView.prototype.onStorage = function(update) {
//    console.log('StorageView.onStorage()');
    var pools = {};
    var groups = {};
    var vDisks = 0;
    var pPiskSeen = {};
    var pDisks = 0;
    var totalSize = 0;
    var allocatedSize = 0;
    var filterVDisk = null;
    if (FilterVDisk) {
        try {
            filterVDisk = FilterVDisk();
        } catch (error) {
            filterVDisk = null;
        }
    }
    for (var numPool in update.StoragePools) {
        var storagePool = update.StoragePools[numPool];
        if (storagePool.Name) {
            storagePool = this.storagePools[storagePool.Name] = storagePool;
            for (var numGroup in storagePool.Groups) {
                var storageGroup = storagePool.Groups[numGroup];
                if (storageGroup.GroupID !== undefined) {
                    var matched = true;
                    try {
                        if (filterVDisk && storageGroup.VDisks.findIndex(filterVDisk) == -1) {
                            matched = false;
                        }
                    } catch (error) {
                        matched = false;
                    }
                    var storage = this.storage[storageGroup.GroupID];
                    if (!matched) {
                        if (storage) {
                            this.removeFromGroup(storage);
                            if (this.observer) {
                                this.observer.unobserve(storage.domElement);
                            }
                            delete this.storage[storageGroup.GroupID];
                        }
                        continue;
                    }
                    pools[storagePool.Name] = true;
                    groups[storageGroup.GroupID] = true;
                    if (!storage) {
                        storageGroup.StoragePool = storagePool;
                        storage = this.storage[storageGroup.GroupID] = new Storage(storageGroup);
                        if (this.observer) {
                            this.observer.observe(storage.domElement);
                        }
                    }
                    storage.updateFromStorage(storageGroup);
                    var allocatedSizeGroup = 0;
                    var totalSize_ = 0;
                    var allocatedSize_ = 0;
                    vDisks += storageGroup.VDisks.length;
                    for (var vDiskNum in storageGroup.VDisks) {
                        var vDisk = storageGroup.VDisks[vDiskNum];
                        if (vDisk && vDisk.AllocatedSize) {
                            allocatedSizeGroup += Number(vDisk.AllocatedSize);
                        }

                        var pDisk = vDisk.PDisk;
                        if (pDisk) {
                            var pDiskId = pDisk.NodeId + '-' + pDisk.PDiskId;
                            if (!pPiskSeen[pDiskId]) {
                                pPiskSeen[pDiskId] = true;
                                pDisks++;
                                if (pDisk.TotalSize && pDisk.AvailableSize) {
                                    var ts = Number(pDisk.TotalSize);
                                    totalSize_ += ts;
                                    allocatedSize_ += ts - Number(pDisk.AvailableSize);
                                }
                            }
                        }
                    }
                    if (storageGroup.AllocatedSize !== undefined && storageGroup.AvailableSize !== undefined) {
                        allocatedSizeGroup = Number(storageGroup.AllocatedSize);
                        allocatedSize_ = allocatedSizeGroup;
                        totalSize_ = allocatedSize_ + Number(storageGroup.AvailableSize);
                    }
                    storage.allocatedSizeBytes = allocatedSizeGroup;
                    totalSize += totalSize_;
                    allocatedSize += allocatedSize_;
                    this.updateGroup(storage);
                }
            }
        }
    }
    this.rebuildGroupHeaders();
    this.statPools.setValue(Object.keys(pools).length);
    this.statGroups.setValue(Object.keys(groups).length);
    this.statVDisks.setValue(vDisks);
    this.statPDisks.setValue(pDisks);
    this.statAllocatedSize.setValue(bytesToGB(allocatedSize));
    this.statTotalSize.setValue(bytesToGB(totalSize));
    if (!this.first) {
        $('#pacman').remove();
        $('#stats_placement').show();
        $('#filter').show();
        $('#vdisk_filter_button').on('click',this.onChangeFilter.bind(this));
        $('#vdisk_filter').on('keyup', function(event) {
            // Number 13 is the "Enter" key on the keyboard
            if (event.keyCode === 13) {
              // Cancel the default action, if needed
              event.preventDefault();
              // Trigger the button element with a click
              this.onChangeFilter();
            }
          }.bind(this)).tooltip({html: true}).attr('data-original-title', 'DiskSpace=="Yellow"<br>!Replicated<br>NodeId==46 && PDisk.PDiskId==1001<br>VDiskId.GroupID==2181038480<br>');
        $('#switches').show();
        this.first = true;
    }
}

StorageView.prototype.onVisibilityChange = function(entries) {
    var now = getTime();
    for (var idx = 0; idx < entries.length; ++idx) {
        var entry = entries[idx];
        var storage = $(entry.target).data('storage');
        if (entry.isIntersecting) {
            if (!storage.visible) {
                storage.appear();
            }
        } else {
            storage.disappear();
        }
    }
}
