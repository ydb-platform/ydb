function Tenant(options) {
    Object.assign(this, {
                      nodes: {},
                      nodeCount: 0
                  },
                  options);
    this.buildDomElement();
}

Tenant.prototype.buildDomElement = function() {
    var row = $('<tr>');
    row.data('tenant', this);

    var cellName = $('<td>', {class: 'tenant_name_place'});
    this.domElementName = cellName[0];
    row.append(this.domElementName);

    var cellCompute = $('<td>', {class: 'tenant_nodes_compute'});
    this.domElementCompute = cellCompute[0];
    row.append(this.domElementCompute);

    var cellStorage = $('<td>', {class: 'tenant_nodes_storage'});
    this.domElementStorage = cellStorage[0];
    row.append(this.domElementStorage);

    var cellNodes = $('<td>', {class: 'tenant_nodes_place'});
    this.domElementNodes = cellNodes[0];
    row.append(this.domElementNodes);

    var cellCpu = $('<td>', {class: 'tenant_cpu_place'});
    this.poolMap = new PoolMap();
    this.domElementCpu = cellCpu[0];
    this.domElementCpu.appendChild(this.poolMap.domElement);
    row.append(this.domElementCpu);

    var cellTablets = $('<td>', {class: 'tenant_tablets_place'});
    this.domElementTablets = cellTablets[0];
    row.append(this.domElementTablets);

    var cellMetricsCpu = $('<td>', {class: 'tenant_metrics_cpu_place'});
    this.domElementMetricsCpu = cellMetricsCpu[0];
    row.append(this.domElementMetricsCpu);

    var cellMetricsMemory = $('<td>', {class: 'tenant_metrics_memory_place'});
    this.domElementMetricsMemory = cellMetricsMemory[0];
    row.append(this.domElementMetricsMemory);

    var cellMetricsNetwork = $('<td>', {class: 'tenant_metrics_network_place'});
    this.domElementMetricsNetwork = cellMetricsNetwork[0];
    row.append(this.domElementMetricsNetwork);

    var cellMetricsStorage = $('<td>', {class: 'tenant_metrics_storage_place'});
    this.domElementMetricsStorage = cellMetricsStorage[0];
    row.append(this.domElementMetricsStorage);

    this.domElement = row[0];
}

Tenant.prototype.mergePoolStats = function(total, stats) {
    stats.forEach(function(item) {
        var totalItem = total[item.Name];
        var threads = item.Threads;
        if (threads === undefined) {
            threads = 1;
        }
        if (totalItem === undefined) {
            total[item.Name] = {
                TotalUsage: item.Usage,
                Threads: threads,
                Counter: 1
            }
        } else {
            totalItem.TotalUsage += item.Usage * threads;
            totalItem.Threads += threads;
            totalItem.Counter++;
        }
    });
}

Tenant.prototype.getTotalPoolStats = function() {
    var totalStats = {};
    for (var nodeId in this.nodes) {
        var node = this.nodes[nodeId];
        if (node && node.PoolStats) {
            this.mergePoolStats(totalStats, node.PoolStats);
        }
    }
    var result = [];
    for (var key in totalStats) {
        var stats = totalStats[key];
        result.push({
                        Name: key,
                        Usage: stats.TotalUsage / stats.Threads,
                        Threads: stats.Threads
                    });
    }
    return result;
}

Tenant.prototype.getTotalPoolUsage = function(totalStats, name) {
    for (var i = 0; i < totalStats.length; ++i) {
        var poolStats = totalStats[i];
        if (poolStats.Name === name) {
            return poolStats.Usage * poolStats.Threads;
        }
    }
    return 0;
}

Tenant.prototype.updateSysInfo = function(update) {
    var nodeInfo = this.nodes[update.NodeId];
    if (nodeInfo === undefined) {
        nodeInfo = this.nodes[update.NodeId] = {};
        //this.nodeCount++;
        //this.domElementNodes.innerHTML = this.nodeCount;
    }

    if (update.PoolStats !== undefined) {
        var totalPoolStats = this.getTotalPoolStats();
        this.poolMap.setPoolMap(totalPoolStats);
        var userPoolStats = this.getTotalPoolUsage(totalPoolStats, 'User');
        this.domElementMetricsCpu.innerHTML = userPoolStats.toFixed(1);
    }

    if (this.name === undefined) {
        this.domElementName.innerHTML = this.name = this.tenantView.getTenantName(update);
    }
    updateObject(nodeInfo, update);
}

Tenant.prototype.getMetricsValueCPU = function(value) {
    return (value / 1000000).toFixed(1);
}

Tenant.prototype.getMetricsValueMemory = function(value) {
    return bytesToSize(value);
}

Tenant.prototype.getMetricsValueNetwork = function(value) {
    return bytesToSize(value);
}

Tenant.prototype.getMetricsValueStorage = function(value) {
    return bytesToSize(value);
}

Tenant.prototype.updateTenantInfo = function(update) {
    if (update.State) {
        this.state = update.State;
    }

    if (this.name === undefined) {
        if (update.Name) {
            this.name = update.Name;
        } else {
            this.name = update.ShardId + ':' + update.PathId;
        }
        this.domElementName.innerHTML = this.name;
    }

    if (update.Resources) {
        var resources = {};
        update.Resources.Required.forEach(function(item) {
            var resource = resources[item.Type];
            if (!resource) {
                resource = resources[item.Type] = {
                    required: 0,
                    allocated: 0
                };
            }
            resource.required += Number(item.Count);
        });
        update.Resources.Allocated.forEach(function(item) {
            var resource = resources[item.Type];
            if (!resource) {
                resource = resources[item.Type] = {
                    required: 0,
                    allocated: 0
                };
            }
            resource.allocated += Number(item.Count);
        });
        var compute = resources['compute'];
        if (compute) {
            this.domElementCompute.innerHTML = getPartValuesText(compute.allocated, compute.required);
        }
        var storage = resources['storage'];
        if (storage) {
            this.domElementStorage.innerHTML = getPartValuesText(storage.allocated, storage.required);
        }
    }

    if (update.NodeIds) {
        if (update.AliveNodes) {
            this.aliveNodes = update.AliveNodes;
        } else {
            this.aliveNodes = 0;
        }
        this.domElementNodes.innerHTML = this.aliveNodes;
    }

    if (update.StateStats) {
        var aliveTablets = 0;
        var totalTablets = 0;
        for (var i = 0; i < update.StateStats.length; ++i) {
            if (update.StateStats[i].VolatileState === 'TABLET_VOLATILE_STATE_RUNNING') {
                aliveTablets += update.StateStats[i].Count;
            }
            totalTablets += update.StateStats[i].Count;
        }
        this.domElementTablets.innerHTML = getPartValuesText(aliveTablets, totalTablets);
    }

    if (update.Metrics) {
        /*if (update.Metrics.CPU) {
            this.domElementMetricsCpu.innerHTML = this.getMetricsValueCPU(update.Metrics.CPU);
        }*/

        if (update.Metrics.Memory) {
            this.domElementMetricsMemory.innerHTML = this.getMetricsValueMemory(update.Metrics.Memory);
        }

        if (update.Metrics.Network) {
            this.domElementMetricsNetwork.innerHTML = this.getMetricsValueNetwork(update.Metrics.Network);
        }

        if (update.Metrics.Storage) {
            this.domElementMetricsStorage.innerHTML = this.getMetricsValueStorage(update.Metrics.Storage);
        }
    }
}
