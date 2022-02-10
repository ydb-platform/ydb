Node.prototype.directApi = false;
Node.prototype.timeout = 10000;

function Node(options) {
    Object.assign(this, {
                      sysInfo: {},
                      nodeInfo: {},
                      poolMap: null,
                      nodeMap: null,
                      diskMap: null,
                      directApi: Node.prototype.directApi
                  },
                  options);
    this.haveDisks = true;
    this.vDiskDeltaUpdates = 0;
    this.refreshInProgress = 0;
    var role = this.PortToRoles[this.Port];
    if (role) {
        this.sysInfo.Roles = [role];
    }
    this.tabletMap = new TabletMap();
    this.poolMap = new PoolMap();
    this.memBlock = new PoolBlock();
    this.diskMap = new DiskMap({node: this});
    this.buildDomElement();
}

Node.prototype.minSysInfoUpdatePeriod = 2000;
Node.prototype.minPDiskInfoUpdatePeriod = 10000;
Node.prototype.minVDiskInfoUpdatePeriod = 10000;
Node.prototype.minTabletInfoUpdatePeriod = 5000;

Node.prototype.PortToRoles = {
    15600: 'Server',
    15601: 'Host',
    15602: 'Proxy',
    15603: 'Pusher',
    15604: 'Storage'
}

Node.prototype.RoleToMonPort = {
    Default: 8765,
    Storage: 8765,
    Server: 8766,
    Pusher: 8763,
    Proxy: 8764,
    Host: 8762
}

Node.prototype.buildDomElement = function() {
    var row = $('<tr>');
    row.data('node', this);
    var cellId = $('<td>', {class: 'node_id_place'});
    var nodeIcons = $('<div>', {class: 'node_icons'});
    var nodeId = $('<span>', {class: 'node_id'});
    cellId.append(nodeIcons);
    cellId.append(nodeId);
    nodeId.html(this.Id);
    row.append(cellId);
    var host = $('<td>');
    var hostName = $('<div>');
    var hostLink = $('<a>', {
                         'href': this.getBaseUrl(),
                         'target': '_blank',
                         'data-original-title': this.Address
                     });
    hostLink.html(this.getHostName());
    //var hostRoles = $('<span>', {class: 'node_roles'});
    hostName.append(hostLink);
    //hostName.append(hostRoles);
    hostLink.tooltip({html: true});
    host.append(hostName);
    var state = $('<div>', {class: 'statecontainer'});
    var loadAverage = $('<div>', {
                            'class': 'usageblock progress',
                            'data-original-title': 'Load average'
                        });
    var loadAverageBar = $('<div>', {
                               class: 'progress-bar',
                               role: 'progressbar'
                           });
    loadAverage.append(loadAverageBar);
    loadAverage.tooltip({html: true});
    state.append(loadAverage);
    host.append(state);
    row.append(host);
    /*var uptime = $('<td>', {class: 'uptime_place'}).hide();
    row.append(uptime);
    var cpu = $('<td>', {class: 'cpu_place'}).hide();
    row.append(cpu);*/
    /*var interconnect = $('<td>', {class: 'interconnect_place'});
    row.append(interconnect);*/
    /*var disks = $('<td>', {class: 'disk_place tablet_place'}).hide();
    row.append(disks);*/
    this.domElement = row[0];
    this.domElementNodeIcons = nodeIcons[0];
    this.domElementHostLink = hostLink[0];
    //this.domElementUpTime = uptime[0];
    //this.domElementRoles = hostRoles[0];
    this.domElementLoadAverageBar = loadAverageBar[0];
}

Node.prototype.getIcon = function(icon, color) {
    return "<span class='glyphicon glyphicon-" + icon + " node_icon' style='color:" + color + "'></span>";
}

Node.prototype.updateUpTime = function() {
    if (this.sysInfo !== undefined) {
        if (this.sysInfo.StartTime) {
            var currentTime = getTime();
            var startTime = this.sysInfo.StartTime;
            var upTime = currentTime - startTime;
            $(this.domElementUpTime).text(upTimeToString(upTime));
        }
    }
}

Node.prototype.updateToolTip = function() {
    var sysInfo = this.sysInfo;
    var tooltip = '<table class="tooltip-table">';

    if (sysInfo) {
        if (sysInfo.DataCenter) {
            tooltip += '<tr><td>Datacenter</td><td>' + sysInfo.DataCenter + '</td></tr>';
        }

        if (sysInfo.Rack) {
            tooltip += '<tr><td>Rack</td><td>' + sysInfo.Rack + '</td></tr>';
        }

        if (sysInfo.Tenants) {
            tooltip += '<tr><td>Tenant</td><td>' + sysInfo.Tenants[0] + '</td></tr>';
        }
    }

    if (this.Host) {
        tooltip += '<tr><td>Host</td><td>' + this.Host + '</td></tr>';
    }

    if (this.Address) {
        tooltip += '<tr><td>Address</td><td>' + this.Address + '</td></tr>';
    }

    if (sysInfo) {
        if (sysInfo.Endpoints !== undefined && sysInfo.Endpoints.length > 0) {
            var endpoints = '';
            sysInfo.Endpoints.forEach(function(item) {
                endpoints += '<tr><td>' + item.Name + '</td><td>' + item.Address + '</td></tr>';
            });
            tooltip += endpoints;
        }

        if (sysInfo.ConfigState !== undefined) {
            tooltip += '<tr><td>Configuration</td><td>' + sysInfo.ConfigState + '</td></tr>';
        }
    }
    tooltip += '</table>'
    this.domElementHostLink.setAttribute('data-original-title', tooltip);
}

Node.prototype.getHost = function() {
    if (this.Host) {
        return this.Host;
    }
    if (this.Address) {
        return this.Address;
    }
    return window.location.hostname;
}

Node.prototype.getEndpoint = function(name) {
    if (this.sysInfo.Endpoints) {
        var endpoint = this.sysInfo.Endpoints.find(function(item) {
            return item.Name === name;
        });
        if (endpoint) {
            return endpoint.Address;
        }
    }
    return undefined;
}

Node.prototype.getViewerPort = function() {
    var viewerEndpoint = this.getEndpoint('http-mon');
    if (viewerEndpoint) {
        return Number(viewerEndpoint.split(':')[1]);
    }
    var role = this.getRoleName();
    if (role) {
        var port = this.RoleToMonPort[role];
        if (port) {
            return Number(port);
        }
    }
    return 8765;
}

Node.prototype.getBaseUrl = function() {
    return getBaseUrlForHost(this.getHost() + ':' + this.getViewerPort());
}

Node.prototype.getViewerUrl = function() {
    return this.getBaseUrl() + 'viewer/';
}

Node.prototype.getUrlFor = function(url) {
    if (url === 'json/config' || this.directApi) {
        return this.getViewerUrl() + url;
    }
    return url;
}

Node.prototype.getHostName = function() {
    var host = this.getHost();
    var idx = host.indexOf('.');
    if (idx !== -1) {
        host = host.substr(0, idx);
    }
    return host;
}

Node.prototype.getDomainName = function() {
    var domain = this.getHost();
    var idx = domain.indexOf('.');
    if (idx !== -1) {
        domain = domain.substr(idx + 1);
    }
    return domain;
}

Node.prototype.hasRole = function() {
    return this.sysInfo.Roles !== undefined && this.sysInfo.Roles.length > 0;
}

Node.prototype.getRoleName = function() {
    if (this.hasRole()) {
        return this.sysInfo.Roles.join(',');
    }
    return 'Default';
}

Node.prototype.getTenantName = function() {
    return this.group.view.getTenantName(this.sysInfo);
}

Node.prototype.getDC = function() {
    if (this.sysInfo.DataCenter !== undefined) {
        return this.sysInfo.DataCenter;
    }
    return 'Unknown';
}

Node.prototype.getRackName = function() {
    if (this.sysInfo.Rack !== undefined) {
        return this.sysInfo.Rack;
    }
    return 'Unknown';
}

Node.prototype.getStatus = function() {
    if (this.sysInfo !== undefined) {
        if (flagToColor(this.sysInfo.SystemState) === green) {
            return 'Good';
        } else {
            return 'Bad';
        }
    }
    return 'Unknown';
}

Node.prototype.getStatusColor = function() {
    if (this.sysInfo && this.sysInfo.SystemState && !this.disconnected) {
        return flagToColor(this.sysInfo.SystemState);
    } else {
        return black;
    }
}

Node.prototype.getAge = function() {
    var uptime = this.getUptime();
    if (uptime) {
        if (uptime < 60 * 1000) {
            return 'Less than a minute';
        } else if (uptime < 60 * 60 * 1000) {
            return 'Less than a hour';
        } else if (uptime < 24 * 60 * 60 * 1000) {
            return 'Less than a day';
        } else {
            return 'More than a day';
        }
    }
    return 'Unknown';
}

Node.prototype.getVersion = function() {
    if (this.sysInfo && this.sysInfo.Version) {
        return this.sysInfo.Version;
    }
    return 'Unknown';
}

Node.prototype.getAgeColor = function() {
    var uptime = this.getUptime();
    if (uptime) {
        if (uptime < 60 * 1000) {
            return red;
        } else if (uptime < 60 * 60 * 1000) {
            return orange;
        } else if (uptime < 24 * 60 * 60 * 1000) {
            return yellow;
        } else {
            return green;
        }
    }
    return black;
}

Node.prototype.getLoadAverage = function(index) {
    if (index === undefined) {
        index = 0;
    }
    if (this.sysInfo !== undefined
        && this.sysInfo.LoadAverage !== undefined
        && this.sysInfo.LoadAverage.length > index
        && this.sysInfo.NumberOfCpus !== undefined
        && this.sysInfo.NumberOfCpus !== 0) {
        return this.sysInfo.LoadAverage[index] / this.sysInfo.NumberOfCpus;
    }
    return 0;
}

Node.prototype.getTooltipText = function() {
    var text = '#' + this.Id + ' ' + this.getHostName();
    if (this.hasRole()) {
        text = text + '<br>' + this.getRoleName();
    }
    return text;
}

Node.prototype.getUptime = function() {
    if (this.sysInfo !== undefined && this.sysInfo.StartTime !== undefined) {
        var now = getTime();
        var uptime = now - this.sysInfo.StartTime;
        if (uptime < 0) {
            return undefined;
        }
        return uptime;
    }
    return undefined;
}

Node.prototype.getDiskUsage = function() {
    if (this.sysInfo !== undefined && this.sysInfo.MaxDiskUsage !== undefined) {
        return this.sysInfo.MaxDiskUsage;
    } else {
        return 0;
    }
}

Node.prototype.getIdGroup = function() {
    var start = Math.floor(this.Id / 100) * 100;
    if (start === 0) {
        return '1..99';
    } else if (start < 1000) {
        return start + '..' + (start + 99);
    } else {
        start = Math.floor((this.Id - 1000) / 3200) * 3200 + 1000;
        return start + '..' + (start + 3199);
    }
}

Node.prototype.refreshSysInfo = function() {
    this.refreshInProgress++;
    var request = {
        alive: 1,
        enums: 1,
        node_id: this.Id,
        timeout: Node.prototype.timeout,
        since: this.sysInfoChangeTime
    };
    $.ajax({
               url: this.getUrlFor('json/sysinfo'),
               data: request,
               success: this.onSysInfo.bind(this),
               error: this.onError.bind(this),
               timeout: request.timeout
           });
}

Node.prototype.refreshNodeInfo = function() {
    var request = {
        merge: 0,
        node_id: this.Id,
        timeout: Node.prototype.timeout,
        since: this.nodeInfoChangeTime
    };
    $.ajax({
               url: this.getUrlFor('json/nodeinfo'),
               data: request,
               success: this.onNodeInfo.bind(this),
               timeout: request.timeout
           });
}

Node.prototype.refreshTabletInfo = function() {
    var request = {
        alive: 1,
        enums: 1,
        group: 'Type,Leader',
        filter: 'State=Active',
        node_id: this.Id,
        timeout: Node.prototype.timeout
    };
    $.ajax({
               url: this.getUrlFor('json/tabletinfo'),
               data: request,
               success: this.onTabletInfo.bind(this),
               error: this.onError.bind(this),
               timeout: request.timeout
           });
}

Node.prototype.refreshPDiskInfo = function() {
    var request = {
        enums: 1,
        node_id: this.Id,
        timeout: Node.prototype.timeout,
        since: this.pDiskInfoChangeTime
    };
    this.pDiskSince = this.pDiskInfoChangeTime;
    $.ajax({
               url: this.getUrlFor('json/pdiskinfo'),
               data: request,
               success: this.onPDiskInfo.bind(this),
               timeout: request.timeout
           });
}

Node.prototype.refreshVDiskInfo = function() {
    var request = {
        enums: 1,
        node_id: this.Id,
        timeout: Node.prototype.timeout,
        since: this.vDiskInfoChangeTime
    };
    $.ajax({
               url: this.getUrlFor('json/vdiskinfo'),
               data: request,
               success: this.onVDiskInfo.bind(this),
               timeout: request.timeout
           });
}

Node.prototype.refreshConfig = function() {
    $.ajax({
               url: this.getUrlFor('json/config'),
               success: this.onConfig.bind(this),
               error: this.onConfigError.bind(this)
           });
}

Node.prototype.onSysInfo = function(data) {
    --this.refreshInProgress;
    if (data.SystemStateInfo !== undefined) {
        for (var i in data.SystemStateInfo) {
            var update = data.SystemStateInfo[i];
            if (!this.sysInfoChangeTime || update.ChangeTime < this.sysInfoChangeTime) {
                this.sysInfoChangeTime = update.ChangeTime;
            }
            this.updateSysInfo(update);
        }
        if (data.ResponseTime != 0) {
            this.sysInfoChangeTime = Number(data.ResponseTime);
        }
    } else {
        if (data.Error || !this.sysInfoChangeTime) {
            this.onDisconnect();
        } else {
            if (this.visible) {
                this.updateUpTime();
            }
        }
    }
    this.sysInfoUpdateTime = getTime();
}

Node.prototype.onTabletInfo = function(data) {
    /*var nodeData = data[this.Id];
    if (nodeData !== undefined && nodeData !== null) {
        var update = nodeData.NodeStateInfo;
        if (update !== undefined) {
            this.updateNodeInfo(update);
        }
        if (nodeData.ResponseTime != 0) {
            this.nodeInfoChangeTime = nodeData.ResponseTime;
        }
    }*/

    if (!data.TabletStateInfo) {
        data.TabletStateInfo = [];
    }
    this.tabletMap.updateTabletInfo(data.TabletStateInfo);

    if (data.ResponseTime != 0) {
        this.tabletInfoChangeTime = data.ResponseTime;
    }
    this.tabletInfoUpdateTime = getTime();
}

Node.prototype.onNodeInfo = function(data) {
    var nodeData = data[this.Id];
    if (nodeData !== undefined && nodeData !== null) {
        var update = nodeData.NodeStateInfo;
        if (update !== undefined) {
            this.updateNodeInfo(update);
        }
        if (nodeData.ResponseTime != 0) {
            this.nodeInfoChangeTime = nodeData.ResponseTime;
        }
    }
}

Node.prototype.onPDiskInfo = function(data) {
    if (data.PDiskStateInfo) {
        this.updatePDiskInfo(data.PDiskStateInfo);
    } else if (this.haveDisks && !this.pDiskSince) {
        //this.haveDisks = false;
    }

    if (data.ResponseTime != 0) {
        this.pDiskInfoChangeTime = data.ResponseTime;
    }
    this.pDiskInfoUpdateTime = getTime();
}

Node.prototype.onVDiskInfo = function(data) {
    if (data.VDiskStateInfo) {
        this.updateVDiskInfo(data.VDiskStateInfo);
    }
    if (this.vDiskDeltaUpdates >= 10) {
        delete this.vDiskInfoChangeTime;
        this.vDiskDeltaUpdates = 0;
    } else if (data.ResponseTime != 0) {
        this.vDiskInfoChangeTime = data.ResponseTime;
        this.vDiskDeltaUpdates++;
    }
    this.vDiskInfoUpdateTime = getTime();
}

Node.prototype.onConfig = function(data) {
    this.config = data;
    if (this.group) {
        var referenceConfig = this.group.view.getConfig();
        if (referenceConfig) {
            var errors = [];
            for (var name in referenceConfig) {
                if (JSON.stringify(this.config[name]) !== JSON.stringify(referenceConfig[name])) {
                    console.error('Config mismatch in ' + name + ' for ' + this.getHost());
                    console.log(this.config[name]);
                    console.log(referenceConfig[name]);
                    errors.push(name);
                }
            }
            if (errors.length > 0) {
                this.domElementNodeIcons.innerHTML = this.getIcon('exclamation-sign', red);
            }
        }
    }
}

Node.prototype.onConfigError = function() {
    this.config = false;
}

Node.prototype.getBackgroundColor = function(color) {
    switch (color) {
    case red:
        return '#ffe0e0';
    case yellow:
        return '#ffffe0';
    case orange:
        return '#fff0e0';
    case blue:
        return '#e0e0ff';
    default:
        return null;
    }
}

Node.prototype.updateSysInfoStatsOnly = function(update) {
    var sysInfo = updateObject(this.sysInfo, update);
    if (sysInfo.SystemState !== undefined) {
        var color = flagToColor(sysInfo.SystemState);
        //this.domElementOverallState.style.backgroundColor = color;
        if (this.color != color) {
            this.domElement.style.backgroundColor = this.getBackgroundColor(color);
        }
        if (sysInfo.StartTime && this.disconnected) {
            var row = $(this.domElement);
            var disks = row.find('.disk_place');
            disks.empty();
            delete this.tabletMap;
            delete this.diskMap;
            this.tabletMap = new TabletMap();
            this.diskMap = new DiskMap({node: this});
            disks.append(this.diskMap.domElement);
            disks.append(this.tabletMap.domElement);
            row.children().css('filter', '');
            this.disconnected = false;
        }
        if (this.group) {
            this.group.view.updateGroup(this);
            this.group.updateNodeState(this);
        }
        this.color = color;
    }
    if (update.Roles !== undefined && update.Roles.length > 0 && !this.fallbackDirectApi) {
        this.directApi = true;
    }
    /*if (sysInfo.Roles !== undefined && sysInfo.Roles.length > 0 && this.domElementRoles.innerHTML.length === 0) {
        this.domElementRoles.innerHTML = sysInfo.Roles.join(',');
    }*/
}

Node.prototype.updateSysInfo = function(update) {
    this.updateSysInfoStatsOnly(update);

    var sysInfo = this.sysInfo;

    if (sysInfo.StartTime && this.visible) {
        this.updateUpTime();
    }

    /*if (sysInfo.MessageBusState !== undefined) {
        this.domElementMessageBusState.style.backgroundColor = flagToColor(sysInfo.MessageBusState);
    }*/

    /*if (sysInfo.GRpcState !== undefined) {
        this.domElementGRpcState.style.backgroundColor = flagToColor(sysInfo.GRpcState);
    }*/

    if (sysInfo.LoadAverage !== undefined) {
        var percent = sysInfo.LoadAverage[0] * 100 / sysInfo.NumberOfCpus;
        if (percent > 100) {
            percent = 100;
        }
        var progress = this.domElementLoadAverageBar;
        if (percent < 75) {
            progress.style.backgroundColor = green;
        } else if (percent < 85) {
            progress.style.backgroundColor = yellow;
        } else if (percent < 95) {
            progress.style.backgroundColor = orange;
        } else {
            progress.style.backgroundColor = red;
        }
        progress.style.width = percent + '%';
        progress.parentNode.setAttribute('data-original-title', 'LoadAverage: ' + sysInfo.LoadAverage + '<br/>Cores: ' + sysInfo.NumberOfCpus);
    }

    if (sysInfo.PoolStats !== undefined) {
        var poolMap = this.poolMap;
        poolMap.setPoolMap(sysInfo.PoolStats);
    }

    if (sysInfo.MemoryUsed !== undefined) {
        var memBlock = this.memBlock;
        var tip = '<html><table class="tooltip-table">';
        memBlock.setText(bytesToGB3(sysInfo.MemoryUsed));
        tip += '<tr><td>MemoryUsed</td><td>' + bytesToGB(sysInfo.MemoryUsed) + '</td></tr>';
        if (sysInfo.MemoryLimit !== undefined) {
            memBlock.setUsage(sysInfo.MemoryUsed / sysInfo.MemoryLimit);
            tip += '<tr><td>MemoryLimit</td><td>' + bytesToGB(sysInfo.MemoryLimit) + '</td></tr>';
        } else {
            //memBlock.setUsage(sysInfo.MemoryUsed / 10000000000);
            //tip += '<tr><td>MemoryLimit</td><td>' + bytesToGB(10000000000) + '</td></tr>';
        }
        tip += '</table></html>';
        memBlock.domElement.setAttribute('data-original-title', tip);
    }

    if (sysInfo.ConfigState === 'Outdated') {
        this.domElementNodeIcons.innerHTML = this.getIcon('exclamation-sign', red);
    }

    this.domElementHostLink.setAttribute('href', this.getBaseUrl());
}

Node.prototype.onNodeClick = function(clickData) {
    this.nodeView.scrollToNodePosition(clickData.position);
}

Node.prototype.updateNodeInfo = function(update) {
    var nodeMap = this.nodeMap;
    if (nodeMap === null) {
        nodeMap = new NodeMap({
                                  nodes: this.nodeView.nodesCount,
                                  maxWidth: 250,
                                  onNodeClick: this.onNodeClick.bind(this)
                              });
    }
    for (var i = 0; i < update.length; i++) {
        var stateInfo = update[i];
        var peerName = stateInfo.PeerName;
        var id = getNodeIdFromPeerName(peerName);
        var nodeInfo = this.nodeInfo[id];
        if (nodeInfo === undefined
                || nodeInfo.Connected !== stateInfo.Connected
                || nodeInfo.ConnectStatus !== stateInfo.ConnectStatus) {
            if (nodeInfo === undefined) {
                nodeInfo = this.nodeInfo[id] = {};
            }
            nodeInfo = updateObject(nodeInfo, stateInfo);
            var node = this.nodeView.nodes[id];
            if (node !== undefined) {
                var position = node.position;
                if (nodeInfo.Connected === undefined) {
                    nodeMap.setNodeMap(position, lightgrey, peerName);
                } else {
                    var color;
                    if (nodeInfo.Connected) {
                        color = 'lightgreen';
                        if (nodeInfo.ConnectStatus !== undefined) {
                            switch (nodeInfo.ConnectStatus) {
                            case 1:
                                color = green;
                                break;
                            case 2:
                                color = yellow;
                                break;
                            case 3:
                                color = orange;
                                break;
                            case 4:
                                color = red;
                                break;
                            }
                        }
                        nodeMap.setNodeMap(position, color, peerName);
                    } else {
                        nodeMap.setNodeMap(position, 'red', peerName);
                    }
                }
            }
        }
    }
    nodeMap.setNodeMap(this.position, '#009900');
    if (this.nodeMap === null) {
        var interconnectPlace = $(this.domElement).find('.interconnect_place');
        interconnectPlace.empty();
        interconnectPlace.append(nodeMap.domElement);
        this.nodeMap = nodeMap;
    }
}

Node.prototype.updatePDiskInfo = function(update) {
    var diskMap = this.diskMap;
    if (!diskMap) {
        this.diskMap = diskMap = new DiskMap({node: this});
    }

    var nodeId = this.Id;
    update.forEach(function(update) {
        if (!update.NodeId) {
            update.NodeId = nodeId;
        }
        diskMap.updatePDiskInfo(update);
    });
}

Node.prototype.updateVDiskInfo = function(update) {
    var diskMap = this.diskMap;
    if (!diskMap) {
        this.diskMap = diskMap = new DiskMap({node: this});
    }

    var nodeId = this.Id;
    var vDisksCount = diskMap.vDisksCount;
    update.forEach(function(update) {
        if (!update.NodeId) {
            update.NodeId = nodeId;
        }
        diskMap.updateVDiskInfo(update);
    });

    if (!this.vDiskInfoChangeTime || vDisksCount != diskMap.vDisksCount) {
        diskMap.resizeVDisks();
    }
}

Node.prototype.onDisconnect = function() {
    if (this.sysInfo && this.sysInfo.StartTime) {
        delete this.sysInfo.StartTime;
        delete this.sysInfoChangeTime;
        delete this.vDiskInfoChangeTime;
        delete this.pDiskInfoChangeTime;
        this.vDiskDeltaUpdates = 0;
    }
    delete this.config;
    if (!this.disconnected) {
        this.updateSysInfoStatsOnly({SystemState: 'Red'});
        $(this.domElement).children().css('filter', 'grayscale(100%)');
        this.disconnected = true;
    }
}

Node.prototype.onError = function(jqXHR, textStatus, errorThrown) {
    this.refreshInProgress = 0;
    if (this.directApi) {
        console.log('Fallback to non-direct API for Node#' + this.Id);
        this.directApi = false;
        this.fallbackDirectApi = true;
    } else {
        this.onDisconnect();
    }
}

Node.prototype.refresh = function() {
    if (this.refreshInProgress === 0) {
        var now = getTime();
        if (!this.sysInfoUpdateTime || now - this.sysInfoUpdateTime > this.minSysInfoUpdatePeriod) {
            this.refreshSysInfo();
        }
        if (!this.tabletInfoUpdateTime || now - this.tabletInfoUpdateTime > this.minTabletInfoUpdatePeriod) {
            this.refreshTabletInfo();
        }
        //this.refreshNodeInfo();
        if (this.haveDisks) {
            if (!this.pDiskInfoUpdateTime || now - this.pDiskInfoUpdateTime > this.minPDiskInfoUpdatePeriod) {
                this.refreshPDiskInfo();
            }
            if (!this.vDiskInfoUpdateTime || now - this.vDiskInfoUpdateTime > this.minVDiskInfoUpdatePeriod) {
                this.refreshVDiskInfo();
            }
        }
        /*if (this.config === undefined && this.sysInfo && this.sysInfo.Host) {
            this.refreshConfig();
        }*/
    }
}

Node.prototype.appear = function() {
    this.visible = true;
    var row = $(this.domElement);
    var uptime = $('<td>', {class: 'uptime_place'});
    var cpu = $('<td>', {class: 'cpu_place'});
    var mem = $('<td>', {class: 'mem_place'});
    var disks = $('<td>', {class: 'disk_place tablet_place'});
    this.domElementUpTime = uptime[0];
    this.updateToolTip();
    this.updateUpTime();
    if (this.poolMap) {
        this.poolMap.restoreTooltips();
        cpu.append(this.poolMap.domElement);
    }
    if (this.memBlock) {
        this.memBlock.restoreTooltips();
        mem.append(this.memBlock.domElement);
    }

    if (this.diskMap) {
        this.diskMap.restoreTooltips();
        this.diskMap.updatePDiskClasses();
        this.diskMap.updateVDiskClasses();
        this.diskMap.resizeVDisks();
        disks.append(this.diskMap.domElement);
    }
    if (this.tabletMap) {
        this.tabletMap.restoreTooltips();
        disks.append(this.tabletMap.domElement);
    }

    row.append(uptime);
    row.append(cpu);
    row.append(mem);
    row.append(disks);
    if (this.disconnected) {
        $(this.domElement).children().css('filter', 'grayscale(100%)');
    }
}

Node.prototype.disappear = function() {
    this.visible = false;
    $(this.domElement).find('.uptime_place').remove();
    $(this.domElement).find('.cpu_place').remove();
    $(this.domElement).find('.mem_place').remove();
    $(this.domElement).find('.disk_place').remove();
    //$(this.domElement).find('.tablet_place').hide();
}
