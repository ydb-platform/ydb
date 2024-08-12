var Parameters = {};

function ViewerNodes(options) {
    Object.assign(this, {
                      overallRefreshTimeout: 60000,
                      maxDiffRequests: 10
                  }, options);
    this.nodeView = null;
    this.netView = null;
    this.sysInfoRequestNum = 0;
    this.nodeInfoRequestNum = 0;
    $.ajax({
               url: "json/config",
               success: this.onConfig.bind(this)
           });
}

ViewerNodes.prototype.onConfig = function(data) {
    this.config = data;
    $.ajax({
               url: "json/nodelist",
               success: this.onNodeList.bind(this),
               error: this.onNodeList.bind(this)
           });
}

ViewerNodes.prototype.onNodeList = function(nlist) {
    if (this.nodeView === null) {
        this.nodeView = new NodeView({
                                         domElement: $('#nodes_view')[0],
                                         config: this.config
                                     });
    }
    if (nlist) {
        var nlen = nlist.length;
        for (var i = 0; i < nlen; i++) {
            var node = new Node(nlist[i]);
            this.nodeView.addNode(node);
        }
        this.nodeView.rebuild();
    }
    this.refreshOverall();
    this.refreshOverallInterval = setInterval(this.refreshOverall.bind(this), this.overallRefreshTimeout);
}

ViewerNodes.prototype.onSysInfo = function(data) {
    data.since = this.sysInfoChangeTime;
    data.refreshTimeout = this.overallRefreshTimeout;
    this.nodeView.onSysInfo(data);
    if (data.ResponseTime != 0) {
        this.sysInfoChangeTime = data.ResponseTime;
    }
}

ViewerNodes.prototype.onNodeInfo = function(data) {
    data.since = this.nodeInfoChangeTime;
    data.refreshTimeout = this.overallRefreshTimeout;
    //this.netView.onNodeInfo(data);
    if (data.ResponseTime != 0) {
        this.nodeInfoChangeTime = data.ResponseTime;
    }
}

ViewerNodes.prototype.onPDiskInfo = function(data) {
    this.nodeView.onPDiskInfo(data);
}

ViewerNodes.prototype.onVDiskInfo = function(data) {
    this.nodeView.onVDiskInfo(data);
}

ViewerNodes.prototype.onDisconnect = function() {
    this.nodeView.onDisconnect();
}

ViewerNodes.prototype.refreshPDiskInfo = function() {
//    console.log('refreshOverall (pdiskinfo)');
    var request = {
        alive: 0,
        enums: 1,
        merge: 0,
        timeout: 10000
    };
    $.ajax({
               url: 'json/pdiskinfo',
               data: request,
               success: this.onPDiskInfo.bind(this)
           });
}

ViewerNodes.prototype.refreshVDiskInfo = function() {
//    console.log('refreshOverall (vdiskinfo)');
    var request = {
        alive: 0,
        enums: 1,
        merge: 0,
        timeout: 10000
    };
    $.ajax({
               url: 'json/vdiskinfo',
               data: request,
               success: this.onVDiskInfo.bind(this)
           });
}

ViewerNodes.prototype.refreshSysInfo = function() {
//    console.log('refreshOverall (sysinfo)');
    if (++this.sysInfoRequestNum >= this.maxDiffRequests) {
        this.sysInfoRequestNum = 0;
        delete this.sysInfoChangeTime;
    }

    var request = {
        alive: 0,
        enums: 1,
        timeout: 10000
    };
    $.ajax({
               url: 'json/sysinfo',
               data: request,
               success: this.onSysInfo.bind(this),
               error: this.onDisconnect.bind(this)
           });
}

ViewerNodes.prototype.refreshNodeInfo = function() {
//    console.log('refreshOverall (nodeinfo)');
    if (++this.nodeInfoRequestNum >= this.maxDiffRequests) {
        this.nodeInfoRequestNum = 0;
        delete this.nodeInfoChangeTime;
    }

    var request = {
        alive: 0,
        enums: 0,
        merge: 0,
        timeout: 10000/*,
        since: this.nodeInfoChangeTime*/
    };
    $.ajax({
               url: 'json/nodeinfo',
               data: request,
               success: this.onNodeInfo.bind(this)
           });
}

ViewerNodes.prototype.refreshOverall = function() {
    this.refreshSysInfo();
    //this.refreshNodeInfo();
    //this.refreshPDiskInfo();
    //this.refreshVDiskInfo();
}

ViewerNodes.prototype.onNodeGroupChange = function(obj) {
    $(obj).parent().find('.btn-info').removeClass('btn-info').addClass('btn-default');
    $(obj).data('original', $(obj).html());
    $(obj).html('<img src="throbber.gif">');
    delete this.nodeView.groupOrder;
    switch (obj.value) {
    case 'dc':
        this.nodeView.getNodeGroupName = function(node) { return node.getDC(); }
        break;
    case 'domain':
        this.nodeView.getNodeGroupName = function(node) { return node.getDomainName(); }
        break;
    case 'rack':
        this.nodeView.getNodeGroupName = function(node) { return node.getRackName(); }
        break;
    case 'host':
        this.nodeView.getNodeGroupName = function(node) { return node.getHostName(); }
        break;
    case 'id':
        this.nodeView.getNodeGroupName = function(node) { return node.getIdGroup(); }
        this.nodeView.groupOrder = function(prev, next) { return Number(prev.split('..')[0]) < Number(next.split('..')[0]); }
        break;
    case 'role':
        this.nodeView.getNodeGroupName = function(node) { return node.getRoleName(); }
        break;
    case 'tenant':
        this.nodeView.getNodeGroupName = function(node) { return node.getTenantName(); }
        break;
    case 'status':
        this.nodeView.getNodeGroupName = function(node) { return node.getStatus(); }
        break;
    case 'uptime':
        this.nodeView.getNodeGroupName = function(node) { return node.getAge(); }
        break;
    case 'version':
        this.nodeView.getNodeGroupName = function(node) { return node.getVersion(); }
        break;
    case 'usage':
        this.nodeView.getNodeGroupName = function(node) { return Math.floor((node.getDiskUsage() * 100 + 0.5) / 5) * 5 + '%'; }
        this.nodeView.groupOrder = function(prev, next) { return Number(prev.substring(0, prev.length - 1)) > Number(next.substring(0, next.length - 1)); }
        break;
    }
    this.nodeView.rebuild();
    $(obj).html($(obj).data('original'));
    $(obj).removeClass('btn-default').addClass('btn-info');
}




function ViewerOverview(options) {
    Object.assign(this, {
                      overallRefreshTimeout: 10000,
                      maxDiffRequests: 10
                  }, options);
    this.overview = null;
    this.sysInfoRequestNum = 0;
    this.nodeInfoRequestNum = 0;
    $.ajax({
               url: "json/config",
               success: this.onConfig.bind(this)
           });
}

ViewerOverview.prototype.onConfig = function(data) {
    this.config = data;
    $.ajax({
               url: "json/nodelist",
               success: this.onNodeList.bind(this),
               error: this.onNodeList.bind(this)
           });
}

ViewerOverview.prototype.onNodeList = function(nlist) {
    if (this.overview === null) {
        this.overview = new Overview({
                                         domElement: $('#overview_view')[0],
                                         config: this.config
                                     });
    }
    if (nlist) {
        var nlen = nlist.length;
        for (var i = 0; i < nlen; i++) {
            var node = new Node(nlist[i]);
            this.overview.addNode(node);
        }
        this.overview.rebuild();
    }
    this.refreshOverall();
    this.refreshOverallInterval = setInterval(this.refreshOverall.bind(this), this.overallRefreshTimeout);
}

ViewerOverview.prototype.onSysInfo = function(data) {
    data.since = this.sysInfoChangeTime;
    data.refreshTimeout = this.overallRefreshTimeout;
    this.overview.onSysInfo(data);
    if (data.ResponseTime != 0) {
        this.sysInfoChangeTime = data.ResponseTime;
    }
}

ViewerOverview.prototype.onDisconnect = function() {
    this.overview.onDisconnect();
}

ViewerOverview.prototype.refreshSysInfo = function() {
//    console.log('refreshOverall (sysinfo)');
    if (++this.sysInfoRequestNum >= this.maxDiffRequests) {
        this.sysInfoRequestNum = 0;
        delete this.sysInfoChangeTime;
    }

    var request = {
        alive: 0,
        enums: 1,
        timeout: 10000,
        since: this.sysInfoChangeTime
    };
    $.ajax({
               url: 'json/sysinfo',
               data: request,
               success: this.onSysInfo.bind(this),
               error: this.onDisconnect.bind(this)
           });
}

ViewerOverview.prototype.refreshOverall = function() {
    this.refreshSysInfo();
}




function ViewerCpu(options) {
    Object.assign(this, {
                      overallRefreshTimeout: 10000,
                      maxDiffRequests: 10
                  }, options);
    this.cpuView = null;
    this.sysInfoRequestNum = 0;
    this.nodeInfoRequestNum = 0;
    $.ajax({
               url: "json/config",
               success: this.onConfig.bind(this)
           });
}

ViewerCpu.prototype.onConfig = function(data) {
    this.config = data;
    $.ajax({
               url: "json/nodelist",
               success: this.onNodeList.bind(this),
               error: this.onNodeList.bind(this)
           });
}

ViewerCpu.prototype.onNodeList = function(nlist) {
    if (this.cpuView === null) {
        this.cpuView = new CpuView({
                                        domElement: $('#cpu_view')[0],
                                        config: this.config
                                    });
    }
    if (nlist) {
        var nlen = nlist.length;
        for (var i = 0; i < nlen; i++) {
            var node = new Node(nlist[i]);
            this.cpuView.addNode(node);
        }
        this.cpuView.rebuild();
    }
    this.refreshOverall();
    this.refreshOverallInterval = setInterval(this.refreshOverall.bind(this), this.overallRefreshTimeout);
}

ViewerCpu.prototype.onSysInfo = function(data) {
    data.since = this.sysInfoChangeTime;
    data.refreshTimeout = this.overallRefreshTimeout;
    this.cpuView.onSysInfo(data);
    if (data.ResponseTime != 0) {
        this.sysInfoChangeTime = data.ResponseTime;
    }
}

ViewerCpu.prototype.onDisconnect = function() {
    this.cpuView.onDisconnect();
}

ViewerCpu.prototype.refreshSysInfo = function() {
//    console.log('refreshOverall (sysinfo)');
    if (++this.sysInfoRequestNum >= this.maxDiffRequests) {
        this.sysInfoRequestNum = 0;
        delete this.sysInfoChangeTime;
    }

    var request = {
        alive: 0,
        enums: 1,
        timeout: 10000,
        since: this.sysInfoChangeTime
    };
    $.ajax({
               url: 'json/sysinfo',
               data: request,
               success: this.onSysInfo.bind(this),
               error: this.onDisconnect.bind(this)
           });
}

ViewerCpu.prototype.refreshOverall = function() {
    this.refreshSysInfo();
}




function ViewerStorage(options) {
    Object.assign(this, {
                      overallRefreshTimeout: 10000,
                      maxDiffRequests: 10
                  }, options);
    this.storageView = null;
    this.sysInfoRequestNum = 0;
    this.nodeInfoRequestNum = 0;
    $.ajax({
               url: "json/config",
               success: this.onConfig.bind(this)
           });
}

ViewerStorage.prototype.onConfig = function(data) {
    this.config = data;
    $.ajax({
               url: "json/nodelist",
               success: this.onNodeList.bind(this),
               error: this.onNodeList.bind(this)
           });
}

ViewerStorage.prototype.onNodeList = function(nlist) {
    if (this.storageView === null) {
        this.storageView = new StorageView({
                                         domElement: $('#storage_view')[0],
                                         config: this.config
                                     });
    }
    if (nlist) {
        var nlen = nlist.length;
        for (var i = 0; i < nlen; i++) {
            var node = new Node(nlist[i]);
            this.storageView.addNode(node);
        }
        this.storageView.rebuild();
    }
    this.refreshOverall();
    this.refreshOverallInterval = setInterval(this.refreshOverall.bind(this), this.overallRefreshTimeout);
}

ViewerStorage.prototype.onStorage = function(data) {
    this.storageView.onStorage(data);
}

ViewerStorage.prototype.onDisconnect = function() {
    this.storageView.onDisconnect();
}

ViewerStorage.prototype.refreshStorage = function() {
//    console.log('refreshOverall (storage)');
    var request = {
        timeout: 10000
    };
    $.ajax({
               url: 'json/storage',
               data: request,
               success: this.onStorage.bind(this)
           });
}

ViewerStorage.prototype.refreshOverall = function() {
    this.refreshStorage();
}

ViewerStorage.prototype.manuallyRefreshOverall = function() {
    clearInterval(this.refreshOverallInterval);
    this.refreshOverallInterval = setInterval(this.refreshOverall.bind(this), this.overallRefreshTimeout);
    this.refreshOverall();
}

var colorsOrder = ['Red', 'Orange', 'Yellow', 'Blue', 'Green', 'Grey'];

ViewerStorage.prototype.onStorageGroupChange = function(obj) {
    $(obj).parent().find('.btn-info').removeClass('btn-info').addClass('btn-default');
    $(obj).data('original', $(obj).html());
    $(obj).html('<img src="throbber.gif">');
    switch (obj.value) {
    case 'pool':
        this.storageView.getStorageGroupName = function(storage) { return storage.StoragePool.Name; }
        this.storageView.getStorageGroupHeader = function(storageGroup) { return bytesToGB0(storageGroup.allocatedSizeBytes) + ' / ' + storageGroup.storageTotal + ' groups'; }
        this.storageView.groupOrder = function(prev, next) { return prev < next; }
        break;
    case 'color':
        this.storageView.getStorageGroupName = function(storage) { return storage.getColor(); }
        this.storageView.getStorageGroupHeader = function(storageGroup) { return storageGroup.storageTotal + ' groups'; }
        this.storageView.groupOrder = function(prev, next) { return colorsOrder.indexOf(prev) < colorsOrder.indexOf(next); }
        break;
    case 'status':
        this.storageView.getStorageGroupName = function(storage) { return storage.getStatus(); }
        this.storageView.getStorageGroupHeader = function(storageGroup) { return storageGroup.storageTotal + ' groups'; }
        this.storageView.groupOrder = function(prev, next) { return prev < next; }
        break;
    case 'usage':
        this.storageView.getStorageGroupName = function(storage) { return Math.floor((storage.getUsage() * 100 + 0.5) / 5) * 5 + '%'; }
        this.storageView.getStorageGroupHeader = function(storageGroup) { return storageGroup.storageTotal + ' groups'; }
        this.storageView.groupOrder = function(prev, next) { return Number(prev.substring(0, prev.length - 1)) > Number(next.substring(0, next.length - 1)); }
        break;
    case 'missing':
        this.storageView.getStorageGroupName = function(storage) {
            var md = storage.getMissingDisks();
            return md === -1 ? "BlobDepot error" : md === 0 ? "Complete" : '-' + md;
        }
        this.storageView.getStorageGroupHeader = function(storageGroup) { return storageGroup.storageTotal + ' groups'; }
        this.storageView.groupOrder = function(prev, next) { return prev < next; }
        break;
    }
    this.storageView.rebuild();
    $(obj).html($(obj).data('original'));
    $(obj).removeClass('btn-default').addClass('btn-info');
}


function ViewerTenants(options) {
    Object.assign(this, {
                      overallRefreshTimeout: 30000,
                      maxDiffRequests: 10
                  }, options);
    this.tenantView = null;
    this.sysInfoRequestNum = 0;
    this.nodeInfoRequestNum = 0;
    $.ajax({
               url: "json/config",
               success: this.onConfig.bind(this)
           });
}

ViewerTenants.prototype.onConfig = function(data) {
    this.config = data;
    $.ajax({
               url: "json/nodelist",
               success: this.onNodeList.bind(this),
               error: this.onNodeList.bind(this)
           });
}

ViewerTenants.prototype.onNodeList = function(nlist) {
    if (this.tenantView === null) {
        this.tenantView = new TenantView({
                                             domElement: $('#tenants_view')[0],
                                             config: this.config
                                         });
    }
    if (nlist) {
        var nlen = nlist.length;
        for (var i = 0; i < nlen; i++) {
            var node = new Node(nlist[i]);
//            this.nodeView.addNode(node);
        }
    }
    this.refreshOverall();
    this.refreshOverallInterval = setInterval(this.refreshOverall.bind(this), this.overallRefreshTimeout);
}

ViewerTenants.prototype.onSysInfo = function(data) {
    data.since = this.sysInfoChangeTime;
    data.refreshTimeout = this.overallRefreshTimeout;
    this.tenantView.onSysInfo(data);
    if (data.ResponseTime != 0) {
        this.sysInfoChangeTime = data.ResponseTime;
    }
}

ViewerTenants.prototype.onDisconnect = function() {
    this.tenantView.onDisconnect();
}

ViewerTenants.prototype.refreshSysInfo = function() {
//    console.log('refreshOverall (sysinfo)');
    if (++this.sysInfoRequestNum >= this.maxDiffRequests) {
        this.sysInfoRequestNum = 0;
        delete this.sysInfoChangeTime;
    }

    var request = {
        alive: 0,
        enums: 1,
        timeout: 10000,
        since: this.sysInfoChangeTime
    };
    $.ajax({
               url: 'json/sysinfo',
               data: request,
               success: this.onSysInfo.bind(this),
               error: this.onDisconnect.bind(this)
           });
}

ViewerTenants.prototype.refreshOverall = function() {
    this.refreshSysInfo();
}


function ViewerNetwork(options) {
    Object.assign(this, {
                      overallRefreshTimeout: 30000,
                      maxDiffRequests: 10
                  }, options);
    this.netView = null;
    this.sysInfoRequestNum = 0;
    this.nodeInfoRequestNum = 0;
    $.ajax({
               url: "json/config",
               success: this.onConfig.bind(this)
           });
}

ViewerNetwork.prototype.onConfig = function(data) {
    this.config = data;
    $.ajax({
               url: "json/nodelist",
               success: this.onNodeList.bind(this),
               error: this.onNodeList.bind(this)
           });
}

ViewerNetwork.prototype.onNodeList = function(nlist) {
    if (this.netView === null) {
        this.netView = new NetView({
                                        domElement: $('#net_view')[0],
                                        config: this.config
                                    });
    }
    if (nlist) {
        var nlen = nlist.length;
        for (var i = 0; i < nlen; i++) {
            var node = new Node(nlist[i]);
            this.netView.addNode(node);
        }
        this.netView.rebuild();
    }
    this.refreshOverall();
    this.refreshOverallInterval = setInterval(this.refreshOverall.bind(this), this.overallRefreshTimeout);
}

ViewerNetwork.prototype.onSysInfo = function(data) {
    data.since = this.sysInfoChangeTime;
    data.refreshTimeout = this.overallRefreshTimeout;
    this.netView.onSysInfo(data);
    if (data.ResponseTime != 0) {
        this.sysInfoChangeTime = data.ResponseTime;
    }
}

ViewerNetwork.prototype.onNodeInfo = function(data) {
    data.since = this.nodeInfoChangeTime;
    data.refreshTimeout = this.overallRefreshTimeout;
    this.netView.onNodeInfo(data);
    if (data.ResponseTime != 0) {
        this.nodeInfoChangeTime = data.ResponseTime;
    }
}


ViewerNetwork.prototype.onDisconnect = function() {
    this.netView.onDisconnect();
}

ViewerNetwork.prototype.refreshSysInfo = function() {
//    console.log('refreshOverall (sysinfo)');
    if (++this.sysInfoRequestNum >= this.maxDiffRequests) {
        this.sysInfoRequestNum = 0;
        delete this.sysInfoChangeTime;
    }

    var request = {
        alive: 0,
        enums: 1,
        timeout: 10000,
        since: this.sysInfoChangeTime
    };
    $.ajax({
               url: 'json/sysinfo',
               data: request,
               success: this.onSysInfo.bind(this),
               error: this.onDisconnect.bind(this)
           });
}

ViewerNetwork.prototype.refreshNodeInfo = function() {
//    console.log('refreshOverall (nodeinfo)');
    if (++this.nodeInfoRequestNum >= this.maxDiffRequests) {
        this.nodeInfoRequestNum = 0;
        delete this.nodeInfoChangeTime;
    }

    var request = {
        alive: 0,
        enums: 0,
        merge: 0,
        timeout: 10000/*,
        since: this.nodeInfoChangeTime*/
    };
    $.ajax({
               url: 'json/nodeinfo',
               data: request,
               success: this.onNodeInfo.bind(this)
           });
}

ViewerNetwork.prototype.refreshOverall = function() {
    this.refreshSysInfo();
    this.refreshNodeInfo();
}



var viewer = null;

function onNodeGroupChange(obj) {
    viewer.onNodeGroupChange(obj);
    return false;
}

function onStorageGroupChange(obj) {
    viewer.onStorageGroupChange(obj);
    return false;
}

function mainNodes() {
    viewer = new ViewerNodes();
}

function mainOverview() {
    viewer = new ViewerOverview();
}

function mainCpu() {
    viewer = new ViewerCpu();
}

function mainStorage() {
    viewer = new ViewerStorage();
}

function mainTenants() {
    viewer = new ViewerTenants();
}

function mainNetwork() {
    viewer = new ViewerNetwork();
}
