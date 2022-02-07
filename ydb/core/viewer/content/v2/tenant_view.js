function TenantView(options) {
    Object.assign(this, {
                      visibleRefreshTimeout: 1000,
                      overallRefreshTimeout: 30000,
                      tenants: {}
                  }, options);
    this.refreshViewInterval = setInterval(this.refreshView.bind(this), this.visibleRefreshTimeout);
    this.buildDomElement();
    this.refreshOverall();
}

TenantView.prototype.buildDomElement = function() {
    var tenantsContainer = $('<table>', {class: 'tenantlist tenant_container'});
    var tenantsHead = $('<thead>');
    var tenantsRow = $('<tr>');
    tenantsRow.append('<th style="text-align:left">Name</th>');
    tenantsRow.append('<th style="text-align:right">Compute</th>');
    tenantsRow.append('<th style="text-align:right">Storage</th>');
    tenantsRow.append('<th style="text-align:right">Nodes</th>');
    tenantsRow.append('<th>CPU</th>');
    tenantsRow.append('<th style="text-align:right">Tablets</th>');
    tenantsRow.append('<th style="text-align:right">CPU</th>');
    tenantsRow.append('<th style="text-align:right">Memory</th>');
    tenantsRow.append('<th style="text-align:right">Network</th>');
    tenantsRow.append('<th style="text-align:right">Storage</th>');
    tenantsHead.append(tenantsRow);
    var tenantsBody = $('<tbody>');
    tenantsContainer.append(tenantsHead);
    tenantsContainer.append(tenantsBody);
    this.domElementTenantsContainer = tenantsContainer[0];
    this.domElementTenantsBody = tenantsBody[0];
    this.domElement.appendChild(this.domElementTenantsContainer);
}

TenantView.prototype.onSysInfo = function(data) {
    if (data.SystemStateInfo !== undefined) {
        for (var i in data.SystemStateInfo) {
            var update = data.SystemStateInfo[i];
            var tenantName = this.getTenantName(update);
            //console.log('Tenant ' + tenantName);
            var tenant = this.tenants[tenantName];
            if (tenant) {
                tenant.updateSysInfo(update);
            } else {
                tenant = this.tenants[tenantName] = new Tenant();
                tenant.tenantView = this;
                tenant.updateSysInfo(update);
                this.domElementTenantsBody.appendChild(tenant.domElement);
            }
        }
        if (data.ResponseTime != 0) {
            this.sysInfoChangeTime = data.ResponseTime;
        }
    } else {
        if (!this.sysInfoChangeTime) {
            this.onDisconnect();
        }
    }
}

TenantView.prototype.onTenantInfo = function(data) {
    if (data.TenantInfo) {
        for (var i = 0; i < data.TenantInfo.length; ++i) {
            var update = data.TenantInfo[i];
            var tenantName = update.Name ? update.Name : update.ShardId + ':' + update.PathId;
            console.log('Tenant ' + tenantName);
            var tenant = this.tenants[tenantName];
            if (tenant) {
                tenant.updateTenantInfo(update);
            } else {
                tenant = this.tenants[tenantName] = new Tenant();
                tenant.updateTenantInfo(update);
                this.domElementTenantsBody.appendChild(tenant.domElement);
            }
        }
    }
}

TenantView.prototype.onDisconnect = function() {
// TODO
}

TenantView.prototype.refreshOverall = function() {
    this.lastRefreshOverall = getTime();
    var request = {
        alive: 0,
        enums: 1,
        timeout: 10000,
        since: this.sysInfoChangeTime
    };
    $.ajax({
               url: 'json/tenantinfo',
               data: request,
               success: this.onTenantInfo.bind(this),
               error: this.onDisconnect.bind(this)
           });
}

TenantView.prototype.isTimeToRefreshOverall = function() {
    return (!this.lastRefreshOverall || (this.lastRefreshOverall <= (getTime() - this.overallRefreshTimeout)));
}

TenantView.prototype.refreshView = function() {
    this.refreshViewTimer = null;
    if (this.isTimeToRefreshOverall()) {
        console.log('refreshOverall (tenants)');
        this.refreshOverall();
    } else {
        /*console.log('refreshView: ' + Object.keys(this.visibleNodes));
        for (var nodeId in this.visibleNodes) {
            var node = this.nodes[nodeId];
            if (!node.visible) {
                delete this.visibleNodes[nodeId];
            } else {
                this.refreshNode(node);
            }
        }*/
    }
}

TenantView.prototype.getConfig = function() {
    return this.config;
}

TenantView.prototype.getRootPath = function() {
    var config = this.getConfig();
    if (config) {
        return '/' + config.DomainsConfig.Domain[0].Name;
    }
}

TenantView.prototype.getTenantName = function(sysInfo) {
    if (sysInfo.Tenants !== undefined && sysInfo.Tenants.length > 0) {
        return sysInfo.Tenants.join(',');
    }
    return this.getRootPath();
}
