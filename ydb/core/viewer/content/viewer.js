'use strict';

var Nodes = {};
var NodesCount = 0;
var Groups = {};
var Tablets = {};
var TabletsCount = 0;
var newtablets = [];
var newtablets_running = false;
var tabletblock_class = "tabletblock";
var grey = "grey";
var green = "lightgreen";
var blue = "#6495ED"; // #4169E1 #4682B4
var yellow = "yellow";
var orange = "orange";
var red = "red";
var TabletRequest = { request: 0, alive: 1, enums: 1, filter: "(State!=Deleted)" };
var PDiskRequest = { request: 0, alive: 1, enums: 1 };
var VDiskRequest = { request: 0, alive: 1, enums: 1 };
var BSGroupRequest = { request: 0, alive: 1, enums: 1 };
var SysRequest = { request: 0, alive: 1, enums: 1 };
var NodeRequest = { request: 0, alive: 1, merge: 0 };
var VDisksDone = false;
var VDiskColors = {
    Initial: grey,
    LocalRecoveryError: red,
    SyncGuidRecoveryError: red,
    SyncGuidRecovery: yellow,
    OK: green
};
var DefaultRefreshTimeout = 500;
var refreshTimeoutBSGroup = function() { return DefaultRefreshTimeout + 1200; }
var refreshTimeoutPDisk = function() { return DefaultRefreshTimeout + 1500; }
var refreshTimeoutVDisk = function() { return DefaultRefreshTimeout + 2300; }
var refreshTimeoutNode = function() { return DefaultRefreshTimeout + 500; }
var refreshTimeoutTablet = function() { return DefaultRefreshTimeout + 700; }
var refreshTimeoutSys = function() { return DefaultRefreshTimeout + 800; }
var refreshTimeoutHive = function() { return DefaultRefreshTimeout + 2100; }
var StatsAnimateStop = {opacity: 1};
var StatsAnimateStart = {opacity: 0.5};
var StatsAnimateDuration = 10000;
var Parameters = {};
var SchemaTabletElements = {};
var VDiskBlockWidthClass = "vdiskblock-wide";
var MaxWideVDisks = 4;

function getPDiskCell(node, pDiskId, pDiskData) {
    if (node.PDisks === undefined) {
        node.PDisks = {};
    }
    var pDisk = node.PDisks[pDiskId];
    if (pDisk === undefined) {
        pDisk = node.PDisks[pDiskId] = {};
    }

    var pDiskCell = pDisk.CellDomElement;
    if (pDiskCell === undefined) {
        if (pDiskData === undefined)
            return null;
        pDiskCell = document.createElement("td");
        pDiskCell.innerHTML = "<div class='diskblock'><table style='width:100%'><tr class='vdiskspart'></tr><tr><td class='pdiskspart' style='padding: 2px 1px 1px 1px' colspan='99'></td></tr></table></div>";
        pDiskCell.PDiskId = pDiskId;
        var path = pDiskData.Path;
        pDiskCell.PDiskPath = path;

        var insertPoint = node.DomElement.lastChild;
        while (insertPoint !== null) {
            if (insertPoint.PDiskId === undefined || path >= insertPoint.PDiskPath) {
                break;
            }
            insertPoint = insertPoint.previousSibling;
        }
        insertPoint = insertPoint.nextSibling;
        node.DomElement.insertBefore(pDiskCell, insertPoint);
        pDisk.CellDomElement = pDiskCell;
    }
    return pDiskCell;
}

function getPDiskPart(node, pDiskId, pDiskData) {
    var pDiskPart = node.PDisks[pDiskId].DomElement;
    if (pDiskPart === undefined) {
        var pDiskCell = getPDiskCell(node, pDiskId, pDiskData);
        //pDiskPart = $(pDiskCell).find("td.pdiskspart").get(0);
        pDiskPart = pDiskCell.firstChild.firstChild.firstChild.firstChild.nextSibling.firstChild;
        node.PDisks[pDiskId].DomElement = pDiskPart;
    }
    return pDiskPart;
}

function initPDiskElement(parentDomElement, pDisk, pDiskId) {
    var childNodes = parentDomElement.childNodes;
    var pDiskElement = undefined;
    for (var i = 0; i < childNodes.length; i++) {
        if (childNodes[i].PDiskId === pDiskId) {
            pDiskElement = childNodes[i];
            break;
        }
    }

    if (pDiskElement === undefined && pDisk.DomElement !== undefined) {
        pDiskElement = document.createElement("div");
        pDiskElement.PDiskId = pDiskId;
        pDiskElement.style.padding = "0px 4px 0px 4px";
        pDiskElement.style.float = "left";
        pDiskElement.style.width = "80px";
        var pDiskChild = pDisk.DomElement.firstChild.cloneNode(true);
        pDiskChild.style.minWidth = "30px";
        pDiskChild.title = getNodeHost(pDisk.NodeId) + " " + pDiskChild.title;
        pDiskElement.appendChild(pDiskChild);
        if (pDisk.ClonedElements === undefined) {
            pDisk.ClonedElements = [];
        }
        pDisk.ClonedElements.push(pDiskChild);
        pDiskElement.PDisk = pDisk;
        pDiskElement.addEventListener("click", onPDiskClick, false);
        var insertPoint = parentDomElement.lastChild;
        while (insertPoint !== null) {
            if (insertPoint.PDiskId === undefined || pDiskId >= insertPoint.PDiskId) {
                break;
            }
            insertPoint = insertPoint.previousSibling;
        }
        if (insertPoint !== null) {
            insertPoint = insertPoint.nextSibling;
        }
        parentDomElement.insertBefore(pDiskElement, insertPoint);
    }
}

function getVDiskPart(nodeId, pDiskId, vDiskId) {
    var node = Nodes[nodeId];
    if (node.VDisks === undefined) {
        node.VDisks = {};
    }
    var vDisk = node.VDisks[vDiskId];
    if (vDisk === undefined) {
        vDisk = node.VDisks[vDiskId] = {};
    }
    var vDiskPart = node.VDisks[vDiskId].DomElement;
    if (vDiskPart === undefined) {
        var pDiskCell = getPDiskCell(node, pDiskId);
        if (pDiskCell === null)
            return null;
        var vDiskRow = pDiskCell.firstChild.firstChild.firstChild.firstChild;
        if (vDiskRow.children.length > MaxWideVDisks && VDiskBlockWidthClass !== "vdiskblock-narrow") {
            VDiskBlockWidthClass = "vdiskblock-narrow";
            $(".vdiskblock-wide").removeClass("vdiskblock-wide").addClass("vdiskblock-narrow");
        }
        vDiskPart = vDiskRow[vDiskId];
        if (vDiskPart === undefined) {
            vDiskPart = document.createElement("td");
            vDiskPart.style.textAlign = 'center';
            vDiskPart.style.fontSize = '12px';
            vDiskPart.style.padding = "1px 1px";
            vDiskRow[vDiskId] = vDiskPart;
            var insertPoint = vDiskRow.lastChild;
            while (insertPoint !== null) {
                if (insertPoint.VDiskId === undefined || vDiskId >= insertPoint.VDiskId) {
                    break;
                }
                insertPoint = insertPoint.previousSibling;
            }
            if (insertPoint !== null)
                insertPoint = insertPoint.nextSibling;
            vDiskRow.insertBefore(vDiskPart, insertPoint);
        }
        node.VDisks[vDiskId].DomElement = vDiskPart;
    }
    return vDiskPart;
}

function getVDiskId(nodeId, vDiskId, pDiskId, vSlotId) {
    return nodeId + "-" + pDiskId + "-" + vSlotId;
}

function getErasureInfo(erasure) {
    switch (erasure) {
    case 0:            // min, total
    case 'none':
        return {Name: "None", Min: 1, Total: 1};
    case 1:
    case 'mirror-3':
        return {Name: "Mirror 3+1", Min: 3, Total: 4};
    case 2:
    case 'block-3-1':
        return {Name: "Block 3+1", Min: 4, Total: 5};
    case 3:
    case 'stripe-3-1':
        return {Name: "Stripe 3+1", Min: 4, Total: 5};
    case 4:
    case 'block-4-2':
        return {Name: "Block 4+2", Min: 6, Total: 8};
    case 5:
    case 'block-3-2':
        return {Name: "Block 3+2", Min: 5, Total: 7};
    case 6:
    case 'stripe-4-2':
        return {Name: "Stripe 4+2", Min: 6, Total: 8};
    case 7:
    case 'stripe-3-2':
        return {Name: "Stripe 3+2", Min: 5, Total: 7};
    case 8:
    case 'mirror-3-2':
        return {Name: "Mirror 3+2", Min: 3, Total: 5};
    case 9:
    case 'mirror-3-dc':
        return {Name: "Mirror 3 DC", Min: 3, Total: 5};
    default:
        return {Name: "Unknown", Min: 1, Total: 1};
    }
}

function getErasureText(erasure) {
    return getErasureInfo(erasure).Name;
}

function getGroupInfo(groupId, erasureSpecies) {
    var group = Groups[groupId];
    if (group === undefined) {
        group = Groups[groupId] = {};
        group.GroupID = groupId;
        group.VDisks = {};
        if (erasureSpecies !== undefined) {
            group.ErasureSpecies = erasureSpecies;
        }
    }
    return group;
}

function getVDiskIdKey(vDiskId) {
    return vDiskId.Ring + "-" + vDiskId.Domain + "-" + vDiskId.VDisk;
}

function getGroupVDiskInfo(vDisk) {
    var vDiskId = vDisk.VDiskId;
    var group = getGroupInfo(vDisk.VDiskId.GroupID);
    var vDiskInfo = group.VDisks[getVDiskIdKey(vDiskId)];
    if (vDiskInfo === undefined) {
        vDiskInfo = group.VDisks[getVDiskIdKey(vDiskId)] = {};
    }
    return vDiskInfo;
}

function setGroupVDiskInfo(vDisk) {
    var group = getGroupInfo(vDisk.VDiskId.GroupID, vDisk.ErasureSpecies);
    group.VDisks[getVDiskIdKey(vDisk.VDiskId)] = vDisk;
}

function getNodeHost(nodeId) {
    var node = Nodes[nodeId];
    if (node !== undefined) {
        if (node.Host !== undefined) {
            return node.Host;
        } else {
            return node.Address;
        }
    } else {
        return nodeId;
    }
}

function getNodeHostWithPort(nodeId, type, def) {
    var node = Nodes[nodeId];
    if (node !== undefined) {
        var host;
        if (node.Host !== undefined) {
            host = node.Host;
        } else {
            host = node.Address;
        }
        var address = undefined;
        if (node.SysInfo && node.SysInfo.Endpoints) {
            var endpoint = node.SysInfo.Endpoints.find(function (item) { return item.Name === type; });
            if (endpoint !== undefined) {
                address = endpoint.Address;
            }
        }
        if (address === undefined) {
            address = def;
        }
        if (address === undefined) {
            address =  ':8765';
        }
        return host + address;
    } else {
        return undefined;
    }
}

function getBaseUrl(nodeId) {
    if (window.location.hostname === 'viewer.ydb.yandex-team.ru') {
        var nodeHost = getNodeHostWithPort(nodeId, 'http-mon', ':8765');
        return window.location.protocol + '//viewer.ydb.yandex-team.ru/' + nodeHost
    } else if (window.location.hostname.indexOf('.bastion.') !== -1){
        var host = getNodeHost(nodeId);
        host = host.replace('cloud.yandex.net', 'ydb.bastion.cloud.yandex-team.ru');
        host = host.replace('cloud-preprod.yandex.net', 'ydb.bastion.cloud.yandex-team.ru');
        host = host.replace('cloud-df.yandex.net', 'ydb.bastion.cloud.yandex-team.ru');
        return window.location.protocol + "//" + host;
    } else {
        return window.location.protocol + "//" + getNodeHost(nodeId) + ":" + window.location.port;
    }
}

function getCliMbUrl(url) {
    if (window.location.hostname === "kikimr.viewer.yandex-team.ru") {
        return "/" + window.location.pathname.split('/')[1] + "/CLI_MB" + url;
    } else {
        return "/CLI_MB" + url;
    }
}

function getVDiskUrl(vDisk) {
    return getBaseUrl(vDisk.NodeId) + "/actors/vdisks/vdisk" + pad9(vDisk.PDiskId) + "_" + pad9(vDisk.VDiskSlotId);
}

function getPDiskUrl(pDisk) {
    return getBaseUrl(pDisk.NodeId) + "/actors/pdisks/pdisk" + pad9(pDisk.PDiskId);
}

function getViewerUrl(nodeId) {
    return getBaseUrl(nodeId) + "/viewer/#nodes";
}

function getVDiskColor(state) {
    var color = VDiskColors[state];
    if (color === undefined) {
        color = grey;
    }
    return color;
}

function onVDiskClick() {
    window.open(getVDiskUrl(this.VDisk));
}

function onVDiskEnter() {
    var groupId = this.VDisk.VDiskId.GroupID;
    var group = Groups[groupId];
    if (group !== undefined) {
        for (var vDiskId in group.VDisks) {
            var vDisk = group.VDisks[vDiskId];
            $(vDisk.DomElement).addClass('vdiskblock-selected');
        }
    }
}

function onVDiskLeave() {
    var groupId = this.VDisk.VDiskId.GroupID;
    var group = Groups[groupId];
    if (group !== undefined) {
        for (var vDiskId in group.VDisks) {
            var vDisk = group.VDisks[vDiskId];
            $(vDisk.DomElement).removeClass('vdiskblock-selected');
        }
    }
}

function setVDiskBlock(cell, vDisk) {
    var state = " ";
    var color = getVDiskColor(vDisk.VDiskState);
    state += vDisk.VDiskState;
    var rank = vDisk.SatisfactionRank;
    if (!vDisk.Replicated) {
        state += " !Replicated";
        if (color === green) {
            color = blue;
        }
    }
    if (rank !== undefined) {
        if (rank.FreshRank.Flag === "Yellow" || rank.LevelRank.Flag === "Yellow" || vDisk.DiskSpace === "Yellow") {
            if (rank.FreshRank.Flag === "Yellow")
                state += " Fresh:yellow";
            if (rank.LevelRank.Flag === "Yellow")
                state += " Level:yellow";
            if (vDisk.DiskSpace === "Yellow")
                state += " Space:yellow";
            color = yellow;
        }
        if (rank.FreshRank.Flag === "Orange" || rank.LevelRank.Flag === "Orange" || vDisk.DiskSpace === "Orange") {
            if (rank.FreshRank.Flag === "Orange")
                state += " Fresh:orange";
            if (rank.LevelRank.Flag === "Orange")
                state += " Level:orange";
            if (vDisk.DiskSpace === "Orange")
                state += " Space:orange";
            color = orange;
        }
        if (rank.FreshRank.Flag === "Red" || rank.LevelRank.Flag === "Red" || vDisk.DiskSpace === "Red") {
            if (rank.FreshRank.Flag === "Red")
                state += " Fresh:red";
            if (rank.LevelRank.Flag === "Red")
                state += " Level:red";
            if (vDisk.DiskSpace === "Red")
                state += " Space:red";
            color = red;
        }
        if (rank.FreshRank.RankPercent !== undefined) {
            state += " Fresh:" + rank.FreshRank.RankPercent + "%";
        }
        if (rank.LevelRank.RankPercent !== undefined) {
            state += " Level:" + rank.LevelRank.RankPercent + "%";
        }
    }
    if (vDisk.UnsyncedVDisks > 0) {
        state += " UnsyncVDisks:" + vDisk.UnsyncedVDisks;
    }
    var div = cell.firstChild;
    if (div === null) {
        div = document.createElement("div");
        div.className = "vdiskblock " + VDiskBlockWidthClass;
        cell.appendChild(div);
        var vdisk = getGroupVDiskInfo(vDisk);
        vdisk.DomElement = div;
        div.addEventListener("click", onVDiskClick, false);
        div.addEventListener("mouseover", onVDiskEnter, false);
        div.addEventListener("mouseout", onVDiskLeave, false);
    }
    vDisk.Color = color;
    div.style.backgroundColor = color;
    div.VDisk = vDisk;
    var vDiskTextId = getVDiskId(vDisk.NodeId, vDisk.VDiskId, vDisk.PDiskId, vDisk.VDiskSlotId);
    div.title = vDiskTextId + state;
    var vDiskIdKey = getVDiskIdKey(vDisk.VDiskId);

    var group = getGroupInfo(vDisk.VDiskId.GroupID);
    var groupVDisk = group.VDisks[vDiskIdKey];
    var vDiskGroupElement = groupVDisk.GroupDomElement;
    if (vDiskGroupElement !== undefined) {
        var vDiskGroupElementNew = div.cloneNode(true);
        vDiskGroupElementNew.VDisk = vDisk;
        vDiskGroupElementNew.addEventListener("click", onVDiskClick, false);
        group.DomElement.replaceChild(vDiskGroupElementNew, vDiskGroupElement);
        groupVDisk.GroupDomElement = vDiskGroupElementNew;
    }

    return color;
}

function onVDiskInfo(vDisksInfo) {
    var vDiskStateInfo = null;
    var refreshTimeout = refreshTimeoutVDisk();
    if (vDisksInfo !== undefined && vDisksInfo !== null && typeof vDisksInfo === "object") {
        if (vDisksInfo.VDiskStateInfo !== undefined) {
            vDiskStateInfo = vDisksInfo.VDiskStateInfo;
            if (vDiskStateInfo !== null) {
                if (VDiskRequest.since === undefined && !VDisksDone) {
                    refreshBSGroupInfo();
                    VDisksDone = true;
                }
                for (var i in vDiskStateInfo) {
                    var vDisk = vDiskStateInfo[i];
                    if (vDisk.VDiskId === undefined || vDisk.PDiskId === undefined) {
                        continue;
                    }

                    var vDiskId = getVDiskId(vDisk.NodeId, vDisk.VDiskId, vDisk.PDiskId, vDisk.VDiskSlotId);
                    var vDiskCell = getVDiskPart(vDisk.NodeId, vDisk.PDiskId, vDiskId);
                    if (vDiskCell === null)
                        continue;
                    var vDiskInfo = getGroupVDiskInfo(vDisk);
                    for (var prop in vDisk) {
                        vDiskInfo[prop] = vDisk[prop];
                    }
                    setVDiskBlock(vDiskCell, vDiskInfo);
                }
                if (++VDiskRequest.request > 60) {
                    VDiskRequest.request = 0;
                    delete VDiskRequest.since;
                } else {
                    VDiskRequest.since = vDisksInfo.ResponseTime;
                }
            }
        }
    } else {
        VDiskRequest.request = 0;
        delete VDiskRequest.since;
    }
    setTimeout(refreshVDiskInfo, refreshTimeout);
    refreshVDiskStats();
}

function getSizesProgressBarHtml(used, total, width) {
    var html = "<div class='progress' style='margin-bottom:1px";
    if (width === undefined)
        width = "200px";
    html += ";width:" + width;
    html += "'><div class='progress-bar ";
    var percent = total > 0 ? ((used * 100 / total).toPrecision(3)) : 0;
    if (percent >= 90) {
        html += "progress-bar-danger";
    } else
    if (percent >= 75) {
        html += "progress-bar-warning";
    } else {
        html += "progress-bar-success";
    }
    html += "' role='progressbar' aria-valuenow='";
    html += percent;
    html += "' aria-valuemin='0' aria-valuemax='100' style='width:" + percent + "%;font-size:10px'><span>" + percent + "%</span></div></div>";
    return html;
}

function asNumber(num) {
    return (num === undefined || num === null) ? 0 : Number(num);
}

function getLatencyText(latency) {
    if (latency === undefined) {
        return "";
    }
    switch (latency) {
    case 'Grey': return "";
    case 'Green': return "<500ms";
    case 'Yellow': return "500..1000ms";
    case 'Orange': return "1000..2000ms";
    case 'Red': return ">2000ms";
    }
    return "";
}

function getLatencyColor(latency) {
    switch (latency) {
    case 'Grey': return grey;
    case 'Green': return green;
    case 'Yellow': return yellow;
    case 'Orange': return orange;
    case 'Red': return red;
    }
    return "black";
}

function updateBytesValue(element, bytes, delta) {
    if (delta !== undefined) {
        var deltaText = "";
        if (element.ValueLast !== undefined) {
            var diff = bytes - element.ValueLast;
            element.ValueLast = bytes;
            element.ValueAccum += diff;
            element.ValueCount++;
            var avgDiff = element.ValueAccum / element.ValueCount;
            if (avgDiff !== 0) {
                if (avgDiff < 0) {
                    deltaText = "<span style=''>-" +bytesToSize(-avgDiff) + "</span>";
                } else {
                    deltaText = "<span style=''>+" +bytesToSize(avgDiff) + "</span>";
                }
            }
            if (element.ValueCount >= 20) {
                element.ValueAccum /= 2;
                element.ValueCount /= 2;
            }
        } else {
            element.ValueOriginal = bytes;
            element.ValueLast = bytes;
            element.ValueAccum = 0;
            element.ValueCount = 0;
        }
        delta.innerHTML = deltaText;
    }
    element.innerHTML = bytesToSize(bytes);
}

function onBSGroupInfo(bsGroupInfo) {
    var bsGroupStateInfo = null;
    var refreshTimeout = refreshTimeoutBSGroup();
    if (bsGroupInfo !== undefined && bsGroupInfo !== null && typeof bsGroupInfo === "object") {
        if (bsGroupInfo.BSGroupStateInfo !== undefined) {
            bsGroupStateInfo = bsGroupInfo.BSGroupStateInfo;
            if (bsGroupStateInfo !== null) {
                for (var i in bsGroupStateInfo) {
                    var newGroup = bsGroupStateInfo[i];
                    var group = getGroupInfo(newGroup.GroupID, newGroup.ErasureSpecies);

                    for (var prop in newGroup) {
                        group[prop] = newGroup[prop];
                    }
                }
            }
        }
        if (++BSGroupRequest.request > 6) {
            BSGroupRequest.request = 0;
            delete BSGroupRequest.since;
        } else {
            BSGroupRequest.since = bsGroupInfo.ResponseTime;
        }
    } else {
        $("#storage").find(".grouplist").find("tr:has(> td)").remove();
        Groups = {};
        BSGroupRequest.request = 0;
        delete BSGroupRequest.since;
    }

    var groupsView = $("#storage").find(".grouplist").get(0);
    var staticTotal = 0;
    var staticUsed = 0;
    var staticAvailable = 0;
    var staticPDisks = {};
    var dynamicTotal = 0;
    var dynamicUsed = 0;
    var dynamicAvailable = 0;
    var dynamicPDisks = {};
    var totalTotal = 0;
    var totalUsed = 0;
    var totalAvailable = 0;
    var totalPDisks = {};
    for (var groupId in Groups) {
        var group = Groups[groupId];
        if (group.ErasureSpecies === undefined)
            continue;
        var groupLatency;
        var groupUsage;
        var groupDelta;
        var groupUsed;
        var groupAvailable;
        var groupTotal;
        var groupPDisks;

        if (group.DomElement === undefined) {
            var groupRow = groupsView.insertRow();
            var groupIdElement = groupRow.insertCell(-1);
            groupIdElement.style.textAlign = "right";
            groupIdElement.innerHTML = group.GroupID;
            var groupTypeElement = groupRow.insertCell(-1);
            groupTypeElement.innerHTML = group.GroupID & 0x80000000 ? "Dynamic" : "Static";
            var groupErasureElement = groupRow.insertCell(-1);
            groupErasureElement.innerHTML = getErasureText(group.ErasureSpecies);
            groupLatency = document.createElement("div");
            groupLatency.className = "latency-block";
            groupRow.insertCell(-1).appendChild(groupLatency);
            var groupCell = groupRow.insertCell(-1);
            var groupElement = document.createElement("div");
            groupUsage = document.createElement("div");
            groupRow.insertCell(-1).appendChild(groupUsage);
            groupDelta = document.createElement("div");
            groupDelta.style.textAlign = "right";
            groupRow.insertCell(-1).appendChild(groupDelta);
            groupUsed = document.createElement("div");
            groupUsed.style.textAlign = "right";
            groupRow.insertCell(-1).appendChild(groupUsed);
            groupAvailable = document.createElement("div");
            groupAvailable.style.textAlign = "right";
            groupRow.insertCell(-1).appendChild(groupAvailable);
            groupTotal = document.createElement("div");
            groupTotal.style.textAlign = "right";
            groupRow.insertCell(-1).appendChild(groupTotal);
            groupPDisks = document.createElement("div");
            groupRow.insertCell(-1).appendChild(groupPDisks);
            groupElement.className = "groupblock";
            groupCell.appendChild(groupElement);
            group.DomElement = groupElement;
        } else {
            groupLatency = group.DomElement.parentNode.previousSibling.firstChild;
            groupUsage = group.DomElement.parentNode.nextSibling.firstChild;
            groupDelta = groupUsage.parentNode.nextSibling.firstChild;
            groupUsed = groupDelta.parentNode.nextSibling.firstChild;
            groupAvailable = groupUsed.parentNode.nextSibling.firstChild;
            groupTotal = groupAvailable.parentNode.nextSibling.firstChild;
            groupPDisks = groupTotal.parentNode.nextSibling.firstChild;
        }

        groupLatency.innerHTML = getLatencyText(group.Latency);
        groupLatency.style.backgroundColor = getLatencyColor(group.Latency);

        var total = 0;
        var used = 0;
        var available = 0;
        var groupPDisksMask = {};

        for (var vDiskIdIdx in group.VDiskIds) {
            var vDiskId = group.VDiskIds[vDiskIdIdx];
            var vDiskIdKey = getVDiskIdKey(vDiskId);
            var groupVDisk = group.VDisks[vDiskIdKey];
            if (groupVDisk === undefined) {
                groupVDisk = group.VDisks[vDiskIdKey] = {};
            }
            var vDiskGroupElement = groupVDisk.GroupDomElement;
            if (vDiskGroupElement === undefined) {
                var vDiskElement = groupVDisk.DomElement;
                if (vDiskElement !== undefined) {
                    vDiskGroupElement = vDiskElement.cloneNode(true);
                    vDiskGroupElement.VDisk = groupVDisk;
                    vDiskGroupElement.addEventListener("click", onVDiskClick, false);
                } else {
                    vDiskGroupElement = document.createElement("div");
                    vDiskGroupElement.className = "vdiskblock " + VDiskBlockWidthClass;
                    vDiskGroupElement.style.backgroundColor = "grey";
                }
                group.DomElement.appendChild(vDiskGroupElement);
                groupVDisk.GroupDomElement = vDiskGroupElement;
            }
            used += asNumber(groupVDisk.AllocatedSize);
            if (groupVDisk.NodeId !== undefined && groupVDisk.PDiskId !== undefined) {
                var node = Nodes[groupVDisk.NodeId];
                if (node.PDisks !== undefined) {
                    var pDisk = node.PDisks[groupVDisk.PDiskId];
                    if (pDisk !== undefined) {
                        var key = String(groupVDisk.NodeId) + "-" + String(groupVDisk.PDiskId);
                        initPDiskElement(groupPDisks, pDisk, key);
                        if (pDisk.AvailableSize !== undefined && pDisk.TotalSize !== undefined) {
                            if (groupPDisksMask[key] === undefined) {
                                total += asNumber(pDisk.TotalSize);
                                available += asNumber(pDisk.AvailableSize);
                            }
                            groupPDisksMask[key] = true;
                            if (totalPDisks[key] === undefined) {
                                totalTotal += asNumber(pDisk.TotalSize);
                                totalAvailable += asNumber(pDisk.AvailableSize);
                                totalPDisks[key] = true;
                            }
                            if (group.GroupID & 0x80000000) {
                                if (dynamicPDisks[key] === undefined) {
                                    dynamicTotal += asNumber(pDisk.TotalSize);
                                    dynamicAvailable += asNumber(pDisk.AvailableSize);
                                    dynamicPDisks[key] = true;
                                }
                            } else {
                                if (staticPDisks[key] === undefined) {
                                    staticTotal += asNumber(pDisk.TotalSize);
                                    staticAvailable += asNumber(pDisk.AvailableSize);
                                    staticPDisks[key] = true;
                                }
                            }
                        }
                    }
                }
            }
        }
        totalUsed += used;
        if (group.GroupID & 0x80000000) {
            dynamicUsed += used;
        } else {
            staticUsed += used;
        }

        if (total !== 0) {
            groupUsage.innerHTML = getSizesProgressBarHtml(used, used + available);
            updateBytesValue(groupUsed, used, groupDelta);
            updateBytesValue(groupAvailable, available);
            updateBytesValue(groupTotal, total);
        }
    }
    var totalStorageElement = $("#storage").find(".totalstorage").get(0);
    var totalElement = totalStorageElement.firstElementChild.nextElementSibling.firstElementChild.firstElementChild.nextElementSibling;
    var dynamicElement = totalElement.parentNode.nextElementSibling.firstElementChild.nextElementSibling;
    var staticElement = dynamicElement.parentNode.nextElementSibling.firstElementChild.nextElementSibling;

    totalElement.innerHTML = getSizesProgressBarHtml(totalUsed, totalTotal);
    totalElement = totalElement.nextElementSibling
    totalElement.innerHTML = bytesToSize(totalUsed);
    totalElement = totalElement.nextElementSibling
    totalElement.innerHTML = bytesToSize(totalAvailable);
    totalElement = totalElement.nextElementSibling
    totalElement.innerHTML = bytesToSize(totalTotal);
    dynamicElement.innerHTML = getSizesProgressBarHtml(dynamicUsed, dynamicTotal);
    dynamicElement = dynamicElement.nextElementSibling;
    dynamicElement.innerHTML = bytesToSize(dynamicUsed);
    dynamicElement = dynamicElement.nextElementSibling;
    dynamicElement.innerHTML = bytesToSize(dynamicAvailable);
    dynamicElement = dynamicElement.nextElementSibling;
    dynamicElement.innerHTML = bytesToSize(dynamicTotal);
    staticElement.innerHTML = getSizesProgressBarHtml(staticUsed, staticTotal);
    staticElement = staticElement.nextElementSibling;
    staticElement.innerHTML = bytesToSize(staticUsed);
    staticElement = staticElement.nextElementSibling;
    staticElement.innerHTML = bytesToSize(staticAvailable);
    staticElement = staticElement.nextElementSibling;
    staticElement.innerHTML = bytesToSize(staticTotal);

    setTimeout(refreshBSGroupInfo, refreshTimeout);
    refreshBSGroupStats();
}

function onPDiskClick() {
    window.open(getPDiskUrl(this.PDisk));
}

function getPDiskIcon(icon, color) {
    return "<span class='glyphicon glyphicon-" + icon + "' style='color:" + color + ";text-shadow: 0 0 1px black;margin-right:2px;'></span>"
}

function getPDiskDeviceIcon(color) {
    return getPDiskIcon("save", color);
}

function getPDiskRealtimeIcon(color) {
    return getPDiskIcon("fire", color);
}

function onPDiskInfo(pDisksInfo) {
    var pDiskStateInfo = null;
    var refreshTimeout = refreshTimeoutPDisk();
    if (pDisksInfo !== undefined && pDisksInfo !== null && typeof pDisksInfo === "object") {
        if (pDisksInfo.PDiskStateInfo !== undefined) {
            pDiskStateInfo = pDisksInfo.PDiskStateInfo;
        }
        if (pDiskStateInfo !== null) {
            for (var idx in pDiskStateInfo) {
                var pDiskInfo = pDiskStateInfo[idx];
                var node = Nodes[pDiskInfo.NodeId];
                if (node.PDisks === undefined) {
                    node.PDisks = {};
                }
                var pDisk = node.PDisks[pDiskInfo.PDiskId];
                if (pDisk === undefined) {
                    pDisk = node.PDisks[pDiskInfo.PDiskId] = pDiskInfo;
                } else {
                    for (var j in pDiskInfo) {
                        pDisk[j] = pDiskInfo[j];
                    }
                }
                var pDiskBlock = pDisk.BlockDomElement;
                if (pDiskBlock === undefined) {
                    var pDiskCell = getPDiskPart(node, pDisk.PDiskId, pDisk);
                    pDiskBlock = pDiskCell.firstChild;
                    if (pDiskBlock === null) {
                        pDiskBlock = document.createElement("div");
                        pDiskBlock.className = 'progress pdiskblock';
                        pDiskBlock.PDisk = pDisk;
                        pDiskBlock.addEventListener("click", onPDiskClick, false);
                        pDiskBlock.style.position = 'relative';
                        pDiskCell.appendChild(pDiskBlock);
                        pDisk.BlockDomElement = pDiskBlock;
                    }
                }

                var state = "";
                if (pDisk.State === 'Normal') {
                    pDiskBlock.style.backgroundColor = "lightblue";
                    state = "Normal";
                    pDisk.Color = green;
                    var total = pDisk.TotalSize;
                    var avail = pDisk.AvailableSize;
                    if (total !== undefined && avail !== undefined) {
                        var percent = ((total - avail) * 100 / total).toPrecision(3);
                        var html = "<div class='progress-bar ";
                        if (percent >= 90) {
                            html += "progress-bar-danger";
                            pDisk.Color = orange;
                        } else
                        if (percent >= 75) {
                            html += "progress-bar-warning";
                            pDisk.Color = yellow;
                        } else {
                            html += "progress-bar-success";
                        }

                        html += "' role='progressbar' aria-valuenow='";
                        html += percent;
                        html += "' aria-valuemin='0' aria-valuemax='100' style='width:" + percent + "%;font-size:10px;vertical-align:middle;padding-left:3px'>" + percent
                                + "%</div><div style='position:absolute;top:1px;right:1px;font-size:12px'>";

                        state += " (Available " + bytesToSize(avail) + " of " + bytesToSize(total) + ")";

                        if (pDisk.Realtime !== undefined) {
                            switch (pDisk.Realtime) {
                            case 'Yellow':
                                if (pDisk.Color !== orange) {
                                    pDisk.Color = yellow;
                                }
                                html += getPDiskRealtimeIcon("yellow");
                                state += " Realtime:yellow";
                                break;
                            case 'Orange':
                                if (pDisk.Color !== red) {
                                    pDisk.Color = orange;
                                }
                                html += getPDiskRealtimeIcon("orange");
                                state += " Realtime:orange";
                                break;
                            case 'Red':
                                pDisk.Color = red;
                                html += getPDiskRealtimeIcon("red");
                                state += " Realtime:red";
                                break;
                            }
                        }

                        if (pDisk.Device !== undefined) {
                            switch (pDisk.Device) {
                            case 'Yellow':
                                if (pDisk.Color !== orange && pDisk.Color !== red) {
                                    pDisk.Color = yellow;
                                }
                                html += getPDiskDeviceIcon("yellow");
                                state += " Device:yellow";
                                break;
                            case 'Orange':
                                pDisk.Color = orange;
                                html += getPDiskDeviceIcon("orange");
                                state += " Device:orange";
                                break;
                            case 'Red':
                                pDisk.Color = red;
                                html += getPDiskDeviceIcon("red");
                                state += " Device:red";
                                break;
                            }
                        }

                        html += "</div>";
                        pDiskBlock.innerHTML = html;
                        if (pDisk.ClonedElements !== undefined) {
                            for (var i = 0; i < pDisk.ClonedElements.length; i++) {
                                pDisk.ClonedElements[i].innerHTML = html;
                            }
                        }
                    }
                } else {
                    switch (pDisk.State) {
                    case 'Initial':
                        pDiskBlock.style.backgroundColor = grey;
                        pDisk.Color = grey;
                        state = pDisk.State;
                        break;
                    case 'Normal':
                        pDisk.Color = green;
                        // wtf?
                        break;
                    case 'InitialFormatRead':
                    case 'InitialSysLogRead':
                    case 'InitialCommonLogRead':
                        pDiskBlock.style.backgroundColor = yellow;
                        pDisk.Color = yellow;
                        state = pDisk.State;
                        break;
                    case 'InitialFormatReadError':
                    case 'InitialSysLogReadError':
                    case 'InitialSysLogParseError':
                    case 'InitialCommonLogReadError':
                    case 'InitialCommonLogParseError':
                    case 'CommonLoggerInitError':
                    case 'OpenFileError':
                    case 'Stopped':
                        pDiskBlock.style.backgroundColor = red;
                        pDisk.Color = red;
                        state = pDisk.State;
                        break;
                    }
                }
                pDiskBlock.title = pDisk.Path + " " + state;
                if (pDisk.ClonedElements !== undefined) {
                    var title = getNodeHost(pDiskInfo.NodeId) + " " + pDisk.Path + " " + state;
                    for (var i = 0; i < pDisk.ClonedElements.length; i++) {
                        pDisk.ClonedElements[i].title = title;
                    }
                }
            }
        }
        if (++PDiskRequest.request > 60) {
            PDiskRequest.request = 0;
            delete PDiskRequest.since;
        } else {
            PDiskRequest.since = pDisksInfo.ResponseTime;
        }
    } else {
        PDiskRequest.request = 0;
        delete PDiskRequest.since;
    }
    setTimeout(refreshPDiskInfo, refreshTimeout);
    refreshPDiskStats();
}

function pad2(val) {
    if (val < 10) {
        return "0" + val;
    } else {
        return val;
    }
}

function pad4(val) {
    var len = String(val).length;
    for (var i = len; i < 4; i++) {
        val = "0" + val;
    }
    return val;
}

function pad9(val) {
    var len = String(val).length;
    for (var i = len; i < 9; i++) {
        val = "0" + val;
    }
    return val;
}

function upTimeToString(upTime) {
    var seconds = Math.floor(upTime / 1000);
    if (seconds < 60) {
        return seconds + "s";
    } else {
        var minutes = Math.floor(seconds / 60);
        seconds = seconds % 60;
        if (minutes < 60) {
            return minutes + ":" + pad2(seconds);
        } else {
            var hours = Math.floor(minutes / 60);
            minutes = minutes % 60;
            if (hours < 24) {
                return hours + ":" + pad2(minutes) + ":" + pad2(seconds);
            } else {
                var days = Math.floor(hours / 24);
                hours = hours % 24;
                return days + "d " + pad2(hours) + ":" + pad2(minutes) + ":" + pad2(seconds);
            }
        }
    }
}

function onDisconnectNode(node) {
    var cell = node.DomElement.cells[2];
    cell.innerHTML = "<span class='glyphicon glyphicon-eye-close'></span>";
    var cells = node.DomElement.cells.length;
    for (var i = cells - 1; i >= 5; i--) {
        node.DomElement.deleteCell(i);
    }
    for (var id in Nodes) {
        if (node.NodeStateInfo !== undefined && node.NodeStateInfo[id] !== undefined) {
            delete node.NodeStateInfo[id].Connected;
        }
        setInterconnectMap(node, id, "lightgrey");
        setPoolsMap(node, []);
    }
    var block = node.DomElement.cells[1].firstChild.nextSibling.firstChild;
    block.style.backgroundColor = "lightgrey";
    block = block.nextSibling;
    block.style.backgroundColor = "lightgrey";
    block = block.nextSibling;
    var progress = block.firstChild;
    progress.style.backgroundColor = "lightgrey";
    progress.style.width = '0%';
    delete node.SysInfo;
    delete node.PDisks;
    delete node.VDisks;
}

function flagToColor(flag) {
    switch (flag) {
    case 'Green':
        return green;
    case 'Yellow':
        return yellow;
    case 'Orange':
        return orange;
    case 'Red':
        return red;
    default:
        return grey;
    }
}

function onSysInfo(sysInfo) {
    var cell;
    var ssInfo;
    var refreshTimeout = refreshTimeoutSys();
    if (sysInfo !== undefined && sysInfo !== null && typeof sysInfo === "object") {
        if (SysRequest.since === undefined) {
            for (var nodeId in Nodes) {
                delete Nodes[nodeId].SysInfo;
            }
        }

        if (sysInfo.SystemStateInfo !== undefined) {
            for (var idx in sysInfo.SystemStateInfo) {
                ssInfo = sysInfo.SystemStateInfo[idx];
                cell = Nodes[ssInfo.NodeId].DomElement.cells[2];
                Nodes[ssInfo.NodeId].SysInfo = ssInfo;
            }
        }
        if (++SysRequest.request > 10) {
            SysRequest.request = 0;
            delete SysRequest.since;
        } else {
            if (sysInfo.ResponseTime !== undefined && Number(sysInfo.ResponseTime) > 0)
            SysRequest.since = sysInfo.ResponseTime;
        }
    } else {
        SysRequest.request = 0;
        delete SysRequest.since;
        for (var nodeId in Nodes) {
            delete Nodes[nodeId].SysInfo;
        }
    }
    var goodNodes = 0;
    var totalNodes = 0;
    var currentTime = new Date().valueOf();
    for (var nodeId in Nodes) {
        var node = Nodes[nodeId];
        ssInfo = node.SysInfo;
        if (ssInfo !== undefined) {
            cell = node.DomElement.cells[2];
            var startTime = ssInfo.StartTime;
            var upTime = currentTime - startTime;
            cell.innerHTML = upTimeToString(upTime);
            var block = node.DomElement.cells[1].firstChild.nextSibling.firstChild;
            if (ssInfo.SystemState !== undefined) {
                block.style.backgroundColor = flagToColor(ssInfo.SystemState);
            }
            block = block.nextSibling;
            if (ssInfo.MessageBusState !== undefined) {
                block.style.backgroundColor = flagToColor(ssInfo.MessageBusState);
            }
            block = block.nextSibling;
            if (ssInfo.LoadAverage !== undefined) {
                var percent = ssInfo.LoadAverage[0] * 100 / ssInfo.NumberOfCpus;
                if (percent > 100) {
                    percent = 100;
                }
                var progress = block.firstChild;
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
                block.title = "LoadAverage: " + ssInfo.LoadAverage + " / " + ssInfo.NumberOfCpus;
            }
            if (ssInfo.PoolStats !== undefined) {
                setPoolsMap(node, ssInfo.PoolStats);
            }
            goodNodes++;
        } else {
            onDisconnectNode(node);
        }
        totalNodes++;
    }
    refreshNodeStats();
    setTimeout(refreshSysInfo, refreshTimeout);
}

var NodeStatsDomElement = null;

function refreshNodeStats() {
    var nodeStats = [0, 0, 0, 0, 0];
    var currentTime = new Date().valueOf();
    for (var id in Nodes) {
        var node = Nodes[id];
        var sysInfo = node.SysInfo;
        if (sysInfo === undefined) {
            nodeStats[0]++;
        } else {
            var startTime = sysInfo.StartTime;
            var upTime = currentTime - startTime;
            if (upTime < 60 * 1000) {
                nodeStats[1]++;
            } else if (upTime < 60 * 60 * 1000) {
                nodeStats[2]++;
            } else if (upTime < 24 * 60 * 60 * 1000) {
                nodeStats[3]++;
            } else {
                nodeStats[4]++;
            }
        }
    }
    if (NodeStatsDomElement === null) {
        NodeStatsDomElement = $("#overview").find(".node-stats").get(0);
        /*// target color is lightgreen #90ee90
        var max_i = nodeStats.length - 1;
        for (var i = 0; i < nodeStats.length; ++i) {
            NodeStatsDomElement.rows[1].cells[i + 1].style.color =
                    "rgb(" + Math.floor(i * 0x90 / max_i)
                    + "," + Math.floor(i * 0xee / max_i)
                    + "," + Math.floor(i * 0x90 / max_i) + ")";
        }*/
    }
    var nodesRow = NodeStatsDomElement.rows[1];
    for (var i = 0; i < nodeStats.length; ++i) {
        var value = nodeStats[i];
        var cell = nodesRow.cells[i + 1];
        if (value === 0) {
            cell.innerHTML = "";
            $(cell).stop().css(StatsAnimateStop);
        } else {
            cell.innerHTML = value;
            $(cell).stop().css(StatsAnimateStop).animate(StatsAnimateStart, StatsAnimateDuration);
        }
    }
}

function updateStatsCell(cell, value) {
    if (value === 0) {
        cell.innerHTML = "";
        $(cell).stop().css(StatsAnimateStop);
    } else {
        cell.innerHTML = value;
        $(cell).stop().css(StatsAnimateStop).animate(StatsAnimateStart, StatsAnimateDuration);
    }
}

var DiskStatsDomElement = null;

function refreshVDiskStats() {
    var vDiskStats = {Grey: 0, Red: 0, Orange: 0, Yellow: 0, Green: 0};
    for (var idGroup in Groups) {
        var group = Groups[idGroup];
        for (var idVDisk in group.VDisks) {
            switch (group.VDisks[idVDisk].Color) {
            case red:
                vDiskStats.Red++;
                break;
            case orange:
                vDiskStats.Orange++;
                break;
            case yellow:
                vDiskStats.Yellow++;
                break;
            case green:
                vDiskStats.Green++;
                break;
            default:
                vDiskStats.Grey++;
                break;
            }
        }
    }
    if (DiskStatsDomElement === null) {
        DiskStatsDomElement = $("#overview").find(".disk-stats").get(0);
    }
    var vDiskRow = DiskStatsDomElement.rows[2];
    updateStatsCell(vDiskRow.cells[1], vDiskStats.Grey);
    updateStatsCell(vDiskRow.cells[2], vDiskStats.Red);
    updateStatsCell(vDiskRow.cells[3], vDiskStats.Orange);
    updateStatsCell(vDiskRow.cells[4], vDiskStats.Yellow);
    updateStatsCell(vDiskRow.cells[5], vDiskStats.Green);
}

function refreshPDiskStats() {
    var pDiskStats = {Grey: 0, Red: 0, Orange: 0, Yellow: 0, Green: 0};
    for (var idNode in Nodes) {
        var node = Nodes[idNode];
        for (var idPDisk in node.PDisks) {
            switch (node.PDisks[idPDisk].Color) {
            case red:
                pDiskStats.Red++;
                break;
            case orange:
                pDiskStats.Orange++;
                break;
            case yellow:
                pDiskStats.Yellow++;
                break;
            case green:
                pDiskStats.Green++;
                break;
            default:
                pDiskStats.Grey++;
                break;
            }
        }
    }
    if (DiskStatsDomElement === null) {
        DiskStatsDomElement = $("#overview").find(".disk-stats").get(0);
    }
    var pDiskRow = DiskStatsDomElement.rows[1];
    updateStatsCell(pDiskRow.cells[1], pDiskStats.Grey);
    updateStatsCell(pDiskRow.cells[2], pDiskStats.Red);
    updateStatsCell(pDiskRow.cells[3], pDiskStats.Orange);
    updateStatsCell(pDiskRow.cells[4], pDiskStats.Yellow);
    updateStatsCell(pDiskRow.cells[5], pDiskStats.Green);
}

function refreshBSGroupStats() {
    var bsGroupStats = {Grey: 0, Red: 0, Orange: 0, Yellow: 0, Green: 0};
    for (var idGroup in Groups) {
        var group = Groups[idGroup];
        var vDiskStats = {Grey: 0, Green: 0};
        for (var idVDisk in group.VDisks) {
            switch (group.VDisks[idVDisk].Color) {
            case green:
                vDiskStats.Green++;
                break;
            default:
                vDiskStats.Grey++;
                break;
            }
        }
        var erasureInfo = getErasureInfo(group.ErasureSpecies);
        if (vDiskStats.Green >= erasureInfo.Total) {
            bsGroupStats.Green++;
        } else if (vDiskStats.Green > erasureInfo.Min) {
            bsGroupStats.Yellow++;
        } else if (vDiskStats.Green === erasureInfo.Min) {
            bsGroupStats.Orange++;
        } else if (vDiskStats.Green > 0) {
            bsGroupStats.Red++;
        } else {
            bsGroupStats.Grey++;
        }
    }
    if (DiskStatsDomElement === null) {
        DiskStatsDomElement = $("#overview").find(".disk-stats").get(0);
    }
    var bsGroupRow = DiskStatsDomElement.rows[3];
    updateStatsCell(bsGroupRow.cells[1], bsGroupStats.Grey);
    updateStatsCell(bsGroupRow.cells[2], bsGroupStats.Red);
    updateStatsCell(bsGroupRow.cells[3], bsGroupStats.Orange);
    updateStatsCell(bsGroupRow.cells[4], bsGroupStats.Yellow);
    updateStatsCell(bsGroupRow.cells[5], bsGroupStats.Green);
}

// temporary constants - to be computed dynamically
var warningOutputQueueSize = 200; // waiting for colorful flags from Interconnect
var errorOutputQueueSize = 500;
var minColorComponent = 0x90;
var maxColorComponent = 0xee;
var rangeColorComponent = maxColorComponent - minColorComponent + 1;

function onNodeInfo(nodeInfo) {
    var nodeStateInfo = null;
    if (NodeRequest.since === undefined) {
        for (var i in Nodes) {
            delete Nodes[i].NodeStateInfo;
        }
    }
    if (++NodeRequest.request > 20) {
        NodeRequest.request = 0;
    }
    delete NodeRequest.since;
    if (nodeInfo !== undefined && nodeInfo !== null && typeof nodeInfo === "object") {
        for (var nodeId in nodeInfo) {
            var node = Nodes[nodeId];
            if (node === undefined)
                continue;
            nodeStateInfo = nodeInfo[nodeId];
            if (nodeStateInfo === null) {
                onDisconnectNode(node);
            } else {
                nodeStateInfo = nodeStateInfo.NodeStateInfo;
                if (node.NodeStateInfo === undefined) {
                    node.NodeStateInfo = {};
                }
                for (var i in nodeStateInfo) {
                    var stateInfo = nodeStateInfo[i];
                    var peerName = stateInfo.PeerName;
                    var id = Number(peerName.split(':')[0]);
                    var ni = node.NodeStateInfo[id];
                    if (ni === undefined
                            || ni.Connected !== stateInfo.Connected
                            || ni.OutputQueueSize !== stateInfo.OutputQueueSize
                            || ni.ConnectStatus !== stateInfo.ConnectStatus) {
                        ni = node.NodeStateInfo[id] = stateInfo;
                        if (stateInfo.Connected === undefined) {
                            setInterconnectMap(node, id, "lightgrey"/*, getNodeHost(id)*/);
                        } else {
                            var interconnectColor;
                            if (stateInfo.Connected) {
                                //var title = getNodeHost(id) + " Connected";
                                if (ni.OutputQueueSize !== undefined) {
                                    //title += ", Queue Size " + bytesToSize(ni.OutputQueueSize);
                                    var r, g, b;
                                    if (ni.OutputQueueSize <= warningOutputQueueSize) {
                                        r = minColorComponent + (ni.OutputQueueSize / warningOutputQueueSize * rangeColorComponent);
                                        g = maxColorComponent;
                                        b = minColorComponent;
                                    } else {
                                        var qs = ni.OutputQueueSize - warningOutputQueueSize;
                                        var rqs = errorOutputQueueSize - warningOutputQueueSize;
                                        if (qs > rqs) {
                                            qs = rqs;
                                        }
                                        r = maxColorComponent;
                                        g = maxColorComponent - (qs / rqs * rangeColorComponent);
                                        b = minColorComponent;
                                    }
                                    interconnectColor = "rgb(" + Math.floor(r) + "," + Math.floor(g) + "," + Math.floor(b) + ")";
                                } else {
                                    interconnectColor = "lightgreen";
                                }
                                if (ni.ConnectStatus !== undefined) {
                                    switch (ni.ConnectStatus) {
                                    case 1:
                                        interconnectColor = green;
                                        break;
                                    case 2:
                                        interconnectColor = yellow;
                                        break;
                                    case 3:
                                        interconnectColor = orange;
                                        break;
                                    case 4:
                                        interconnectColor = red;
                                        break;
                                    }
                                }
                                setInterconnectMap(node, id, interconnectColor/*, title*/);
                            } else {
                                setInterconnectMap(node, id, "red"/*, getNodeHost(id) + " Disconnected"*/);
                            }
                        }
                    }
                }
                if (NodeRequest.request !== 0
                        && (NodeRequest.since === undefined
                        || NodeRequest.since > nodeInfo[nodeId].ResponseTime)) {
                    NodeRequest.since = nodeInfo[nodeId].ResponseTime;
                }
            }
        }
    } else {
       // NodeInfoChangeTime = null;
    }
    setTimeout(refreshNodeInfo, refreshTimeoutNode());
}

function clearNodeInfo(nodeId) {
    return;
    /*var row = nodelist[nodeId - 1].DomElement;
    var cell = row.cells[3];
    var n = nodelist.length;
    for (var i = 0; i < n; i++) {
        getNodeCell(cell, i + 1).innerHTML = getNodeBlock(nodelist[i].Host, "lightgrey");
    }
    while (row.cells.length > 4) {
        row.removeChild(row.cells[4]);
    }*/
}

function tabletStateToColor(state) {
    switch (state) {
    case "Created": return "grey";
    case "ResolveStateStorage": return "lightgrey";
    case "Candidate": return "lightgrey";
    case "BlockBlobStorage": return "lightgrey";
    case "RebuildGraph": return "yellow";
    case "WriteZeroEntry": return "yellow";
    case "Restored": return "yellow";
    case "Discover": return "orange";
    case "Lock": return "lightblue";
    case "Dead": return "black";
    case "Active": return "lightgreen";
    }
    return "brown";
}

function tabletToColor(tablet) {
    return tabletStateToColor(tablet.State);
}

function tabletToString(tablet) {
    return tablet.State;
}

function tabletTypeToString(type) {
    switch(type) {
    case "OldTxProxy":
        return "TxProxy";
    }
    return type;
}

function tabletTypeToSymbol(type) {
    switch(type) {
    case "SchemeShard":
        return "SS";
    case "DataShard":
        return "DS";
    case "Hive":
        return "H";
    case "Coordinator":
        return "C";
    case "Mediator":
        return "M";
    case "OldTxProxy":
    case "TxProxy":
        return "P";
    case "BSController":
        return "BS";
    case "Dummy":
        return "DY";
    case "JobRunner":
        return "JR";
    case "RTMRPartition":
        return "RP";
    case "KeyValue":
        return "KV";
    case "PersQueue":
        return "PQ";
    case "PersQueueReadBalancer":
        return "PB";
    case "NodeBroker":
        return "NB";
    case "TxAllocator":
        return "TA";
    case "Cms":
        return "CM";
    case "BlockStorePartition":
        return "BP";
    case "BlockStoreVolume":
        return "BV";
    case "Console":
        return "CN";
    case "TenantSlotBroker":
        return "TB";
    case "Kesus":
        return "K";
    case "OlapShard":
        return "OS";
    case "ColumnShard":
        return "CS";
    case "SequenceShard":
        return "S";
    case "ReplicationController":
        return "RC";
    case "TestShard":
        return "TS";
    case "BlobDepot":
        return "BD";
    }
    return "XX";
}

function tabletTypeToColor(type) {
    switch(type) {
    case "SchemeShard":
        return "yellow";
    case "DataShard":
        return "#00BFFF";
    case "Hive":
        return "orange";
    case "Coordinator":
        return "#4682B4";
    case "Mediator":
        return "brown";
    case "OldTxProxy":
    case "TxProxy":
    case "TxAllocator":
        return "#DDA0DD";
    case "BSController":
        return "cyan";
    case "Dummy":
        return "grey";
    case "JobRunner":
        return "lightgrey";
    case "RTMRPartition":
    case "NodeBroker":
        return "maroon";
    case "KeyValue":
    case "Cms":
        return "pink";
    case "PersQueue":
        return "darksalmon";
    case "PersQueueReadBalancer":
        return "darkmagenta";
    case "BlockStorePartition":
        return "#B0E0E6";
    case "BlockStoreVolume":
        return "#B0C4DE";
    case "Console":
        return "SlateBlue";
    case "TenantSlotBroker":
        return "SlateGray";
    default:
        return "white";
    }
}

function tabletTypeToTextColor(type) {
    switch(type) {
    case "SchemeShard":
    case "BSController":
    case "BlockStorePartition":
        return "black";
    default:
        return "white";
    }
}

function updateTablets() {
    if (Object.keys(Tablets).length < 200000) {
        if (!newtablets_running) {
            newtablets_running = true;
            runUpdateTablets();
        }
    } else {
        newtablets = [];
        $("#panel-all-tablets").html("<i>Too many tablets to display</i>");
    }
}

var TabletGroups = {};
var TabletGroupingFunction = function(tablet) { return "Tablets"; }
var TabletColoringFunction = function(tablet) {
    return {
        backgroundColor: tabletToColor(tablet),
        color: tablet.State === "Dead" ? "white" : "black"
    };
}

function onTabletColorChange(obj) {
    switch (obj.value) {
    case "state":
        TabletColoringFunction = function(tablet) {
            return {
                backgroundColor: tabletToColor(tablet),
                color: tablet.State === "Dead" ? "white" : "black"
            };
        }
        break;
    case "type":
        TabletColoringFunction = function(tablet) {
            return {
                backgroundColor: tabletTypeToColor(tablet.Type),
                color: tabletTypeToTextColor(tablet.Type)
            };
        }
        break;
    }
    for (var id in Tablets) {
        newtablets.push(Tablets[id]);
    }
    updateTablets();
}

function onTabletGroupChange(obj) {
    switch (obj.value) {
    case "ungrouped":
        TabletGroupingFunction = function(tablet) { return "Tablets"; }
        break;
    case "type":
        TabletGroupingFunction = function(tablet) { return tabletTypeToString(tablet.Type); }
        break;
    case "state":
        TabletGroupingFunction = function(tablet) { return tabletToString(tablet); }
        break;
    case "node":
        TabletGroupingFunction = function(tablet) {
            if (tablet.NodeId !== undefined && tablet.NodeId !== 0) {
                return tablet.NodeId + " " + getNodeHost(tablet.NodeId);
            } else {
                return "Unassigned";
            }
        }
        break;
    }
    for (var id in Tablets) {
        newtablets.push(Tablets[id]);
    }
    updateTablets();
}

function getInsertPanel(panelTablets, groupValue) {
    if (groupValue !== null) {
        var tabletGroup = TabletGroups[groupValue];
        if (tabletGroup === undefined) {
            var id = Object.keys(TabletGroups).length;
            tabletGroup = TabletGroups[groupValue] = {};
            var tabletGroupElement = document.createElement("div");
            tabletGroupElement.className = "tabletgroupblock";
            tabletGroupElement.setAttribute("group-value", groupValue);

            var tabletGroupLabelElement = $("<button/>", {
                                                "type": "button",
                                                "class": "tabletgroupname btn btn-info collapsed",
                                                "style": "padding-top:3px; padding-bottom:3px; padding-right:5px; margin-bottom:5px",
                                                "data-toggle": "collapse",
                                                "data-target": "#tabletgroupcontainer" + id
                                            }).html(groupValue).get(0);

            tabletGroupElement.appendChild(tabletGroupLabelElement);

            var tabletGroupContainerElement = document.createElement("div");
            tabletGroupContainerElement.id = "tabletgroupcontainer" + id;
            tabletGroupContainerElement.className = "tabletgroupcontainer collapse";
            tabletGroupElement.appendChild(tabletGroupContainerElement);
            tabletGroup.DomElement = tabletGroupContainerElement;

            var tabletGroupClearFixElement = document.createElement("div");
            tabletGroupClearFixElement.style.clear = "both";
            tabletGroupElement.appendChild(tabletGroupClearFixElement);

            for (var child = panelTablets.firstChild; child !== null; child = child.nextSibling)
            if (groupValue < child.firstChild.innerHTML) {
                panelTablets.insertBefore(tabletGroupElement, child);
                break;
            }

            if (child === null) {
                panelTablets.appendChild(tabletGroupElement);
            }
        }
        return tabletGroup.DomElement;
    } else {
        return panelTablets;
    }
}

function clearEmptyGroups() {
    $("#panel-all-tablets").find(".tabletgroupblock").each(function() {
        //var container = $(this).find(".tabletgroupcontainer").get(0);
        var jthis = $(this);
        var length = jthis.find(".tabletgroupcontainer").children().length;
        var groupValue = this.getAttribute("group-value");
        //var length = container.children.length;
        if (length === 0) {
            delete TabletGroups[groupValue];
            this.parentNode.removeChild(this);
        } else {
            jthis.find(".tabletgroupname").html(groupValue + " (" + length + ")");
        }
    });
}

function getTabletInsertPoint(panelTablets, tabletElement, tabletType) {
    var tabletTypesDomElements = panelTablets.TabletTypesDomElements;
    if (tabletTypesDomElements === undefined) {
        tabletTypesDomElements = panelTablets.TabletTypesDomElements = [];
    }
    var len = tabletTypesDomElements.length;
    for (var idx = 0; idx < len; idx++) {
        var tabletTypeDomElement = tabletTypesDomElements[idx];
        if (tabletType < tabletTypeDomElement.Type) {
            tabletElement.TopOrderElement = true;
            tabletTypesDomElements.splice(idx, 0, {
                                              Type: tabletType,
                                              DomElement: tabletElement
                                          });
            return tabletTypeDomElement.DomElement;
        }
        if (tabletType === tabletTypeDomElement.Type) {
            if (idx + 1 < len) {
                var nextTabletTypeDomElement = tabletTypesDomElements[idx + 1];
                return nextTabletTypeDomElement.DomElement;
            } else {
                return null;
            }
        }
    }
    tabletElement.TopOrderElement = true;
    tabletTypesDomElements.push({
                                    Type: tabletType,
                                    DomElement: tabletElement
                                });
    return null;
}

function insertTablet(panelTablets, tabletElement, tabletType) {
    if (tabletType !== undefined) {
        if (tabletElement.TopOrderElement) {
            var panel = tabletElement.parentElement;
            var types = panel.TabletTypesDomElements;
            var next = tabletElement.nextSibling;
            var len = types.length;
            var idx = 0;
            for (; idx < len; idx++) {
                if (types[idx].Type === tabletType)
                    break;
            }
            if (idx < len) {
                if (next !== null) {
                    var nextType = next.Tablet.Type;
                    if (nextType !== tabletType)
                        next = null;
                }
                if (next !== null) {
                    types[idx].DomElement = next;
                    next.TopOrderElement = true;
                } else {
                    types.splice(idx, 1);
                }
            }
            tabletElement.TopOrderElement = false;
        }
        var tabletElementInsertPoint = getTabletInsertPoint(panelTablets, tabletElement, tabletType);
        if (tabletElementInsertPoint === null || tabletElementInsertPoint.parentNode !== panelTablets) {
            panelTablets.appendChild(tabletElement);
        } else {
            panelTablets.insertBefore(tabletElement, tabletElementInsertPoint);
        }
        return;
    }
    panelTablets.appendChild(tabletElement);
}

function removeTablet(panelTablets, tabletElement, tabletType) {
    panelTablets.removeChild(tabletElement);
}

function onTabletClick() {
    var url = "tablet?id=" + this.Tablet.TabletId + "&type=" + this.Tablet.Type;
    window.open(url);
}

function runUpdateTablets() {
    var panelAllTablets = $("#panel-all-tablets").get(0);
    var panelSchema = $("#panel-schema").find(".panel-body");
    var panelTablets = $("#panel-tablets").find(".panel-body");

    if (panelAllTablets.firstChild !== null && panelAllTablets.firstChild.nodeName === "IMG") {
        panelAllTablets.removeChild(panelAllTablets.firstChild);
    }

    var maxupdates = 100;
    while (newtablets.length > 0 && maxupdates-- > 0) {
        var tablet = newtablets[0];
        newtablets.shift();
        var tabobj = Tablets[tablet.TabletId];
        if (tabobj !== undefined) {
            tablet = tabobj;
        }
        if (tablet.State === "Deleted") {
            var tabletElement = tablet.DomElement;
            if (tabletElement !== undefined) {
                if (tabletElement.parentElement !== null) {
                    removeTablet(tabletElement.parentElement, tabletElement, tablet.Type);
                }
            }
            delete Tablets[tablet.TabletId];
            continue;
        }

        var tabletElement = tablet.DomElement;
        if (tabletElement === undefined) {
            if (tablet.State === undefined)
                continue;
            tabletElement = document.createElement("div");
            tabletElement.className = tabletblock_class;
            if (TabletsCount < 50000) {
                tabletElement.innerHTML = tabletTypeToSymbol(tablet.Type);
            }
            tabletElement.addEventListener("click", onTabletClick, false);
            tabletElement.Tablet = tablet;
            tablet.DomElement = tabletElement;
        }
        var groupValue = TabletGroupingFunction(tablet);
        if (tablet.State === undefined) {
            if (tabletElement.parentElement !== null) {
                removeTablet(tabletElement.parentElement, tabletElement, tablet.Type);
            }
            continue;
        } else if (tablet.GroupValue === undefined || tablet.GroupValue !== groupValue) {
            insertTablet(getInsertPanel(panelAllTablets, groupValue), tabletElement, tablet.Type);
            tablet.GroupValue = groupValue;
        }

        var tabletStyle = TabletColoringFunction(tablet);
        var title;
        if (TabletsCount < 50000) {
            title = tabletTypeToString(tablet.Type) + " " + tablet.TabletId + " " + tabletToString(tablet) + " on node " + tablet.NodeId + " (" + getNodeHost(tablet.NodeId) + ")";
            tabletElement.title = title;
        }
        $(tabletElement).css(tabletStyle);

        var clonedTabletElement = SchemaTabletElements[tablet.TabletId];
        if (clonedTabletElement !== undefined) {
            if (clonedTabletElement === null) {
                addTreeNodeTablet(tablet);
            } else {
                if (TabletsCount < 50000) {
                    clonedTabletElement.title = title;
                }
                $(clonedTabletElement).css(tabletStyle);
            }
        }

        /*panelSchema.find("div[tabletid='" + tablet.TabletId + "']").css(tabletStyle);
        var panelTabletElement = panelTablets.find("div[tabletid='" + tablet.TabletId + "']").get(0);
        if (panelTabletElement !== undefined) {
            panelTabletElement.title = title;
            panelTabletElement.style = tabletStyle;
        }*/
    }
    clearEmptyGroups();
    if (newtablets.length > 0) {
        setTimeout(runUpdateTablets, 0);
    } else {
        newtablets_running = false;
    }
}

function getTabletTypeFromHiveTabletType(type) {
    switch(type) {
    case 1:
    case 16:
        return "SchemeShard";
    case 2:
    case 18:
        return "DataShard";
    case 3:
    case 14:
        return "Hive";
    case 4:
    case 13:
        return "Coordinator";
    case 5:
        return "Mediator";
    case 6:
    case 17:
        return "TxProxy";
    case 7:
    case 15:
        return "BSController";
    case 8:
        return "Dummy";
    case 9:
    case 19:
        return "JobRunnerPoolManager";
    case 10:
        return "RTMRPartition";
    case 11:
    case 12:
        return "KeyValue";
    case 20:
        return "PersQueue";
    case 34:
        return "OlapShard";
    case 35:
        return "ColumnShard";
    case 36:
        return "TestShard";
    case 39:
        return "BlobDepot";
    }
    return type;
}

var HivesScheduled = {};

function onHiveInfo(result, hiveId) {
    if (result !== null) {
        var tablets = result.Tablets;
        if (tablets !== undefined) {
            for (var idx in tablets) {
                var tabletInfo = tablets[idx];
                var tablet = Tablets[tabletInfo.TabletID];
                if (tablet === undefined) {
                    tablet = {

                                                    TabletId: tabletInfo.TabletID,
                                                    Type: getTabletTypeFromHiveTabletType(tabletInfo.TabletType),
                                                    NodeId: tabletInfo.NodeID,
                                                    ChangeTime: "0",
                                                    ChannelGroupIDs: [],
                                                    FromHive: true,
                                                    State: "Dead"
                             };
                    Tablets[tabletInfo.TabletID] = tablet;
                    newtablets.push(tablet);
                }
            }
        }
        updateTablets();
    }
    setTimeout(function() { refreshHiveInfo(hiveId); }, refreshTimeoutHive());
}

function refreshHiveInfo(id) {
    $.ajax({
               url: "json/hiveinfo?hive_id=" + id,
               success: function(result) { onHiveInfo(result, id); },
               error: function() { onHiveInfo(null, id); }
           });
}

function onNewTabletAvailable(tabletInfo) {
    if (tabletInfo.Type === "Hive") { // Hive
        if (HivesScheduled[tabletInfo.TabletId] === undefined) {
            HivesScheduled[tabletInfo.TabletId] = true;
            refreshHiveInfo(tabletInfo.TabletId);
        }
    }
}

var TabletStatsTypes = {};
var TabletStatsDomElement = null;

var TabletStates = ["Created", "ResolveStateStorage", "Candidate", "BlockBlobStorage", "RebuildGraph", "WriteZeroEntry",
                    "Restored", "Discover", "Lock", "Dead", "Active"];
var TabletStatesPositions = {Dead: 0, Created: 1, ResolveStateStorage: 2, Lock: 3, Discover: 4, Candidate: 5,
    BlockBlobStorage: 6, RebuildGraph: 7, WriteZeroEntry: 8, Restored: 9, Active: 10};

function tabletStateToPosition(state) {
    return TabletStatesPositions[state];
}

function refreshTabletStats() {
    var TabletStats = {};
    for (var type in TabletStatsTypes) {
        TabletStats[type] = {};
    }
    var tabletAge = [0, 0, 0, 0, 0];
    var currentTime = new Date().valueOf();
    for (var id in Tablets) {
        var tablet = Tablets[id];
        var type = tablet.Type;
        if (type === undefined || type === "undefined")
            continue;
        var stats = TabletStats[type];
        if (stats === undefined) {
            stats = TabletStats[type] = {};
        }
        var state = tablet.State;
        var num = stats[state];
        if (num === undefined) {
            stats[state] = 1;
        } else {
            stats[state] = num + 1;
        }
        if (tablet.State === "Dead") {
            tabletAge[0]++;
        } else {
            var upTime = currentTime - tablet.ChangeTime;
            if (upTime < 60 * 1000) {
                tabletAge[1]++;
            } else if (upTime < 60 * 60 * 1000) {
                tabletAge[2]++;
            } else if (upTime < 24 * 60 * 60 * 1000) {
                tabletAge[3]++;
            } else {
                tabletAge[4]++;
            }
        }
    }
    if (TabletStatsDomElement === null) {
        TabletStatsDomElement = $("#overview").find(".tablet-stats").get(0);
    }
    for (var type in TabletStats) {
        var tabletStatType = TabletStatsTypes[type];
        if (tabletStatType === undefined) {
            tabletStatType = TabletStatsTypes[type] = {};
            var tabletStatsRowDomElement = TabletStatsDomElement.insertRow(-1);
            var header = tabletStatsRowDomElement.insertCell(-1);
            //header.style = "font-weight:bold;text-align:right";
            header.innerHTML = tabletTypeToString(type);
            for (var i = 0; i < TabletStates.length; i++) {
                var cell = tabletStatsRowDomElement.insertCell(-1);
                cell.className = "tcounter";
            }
            for (var i = 0; i < TabletStates.length; i++) {
                var state = TabletStates[i];
                tabletStatsRowDomElement.cells[tabletStateToPosition(state) + 1].style.color = tabletStateToColor(state);
            }

            tabletStatType.DomElement = tabletStatsRowDomElement;
        }
        var tabletStatRowDomElement = tabletStatType.DomElement;
        var cells = tabletStatRowDomElement.cells;
        var states = TabletStats[type];
        for (var i = 0; i < TabletStates.length; i++) {
            var state = TabletStates[i];
            var cell = cells[tabletStateToPosition(state) + 1];
            var cnt = states[state];
            if (cnt === undefined) {
                cell.innerHTML = "";
                $(cell).stop().css(StatsAnimateStop);
            } else {
                cell.innerHTML = cnt;
                $(cell).stop().css(StatsAnimateStop).animate(StatsAnimateStart, StatsAnimateDuration);
            }
        }
    }

    if (NodeStatsDomElement !== null) {
        var tabletsRow = NodeStatsDomElement.rows[2];
        for (var i = 0; i < tabletAge.length; ++i) {
            var value = tabletAge[i];
            var cell = tabletsRow.cells[i + 1];
            if (value === 0) {
                cell.innerHTML = "";
                $(cell).stop().css(StatsAnimateStop)
            } else {
                cell.innerHTML = value;
                $(cell).stop().css(StatsAnimateStop).animate(StatsAnimateStart, StatsAnimateDuration);
            }
        }
    }
}

var enums = {}

function fromEnums(a) {
    var b = enums[a];
    if (b === undefined) {
        return enums[a] = String(a);
    } else {
        return b;
    }
}

function onTabletInfo(result) {
    var tabletStateInfo = null;
    var refreshTimeout = refreshTimeoutTablet();
    var tabLen;
    if (result !== undefined && result !== null && typeof result === "object") {
        if (result.TabletStateInfo !== undefined) {
            tabletStateInfo = result.TabletStateInfo;
        }

        if (tabletStateInfo !== null && tabletStateInfo.length > 1000) {
            if (tabletStateInfo.length > 5000) {
                DefaultRefreshTimeout = 5000;
            } else {
                DefaultRefreshTimeout = 1000;
            }
        }

        if (tabletStateInfo !== null) {
            if (tabletStateInfo.length >= 8000) {
                if (tabletStateInfo.length >= 50000) {
                    tabletblock_class = "tabletblock-extra-small";
                } else {
                    tabletblock_class = "tabletblock-small";
                }
            }
            for(var idx in tabletStateInfo) {
                var tabletInfo = tabletStateInfo[idx];
                var tablet = Tablets[tabletInfo.TabletId];
                if (tablet === undefined) {
                    if (tabletInfo.State === "Deleted") {
                        continue;
                    }
                    tabletInfo.State = fromEnums(tabletInfo.State);
                    tabletInfo.Type = fromEnums(tabletInfo.Type);
                    tablet = Tablets[tabletInfo.TabletId] = tabletInfo;
                    newtablets.push(tabletInfo);
                    if (tabletInfo.State === "Active") {
                        onNewTabletAvailable(tabletInfo);
                    }
                } else {
                    if (Number(tablet.ChangeTime) < Number(tabletInfo.ChangeTime)) {
                        if (tablet.State !== tabletInfo.State
                                || tablet.NodeId !== tabletInfo.NodeId) {
                            newtablets.push(tabletInfo);
                        }

                        tabletInfo.DomElement = tablet.DomElement;
                        tabletInfo.GroupValue = tablet.GroupValue;
                        tabletInfo.State = fromEnums(tabletInfo.State);
                        tabletInfo.Type = fromEnums(tabletInfo.Type);
                        Tablets[tabletInfo.TabletId] = tabletInfo;

                        if (tabletInfo.State === "Active") {
                            onNewTabletAvailable(tabletInfo);
                        }
                    }
                }
                if (TabletRequest.since === undefined) {
                    tablet.Touched = true;
                }
            }
        }
        if (TabletRequest.since === undefined) {
            for (var id in Tablets) {
                var tab = Tablets[id];
                if (!tab.Touched && tab.FromHive === undefined) {
                    delete tab.State;
                    newtablets.push(tab);
                    delete Tablets[id];
                }
            }
        }
        if (++TabletRequest.request > 60) {
            TabletRequest.request = 0;
            delete TabletRequest.since;
        } else {
            TabletRequest.since = result.ResponseTime;
        }
    } else {
        for (var id in Tablets) {
            delete Tablets[id].State;
            newtablets.push(Tablets[id]);
            delete Tablets[id];
        }
        TabletRequest.request = 0;
        delete TabletRequest.since;
    }
    TabletsCount = Object.keys(Tablets).length;
    if (newtablets.length > 0) {
        updateTablets();
    }
    refreshTabletStats();
    setTimeout(refreshTabletInfo, refreshTimeout);
}

function refreshSysInfo() {
    $.ajax({
               url: "json/sysinfo",
               data: SysRequest,
               success: onSysInfo,
               error: function() { onSysInfo(null); }
           });
}

function refreshNodeInfo() {
    $.ajax({
               url: "json/nodeinfo",
               data: NodeRequest,
               success: onNodeInfo,
               error: function() { onNodeInfo(null); }
           });
}

function refreshPDiskInfo() {
    $.ajax({
               url: "json/pdiskinfo",
               data: PDiskRequest,
               success: onPDiskInfo,
               error: function() { onPDiskInfo(null); }
           });
}

function refreshTabletInfo() {
    $.ajax({
               url: "json/tabletinfo",
               data: TabletRequest,
               success: onTabletInfo,
               error: function() { onTabletInfo(null); }
           });
}

function refreshVDiskInfo() {
    $.ajax({
               url: "json/vdiskinfo",
               data: VDiskRequest,
               success: onVDiskInfo,
               error: function() { onVDiskInfo(null); }
           });
}

function refreshBSGroupInfo() {
    $.ajax({
               url: "json/bsgroupinfo",
               data: BSGroupRequest,
               success: onBSGroupInfo,
               error: function() { onBSGroupInfo(null); }
           });
}

var InterconnectHeight = 36;

function getInterconnectUrl(node, peerNode) {
    return getBaseUrl(node) + "/actors/interconnect/peer" + pad4(peerNode);
}

function onInterconnectClick() {
    window.open(getInterconnectUrl(this.NodeId, this.PeerNodeId));
}

function buildInterconnectMap(node, interconnect) {
    if (NodesCount >= 64) {
        var canvas = document.createElement("canvas");
        canvas.width = NodesCount;
        canvas.height = InterconnectHeight;
        canvas.style.verticalAlign = "middle";
        interconnect.appendChild(canvas);
        var context = canvas.getContext("2d");
        context.translate(0.5, 0.5);
        //context.lineWidth = 0.5;
        context.fillStyle = "lightgrey";
        context.fillRect(0, 0, NodesCount, InterconnectHeight - 1);
        node.InterconnectCellDomElement = context;
    } else {
        var table = document.createElement("table");
        interconnect.appendChild(table);
        var interconnectrow = table.insertRow(-1);
        node.InterconnectCellDomElement = {};
        for (var j in Nodes) {
            var interconnectcell = interconnectrow.insertCell(-1);
            var interconnectdiv = document.createElement("div");
            if (NodesCount >= 128) {
                interconnectdiv.className = "nodeblocks";
                interconnectdiv.style.width = "1px";
                interconnectcell.style.padding = "0px";
            } else if (NodesCount >= 64) {
                interconnectdiv.className = "nodeblock";
                interconnectdiv.style.width = "3px";
                interconnectdiv.style.boxShadow = "1px 1px 0px grey";
                interconnectcell.style.padding = "1px";
            } else if (NodesCount >= 32) {
                interconnectdiv.className = "nodeblock";
                interconnectdiv.style.width = "6px";
                interconnectdiv.style.boxShadow = "1px 1px 1px grey";
                interconnectcell.style.padding = "1px";
            } else {
                interconnectdiv.className = "nodeblock";
                interconnectdiv.style.width = "12px";
                interconnectdiv.style.boxShadow = "1px 1px 1px grey";
                interconnectcell.style.padding = "1px";
            }
            interconnectdiv.title = getNodeHost(j);
            interconnectdiv.NodeId = node.Id;
            interconnectdiv.PeerNodeId = j;
            interconnectdiv.addEventListener("click", onInterconnectClick, false);
            interconnectcell.appendChild(interconnectdiv);
            node.InterconnectCellDomElement[j] = interconnectdiv;
        }
    }
}

function setInterconnectMap(node, id, color, title) {
    if (node.InterconnectCellDomElement[1] !== undefined) {
        var div = node.InterconnectCellDomElement[id];
        div.style.backgroundColor = color;
        if (title !== undefined) {
            div.title = title;
        }
    } else {
        var context = node.InterconnectCellDomElement;
        context.strokeStyle = color;
        context.beginPath();
        context.moveTo(id - 1, 0);
        context.lineTo(id - 1, InterconnectHeight - 1);
        context.stroke();
    }
}

function buildPoolsMap(node, pools) {
    var table = document.createElement("table");
    pools.appendChild(table);
    var poolsrow = table.insertRow(-1);
    node.PoolsCellDomElement = poolsrow;
}
/*
var PoolColors = [
            {usage: 0, R: 144, G: 238, B: 144},
            {usage: 50, R: 255, G: 255, B: 0},
            {usage: 75, R: 255, G: 165, B: 0},
            {usage: 100, R: 255, G: 0, B: 0}
        ];
*/

var PoolColors = [
            {usage: 0, R: 144, G: 238, B: 144},
            {usage: 75, R: 255, G: 255, B: 0},
            {usage: 100, R: 255, G: 0, B: 0}
        ];

function getPoolColor(usage) {
    var idx = 1;
    for (var idx = 1; idx < PoolColors.length && usage > PoolColors[idx].usage; idx++);
    var a = PoolColors[idx - 1];
    var b = PoolColors[idx];
    var R = a.R + ((usage - a.usage) / (b.usage - a.usage)) * (b.R - a.R);
    var G = a.G + ((usage - a.usage) / (b.usage - a.usage)) * (b.G - a.G);
    var B = a.B + ((usage - a.usage) / (b.usage - a.usage)) * (b.B - a.B);
    return "rgb(" + R.toFixed() + "," + G.toFixed() + "," + B.toFixed() + ")";
}

function setPoolsMap(node, pools) {
    var poolsrow = node.PoolsCellDomElement;
    var cells = poolsrow.cells;
    while (cells.length > pools.length) {
        poolsrow.deleteCell(cells.length - 1);
    }
    for (var idx = 0; idx < pools.length; idx++) {
        var cell;
        var pool;
        var inner;
        if (idx >= cells.length) {
            cell = poolsrow.insertCell(idx);
            cell.style.padding = "1px";
            var pooldiv = document.createElement("div");
            pooldiv.className = "nodeblock";
            pooldiv.style.width = "12px";
            pooldiv.style.boxShadow = "1px 1px 1px grey";
            pooldiv.style.padding = "1px";
            pooldiv.style.backgroundColor = "white";
            var innerdiv = document.createElement("div");
            innerdiv.style.width = "100%";
            innerdiv.style.height = "50%";
            //innerdiv.style.backgroundColor = 'white';
            innerdiv.style.backgroundColor = '#f7f7f7';
            pooldiv.appendChild(innerdiv);
            cell.appendChild(pooldiv);
            pool = pooldiv;
            inner = innerdiv;
        } else {
            cell = cells[idx];
            pool = cell.firstChild;
            inner = pool.firstChild;
        }
        var usage = (pools[idx].Usage * 100).toFixed();
        //usage = 20 * idx + 15;
        if (pools[idx].Name !== "") {
            cell.title = pools[idx].Name + " " + usage + "%";
        } else {
            cell.title = usage + "%";
        }
        if (usage < 0) {
            usage = 0;
        }
        if (usage > 100) {
            usage = 100;
        }
        var height = 100 - usage;
        pool.style.backgroundColor = getPoolColor(usage);
        inner.style.height = height + "%";
    }
}

function onNodeList(nlist) {
    if (nlist !== null && NodesCount === 0) {
        if (nlist.length >= 128) {
            tabletblock_class = "tabletblock-small";
        }
        var htmlNodelist = $(".nodelist").get(0);
        var nlen = nlist.length;
        for (var i = 0; i < nlen; i++) {
            Nodes[nlist[i].Id] = nlist[i];
        }
        NodesCount = nlen;
        for (var i in Nodes) {
            var node = Nodes[i];
            var row = node.DomElement;
            if (row === undefined) {
                row = htmlNodelist.insertRow(-1);
                node.DomElement = row;
                row.insertCell(0).innerHTML = node.Id;
                var host = row.insertCell(1);
                host.innerHTML = "<div><a href='" + getViewerUrl(i) + "'>" + node.Host + "</a></div>" +
                                 "<div class='statecontainer'><div class='stateblock' title='Overall'></div>" +
                                 "<div class='stateblock' title='MessageBus' style='text-align:center'>MB</div>" +
                                 "<div class='usageblock progress' title='LoadAverage'><div class='progress-bar' role='progressbar'></div></div></div>";
                host.title = node.Address + ":" + node.Port;
                row.insertCell(2).style.textAlign = "right"; // uptime
                var cpu = row.insertCell(3);
                buildPoolsMap(node, cpu);
                var interconnect = row.insertCell(4); // interconnect status
                buildInterconnectMap(node, interconnect);
            }
        }
        // first run only
        refreshSysInfo();
        refreshNodeInfo();
        refreshPDiskInfo();
        refreshVDiskInfo();
        refreshTabletInfo();
    }
    if (nlist === null) {
        for (var i in Nodes) {
            //clearSysInfo(i, null);
            //clearNodeInfo(i, null);
        }
    }

    setTimeout(function(){$.ajax({
                                     url: "json/nodelist",
                                     success: function(result) { onNodeList(result); },
                                     error: function() { onNodeList(null); }
                                 });}, 120000);
}

function concatPaths(parent, child) {
    if (parent === "/") {
        return parent + child;
    } else {
        return parent + "/" + child;
    }
}

var OpeningPath = null;

function onTreeDataComplete(result, obj, cb) {
    if (result !== null && result.PathDescription) {
        var children = [];
        if (result.PathDescription.Children) {
            var clen = result.PathDescription.Children.length;
            for (var i = 0; i < clen; i++) {
                var child = {};
                child.id = concatPaths(result.Path, result.PathDescription.Children[i].Name);
                child.parent = result.Path;
                child.text = result.PathDescription.Children[i].Name;
                switch (result.PathDescription.Children[i].PathType) {
                case 1:     // Directory
                    child.children = true;
                    child.icon = "glyphicon glyphicon-folder-close schema-good";
                    break;
                case 2:     // Table
                    child.children = true;
                    child.icon = "glyphicon glyphicon-list schema-good";
                    break;
                case 3:     // PQ Group
                    child.icon = "glyphicon glyphicon-sort-by-alphabet schema-good";
                    break;
                case 4:     // SubDomain
                    child.children = true;
                    child.icon = "glyphicon glyphicon-tasks schema-good";
                    break;
                case 10:    // ExtSubDomain
                    child.children = true;
                    child.icon = "glyphicon glyphicon-asterisk schema-good";
                    break;
                case 12:    // OlapStore
                    child.children = true;
                    child.icon = "glyphicon glyphicon-list schema-good";
                    break;
                }
                child.data = function(obj1, cb1) { onTreeData(obj1, cb1); };
                children.push(child);
            }
            obj.describe = result;
        } else if (result.PathDescription.Table) {
            if (result.PathDescription.Table.TableIndexes) {
                var ilen = result.PathDescription.Table.TableIndexes.length;
                for (var i = 0; i < ilen; i++) {
                    var child = {};
                    child.id = concatPaths(result.Path, result.PathDescription.Table.TableIndexes[i].Name);
                    child.parent = result.Path;
                    child.text = result.PathDescription.Table.TableIndexes[i].Name;
                    child.children = true;
                    child.icon = "glyphicon glyphicon-book schema-good";
                    child.data = function(obj1, cb1) { onTreeData(obj1, cb1); };
                    children.push(child);
                }
                obj.describe = result;
            }
            if (result.PathDescription.Table.CdcStreams) {
                var ilen = result.PathDescription.Table.CdcStreams.length;
                for (var i = 0; i < ilen; i++) {
                    var child = {};
                    child.id = concatPaths(result.Path, result.PathDescription.Table.CdcStreams[i].Name);
                    child.parent = result.Path;
                    child.text = result.PathDescription.Table.CdcStreams[i].Name;
                    child.children = true;
                    child.icon = "glyphicon glyphicon-book schema-good";
                    child.data = function(obj1, cb1) { onTreeData(obj1, cb1); };
                    children.push(child);
                }
                obj.describe = result;
            }
        }
        cb.call(this, children);
    } else {
        cb.call(this, null);
    }

    if (OpeningPath !== null && OpeningPath.startsWith(obj.id)) {
        var pos = obj.id.length;
        if (OpeningPath[pos] === '/') {
            pos++;
        }
        var idx = OpeningPath.substr(pos).indexOf('/');
        if (idx === -1) {
            $("#schema-tree").jstree('select_node', OpeningPath);
            OpeningPath = null;
        } else {
            var openNode = OpeningPath.substr(0, pos + idx);
            $("#schema-tree").jstree('open_node', openNode);
        }
    }
}

var schemaGoodTablets = 0;
var schemaTotalTablets = 0;

function addTreeNodeTablet(tablet) {
    var panelTablets = $("#panel-tablets").find(".panel-body").get(0);
    var id = tablet.TabletId;
    if (tablet.DomElement !== undefined) {
        var clonedTablet = tablet.DomElement.cloneNode(true);
        clonedTablet.Tablet = tablet;
        clonedTablet.addEventListener("click", onTabletClick, false);
        panelTablets.appendChild(clonedTablet);
        SchemaTabletElements[id] = clonedTablet;
    }
    if (tablet.State === "Active") {
        schemaGoodTablets++;
    }
    $("#schema-tablets").html(Math.trunc(schemaGoodTablets*100/schemaTotalTablets) + "% (" + schemaGoodTablets + "/" + schemaTotalTablets + ")")
}

function onTreeNodeTabletsComplete() {
    var panelTablets = $("#panel-tablets").find(".panel-body").get(0);
    while (panelTablets.firstChild)
        panelTablets.removeChild(panelTablets.firstChild);
    schemaGoodTablets = 0;
    schemaTotalTablets = 0;
    for (var id in SchemaTabletElements) {
        var tablet = Tablets[id];
        if (tablet !== undefined) {
            if (tablet.DomElement !== undefined) {
                var clonedTablet = tablet.DomElement.cloneNode(true);
                clonedTablet.addEventListener("click", onTabletClick, false);
                clonedTablet.Tablet = tablet;
                panelTablets.appendChild(clonedTablet);
                SchemaTabletElements[id] = clonedTablet;
                if (tablet.State === "Active") {
                    schemaGoodTablets++;
                }
            }
        }
        schemaTotalTablets++;
    }
    $("#schema-tablets").html(Math.trunc(schemaGoodTablets*100/schemaTotalTablets) + "% (" + schemaGoodTablets + "/" + schemaTotalTablets + ")")
}

function schemaPathTypeToString(pathType) {
    switch (pathType) {
    case 1:
        return "Directory";
    case 2:
        return "Table";
    case 3:
        return "PersQueueGroup";
    case 7:
        return "Kesus";
    case 10:
        return "Tenant";
    case 15:
        return "Sequence";
    case 16:
        return "Replication";
    }
}

function getCounter(counters, name) {
    var result = $.grep(counters, function(e){ return e.Name === name; });
    if (result.length > 0) {
        return Number(result[0].Value);
    } else {
        return 0;
    }
}

function onTreeNodeChildrenDataSizeComplete(dataSizeCell, result) {
    if (result === null) {
        dataSizeCell.innerHTML = "";
        return;
    }
    var dataSize = 0;
    dataSize += getCounter(result.TabletCounters.ExecutorCounters.SimpleCounters, "LogRedoMemory");
    dataSize += getCounter(result.TabletCounters.ExecutorCounters.SimpleCounters, "DbIndexBytes");
    dataSize += getCounter(result.TabletCounters.ExecutorCounters.SimpleCounters, "DbDataBytes");
    dataSize += getCounter(result.TabletCounters.AppCounters.SimpleCounters, "KV/RecordBytes");
    dataSize += getCounter(result.TabletCounters.AppCounters.SimpleCounters, "KV/TrashBytes");
    dataSizeCell.innerHTML = bytesToSize(dataSize);
}

function onTreeNodeChildrenComplete(path, children) {
    var panelTablets = $("#panel-tablets").find(".panel-body").get(0);
    panelTablets.innerHTML = "<table class='children-table'><tr><th>Name</th><th>Type</th><th>Size</th></tr></table>";
    var tableElement = panelTablets.firstChild;
    for (var idx in children) {
        var child = children[idx];
        var row = tableElement.insertRow(-1);
        row.insertCell(-1).innerHTML = child.Name;
        row.insertCell(-1).innerHTML = schemaPathTypeToString(child.PathType);
        var dataSizeCell = row.insertCell(-1);
        if (child.PathType === 2 || child.PathType === 3) {
            dataSizeCell.innerHTML = "<img src='throbber.gif'></img>";
            (function(cell){
                $.ajax({
                           url: "json/tabletcounters?path=" + path + "/" + child.Name,
                           success: function(result) { onTreeNodeChildrenDataSizeComplete(cell, result); }
                       });
            })(dataSizeCell);
        } else {
            dataSizeCell.innerHTML = "";
        }
    }
}

var sizes = ["", "K", "M", "G", "T", "P", "E"];

function bytesToSize(bytes) {
    if (isNaN(bytes)) {
        return "";
    }
    if (bytes < 1024)
        return String(bytes);
    var i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)));
    if (i >= sizes.length) {
        i = sizes.length - 1;
    }
    var val = bytes / Math.pow(1024, i);
    if (val > 999) {
        val = val / 1024;
        i++;
    }
    return val.toPrecision(3) + sizes[i];
}

function valueToNumber(value) {
    return Number(value).toLocaleString();
}

function onTopicNodeComplete(result, obj) {
    if (result === null)
        return;
    var panelSchema = $("#panel-schema").find(".panel-body").get(0);
    panelSchema.innerHTML = "<table class='proplist'><table>";
    var tab = $(panelSchema).find("table").get(0);
    for (var i = 0; i < result.LabeledCountersByGroup.length; ++i) {
        var labeledCountersByGroup = result.LabeledCountersByGroup[i];
        var row = tab.insertRow();
        row.insertCell(-1).innerHTML = labeledCountersByGroup.Group;
        for (var idx = 0; idx < labeledCountersByGroup.LabeledCounter.length; ++idx) {
            var row = tab.insertRow();
            row.insertCell(-1).innerHTML = labeledCountersByGroup.LabeledCounter[idx].Name;
            row.insertCell(-1).innerHTML = labeledCountersByGroup.LabeledCounter[idx].Value;
        }
    }
}

function onTreeNodeComplete(result, obj) {
    if (result === null)
        return;
    var panelInfo = $("#panel-info").find(".panel-body").get(0);
    var panelSchema = $("#panel-schema").find(".panel-body").get(0);
    var panelTablets = $("#panel-tablets").find(".panel-body").get(0);
    panelInfo.innerHTML = "<table class='proplist'><table>";
    var tab = $(panelInfo).find("table").get(0);
    var row;
    var name;
    var value;
    if (result.Path !== undefined) {
        row = tab.insertRow();
        name = row.insertCell(-1);
        value = row.insertCell(-1);
        name.innerHTML = "Path";
        value.innerHTML = result.Path;
        Parameters.path = result.Path;
        window.location.hash = $.param(Parameters);
    }

    panelSchema.innerHTML = "";
    panelTablets.innerHTML = "";
    var dataSize = null;
    SchemaTabletElements = {};
    if (result.PathDescription !== undefined) {
        var table = result.PathDescription.Table;
        if (table !== undefined) {
            var schemaHtml = "<table class='schema-table'><tr><th>&#128273;</th><th>Id</th><th>Type</th><th>Name</th></tr>";
            var colLen = table.Columns.length;
            var column;
            var key;
            for (var i = 0; i < colLen; i++) {
                column = table.Columns[i];
                if (table.KeyColumnIds.find(function(n){return n === column.Id;}) !== undefined) {
                    key = "&#10003;";
                } else {
                    key = "&nbsp;"
                }
                schemaHtml += "<tr><td>" + key + "</td><td>" + column.Id + "</td><td>" + column.Type + "</td><td>" + column.Name + "</td></tr>";
            }
            schemaHtml += "</table>";
            panelSchema.innerHTML = schemaHtml;

            var indexes = table.TableIndexes;
            if (indexes !== undefined) {
                var ilen = indexes.length;
                var schemaHtml = "<br><table class='schema-table'><tr><th>IndexName</th><th>Columns</th></tr>";
                for (var i = 0; i < ilen; i++) {
                    schemaHtml += "<tr><td>" + indexes[i].Name + "</td><td>";
                    var jlen = indexes[i].KeyColumnNames.length;
                    for (var j = 0; j < jlen; j++) {
                        schemaHtml += indexes[i].KeyColumnNames[j];
                        if (j < jlen - 1) {
                            schemaHtml += ", ";
                        }
                    }
                    schemaHtml += "</td></tr>"
                }
                schemaHtml += "</table>";
                panelSchema.innerHTML += schemaHtml;
            }

            var cdcStreams = table.CdcStreams;
            if (cdcStreams !== undefined) {
                var ilen = cdcStreams.length;
                var schemaHtml = "<br><table class='schema-table'><tr><th>StreamName</th><th>Mode</th></tr>";
                for (var i = 0; i < ilen; i++) {
                    schemaHtml += "<tr>";
                    schemaHtml += "<td>" + cdcStreams[i].Name + "</td>";
                    schemaHtml += "<td>" + cdcStreams[i].Mode + "</td>";
                    schemaHtml += "</tr>"
                }
                schemaHtml += "</table>";
                panelSchema.innerHTML += schemaHtml;
            }
        }

        var tabLen;
        var tablet;
        var partitions = result.PathDescription.TablePartitions;
        if (partitions !== undefined) {
            tabLen = partitions.length;
            for (var j = 0; j < tabLen; j++) {
                tablet = String(partitions[j].DatashardId);
                SchemaTabletElements[tablet] = null;
            }
        }
        var pqgroup = result.PathDescription.PersQueueGroup;
        if (pqgroup !== undefined) {
            var pqpartitions = result.PathDescription.PersQueueGroup.Partitions;
            if (pqpartitions !== undefined) {
                panelSchema.innerHTML = "";
                $.ajax({
                           url: "json/topicinfo?path=" + obj.id,
                           success: function(result) { onTopicNodeComplete(result, obj); }
                       });
                var partElement;
                tabLen = pqpartitions.length;
                for (var k = 0; k < tabLen; k++) {
                    tablet = String(pqpartitions[k].TabletId);
                    SchemaTabletElements[tablet] = null;
                }
            }
            if (pqgroup.BalancerTabletID !== undefined) {
                SchemaTabletElements[pqgroup.BalancerTabletID] = null;
            }
        }
        if (result.PathDescription.Self.PathType === 4 || result.PathDescription.Self.PathType === 10) {
            var subdomain = result.PathDescription.DomainDescription;
            if (subdomain !== undefined) {
                var params = subdomain.ProcessingParams;
                if (params !== undefined) {
                    if (params.Coordinators !== undefined) {
                        tabLen = params.Coordinators.length;
                        for (var k = 0; k < tabLen; k++) {
                            tablet = String(params.Coordinators[k]);
                            SchemaTabletElements[tablet] = null;
                        }
                    }
                    if (params.Mediators !== undefined) {
                        tabLen = params.Mediators.length;
                        for (var k = 0; k < tabLen; k++) {
                            tablet = String(params.Mediators[k]);
                            SchemaTabletElements[tablet] = null;
                        }
                    }
                    if (params.SchemeShard !== undefined) {
                        tablet = String(params.SchemeShard);
                        SchemaTabletElements[tablet] = null;
                    }
                }
            }
        }
        if (result.PathDescription.Self.PathType === 12) {
            var olapStore = result.PathDescription.OlapStoreDescription;
            if (olapStore !== undefined) {
                if (olapStore.OlapShards !== undefined) {
                    tabLen = olapStore.OlapShards.length;
                    for (var k = 0; k < tabLen; k++) {
                        tablet = String(olapStore.OlapShards[k]);
                        SchemaTabletElements[tablet] = null;
                    }
                }
                if (olapStore.ColumnShards !== undefined) {
                    tabLen = olapStore.ColumnShards.length;
                    for (var k = 0; k < tabLen; k++) {
                        tablet = String(olapStore.ColumnShards[k]);
                        SchemaTabletElements[tablet] = null;
                    }
                }
            }
        }
    }

    if (result.PathDescription.Self.PathType === 7) {
        tablet = String(result.PathDescription.Kesus.KesusTabletId);
        SchemaTabletElements[tablet] = null;

        row = tab.insertRow();
        name = row.insertCell(-1);
        value = row.insertCell(-1);
        name.innerHTML = "PathId";
        value.innerHTML = String(result.PathDescription.Kesus.PathId);

        row = tab.insertRow();
        name = row.insertCell(-1);
        value = row.insertCell(-1);
        name.innerHTML = "KesusTabletId";
        value.innerHTML = String(result.PathDescription.Kesus.KesusTabletId);

        row = tab.insertRow();
        name = row.insertCell(-1);
        value = row.insertCell(-1);
        name.innerHTML = "Version";
        value.innerHTML = String(result.PathDescription.Kesus.Version);

        row = tab.insertRow();
        name = row.insertCell(-1);
        value = row.insertCell(-1);
        name.innerHTML = "Self check period (ms)";
        value.innerHTML = String(result.PathDescription.Kesus.Config.self_check_period_millis);

        row = tab.insertRow();
        name = row.insertCell(-1);
        value = row.insertCell(-1);
        name.innerHTML = "Session grace period (ms)";
        value.innerHTML = String(result.PathDescription.Kesus.Config.session_grace_period_millis);

        row = tab.insertRow();
        name = row.insertCell(-1);
        value = row.insertCell(-1);
        name.innerHTML = "Read consistency mode";
        value.innerHTML = String(result.PathDescription.Kesus.Config.read_consistency_mode);

        row = tab.insertRow();
        name = row.insertCell(-1);
        value = row.insertCell(-1);
        name.innerHTML = "Attach consistency mode";
        value.innerHTML = String(result.PathDescription.Kesus.Config.attach_consistency_mode);
    }

    if (Object.keys(SchemaTabletElements).length !== 0) {
        panelTablets.innerHTML = "<img src='throbber.gif'></img>";
        row = tab.insertRow();
        name = row.insertCell(-1);
        value = row.insertCell(-1);
        name.innerHTML = "Tablets";
        value.innerHTML = Object.keys(SchemaTabletElements).length;
        value.id = "schema-tablets";
        onTreeNodeTabletsComplete();
    }

    if (result.PathDescription !== undefined && result.PathDescription.Children !== undefined
        && result.PathDescription.Self.PathType !== 4 && result.PathDescription.Self.PathType !== 12) {
        onTreeNodeChildrenComplete(result.Path, result.PathDescription.Children);
    }

    if (result.PathDescription.TablePartitions !== undefined) {
        row = tab.insertRow();
        name = row.insertCell(-1);
        value = row.insertCell(-1);
        name.innerHTML = "Partitions";
        value.innerHTML = result.PathDescription.TablePartitions.length;
    }

    if (result.PathDescription.TabletMetrics !== undefined) {
        var metrics = result.PathDescription.TabletMetrics;
        if (metrics.CPU && Number(metrics.CPU)) {
            row = tab.insertRow(-1);
            row.insertCell(-1).innerHTML = "CPU";
            row.insertCell(-1).innerHTML = (metrics.CPU / 10000).toFixed(2) + "%"; // 1sec (1000000uS) per 1sec = 100%
        }
        if (metrics.Memory && Number(metrics.Memory)) {
            row = tab.insertRow(-1);
            row.insertCell(-1).innerHTML = "Memory";
            row.insertCell(-1).innerHTML = bytesToSize(metrics.Memory);
        }
        if (metrics.Network && Number(metrics.Network)) {
            row = tab.insertRow(-1);
            row.insertCell(-1).innerHTML = "Network";
            row.insertCell(-1).innerHTML = bytesToSize(metrics.Network) + "/s";
        }
        if (metrics.ReadThroughput && Number(metrics.ReadThroughput)) {
            row = tab.insertRow(-1);
            row.insertCell(-1).innerHTML = "Read";
            row.insertCell(-1).innerHTML = bytesToSize(metrics.ReadThroughput) + "/s";
        }
        if (metrics.WriteThroughput && Number(metrics.WriteThroughput)) {
            row = tab.insertRow(-1);
            row.insertCell(-1).innerHTML = "Write";
            row.insertCell(-1).innerHTML = bytesToSize(metrics.WriteThroughput) + "/s";
        }
    }

    if (result.PathDescription.Self.PathType === 2 || result.PathDescription.Self.PathType === 3) {
        row = tab.insertRow(-1);
        row.insertCell(-1).innerHTML = "Data Size";
        var dataSizeCell = row.insertCell(-1);
        if (result.PathDescription.TabletMetrics !== undefined && result.PathDescription.TabletMetrics.Storage !== undefined) {
            dataSizeCell.innerHTML = bytesToSize(result.PathDescription.TabletMetrics.Storage);
        } else {
            dataSizeCell.innerHTML = "<img src='throbber.gif'></img>";
            (function(cell){
                $.ajax({
                           url: "json/tabletcounters?path=" + obj.id,
                           success: function(result) { onTreeNodeChildrenDataSizeComplete(cell, result); }
                       });
            })(dataSizeCell);
        }
    }

    if (result.PathDescription.TableStats !== undefined) {
        var stats = result.PathDescription.TableStats;
        if (stats.DataSize && Number(stats.DataSize)) {
            row = tab.insertRow(-1);
            row.insertCell(-1).innerHTML = "Size";
            row.insertCell(-1).innerHTML = bytesToSize(stats.DataSize);
        }
        if (stats.RowCount && Number(stats.RowCount)) {
            row = tab.insertRow(-1);
            row.insertCell(-1).innerHTML = "Rows";
            row.insertCell(-1).innerHTML = valueToNumber(stats.RowCount);
        }
        if (stats.LastAccessTime && Number(stats.LastAccessTime)) {
            row = tab.insertRow(-1);
            row.insertCell(-1).innerHTML = "Last Access";
            row.insertCell(-1).innerHTML = (new Date(Number(stats.LastAccessTime))).toISOString();
        }
        if (stats.LastUpdateTime && Number(stats.LastUpdateTime)) {
            row = tab.insertRow(-1);
            row.insertCell(-1).innerHTML = "Last Update";
            row.insertCell(-1).innerHTML = (new Date(Number(stats.LastUpdateTime))).toISOString();
        }
        if (stats.ImmediateTxCompleted && Number(stats.ImmediateTxCompleted)) {
            row = tab.insertRow(-1);
            row.insertCell(-1).innerHTML = "Immediate Tx Completed";
            row.insertCell(-1).innerHTML = valueToNumber(stats.ImmediateTxCompleted);
        }
        if (stats.PlannedTxCompleted && Number(stats.PlannedTxCompleted)) {
            row = tab.insertRow(-1);
            row.insertCell(-1).innerHTML = "Planned Tx Completed";
            row.insertCell(-1).innerHTML = valueToNumber(stats.PlannedTxCompleted);
        }
        if (stats.TxRejectedByOverload && Number(stats.TxRejectedByOverload)) {
            row = tab.insertRow(-1);
            row.insertCell(-1).innerHTML = "Tx Rejected by Overload";
            row.insertCell(-1).innerHTML = valueToNumber(stats.TxRejectedByOverload);
        }
        if (stats.TxRejectedBySpace && Number(stats.TxRejectedBySpace)) {
            row = tab.insertRow(-1);
            row.insertCell(-1).innerHTML = "Tx Rejected by Space";
            row.insertCell(-1).innerHTML = valueToNumber(stats.TxRejectedBySpace);
        }
        if (stats.TxCompleteLagMsec && Number(stats.TxCompleteLagMsec)) {
            row = tab.insertRow(-1);
            row.insertCell(-1).innerHTML = "Tx completion lag (msec)";
            row.insertCell(-1).innerHTML = valueToNumber(stats.TxCompleteLagMsec);
        }
        if (stats.InFlightTxCount && Number(stats.InFlightTxCount)) {
            row = tab.insertRow(-1);
            row.insertCell(-1).innerHTML = "In flight Tx count";
            row.insertCell(-1).innerHTML = valueToNumber(stats.InFlightTxCount);
        }

        if (stats.RowUpdates && Number(stats.RowUpdates)) {
            row = tab.insertRow(-1);
            row.insertCell(-1).innerHTML = "Row Updates";
            row.insertCell(-1).innerHTML = valueToNumber(stats.RowUpdates);
        }
        if (stats.RowDeletes && Number(stats.RowDeletes)) {
            row = tab.insertRow(-1);
            row.insertCell(-1).innerHTML = "Row Deletes";
            row.insertCell(-1).innerHTML = valueToNumber(stats.RowDeletes);
        }
        if (stats.RowReads && Number(stats.RowReads)) {
            row = tab.insertRow(-1);
            row.insertCell(-1).innerHTML = "Row Reads";
            row.insertCell(-1).innerHTML = valueToNumber(stats.RowReads);
        }
        if (stats.RangeReads && Number(stats.RangeReads)) {
            row = tab.insertRow(-1);
            row.insertCell(-1).innerHTML = "Range Reads";
            row.insertCell(-1).innerHTML = valueToNumber(stats.RangeReads);
        }
        if (stats.RangeReadRows && Number(stats.RangeReadRows)) {
            row = tab.insertRow(-1);
            row.insertCell(-1).innerHTML = "Range Read Rows";
            row.insertCell(-1).innerHTML = valueToNumber(stats.RangeReadRows);
        }
    }

    if (result.PathDescription.PersQueueGroup !== undefined) {
        row = tab.insertRow();
        name = row.insertCell(-1);
        value = row.insertCell(-1);
        name.innerHTML = "Partitions";
        value.innerHTML = result.PathDescription.PersQueueGroup.TotalGroupCount;

        var partitionConfig = result.PathDescription.PersQueueGroup.PQTabletConfig.PartitionConfig;

        row = tab.insertRow();
        name = row.insertCell(-1);
        value = row.insertCell(-1);
        name.innerHTML = "Max Count";
        value.innerHTML = partitionConfig.MaxCountInPartition;

        row = tab.insertRow();
        name = row.insertCell(-1);
        value = row.insertCell(-1);
        name.innerHTML = "Max Size";
        value.innerHTML = bytesToSize(partitionConfig.MaxSizeInPartition);
        value.title = partitionConfig.MaxSizeInPartition;

        if (partitionConfig.ImportantClientId !== undefined) {
            row = tab.insertRow();
            name = row.insertCell(-1);
            value = row.insertCell(-1);
            name.innerHTML = "Important Client Id";
            value.innerHTML = partitionConfig.ImportantClientId;
        }

        row = tab.insertRow();
        name = row.insertCell(-1);
        value = row.insertCell(-1);
        name.innerHTML = "Life Time";
        value.innerHTML = partitionConfig.LifetimeSeconds + " seconds";
    }

    if ((result.PathDescription.Self.PathType === 10 || result.PathDescription.Self.PathType === 4) && result.PathDescription.DomainDescription !== undefined) {
        if (result.PathDescription.DomainDescription.ProcessingParams !== undefined) {
            if (result.PathDescription.DomainDescription.ProcessingParams.Version !== undefined) {
                row = tab.insertRow();
                row.insertCell(-1).innerHTML = "SubdomainVersion";
                row.insertCell(-1).innerHTML = result.PathDescription.DomainDescription.ProcessingParams.Version;
            }
            if (result.PathDescription.DomainDescription.ProcessingParams.PlanResolution !== undefined) {
                row = tab.insertRow();
                row.insertCell(-1).innerHTML = "Plan Resolution";
                row.insertCell(-1).innerHTML = result.PathDescription.DomainDescription.ProcessingParams.PlanResolution + "ms";
            }
        }
        if (result.PathDescription.DomainDescription.DomainKey !== undefined) {
            row = tab.insertRow();
            row.insertCell(-1).innerHTML = "DomainKey";
            row.insertCell(-1).innerHTML = result.PathDescription.DomainDescription.DomainKey.SchemeShard + " : " + result.PathDescription.DomainDescription.DomainKey.PathId;
        }
        if (result.PathDescription.DomainDescription.ResourcesDomainKey !== undefined) {
            row = tab.insertRow();
            row.insertCell(-1).innerHTML = "ResourcesDomainKey";
            row.insertCell(-1).innerHTML = result.PathDescription.DomainDescription.ResourcesDomainKey.SchemeShard + " : " + result.PathDescription.DomainDescription.ResourcesDomainKey.PathId;
        }
        if (result.PathDescription.DomainDescription.PathsInside !== undefined) {
            row = tab.insertRow();
            row.insertCell(-1).innerHTML = "Pathes inside";
            row.insertCell(-1).innerHTML = result.PathDescription.DomainDescription.PathsInside + " / " + result.PathDescription.DomainDescription.PathsLimit;
        }
        if (result.PathDescription.DomainDescription.ShardsInside !== undefined) {
            row = tab.insertRow();
            row.insertCell(-1).innerHTML = "Shards inside";
            row.insertCell(-1).innerHTML = result.PathDescription.DomainDescription.ShardsInside + " / " + result.PathDescription.DomainDescription.ShardsLimit;
        }
        if (result.PathDescription.DomainDescription.StoragePools !== undefined) {
            var storagePools = result.PathDescription.DomainDescription.StoragePools;
            tabLen = storagePools.length;
            for (var j = 0; j < tabLen; j++) {
                row = tab.insertRow();
                row.insertCell(-1).innerHTML = "Storage pool";
                row.insertCell(-1).innerHTML = storagePools[j].Kind + " -> " + storagePools[j].Name;

            }
        }
    }

    if (result.PathDescription.BackupProgress !== undefined) {
        row = tab.insertRow();
        name = row.insertCell(-1);
        value = row.insertCell(-1);
        name.innerHTML = "Backup progress";

        var total = result.PathDescription.BackupProgress.Total;
        var complete = total -
            result.PathDescription.BackupProgress.NotCompleteYet;
        var procent = Math.round(complete * 100 / total);
        value.innerHTML = complete + "/" + total + " (" + procent + "%)";
    }

    var backupResultList = result.PathDescription.LastBackupResult;
    if (backupResultList !== undefined && backupResultList.length > 0) {
        var backupResult = backupResultList[backupResultList.length - 1];
        if (backupResult.CompleteTimeStamp !== undefined) {
            row = tab.insertRow();
            name = row.insertCell(-1);
            value = row.insertCell(-1);
            name.innerHTML = "Last backup time";
            var timestampMS = backupResult.CompleteTimeStamp * 1000;
            var dateObj = new Date(timestampMS);
            value.innerHTML = dateObj.toLocaleString();
        }

        if (backupResult.ErrorCount !== undefined) {
            row = tab.insertRow();
            name = row.insertCell(-1);
            value = row.insertCell(-1);
            name.innerHTML = "Last backup error count";
            value.innerHTML = backupResult.ErrorCount;
        }
    }
}

function onTreeNodeSelected(e, data) {
    var obj = data.node;
    $.ajax({
               url: "json/describe?path=" + obj.id,
               success: function(result) { onTreeNodeComplete(result, obj); }
           });
}

function onTreeData(obj, cb) {
    if (obj.id === "#") {
        cb.call(this, {
                    id: "/",
                    parent: "#",
                    text: "/",
                    children: true,
                    icon: "glyphicon glyphicon-cloud schema-good",
                    data: function (obj1, cb1) { onTreeData(obj1, cb1); }
                    });
    } else {
        $.ajax({
                   url: "json/describe?path=" + obj.id,
                   success: function(result) { onTreeDataComplete(result, obj, cb); },
                   error: function(result) { cb.call(this, null); }
               });
    }
}

function onExecSqlResult(result) {
    $("#panel-sql-button-run").html("<span class='glyphicon glyphicon-play'></span>");
    $("#panel-sql-result").show();
    $("#panel-sql-error").hide();
    var table = $("#sql-result")[0];
    $("#sql-result th").remove();
    $("#sql-result tr").remove();
    var header = table.createTHead();
    var header_row = header.insertRow(0);
    if (result.length > 0) {
        for (var prop in result[0]) {
            var cell = document.createElement("TH");
            header_row.appendChild(cell);
            cell.innerHTML = prop;
        }
    }
    for (var idx = 0; idx < result.length; ++idx) {
        var row = table.insertRow(-1);
        for (var prop in result[idx]) {
            var cell = document.createElement("TD");
            row.appendChild(cell);
            cell.innerHTML = result[idx][prop];
        }
    }
}

function onExecSqlError(result) {
    $("#panel-sql-button-run").html("<span class='glyphicon glyphicon-play'></span>");
    $("#panel-sql-result").hide();
    $("#panel-sql-error").show();
    var text = "?";
    if (result !== null) {
        if (result.responseText !== undefined) {
            text = result.responseText;
            // TODO(xenoxeno): return only ErrorReason part
        }
    }
    $("#sql-error").text(text);
}

function onExecSql() {
    var sql = $("#panel-sql-text").val();
    $("#panel-sql-button-run").html("<img src='throbber.gif'></img>");
    $.ajax({
               type: "POST",
               url: 'json/query',
               data: JSON.stringify({
                   query: sql
               }),
               contentType: "application/json; charset=utf-8",
               dataType: "json",
               success: onExecSqlResult,
               error: onExecSqlError
           });
}

function main() {
    $.ajax({
               url: "json/nodelist",
               success: function(result) { onNodeList(result); },
               error: function() { onNodeList(null); }
           });
    $(function () { $('#schema-tree').jstree({ core: { data: function (obj, cb) { onTreeData(obj, cb); } } }); });
    $("#schema-tree").on("select_node.jstree", function(e, data) { onTreeNodeSelected(e, data); });
    $("#panel-sql-button-run").on("click", onExecSql);

    if (Parameters.path !== undefined) {
        $("#schema-tree").on('loaded.jstree', function() {
            OpeningPath = Parameters.path;
            $("#schema-tree").jstree('open_node', '/');
        });
    }
}
