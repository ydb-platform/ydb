#pragma once

#include <util/string/type.h>

namespace NKikimr::NVisual {

const std::string_view FG_SVG_HEADER = R"scr(<?xml version="1.0" standalone="no"?>
<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">
<svg version="1.1" width="%.2f" height="%.2f" onload="init(evt)" viewBox="0 0 %.2f %.2f" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">
<defs><linearGradient id="background" y1="0" y2="1" x1="0" x2="0"><stop stop-color="#eeeeee" offset="5%%"/><stop stop-color="#eeeeb0" offset="95%%"/>
</linearGradient></defs>
<style type="text/css">
.graphElement:hover { stroke:black; stroke-width:0.5; cursor:pointer; }
.TaskNavigationButton:hover {cursor:pointer; }
</style>" )scr";

const std::string_view FG_SVG_FOOTER = R"scr(</svg>)scr";

const std::string_view FG_SVG_BACKGROUND = R"scr(<rect x="0" y="0" width="%.2f" height="%.2f" fill="url(#background)" />)scr";

const std::string_view FG_SVG_TITLE = R"scr(<text text-anchor="middle" x="600.00"
        y="%.2f" font-size="17" font-family="Verdana" fill="rgb(0, 0, 0)">%s</text>)scr";

const std::string_view FG_SVG_INFO_BAR = R"scr(
<text id="infoBar_%s" text-anchor="left" x="10.00" y="%.2f" font-size="12" font-family="Verdana" fill="rgb(0, 0, 0)"> </text>)scr";

const std::string_view FG_SVG_RESET_ZOOM = R"scr(
<text
        id="resetZoom" onclick="resetZoom()" style="opacity:0.0;cursor:pointer" text-anchor="left" x="10.00" y="24.00"
        font-size="12" font-family="Verdana" fill="rgb(0, 0, 0)">Reset Zoom</text>)scr";

const std::string_view FG_SVG_SEARCH = R"scr(
        <text id="search" onmouseover="onSearchHover()" onmouseout="onSearchOut()" onclick="startSearch()" style="opacity:0.1;cursor:pointer"
        text-anchor="left" x="%.2f" y="24.00" font-size="12" font-family="Verdana"
        fill="rgb(0, 0, 0)">Search</text><text id="matched" text-anchor="left" x="1090.00" y="1637.00" font-size="12"
        font-family="Verdana" fill="rgb(0, 0, 0)"> </text> )scr";

const std::string_view FG_SVG_GRAPH_ELEMENT = R"scr(
<g class="graphElement" onmouseover="onGraphMouseOver(this)" onmouseout="onGraphMouseOut(this)" onclick="zoom(this)">
    <title>%s</title><rect data-type="%s" stage-id="%u" x="%.2f" y="%.2f" width="%.2f" height="%.2f" fill="%s" />
    <text text-anchor="left" x="%.2f" y="%.2f" font-size="12" font-family="Verdana" fill="rgb(0, 0, 0)">%s</text>
</g>)scr";

const std::string_view FG_SVG_TASK_PROFILE_ELEMENT = R"scr(
<g class="taskProfile-%s-%u" onmouseover="onGraphMouseOver(this)" onmouseout="onGraphMouseOut(this)" onclick="showTasks(this)">
    <title>%s</title><rect data-type="%s" data-weight="%f" x="%.2f" y="%.2f" width="%.2f" height="%.2f" fill="rgb(139, 174, %d)" />
    <text text-anchor="left" x="%.2f" y="%.2f" font-size="12" font-family="Verdana" fill="rgb(0, 0, 0)">%s</text>
</g>)scr";

const std::string_view FG_SVG_TASK_PROFILE_BACKGROUND = R"scr(
<g class="taskBackground" style="display:none">
    <rect x="0" y="0" width="0" height="0" fill="url(#background)" />
    <rect x="0.00" y="0" width="0" height="0" fill="#cee7e9" />
    <text id="TaskTotal" text-anchor="left" x="10.00" y="0" font-size="12" font-family="Verdana" fill="rgb(0, 0, 0)"></text>
    <g class="TaskNavigationButton" onclick="goToFirstTask()" >
        <rect  x="10.00" y="0" width="36.00" height="15" fill="rgb(0, 200, 200)" style="display:none"/>
        <text text-anchor="middle" x="28.00" y="810" font-size="12" font-family="Verdana" fill="rgb(0, 0, 0)" style="display:none">&lt;&lt;</text>
    </g>
    <g class="TaskNavigationButton" onclick="goToPre≤vTask()" >
        <rect  x="50.00" y="0" width="36.00" height="15" fill="rgb(0, 200, 200)" style="display:none"/>
        <text text-anchor="middle" x="68.00" y="810" font-size="12" font-family="Verdana" fill="rgb(0, 0, 0)" style="display:none">&lt;</text>
    </g>
    <g class="TaskNavigationButton" onclick="goToNextTask()" >
        <rect  x="90.00" y="0" width="36.00" height="15" fill="rgb(0, 200, 200)" style="display:none"/>
        <text text-anchor="middle" x="108.00" y="810" font-size="12" font-family="Verdana" fill="rgb(0, 0, 0)" style="display:none">&gt;</text>
    </g>
    <g class="TaskNavigationButton" onclick="goToLastTask()" >
        <rect  x="130.00" y="0" width="36.00" height="15" fill="rgb(0, 200, 200)" style="display:none"/>
        <text text-anchor="middle" x="148.00" y="810" font-size="12" font-family="Verdana" fill="rgb(0, 0, 0)" style="display:none">&gt;&gt;</text>
    </g>
    <g class="TaskNavigationButton" onclick="hideTasks()" >
        <rect  x="210.00" y="0" width="60.00" height="15" fill="rgb(0, 200, 200)" style="display:none"/>
        <text text-anchor="middle" x="240.00" y="810" font-size="12" font-family="Verdana" fill="rgb(0, 0, 0)" style="display:none">Close</text>
    </g>
</g>)scr";

const std::string_view FG_SVG_SCRIPT = R"scr(
<script type="text/ecmascript"><![CDATA[var nametype = 'Function:';
var fontSize = 12;
var fontWidth = 0.59;
var xpad = 10;
var tasksPerPage = 50;
var taskListOffset = 0;
var activeTaskStageId = -1;
var activeTaskDataType = "";
]]><![CDATA[var infoBar, searchButton, foundText, svg;
function init(evt) {
    searchButton = document.getElementById("search");
    foundText = document.getElementById("matched");
    svg = document.getElementsByTagName("svg")[0];
    searching = 0;
}
// Show element title in bottom bar
function onGraphMouseOver(node) {		// show
    info = getNodeTitle(node);
    dataType = getNodeDataType(node)

    var infoBar = document.getElementById("infoBar_" + dataType).firstChild;
    infoBar.nodeValue = info;
}

function onGraphMouseOut(node) {			// clear
    dataType = getNodeDataType(node)

    var infoBar = document.getElementById("infoBar_" + dataType).firstChild;
    infoBar.nodeValue = ' ';
}
// ctrl-F for search
window.addEventListener("keydown",function (e) {
        if (e.keyCode === 114 || (e.ctrlKey && e.keyCode === 70)) {
            e.preventDefault();
            startSearch();
        }
})
// helper functions
function findElement(parent, name, attr) {
    var children = parent.childNodes;
    for (var i=0; i<children.length;i++) {

        if (children[i].tagName == name)
            return (attr != undefined) ? children[i].attributes[attr].value : children[i];
    }
    return;
}

function backupAttribute(element, attr, mark, value) {
    if (element.attributes[attr] == undefined) return;
    if (element.attributes[attr + ".bk" + mark] == undefined){
        oldValue = element.attributes[attr].value;
        element.setAttribute(attr + ".bk" + mark, oldValue);
    }
    if (value != undefined) {
        element.setAttribute(attr, value);
    }
}
function restoreAttribute(element, attr, mark) {
    if (element.attributes[attr + ".bk" + mark] == undefined) return;
    element.attributes[attr].value = element.attributes[attr + ".bk" + mark].value;
    element.removeAttribute(attr + ".bk" + mark);
}
function getNodeTitle(element) {
    var text = findElement(element, "title").firstChild.nodeValue;
    return (text)
}
function getNodeDataType(element) {
    var text = findElement(element, "rect", "data-type");
    return text
}

function adjustText(element) {
    if(findElement(element, "title") == undefined) {
        return;
    }

    var textElement = findElement(element, "text");
    var rect = findElement(element, "rect");
    var width = parseFloat(rect.attributes["width"].value) - 3;
    var title = findElement(element, "title").textContent.replace(/\\([^(]*\\)\$/,"");
    textElement.attributes["x"].value = parseFloat(rect.attributes["x"].value) + 3;
    // Not enough space for any text
    if (2*fontSize*fontWidth > width) {
        textElement.textContent = "";
        return;
    }
    textElement.textContent = title;
    // Enough space for full text
    if (/^ *\$/.test(title) || textElement.getSubStringLength(0, title.length) < width) {
        return;
    }

    for (var x=title.length-2; x>0; x--) {
        if (textElement.getSubStringLength(0, x+2) <= width) {
            textElement.textContent = title.substring(0,x) + "..";
            return;
        }
    }
    textElement.textContent = "";
}

// Zoom processing
function zoomChild(element, x, ratio) {
    if (element.attributes != undefined) {
        if (element.attributes["x"] != undefined) {
            backupAttribute(element, "x", "zoom", (parseFloat(element.attributes["x"].value) - x - xpad) * ratio + xpad);
            if(element.tagName == "text") element.attributes["x"].value = findElement(element.parentNode, "rect", "x") + 3;
        }
        if (element.attributes["width"] != undefined) {
            backupAttribute(element, "width", "zoom", parseFloat(element.attributes["width"].value) * ratio);
        }
    }
    if (element.childNodes == undefined) return;
    for(var i=0, child=element.childNodes; i<child.length; i++) {
        zoomChild(child[i], x - xpad, ratio);
    }
}
function zoomParent(element) {
    if (element.attributes) {
        if (element.attributes["x"] != undefined) {
            backupAttribute(element, "x", "zoom", xpad);
        }
        if (element.attributes["width"] != undefined) {
            backupAttribute(element, "width", "zoom", parseInt(svg.width.baseVal.value) - (xpad*2));
        }
    }
    if (element.childNodes == undefined) return;
    for(var i=0, child=element.childNodes; i<child.length; i++) {
        zoomParent(child[i]);
    }
}

function zoomElement(element, type, xmin, xmax, ymin, ratio, overrideOnClick) {
    var rect = findElement(element, "rect").attributes;

    if(rect["data-type"].value != type) {
        return;
    }

    var currentX = parseFloat(rect["x"].value);
    var currentWidth = parseFloat(rect["width"].value);
    var comparisonOffset = 0.0001;

    if (parseFloat(rect["y"].value) > ymin) {
        if (currentX <= xmin && (currentX+currentWidth+comparisonOffset) >= xmax) {
            element.style["opacity"] = "0.5";
            zoomParent(element);
            if(overrideOnClick && element.onclick) {
                element.onclick = function(element){resetZoom(); zoom(this);};
            }
            adjustText(element);
        }
        else {
            element.style["display"] = "none";
        }
    }
    else {
        if (currentX < xmin || currentX + comparisonOffset >= xmax) {
            element.style["display"] = "none";
        }
        else {
            zoomChild(element, xmin, ratio);
            if(overrideOnClick && element.onclick) {
                element.onclick = function(element){zoom(this);};
            }
            adjustText(element);
        }
    }
}

function zoom(node) {
    var attr = findElement(node, "rect").attributes;
    var type = attr["data-type"].value
    var width = parseFloat(attr["width"].value);
    var xmin = parseFloat(attr["x"].value);
    var xmax = parseFloat(xmin + width);
    var ymin = parseFloat(attr["y"].value);
    var ratio = (svg.width.baseVal.value - 2*xpad) / width;
    var resetZoomBtn = document.getElementById("resetZoom");
    resetZoomBtn.style["opacity"] = "1.0";
    var el = document.getElementsByClassName("graphElement");
    for(var i=0;i<el.length;i++){
        zoomElement(el[i], type, xmin, xmax, ymin, ratio, true)
    }

    el = document.getElementsByTagName("g");
    for (var i=0; i < el.length; i++) {
        className = el[i].attributes["class"].value;
        if(!className.startsWith("taskProfile"))
        {
            continue;
        }
        zoomElement(el[i], type, xmin, xmax, ymin, ratio, false)
    }
}

function resetElementZoom(element) {
    if (element.attributes != undefined) {
        restoreAttribute(element, "x", "zoom");
        restoreAttribute(element, "width", "zoom");
    }

    if (element.childNodes == undefined) return;
    for(var i=0, children=element.childNodes; i<children.length; i++) {
        resetElementZoom(children[i]);
    }
}

function resetZoom() {
    var resetZoomBtn = document.getElementById("resetZoom");
    resetZoomBtn.style["opacity"] = "0.0";
    var element = document.getElementsByTagName("g");

    for(i=0;i<element.length;i++) {
        element[i].style["display"] = "block";
        element[i].style["opacity"] = "1";
        resetElementZoom(element[i]);
        adjustText(element[i]);
    }
}

// Floating task view

function getElementByStageId(stageId) {
    var el = document.getElementsByClassName("graphElement");

    for (var i=0; i < el.length; i++) {
        rect = findElement(el[i], "rect")
        stageIdAttr = rect.attributes['stage-id']
        if( stageIdAttr != undefined && stageIdAttr.value == stageId)
        {
            return el[i]
        }
    }
}

function showNavigationButtons(tasksZoneBottom) {
    buttons = document.getElementsByClassName("TaskNavigationButton");
    for( var i=0; i< buttons.length; i++)
    {
        rect = findElement(buttons[i], "rect")
        rect.attributes["y"].value = tasksZoneBottom - 15
        rect.style["display"] = "block"

        text = findElement(buttons[i], "text")
        text.attributes["y"].value = tasksZoneBottom - 5
        text.style["display"] = "block"
    }
}

function hideNavigationButtons(tasksZoneBottom) {
    buttons = document.getElementsByClassName("TaskNavigationButton");
    for( var i=0; i< buttons.length; i++)
    {
        findElement(buttons[i], "rect").style["display"] = "none"
        findElement(buttons[i], "text").style["display"] = "none"
    }
}

function showTasks(node){
    tasks = document.getElementsByTagName("g");
    for(var i = 0; i < tasks.length; i++ )
    {
        className = tasks[i].attributes["class"].value;
        if(className.startsWith("taskProfile"))
        {
            tasks[i].style["display"] = "none";
        }
    }

    className = node.attributes["class"].value.split('-', 3)
    stageId = className[2];
    dataType = className[1];
    activeTaskStageId = stageId;
    activeTaskDataType = dataType;

    showTasksForStage()
}

function showTaskBackground(taskZoneY, taskZoneWidth, taskZoneHeight) {
    taskBackground = document.getElementsByClassName("taskBackground")[0];
    taskBackground.style["display"] = "block"

    tbRect = taskBackground.children[0]
    tbRect.style["display"] = "block"
    tbRect.attributes["width"].value = svg.width.baseVal.value;
    tbRect.attributes["height"].value = svg.height.baseVal.value;

    tbRect = taskBackground.children[1]
    tbRect.style["display"] = "block"
    tbRect.attributes["y"].value = taskZoneY;
    tbRect.attributes["width"].value = taskZoneWidth;
    tbRect.attributes["height"].value = taskZoneHeight;
    showNavigationButtons(taskZoneHeight + taskZoneY);
}

function hideTaskBackground() {
    taskBackground = document.getElementsByClassName("taskBackground")[0];
    taskBackground.style["display"] = "none"

    tbRect = taskBackground.children[0]
    tbRect.style["display"] = "none"

    tbRect = taskBackground.children[1]
    tbRect.style["display"] = "none"
    hideNavigationButtons();
}

function showTasksForStage(){
    stageId = activeTaskStageId
    stageRect = getElementByStageId(stageId)
    zoom(stageRect)

    taskZoneY = 60;
    taskZoneX = 10
    taskFieldHeight = 15;
    taskZoneWidth = svg.width.baseVal.value;

    tasks = document.getElementsByClassName("taskProfile-" + activeTaskDataType + "-" + stageId);
    maxWeight = parseFloat(findElement(tasks[0], "rect").attributes["data-weight"].value);
    maxWeight = maxWeight == 0 ? 1 : maxWeight;

    tasksNum=(tasksPerPage < tasks.length) ? tasksPerPage : tasks.length;
    taskZoneHeight = (4 + tasksNum) * taskFieldHeight;

    showTaskBackground(taskZoneY, taskZoneWidth, taskZoneHeight);

    if (tasksPerPage >= tasks.length) {
        taskListOffset = 0;
    }
    else if(taskListOffset + tasksPerPage > tasks.length ) {
        taskListOffset = tasks.length - tasksPerPage;
    }

    for (var i=0; i < tasks.length; i++) {
        rect = findElement(tasks[i], "rect");
        text = findElement(tasks[i], "text");

        if(i < taskListOffset || i >= taskListOffset + tasksPerPage) {
            tasks[i].style["display"] = "none"
            continue;
        }

        tasks[i].style["display"] = "block"

        weight = rect.attributes["data-weight"].value;
        heightOffset = i - taskListOffset;
        backupAttribute(rect, "y", "tasks", taskZoneY + (heightOffset + 1) * taskFieldHeight);
        backupAttribute(rect, "x", "tasks", taskZoneX);
        backupAttribute(rect, "width", "tasks", (taskZoneWidth - 10) * weight / maxWeight);

        backupAttribute(text, "y", "tasks", taskZoneY + (heightOffset + 2) * taskFieldHeight - 2);
        backupAttribute(text, "x", "tasks", taskZoneX + 2);
        text.textContent = getNodeTitle(tasks[i]);
    }

    total = document.getElementById("TaskTotal")
    total.style["display"] = "block"
    total.setAttribute("y", taskZoneY + taskZoneHeight - 24);

    lastTask = taskListOffset + tasksPerPage > (tasks.length -1) ? tasks.length : taskListOffset + tasksPerPage + 1;
    total.textContent = "Showing tasks: " + (taskListOffset + 1) + "-" + lastTask + " from " + tasks.length + " from stage " + stageId;
}

function goToFirstTask() {
    taskListOffset = 0;
    showTasksForStage();
}
function goToPrevTask() {
    taskListOffset = taskListOffset < tasksPerPage ? 0 : taskListOffset - tasksPerPage;
    showTasksForStage();
}
function goToNextTask() {
    taskListOffset += tasksPerPage;
    showTasksForStage();
}
function goToLastTask() {
    taskListOffset = Number.MAX_SAFE_INTEGER;
    showTasksForStage();
}

function hideTasks() {
    tasks = document.getElementsByTagName("g");
    for (var i=0; i < tasks.length; i++) {
        className = tasks[i].attributes["class"].value;
        if(!className.startsWith("taskProfile"))
        {
            continue;
        }
        tasks[i].style["display"] = "block"

        rect = findElement(tasks[i], "rect");
        restoreAttribute(rect, "y", "tasks");
        restoreAttribute(rect, "x", "tasks");
        restoreAttribute(rect, "width", "tasks");

        text = findElement(tasks[i], "text");
        restoreAttribute(text, "y", "tasks");
        restoreAttribute(text, "x", "tasks");

        adjustText(tasks[i])
    }
    hideTaskBackground();

    total = document.getElementById("TaskTotal")
    total.style["display"] = "none"

    stageRect = getElementByStageId(activeTaskStageId)
    resetZoom()
    zoom(stageRect)

    activeTaskStageId = -1;
    taskListOffset=0;
}

// search
function dropSearch() {
    var el = document.getElementsByTagName("rect");
    for (var i=0; i < el.length; i++) {
        restoreAttribute(el[i], "fill", "search")
    }
}

function startSearch() {
    if (!searching) {
        var pattern = prompt("Enter a string to search (regexp allowed)", "");
        if (pattern != null) {
            search(pattern)
        }
    } else {
        dropSearch();
        searching = 0;
        searchButton.style["opacity"] = "0.1";
        searchButton.firstChild.nodeValue = "Search"
        foundText.style["opacity"] = "0.0";
        foundText.firstChild.nodeValue = ""
    }
}

function search(pattern) {
    var regex = new RegExp(pattern);
    var graphElements = document.getElementsByTagName("g");
    for (var i = 0; i < graphElements.length; i++) {
        var element = graphElements[i];
        if (element.attributes["class"].value != "graphElement")
            continue;
        var titleFunc = (getNodeTitle(element));
        var rect = findElement(element, "rect");

        if (titleFunc == null || rect == null)
            continue;

        if (titleFunc.match(regex)) {
            backupAttribute(rect, "fill", "search", 'rgb(120,80,230)');
            searching = 1;
        }
    }
    if (searching)
    {
        searchButton.style["opacity"] = "1.0";
        searchButton.firstChild.nodeValue = "Reset Search"
    }
}

function onSearchHover() {
    searchButton.style["opacity"] = "1.0";
}

function onSearchOut() {
    if (searching) {
        searchButton.style["opacity"] = "1.0";
    } else {
        searchButton.style["opacity"] = "0.1";
    }
}
]]></script> )scr";
}
