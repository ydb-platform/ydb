if (typeof jQuery === 'undefined') {
    console.error('jQuery is not loaded! Check the library connection.');
    document.body.innerHTML = '<div style="padding: 20px; color: red; font-weight: bold;">Error: jQuery is not loaded. Check the browser console for more information.</div>';
} else {
    console.log('jQuery version:', jQuery.fn.jquery);
}

let currentData = null;

const poolColors = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", 
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
    "#aec7e8", "#ffbb78", "#98df8a", "#ff9896", "#c5b0d5",
    "#c49c94", "#f7b6d2", "#c7c7c7", "#dbdb8d", "#9edae5"
];

const poolColorMap = {};

const colorScheme = {
    budget: "#007bff",
    lostCpu: "#dc3545",
    freeSharedCpu: "#28a745",
    avgPingUs: "#fd7e14", 
    currentThreadCount: "#6f42c1",
    localQueueSize: "#20c997",
    totalCpu: "#fd7e14",
    totalThreads: "#20c997"
};

let selectedPools = new Set();
let visiblePools = new Set();

let dataContainer, loadingIndicator, errorContainer, refreshButton, levelSelect, iterationFrom, iterationTo, timeMode, timeFrom, timeTo, sortNewestFirstCheckbox;
let chartMetricSelect, poolSelect, poolMetricSelect, refreshChartButton;
let cpuMetricSelect, cpuValueSelect, refreshCpuButton, showAllPoolsCheckbox, poolCheckboxesContainer, poolSelectorContainer;

function recalculateChartSizes() {
    console.log("Forced recalculation of chart sizes...");
    
    const chartElements = document.querySelectorAll('svg[id$="Chart"]');
    
    chartElements.forEach(element => {
        element.style.display = 'block';
        element.style.visibility = 'visible';
        
        const rect = element.getBoundingClientRect();
        console.log(`Element ${element.id} size:`, {
            width: rect.width,
            height: rect.height
        });
    });
    
    if (currentData) {
        const activeTabId = document.querySelector('#mainTabs .nav-link.active').getAttribute('data-bs-target');
        
        if (activeTabId === '#chartsTab') {
            console.log('Redrawing charts on Metrics tab after size change');
            renderMetricsChart();
            renderPoolChart();
        } else if (activeTabId === '#cpuTab') {
            console.log('Redrawing charts on CPU tab after size change');
            renderCpuCharts();
        }
    }
}

$(document).ready(function() {
    console.log("Initializing application...");
    
    if (typeof bootstrap === 'undefined') {
        console.error('Bootstrap is not loaded! Check the connection of bootstrap.js library.');
        $('#mainTabsContent').before('<div class="alert alert-danger">Error: Bootstrap JS is not loaded. Check the browser console for more information.</div>');
    } else {
        console.log('Bootstrap is available');
        
        const tabElements = document.querySelectorAll('#mainTabs button[data-bs-toggle="tab"]');
        if (tabElements.length > 0) {
            tabElements.forEach(tabEl => {
                tabEl.addEventListener('click', function(event) {
                    event.preventDefault();
                    const tabId = this.getAttribute('data-bs-target').substring(1);
                    console.log('Switching to tab:', tabId);
                    
                    document.querySelectorAll('#mainTabs .nav-link').forEach(t => {
                        t.classList.remove('active');
                        t.setAttribute('aria-selected', 'false');
                    });
                    
                    document.querySelectorAll('#mainTabsContent .tab-pane').forEach(p => {
                        p.classList.remove('show', 'active');
                        p.classList.remove('fade');
                    });
                    
                    this.classList.add('active');
                    this.setAttribute('aria-selected', 'true');
                    
                    const targetPane = document.querySelector("#" + tabId);
                    if (targetPane) {
                        targetPane.classList.add('show', 'active');
                        
                        setTimeout(recalculateChartSizes, 50);
                        
                        if (currentData) {
                            if (tabId === 'chartsTab') {
                                console.log('Updating charts on Metrics tab');
                                setTimeout(() => {
                                    renderMetricsChart();
                                    renderPoolChart();
                                }, 100);
                            } else if (tabId === 'cpuTab') {
                                console.log('Updating charts on CPU tab');
                                setTimeout(() => {
                                    renderCpuCharts();
                                }, 100);
                            }
                        }
                    }
                    
                    const currentUrlParams = new URLSearchParams(window.location.search);
                    currentUrlParams.set('tab', tabId);
                    const newUrl = `${window.location.pathname}?${currentUrlParams.toString()}`;
                    history.pushState({tab: tabId}, '', newUrl);
                    console.log("URL updated:", newUrl);
                });
            });
            
            console.log('Tabs initialized');
        }
    }
    
    dataContainer = $('#dataContainer');
    loadingIndicator = $('#loading');
    errorContainer = $('#error');
    refreshButton = $('#refreshButton');
    levelSelect = $('#levelSelect');
    iterationFrom = $('#iterationFrom');
    iterationTo = $('#iterationTo');
    timeMode = $('#timeMode');
    timeFrom = $('#timeFrom');
    timeTo = $('#timeTo');
    sortNewestFirstCheckbox = $('#sortNewestFirst');
    chartMetricSelect = $('#chartMetricSelect');
    poolSelect = $('#poolSelect');
    poolMetricSelect = $('#poolMetricSelect');
    refreshChartButton = $('#refreshChartButton');
    cpuMetricSelect = $('#cpuMetricSelect');
    cpuValueSelect = $('#cpuValueSelect');
    refreshCpuButton = $('#refreshCpuButton');
    showAllPoolsCheckbox = $('#showAllPools');
    poolCheckboxesContainer = $('#poolCheckboxes');
    poolSelectorContainer = $('#poolSelectorContainer');

    const urlParams = new URLSearchParams(window.location.search);
    const fromIterationParam = urlParams.get('fromIteration');
    const toIterationParam = urlParams.get('toIteration');
    const tabParam = urlParams.get('tab');

    if (fromIterationParam !== null) {
        iterationFrom.val(fromIterationParam);
        console.log("Set 'From iteration' value from URL:", fromIterationParam);
    }
    if (toIterationParam !== null) {
        iterationTo.val(toIterationParam);
        console.log("Set 'To iteration' value from URL:", toIterationParam);
    }

    const validTabIds = ['dataTab', 'chartsTab', 'cpuTab'];
    const defaultTabId = 'dataTab';
    let targetTabId = defaultTabId;

    if (tabParam && validTabIds.includes(tabParam)) {
        targetTabId = tabParam;
        console.log(`Activating tab from URL: #${targetTabId}`);
    } else {
        console.log(`Activating default tab: #${defaultTabId}`);
    }

    document.querySelectorAll('#mainTabs .nav-link').forEach(t => {
        t.classList.remove('active');
        t.setAttribute('aria-selected', 'false');
    });
    document.querySelectorAll('#mainTabsContent .tab-pane').forEach(p => {
        p.classList.remove('show', 'active');
    });

    const targetTabButton = document.querySelector(`#mainTabs button[data-bs-target="#${targetTabId}"]`);
    const targetTabPane = document.getElementById(targetTabId);

    if (targetTabButton && targetTabPane) {
        targetTabButton.classList.add('active');
        targetTabButton.setAttribute('aria-selected', 'true');
        targetTabPane.classList.add('show', 'active');
    } else {
        console.warn(`Could not find elements for tab #${targetTabId}, activating ${defaultTabId}`);
        document.querySelector(`#mainTabs button[data-bs-target="#${defaultTabId}"]`)?.classList.add('active');
        document.querySelector(`#mainTabs button[data-bs-target="#${defaultTabId}"]`)?.setAttribute('aria-selected', 'true');
        document.getElementById(defaultTabId)?.classList.add('show', 'active');
    }

    console.log("Checking form elements:", {
        refreshButton: refreshButton.length > 0,
        levelSelect: levelSelect.length > 0,
        iterationFrom: iterationFrom.length > 0,
        iterationTo: iterationTo.length > 0,
        chartMetricSelect: chartMetricSelect.length > 0,
        cpuMetricSelect: cpuMetricSelect.length > 0
    });

    refreshButton.on('click', function() {
        console.log("Refresh button clicked");
        fetchData();
    });
    
    levelSelect.on('change', function() {
        console.log("Detail level changed:", levelSelect.val());
        fetchData();
    });
    
    iterationFrom.on('change', function() {
        console.log("'From iteration' value changed:", iterationFrom.val());
        fetchData();
        
        const currentUrlParams = new URLSearchParams(window.location.search);
        currentUrlParams.set('fromIteration', iterationFrom.val());
        const newUrl = `${window.location.pathname}?${currentUrlParams.toString()}`;
        history.pushState(null, '', newUrl);
        console.log("URL updated (fromIteration):");
    });
    
    iterationTo.on('change', function() {
        const toValue = iterationTo.val();
        console.log("'To iteration' value changed:", toValue);
        fetchData();
        
        const currentUrlParams = new URLSearchParams(window.location.search);
        if (toValue) {
            currentUrlParams.set('toIteration', toValue);
        } else {
            currentUrlParams.delete('toIteration');
        }
        const newUrl = `${window.location.pathname}?${currentUrlParams.toString()}`;
        history.pushState(null, '', newUrl);
        console.log("URL updated (toIteration):");
    });
    
    timeMode.on('change', function() {
        const isTimeMode = $(this).is(':checked');
        $('#iterationMode').toggle(!isTimeMode);
        $('#timeModeControls').toggle(isTimeMode);
        console.log("Time mode changed:", isTimeMode);
        fetchData();
    });
    
    timeFrom.on('change', function() {
        console.log("'From time' value changed:", timeFrom.val());
        fetchData();
    });
    
    timeTo.on('change', function() {
        console.log("'To time' value changed:", timeTo.val());
        fetchData();
    });
    
    sortNewestFirstCheckbox.on('change', function() {
        console.log("Sort order changed, newest first:", sortNewestFirstCheckbox.prop('checked'));
        if (currentData) {
            renderData(currentData);
        }
    });
    
    chartMetricSelect.on('change', function() {
        console.log("Metric selected:", chartMetricSelect.val());
        renderMetricsChart();
    });
    
    poolSelect.on('change', function() {
        console.log("Pool selected:", poolSelect.val());
        renderPoolChart();
    });
    
    poolMetricSelect.on('change', function() {
        console.log("Pool metric selected:", poolMetricSelect.val());
        renderPoolChart();
    });
    
    refreshChartButton.on('click', function() {
        console.log("Refresh chart button clicked");
        fetchData();
    });
    
    cpuMetricSelect.on('change', function() {
        console.log("CPU metric type selected:", cpuMetricSelect.val());
        renderCpuCharts();
    });
    
    cpuValueSelect.on('change', function() {
        console.log("CPU value selected:", cpuValueSelect.val());
        renderCpuCharts();
    });
    
    refreshCpuButton.on('click', function() {
        console.log("Refresh button clicked for CPU charts");
        fetchData();
    });
    
    showAllPoolsCheckbox.on('change', function() {
        console.log("Show all pools mode changed:", showAllPoolsCheckbox.prop('checked'));
        if ($(this).is(':checked')) {
            poolSelectorContainer.hide();
        } else {
            poolSelectorContainer.show();
        }
        renderCpuCharts();
    });

    poolCheckboxesContainer.on('change', '.pool-checkbox', function() {
        const poolName = $(this).data('pool');
        if ($(this).is(':checked')) {
            selectedPools.add(poolName);
        } else {
            selectedPools.delete(poolName);
        }
        console.log("Selected pools updated:", Array.from(selectedPools));
        renderCpuCharts();
    });
    
    dataContainer.on('click', '.pool-toggle-button', function() {
        const targetId = $(this).data('bs-target');
        const targetBody = $(targetId);
        const isExpanded = $(this).attr('aria-expanded') === 'true';
        console.log(`Toggling pool body ${targetId}, currently expanded: ${isExpanded}`);
        if (isExpanded) {
            targetBody.collapse('hide');
            $(this).attr('aria-expanded', 'false');
            $(this).text('+');
        } else {
            targetBody.collapse('show');
            $(this).attr('aria-expanded', 'true');
            $(this).text('-');
        }
    });
    
    let resizeTimeout;
    $(window).on('resize', function() {
        clearTimeout(resizeTimeout);
        resizeTimeout = setTimeout(function() {
            console.log("Window resized, recalculating charts");
            recalculateChartSizes();
        }, 250);
    });
    
    console.log("Loading initial data...");
    fetchData();
}); 
