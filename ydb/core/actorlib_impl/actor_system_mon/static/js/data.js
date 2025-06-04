function assignPoolColors(data) {
    if (!data || !data.history || !data.history[0] || !data.history[0].pools) {
        return;
    }
    
    const poolNames = data.history[0].pools.map(pool => pool.name).filter(Boolean);
    
    poolNames.forEach((name, idx) => {
        if (!poolColorMap[name]) {
            poolColorMap[name] = poolColors[idx % poolColors.length];
        }
    });
}

function prepareCpuData() {
    const result = {
        iterations: [],
        timestamps: [],
        budget: [],
        pools: {},
        totalCpu: [],
        totalElapsedCpu: [],
        totalUsedCpu: [],
        poolsThreadCount: {},
        totalThreadCount: [],
        totalMaxThreadCount: []
    };
    
    const sortedData = [...currentData.history].sort((a, b) => a.timestamp - b.timestamp);
    
    console.log("prepareCpuData - sortedData:", sortedData.length ? "found" : "no data");
    if (sortedData.length > 0) {
        console.log("Sample item:", JSON.stringify(sortedData[0], null, 2));
    }
    
    visiblePools.clear();
    if (showAllPoolsCheckbox.prop('checked')) {
        selectedPools.forEach(pool => visiblePools.add(pool));
    } else {
        selectedPools.forEach(pool => visiblePools.add(pool));
    }
    
    console.log("Selected pools:", Array.from(selectedPools));
    console.log("Visible pools:", Array.from(visiblePools));
    
    sortedData.forEach(item => {
        result.iterations.push(item.iteration);
        result.timestamps.push(item.timestamp);
        result.budget.push(item.budget);
        
        let iterationElapsedCpu = 0;
        let iterationUsedCpu = 0;
        let iterationThreadCount = 0;
        let iterationMaxThreadCount = 0;
        
        if (item.pools && item.pools.length > 0) {
            item.pools.forEach(pool => {
                const poolName = pool.name || 'Unknown';
                
                if (!result.pools[poolName]) {
                    result.pools[poolName] = {
                        elapsedCpu: {
                            cpu: [],
                            lastSecondCpu: []
                        },
                        usedCpu: {
                            cpu: [],
                            lastSecondCpu: []
                        }
                    };
                }
                
                if (!result.poolsThreadCount[poolName]) {
                    result.poolsThreadCount[poolName] = {
                        current: [],
                        potential: []
                    };
                }
                
                let poolElapsedCpu = 0;
                let poolElapsedCpuLastSec = 0;
                let poolUsedCpu = 0;
                let poolUsedCpuLastSec = 0;
                
                if (pool.threads && pool.threads.length > 0) {
                    pool.threads.forEach(thread => {
                        if (thread.elapsedCpu) {
                            poolElapsedCpu += thread.elapsedCpu.cpu || 0;
                            poolElapsedCpuLastSec += thread.elapsedCpu.lastSecondCpu || 0;
                        }
                        
                        if (thread.usedCpu) {
                            poolUsedCpu += thread.usedCpu.cpu || 0;
                            poolUsedCpuLastSec += thread.usedCpu.lastSecondCpu || 0;
                        }
                    });
                }
                
                result.pools[poolName].elapsedCpu.cpu.push(poolElapsedCpu);
                result.pools[poolName].elapsedCpu.lastSecondCpu.push(poolElapsedCpuLastSec);
                result.pools[poolName].usedCpu.cpu.push(poolUsedCpu);
                result.pools[poolName].usedCpu.lastSecondCpu.push(poolUsedCpuLastSec);
                
                result.poolsThreadCount[poolName].current.push(pool.currentThreadCount || 0);
                result.poolsThreadCount[poolName].potential.push(pool.potentialMaxThreadCount || 0);
                
                iterationElapsedCpu += poolElapsedCpu;
                iterationUsedCpu += poolUsedCpu;
                iterationThreadCount += pool.currentThreadCount || 0;
                iterationMaxThreadCount += pool.potentialMaxThreadCount || 0;
            });
        }
        
        result.totalElapsedCpu.push(iterationElapsedCpu);
        result.totalUsedCpu.push(iterationUsedCpu);
        result.totalThreadCount.push(iterationThreadCount);
        result.totalMaxThreadCount.push(iterationMaxThreadCount);
        
        const metricType = cpuMetricSelect.val();
        if (metricType === 'elapsedCpu') {
            result.totalCpu.push(iterationElapsedCpu);
        } else {
            result.totalCpu.push(iterationUsedCpu);
        }
    });
    
    console.log("CPU data prepared:", {
        iterations: result.iterations.length,
        totalElapsedCpu: result.totalElapsedCpu.length,
        totalUsedCpu: result.totalUsedCpu.length,
        pools: Object.keys(result.pools).length
    });
    
    return result;
}

function getApiUrl() {
    const level = 'thread';
    const isTimeMode = timeMode.is(':checked');
    
    console.log("getApiUrl: requesting data with level:", level, "timeMode:", isTimeMode);
    
    const baseUrl = getBaseUrl();
    console.log("getApiUrl: base URL:", baseUrl);
    
    if (isTimeMode) {
        const from = timeFrom.val();
        const to = timeTo.val();
        
        if (from < 0) {
            return `${baseUrl}?level=${level}&last_window_ts=${Math.abs(from)}`;
        }
        
        if (!to) {
            return `${baseUrl}?level=${level}&window_ts_start=${from}`;
        }
        
        return `${baseUrl}?level=${level}&window_ts_start=${from}&window_ts_count=${to - from}`;
    } else {
        const from = iterationFrom.val();
        const to = iterationTo.val();
        
        if (from < 0) {
            return `${baseUrl}?level=${level}&last_iteration=${Math.abs(from)}`;
        }
        
        if (!to) {
            return `${baseUrl}?level=${level}&window_iteration_start=${from}`;
        }
        
        return `${baseUrl}?level=${level}&window_iteration_start=${from}&window_iteration_count=${to - from}`;
    }
}

function getBaseUrl() {
    const currentUrl = window.location.href;
    console.log("Current URL:", currentUrl);
    
    const nodeMatch = currentUrl.match(/\/node\/(\d+)/);
    
    if (nodeMatch) {
        const nodePrefix = nodeMatch[0];
        console.log("Node prefix found:", nodePrefix);
        return `${nodePrefix}/actors/actor_system`;
    } else {
        return "/actors/actor_system";
    }
}

function fetchData() {
    const url = getApiUrl();
    console.log("Starting data load from URL:", url);
    loadingIndicator.show();
    errorContainer.hide().empty();
    dataContainer.empty();

    $.ajax({
        url: url,
        method: 'GET',
        dataType: 'json',
        success: function(data) {
            console.log("Data loaded successfully, number of iterations:", data?.history?.length || 0);
            
            if (data && data.history && data.history.length > 0) {
                const firstItem = data.history[0];
                console.log("First iteration:", {
                    iteration: firstItem.iteration,
                    timestamp: firstItem.timestamp,
                    budget: firstItem.budget,
                    lostCpu: firstItem.lostCpu,
                    freeSharedCpu: firstItem.freeSharedCpu,
                    poolsCount: firstItem.pools?.length || 0
                });
                
                if (firstItem.pools && firstItem.pools.length > 0) {
                    const firstPool = firstItem.pools[0];
                    console.log("First pool:", {
                        name: firstPool.name,
                        threadCount: firstPool.currentThreadCount,
                        hasThreads: !!firstPool.threads,
                        threadsCount: firstPool.threads?.length || 0
                    });
                    
                    if (firstPool.threads && firstPool.threads.length > 0) {
                        const firstThread = firstPool.threads[0];
                        console.log("First thread:", {
                            usedCpu: firstThread.usedCpu,
                            elapsedCpu: firstThread.elapsedCpu,
                            parkedCpu: firstThread.parkedCpu
                        });
                    }
                }
            }
            
            loadingIndicator.hide();
            
            currentData = data;
            renderData(data);
            
            console.log("Waiting before updating charts...");
            setTimeout(function() {
                recalculateChartSizes();
                console.log("Updating charts after data load");
                updateAllCharts();
            }, 250);
        },
        error: function(jqXHR, textStatus, errorThrown) {
            console.error("AJAX request error:", {
                status: jqXHR.status,
                statusText: jqXHR.statusText,
                error: errorThrown,
                textStatus: textStatus,
                responseText: jqXHR.responseText?.substring(0, 200)
            });
            
            loadingIndicator.hide();
            errorContainer.text(`Failed to load data from ${url}. Status: ${jqXHR.status} ${jqXHR.statusText}. ${errorThrown}. Check console for details.`);
            errorContainer.show();
            if (jqXHR.responseText && jqXHR.getResponseHeader('content-type').indexOf('json') === -1) {
                 errorContainer.append($('<pre>').text(jqXHR.responseText));
            }
        }
    });
} 
