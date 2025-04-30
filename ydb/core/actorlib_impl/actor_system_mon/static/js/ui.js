function updateAllCharts() {
    if (!currentData) {
        console.log("updateAllCharts: no data to update charts");
        return;
    }
    
    console.log("updateAllCharts: starting chart update");
    
    const metricsChartExists = document.getElementById("metricsChart") !== null;
    const poolChartExists = document.getElementById("poolChart") !== null;
    const cpuPoolsChartExists = document.getElementById("cpuPoolsChart") !== null;
    const threadsChartExists = document.getElementById("threadsChart") !== null;
    const budgetChartExists = document.getElementById("budgetChart") !== null;
    
    console.log("updateAllCharts: checking DOM element existence:", {
        metricsChartExists,
        poolChartExists,
        cpuPoolsChartExists,
        threadsChartExists,
        budgetChartExists
    });

    const activeTab = document.querySelector('#mainTabs .nav-link.active');
    const activeTabId = activeTab ? activeTab.getAttribute('data-bs-target') : null;
    
    console.log("updateAllCharts: active tab:", activeTabId);
    
    if (activeTabId === '#chartsTab') {
        if (metricsChartExists) renderMetricsChart();
        if (poolChartExists) renderPoolChart();
        if (cpuPoolsChartExists && threadsChartExists && budgetChartExists) {
            renderCpuCharts();
        } else {
            console.error("updateAllCharts: Elements for CPU charts not found");
        }
    } else if (activeTabId === '#cpuTab') {
        if (cpuPoolsChartExists && threadsChartExists && budgetChartExists) {
            renderCpuCharts();
        } else {
            console.error("updateAllCharts: Не найдены элементы для CPU графиков");
        }
    } else {
        if (metricsChartExists) renderMetricsChart();
        if (poolChartExists) renderPoolChart();
        if (cpuPoolsChartExists && threadsChartExists && budgetChartExists) {
            renderCpuCharts();
        }
    }
    
    console.log("updateAllCharts: charts updated");
}

function renderData(data) {
    dataContainer.empty();
    if (!data || !data.history || data.history.length === 0) {
        dataContainer.html('<div class="alert alert-warning">No history data found.</div>');
        return;
    }

    currentData = data;
    
    assignPoolColors(data);

    let historyItems = [...data.history];
    
    if (sortNewestFirstCheckbox.prop('checked')) {
        historyItems.reverse();
    }

    updatePoolSelectAndCheckboxes(data);

    historyItems.forEach((item, idx) => {
        const iterationCard = $(`
            <div class="card iteration-card">
                <div class="card-header iteration-card-header">
                    Iteration ${item.iteration} (Timestamp: ${item.timestamp})
                </div>
                <div class="card-body iteration-card-body">
                    <p><strong>Budget:</strong> ${item.budget?.toFixed(5) ?? 'N/A'}</p>
                    <p><strong>Lost CPU:</strong> ${item.lostCpu ?? 'N/A'}</p>
                    <p><strong>Free Shared CPU:</strong> ${item.freeSharedCpu ?? 'N/A'}</p>
                    <div class="pools-container mt-3"></div>
                    <div class="shared-pool-container mt-3"></div>
                </div>
            </div>
        `);

        const poolsContainer = iterationCard.find('.pools-container');
        const sharedPoolContainer = iterationCard.find('.shared-pool-container');

        if (item.pools && item.pools.length > 0) {
            poolsContainer.append('<h5>Pools</h5>');
            item.pools.forEach((pool, poolIdx) => {
                const poolId = `pool-${item.iteration}-${poolIdx}`;
                const poolBodyId = `pool-body-${item.iteration}-${poolIdx}`;
                
                let totalElapsedCpu = 0;
                let totalUsedCpu = 0;
                if (pool.threads && pool.threads.length > 0) {
                    pool.threads.forEach(thread => {
                        totalElapsedCpu += thread.elapsedCpu?.cpu || 0;
                        totalUsedCpu += thread.usedCpu?.cpu || 0;
                    });
                }
                
                const poolCard = $(`
                    <div class="card pool-card" id="${poolId}">
                        <div class="card-header pool-card-header d-flex justify-content-between align-items-center">
                            <span>
                                Pool: <strong>${pool.name || 'Unknown'}</strong>
                                <small class="text-muted ms-2">Op: ${pool.operation || 'N/A'}</small>
                            </span>
                            <button class="btn btn-sm btn-outline-secondary pool-toggle-button" data-bs-target="#${poolBodyId}" aria-expanded="true">
                                -
                            </button>
                        </div>
                        <div class="card-body pool-card-body collapse show" id="${poolBodyId}">
                            <div class="row">
                                <div class="col-md-6">
                                    <p><strong>Threads:</strong> ${pool.currentThreadCount ?? 'N/A'} / ${pool.potentialMaxThreadCount ?? 'N/A'} (Pot. Max)</p>
                                    ${pool.defaultThreadCount !== undefined ? `
                                    <p><strong>Config Threads:</strong> ${pool.minThreadCount}-${pool.defaultThreadCount}-${pool.maxThreadCount} (Min-Def-Max)</p>
                                    <p><strong>Priority:</strong> ${pool.priority}</p>
                                    ` : ''}
                                    <p><strong>Queue Size:</strong> ${pool.localQueueSize ?? 'N/A'}</p>
                                    ${pool.minLocalQueueSize !== undefined ? `
                                    <p><strong>Queue Limits:</strong> ${pool.minLocalQueueSize}-${pool.maxLocalQueueSize} (Min-Max)</p>
                                    ` : ''}
                                    <p><strong>Total Elapsed CPU:</strong> ${totalElapsedCpu.toFixed(5)}</p> 
                                </div>
                                <div class="col-md-6">
                                    <p><strong>Avg Ping (us):</strong> ${pool.avgPingUs ?? 'N/A'} (Small Window: ${pool.avgPingUsWithSmallWindow ?? 'N/A'}, Max: ${pool.maxAvgPingUs ?? 'N/A'})</p>
                                    <p>
                                        <strong>Status:</strong>
                                        ${pool.isNeedy ? '<span class="badge bg-warning text-dark">Needy</span> ' : ''}
                                        ${pool.isStarved ? '<span class="badge bg-danger">Starved</span> ' : ''}
                                        ${pool.isHoggish ? '<span class="badge bg-success">Hoggish</span> ' : ''}
                                        ${!(pool.isNeedy || pool.isStarved || pool.isHoggish) ? 'Normal' : ''}
                                    </p>
                                    <p><strong>Total Used CPU:</strong> ${totalUsedCpu.toFixed(5)}</p> 
                                </div>
                            </div>
                            <div class="threads-container mt-3"></div>
                        </div>
                    </div>
                `);

                if (pool.threads && pool.threads.length > 0) {
                    poolCard.find('.threads-container').append('<h6>Threads</h6>').append(renderThreads(pool.threads));
                }

                poolsContainer.append(poolCard);
            });
        }

        if (item.shared && item.shared.threads && levelSelect.val() === 'thread') {
            sharedPoolContainer.append('<h5>Shared Pool Threads</h5>');
            sharedPoolContainer.append(renderSharedThreads(item.shared.threads));
        }

        dataContainer.append(iterationCard);
    });
}

function updatePoolSelectAndCheckboxes(data) {
    if (!data || !data.history || !data.history[0] || !data.history[0].pools) {
        return;
    }
    
    poolSelect.empty();
    
    const pools = data.history[0].pools;
    pools.forEach(pool => {
        if (pool.name) {
            poolSelect.append($('<option>').val(pool.name).text(pool.name));
        }
    });
    
    poolCheckboxesContainer.empty();
    
    if (selectedPools.size === 0) {
        pools.forEach(pool => {
            if (pool.name) {
                selectedPools.add(pool.name);
                visiblePools.add(pool.name);
            }
        });
    }
    
    pools.forEach(pool => {
        if (pool.name) {
            const color = poolColorMap[pool.name] || "#000000";
            const isChecked = selectedPools.has(pool.name);
            
            const checkbox = $(`
                <div class="pool-checkbox-wrapper" style="border: 1px solid ${color};">
                    <input type="checkbox" class="pool-checkbox" data-pool="${pool.name}" ${isChecked ? 'checked' : ''}>
                    <span class="pool-color-indicator" style="background-color: ${color};"></span>
                    <span>${pool.name}</span>
                </div>
            `);
            
            poolCheckboxesContainer.append(checkbox);
        }
    });
    
    if (poolSelect.val()) {
        renderPoolChart();
    }
} 