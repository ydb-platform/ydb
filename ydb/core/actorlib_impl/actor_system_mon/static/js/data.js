// Устанавливаем цвета для пулов
function assignPoolColors(data) {
    if (!data || !data.history || !data.history[0] || !data.history[0].pools) {
        return;
    }
    
    // Получаем все имена пулов в первой итерации
    const poolNames = data.history[0].pools.map(pool => pool.name).filter(Boolean);
    
    // Назначаем цвета для каждого пула
    poolNames.forEach((name, idx) => {
        if (!poolColorMap[name]) {
            poolColorMap[name] = poolColors[idx % poolColors.length];
        }
    });
}

// Подготовка данных для графиков CPU
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
    
    // Отсортированные по времени данные
    const sortedData = [...currentData.history].sort((a, b) => a.timestamp - b.timestamp);
    
    // Обновляем visiblePools на основе showAllPools
    visiblePools.clear();
    if (showAllPoolsCheckbox.prop('checked')) {
        selectedPools.forEach(pool => visiblePools.add(pool));
    } else {
        selectedPools.forEach(pool => visiblePools.add(pool));
    }
    
    // Извлекаем данные
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
                
                // Создаем данные для пула если их еще нет
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
                
                // Суммируем CPU каждого потока в пуле
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
                
                // Добавляем данные CPU для пула
                result.pools[poolName].elapsedCpu.cpu.push(poolElapsedCpu);
                result.pools[poolName].elapsedCpu.lastSecondCpu.push(poolElapsedCpuLastSec);
                result.pools[poolName].usedCpu.cpu.push(poolUsedCpu);
                result.pools[poolName].usedCpu.lastSecondCpu.push(poolUsedCpuLastSec);
                
                // Данные по потокам
                result.poolsThreadCount[poolName].current.push(pool.currentThreadCount || 0);
                result.poolsThreadCount[poolName].potential.push(pool.potentialMaxThreadCount || 0);
                
                // Добавляем в общую сумму
                iterationElapsedCpu += poolElapsedCpu;
                iterationUsedCpu += poolUsedCpu;
                iterationThreadCount += pool.currentThreadCount || 0;
                iterationMaxThreadCount += pool.potentialMaxThreadCount || 0;
            });
        }
        
        // Добавляем общие данные по итерации
        result.totalElapsedCpu.push(iterationElapsedCpu);
        result.totalUsedCpu.push(iterationUsedCpu);
        result.totalThreadCount.push(iterationThreadCount);
        result.totalMaxThreadCount.push(iterationMaxThreadCount);
        
        // Выбираем значение в зависимости от выбранного типа
        const metricType = cpuMetricSelect.val();
        if (metricType === 'elapsedCpu') {
            result.totalCpu.push(iterationElapsedCpu);
        } else {
            result.totalCpu.push(iterationUsedCpu);
        }
    });
    
    return result;
}

function fetchData() {
    const url = getApiUrl();
    console.log("Начинаем загрузку данных с URL:", url);
    loadingIndicator.show();
    errorContainer.hide().empty();
    dataContainer.empty();

    $.ajax({
        url: url,
        method: 'GET',
        dataType: 'json',
        success: function(data) {
            console.log("Данные успешно загружены, количество итераций:", data?.history?.length || 0);
            loadingIndicator.hide();
            renderData(data); // Вызываем renderData из ui.js
            
            // Обновляем графики на активной вкладке
            updateAllCharts(); // Вызываем updateAllCharts из ui.js
        },
        error: function(jqXHR, textStatus, errorThrown) {
            console.error("Ошибка AJAX запроса:", {
                status: jqXHR.status,
                statusText: jqXHR.statusText,
                error: errorThrown,
                textStatus: textStatus,
                responseText: jqXHR.responseText?.substring(0, 200) // Показываем начало ответа
            });
            
            loadingIndicator.hide();
            errorContainer.text(`Failed to load data from ${url}. Status: ${jqXHR.status} ${jqXHR.statusText}. ${errorThrown}. Check console for details.`);
            errorContainer.show();
            // Попробуем показать ответ, если он есть и это не JSON
            if (jqXHR.responseText && jqXHR.getResponseHeader('content-type').indexOf('json') === -1) {
                 errorContainer.append($('<pre>').text(jqXHR.responseText));
            }
        }
    });
} 