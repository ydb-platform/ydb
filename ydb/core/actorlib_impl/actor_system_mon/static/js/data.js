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
    
    // Отладка
    console.log("prepareCpuData - sortedData:", sortedData.length ? "найдено" : "нет данных");
    if (sortedData.length > 0) {
        console.log("Sample item:", JSON.stringify(sortedData[0], null, 2));
    }
    
    // Обновляем visiblePools на основе showAllPools
    visiblePools.clear();
    if (showAllPoolsCheckbox.prop('checked')) {
        selectedPools.forEach(pool => visiblePools.add(pool));
    } else {
        selectedPools.forEach(pool => visiblePools.add(pool));
    }
    
    // Проверяем наличие данных о пулах
    console.log("Selected pools:", Array.from(selectedPools));
    console.log("Visible pools:", Array.from(visiblePools));
    
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
    
    // Отладка
    console.log("CPU data prepared:", {
        iterations: result.iterations.length,
        totalElapsedCpu: result.totalElapsedCpu.length,
        totalUsedCpu: result.totalUsedCpu.length,
        pools: Object.keys(result.pools).length
    });
    
    return result;
}

function getApiUrl() {
    // Всегда используем максимальный уровень детализации 'thread' для получения всех данных
    const level = 'thread'; // Было: levelSelect.val();
    const isTimeMode = timeMode.is(':checked');
    
    console.log("getApiUrl: запрашиваем данные с уровнем:", level, "timeMode:", isTimeMode);
    
    // Получаем базовый URL с учетом возможного префикса node/XXXX
    const baseUrl = getBaseUrl();
    console.log("getApiUrl: базовый URL:", baseUrl);
    
    if (isTimeMode) {
        const from = timeFrom.val();
        const to = timeTo.val();
        
        // Если from отрицательное, это означает "текущее время минус значение"
        if (from < 0) {
            return `${baseUrl}?level=${level}&last_window_ts=${Math.abs(from)}`;
        }
        
        // Если to не указано, используем текущее время
        if (!to) {
            return `${baseUrl}?level=${level}&window_ts_start=${from}`;
        }
        
        // Полный диапазон времени
        return `${baseUrl}?level=${level}&window_ts_start=${from}&window_ts_count=${to - from}`;
    } else {
        const from = iterationFrom.val();
        const to = iterationTo.val();
        
        // Если from отрицательное, это означает "текущая итерация минус значение"
        if (from < 0) {
            return `${baseUrl}?level=${level}&last_iteration=${Math.abs(from)}`;
        }
        
        // Если to не указано, используем "last"
        if (!to) {
            return `${baseUrl}?level=${level}&window_iteration_start=${from}`;
        }
        
        // Полный диапазон итераций
        return `${baseUrl}?level=${level}&window_iteration_start=${from}&window_iteration_count=${to - from}`;
    }
}

// Функция для получения базового URL с учетом возможного префикса node/XXXX
function getBaseUrl() {
    // Получаем текущий URL страницы
    const currentUrl = window.location.href;
    console.log("Текущий URL:", currentUrl);
    
    // Проверяем, есть ли в URL паттерн node/XXXX
    const nodeMatch = currentUrl.match(/\/node\/(\d+)/);
    
    if (nodeMatch) {
        // Если есть префикс node/XXXX, добавляем его к пути запроса
        const nodePrefix = nodeMatch[0]; // Например, "/node/50001"
        console.log("Найден префикс ноды:", nodePrefix);
        return `${nodePrefix}/actors/actor_system`;
    } else {
        // Стандартный путь без префикса
        return "/actors/actor_system";
    }
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
            
            // Подробная отладка полученных данных
            if (data && data.history && data.history.length > 0) {
                // Проверяем первую итерацию
                const firstItem = data.history[0];
                console.log("Первая итерация:", {
                    iteration: firstItem.iteration,
                    timestamp: firstItem.timestamp,
                    budget: firstItem.budget,
                    lostCpu: firstItem.lostCpu,
                    freeSharedCpu: firstItem.freeSharedCpu,
                    poolsCount: firstItem.pools?.length || 0
                });
                
                // Если есть пулы, проверяем первый пул
                if (firstItem.pools && firstItem.pools.length > 0) {
                    const firstPool = firstItem.pools[0];
                    console.log("Первый пул:", {
                        name: firstPool.name,
                        threadCount: firstPool.currentThreadCount,
                        hasThreads: !!firstPool.threads,
                        threadsCount: firstPool.threads?.length || 0
                    });
                    
                    // Если есть потоки, проверяем первый поток
                    if (firstPool.threads && firstPool.threads.length > 0) {
                        const firstThread = firstPool.threads[0];
                        console.log("Первый поток:", {
                            usedCpu: firstThread.usedCpu,
                            elapsedCpu: firstThread.elapsedCpu,
                            parkedCpu: firstThread.parkedCpu
                        });
                    }
                }
            }
            
            loadingIndicator.hide();
            
            // Обновляем данные и рендерим контент
            currentData = data;
            renderData(data); // Вызываем renderData из ui.js
            
            // Делаем паузу и затем обновляем графики
            console.log("Ожидаем перед обновлением графиков...");
            setTimeout(function() {
                // Сначала проверяем размеры контейнеров графиков
                recalculateChartSizes();
                
                // Затем обновляем графики
                console.log("Обновляем графики после загрузки данных");
                updateAllCharts(); // Вызываем updateAllCharts из ui.js
            }, 250);
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