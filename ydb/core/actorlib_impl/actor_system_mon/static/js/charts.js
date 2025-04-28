// Функция для отрисовки графика метрик
function renderMetricsChart() {
    if (!currentData || !currentData.history || currentData.history.length === 0) {
        return;
    }

    const svg = d3.select("#metricsChart");
    svg.selectAll("*").remove(); // Очищаем предыдущий график
    
    const margin = {top: 20, right: 80, bottom: 40, left: 60};
    const width = svg.node().getBoundingClientRect().width - margin.left - margin.right;
    const height = svg.node().getBoundingClientRect().height - margin.top - margin.bottom;
    
    const g = svg.append("g")
        .attr("transform", `translate(${margin.left},${margin.top})`);
    
    // Получаем отсортированные по времени данные
    const metricData = [...currentData.history].sort((a, b) => a.timestamp - b.timestamp);
    
    // Создаем шкалы
    const x = d3.scaleLinear()
        .domain(d3.extent(metricData, d => d.iteration))
        .range([0, width]);
    
    // Находим минимальное и максимальное значение метрики
    const selectedMetric = chartMetricSelect.val();
    const metricValues = metricData.map(d => d[selectedMetric]).filter(d => d !== undefined && d !== null);
    
    // Если нет данных, выходим
    if (metricValues.length === 0) {
        g.append("text")
            .attr("x", width / 2)
            .attr("y", height / 2)
            .attr("text-anchor", "middle")
            .text("Нет данных для выбранной метрики");
        return;
    }
    
    const y = d3.scaleLinear()
        .domain([0, d3.max(metricValues) * 1.1]) // Добавляем 10% отступа сверху
        .range([height, 0]);
    
    // Создаем оси
    g.append("g")
        .attr("class", "grid")
        .attr("transform", `translate(0,${height})`)
        .call(d3.axisBottom(x).ticks(10).tickSize(-height))
        .append("text")
        .attr("class", "axis-label")
        .attr("x", width / 2)
        .attr("y", 35)
        .text("Итерация");
    
    g.append("g")
        .attr("class", "grid")
        .call(d3.axisLeft(y).ticks(10).tickSize(-width))
        .append("text")
        .attr("class", "axis-label")
        .attr("transform", "rotate(-90)")
        .attr("x", -height / 2)
        .attr("y", -45)
        .text(selectedMetric);
    
    // Создаем линию
    const line = d3.line()
        .defined(d => d[selectedMetric] !== undefined && d[selectedMetric] !== null)
        .x(d => x(d.iteration))
        .y(d => y(d[selectedMetric]))
        .curve(d3.curveMonotoneX);
    
    // Рисуем линию
    g.append("path")
        .datum(metricData)
        .attr("class", `line line-${selectedMetric}`)
        .attr("d", line)
        .style("stroke", colorScheme[selectedMetric] || "#007bff");
    
    // Добавляем точки на линию
    g.selectAll(".dot")
        .data(metricData.filter(d => d[selectedMetric] !== undefined && d[selectedMetric] !== null))
        .enter().append("circle")
        .attr("class", "dot")
        .attr("cx", d => x(d.iteration))
        .attr("cy", d => y(d[selectedMetric]))
        .attr("r", 3)
        .style("fill", colorScheme[selectedMetric] || "#007bff");
    
    // Создаем tooltip
    const tooltip = d3.select("body").append("div")
        .attr("class", "tooltip")
        .style("opacity", 0);
    
    // Добавляем интерактивность к точкам
    g.selectAll(".dot")
        .on("mouseover", function(d) {
            tooltip.transition()
                .duration(200)
                .style("opacity", .9);
            tooltip.html(`Итерация: ${d.iteration}<br>${selectedMetric}: ${d[selectedMetric].toFixed(5)}`)
                .style("left", (d3.event.pageX + 10) + "px")
                .style("top", (d3.event.pageY - 28) + "px");
            d3.select(this)
                .attr("r", 5);
        })
        .on("mouseout", function() {
            tooltip.transition()
                .duration(500)
                .style("opacity", 0);
            d3.select(this)
                .attr("r", 3);
        });
}

// Функция для отрисовки графика пула
function renderPoolChart() {
    if (!currentData || !currentData.history || currentData.history.length === 0) {
        return;
    }

    const selectedPool = poolSelect.val();
    const selectedMetric = poolMetricSelect.val();
    
    if (!selectedPool || !selectedMetric) {
        return;
    }
    
    const svg = d3.select("#poolChart");
    svg.selectAll("*").remove(); // Очищаем предыдущий график
    
    const margin = {top: 20, right: 80, bottom: 40, left: 60};
    const width = svg.node().getBoundingClientRect().width - margin.left - margin.right;
    const height = svg.node().getBoundingClientRect().height - margin.top - margin.bottom;
    
    const g = svg.append("g")
        .attr("transform", `translate(${margin.left},${margin.top})`);
    
    // Подготавливаем данные для графика
    const poolData = [];
    currentData.history.forEach(item => {
        if (item.pools) {
            const pool = item.pools.find(p => p.name === selectedPool);
            if (pool && pool[selectedMetric] !== undefined && pool[selectedMetric] !== null) {
                poolData.push({
                    iteration: item.iteration,
                    value: pool[selectedMetric]
                });
            }
        }
    });
    
    // Сортируем данные по итерации
    poolData.sort((a, b) => a.iteration - b.iteration);
    
    // Если нет данных, выходим
    if (poolData.length === 0) {
        g.append("text")
            .attr("x", width / 2)
            .attr("y", height / 2)
            .attr("text-anchor", "middle")
            .text("Нет данных для выбранного пула и метрики");
        return;
    }
    
    // Создаем шкалы
    const x = d3.scaleLinear()
        .domain(d3.extent(poolData, d => d.iteration))
        .range([0, width]);
    
    const y = d3.scaleLinear()
        .domain([0, d3.max(poolData, d => d.value) * 1.1]) // Добавляем 10% отступа сверху
        .range([height, 0]);
    
    // Создаем оси
    g.append("g")
        .attr("class", "grid")
        .attr("transform", `translate(0,${height})`)
        .call(d3.axisBottom(x).ticks(10).tickSize(-height))
        .append("text")
        .attr("class", "axis-label")
        .attr("x", width / 2)
        .attr("y", 35)
        .text("Итерация");
    
    g.append("g")
        .attr("class", "grid")
        .call(d3.axisLeft(y).ticks(10).tickSize(-width))
        .append("text")
        .attr("class", "axis-label")
        .attr("transform", "rotate(-90)")
        .attr("x", -height / 2)
        .attr("y", -45)
        .text(selectedMetric);
    
    // Создаем линию
    const line = d3.line()
        .x(d => x(d.iteration))
        .y(d => y(d.value))
        .curve(d3.curveMonotoneX);
    
    // Рисуем линию
    g.append("path")
        .datum(poolData)
        .attr("class", "line")
        .attr("d", line)
        .style("stroke", poolColorMap[selectedPool] || colorScheme[selectedMetric] || "#20c997");
    
    // Добавляем точки на линию
    g.selectAll(".dot")
        .data(poolData)
        .enter().append("circle")
        .attr("class", "dot")
        .attr("cx", d => x(d.iteration))
        .attr("cy", d => y(d.value))
        .attr("r", 3)
        .style("fill", poolColorMap[selectedPool] || colorScheme[selectedMetric] || "#20c997");
    
    // Создаем tooltip
    const tooltip = d3.select("body").append("div")
        .attr("class", "tooltip")
        .style("opacity", 0);
    
    // Добавляем интерактивность к точкам
    g.selectAll(".dot")
        .on("mouseover", function(d) {
            tooltip.transition()
                .duration(200)
                .style("opacity", .9);
            tooltip.html(`Итерация: ${d.iteration}<br>${selectedMetric}: ${d.value.toFixed(5)}`)
                .style("left", (d3.event.pageX + 10) + "px")
                .style("top", (d3.event.pageY - 28) + "px");
            d3.select(this)
                .attr("r", 5);
        })
        .on("mouseout", function() {
            tooltip.transition()
                .duration(500)
                .style("opacity", 0);
            d3.select(this)
                .attr("r", 3);
        });
}

// Функция для отрисовки всех CPU графиков
function renderCpuCharts() {
    if (!currentData || !currentData.history || currentData.history.length === 0) {
        return;
    }
    
    // Подготавливаем данные для всех графиков
    const cpuData = prepareCpuData(); // Вызываем prepareCpuData из data.js
    
    // Рисуем график CPU Pools
    renderCpuPoolsChart(cpuData);
    
    // Рисуем график количества потоков
    renderThreadsChart(cpuData);
    
    // Рисуем график Budget
    renderBudgetChart(cpuData);
}

// Функция для отрисовки графика потребления CPU по пулам
function renderCpuPoolsChart(cpuData) {
    const svg = d3.select("#cpuPoolsChart");
    svg.selectAll("*").remove(); // Очищаем предыдущий график
    
    const margin = {top: 30, right: 80, bottom: 40, left: 60};
    const width = svg.node().getBoundingClientRect().width - margin.left - margin.right;
    const height = svg.node().getBoundingClientRect().height - margin.top - margin.bottom;
    
    const g = svg.append("g")
        .attr("transform", `translate(${margin.left},${margin.top})`);
    
    // Если нет данных, выходим
    if (cpuData.iterations.length === 0) {
        g.append("text")
            .attr("x", width / 2)
            .attr("y", height / 2)
            .attr("text-anchor", "middle")
            .text("Нет данных для построения графика");
        return;
    }
    
    // Выбираем метрику и значение
    const metricType = cpuMetricSelect.val(); // elapsedCpu или usedCpu
    const valueType = cpuValueSelect.val();   // cpu или lastSecondCpu
    
    // Создаем шкалы
    const x = d3.scaleLinear()
        .domain(d3.extent(cpuData.iterations))
        .range([0, width]);
    
    // Создаем массив данных для формирования области
    const areaData = [];
    
    // Проходим по всем пулам и добавляем только видимые
    const visiblePoolNames = Array.from(visiblePools);
    const visiblePoolData = {};
    
    visiblePoolNames.forEach(poolName => {
        if (cpuData.pools[poolName]) {
            const values = cpuData.pools[poolName][metricType][valueType];
            visiblePoolData[poolName] = values;
        }
    });
    
    // Находим максимальные значения для шкалы Y
    let maxValue = 0;
    cpuData.iterations.forEach((_, i) => {
        let total = 0;
        visiblePoolNames.forEach(poolName => {
            if (visiblePoolData[poolName] && visiblePoolData[poolName][i] !== undefined) {
                total += visiblePoolData[poolName][i];
            }
        });
        maxValue = Math.max(maxValue, total);
    });
    
    const y = d3.scaleLinear()
        .domain([0, maxValue * 1.1]) // Добавляем 10% отступа сверху
        .range([height, 0]);
    
    // Создаем области для каждого пула
    const stackData = [];
    cpuData.iterations.forEach((iteration, i) => {
        const point = { iteration };
        let cumulative = 0;
        
        visiblePoolNames.forEach(poolName => {
            if (visiblePoolData[poolName] && visiblePoolData[poolName][i] !== undefined) {
                const value = visiblePoolData[poolName][i];
                point[poolName] = value;
                point[poolName + '_start'] = cumulative;
                point[poolName + '_end'] = cumulative + value;
                cumulative += value;
            }
        });
        
        stackData.push(point);
    });
    
    // Создаем оси
    g.append("g")
        .attr("class", "grid")
        .attr("transform", `translate(0,${height})`)
        .call(d3.axisBottom(x).ticks(10).tickSize(-height))
        .append("text")
        .attr("class", "axis-label")
        .attr("x", width / 2)
        .attr("y", 35)
        .text("Итерация");
    
    g.append("g")
        .attr("class", "grid")
        .call(d3.axisLeft(y).ticks(10).tickSize(-width))
        .append("text")
        .attr("class", "axis-label")
        .attr("transform", "rotate(-90)")
        .attr("x", -height / 2)
        .attr("y", -45)
        .text(metricType === 'elapsedCpu' ? 'Elapsed CPU' : 'Used CPU');
    
    // Создаем легенду
    const legend = svg.append("g")
        .attr("class", "chart-legend")
        .attr("transform", `translate(${margin.left}, 10)`)
        .selectAll(".legend-item")
        .data(visiblePoolNames)
        .enter()
        .append("g")
        .attr("class", "legend-item")
        .attr("transform", (d, i) => `translate(${i * 120}, 0)`)
        .on("click", function(poolName) {
            // Удаляем пул из видимых или добавляем обратно
            if (visiblePools.has(poolName)) {
                visiblePools.delete(poolName);
                d3.select(this).classed("legend-item-disabled", true);
            } else {
                visiblePools.add(poolName);
                d3.select(this).classed("legend-item-disabled", false);
            }
            renderCpuCharts();
        });
    
    legend.append("rect")
        .attr("class", "legend-color")
        .attr("width", 16)
        .attr("height", 10)
        .attr("fill", d => poolColorMap[d] || "#000000");
    
    legend.append("text")
        .attr("class", "legend-text")
        .attr("x", 20)
        .attr("y", 9)
        .text(d => d);
    
    // Для каждого пула рисуем область
    visiblePoolNames.forEach(poolName => {
        const area = d3.area()
            .x(d => x(d.iteration))
            .y0(d => y(d[poolName + '_start'] || 0))
            .y1(d => y(d[poolName + '_end'] || 0))
            .curve(d3.curveMonotoneX);
        
        g.append("path")
            .datum(stackData)
            .attr("class", "area")
            .attr("fill", poolColorMap[poolName] || "#000000")
            .attr("d", area);
    });
    
    // Создаем tooltip
    const tooltip = d3.select("body").append("div")
        .attr("class", "tooltip")
        .style("opacity", 0);
    
    // Создаем невидимую область для отслеживания движения мыши
    g.append("rect")
        .attr("width", width)
        .attr("height", height)
        .attr("fill", "none")
        .attr("pointer-events", "all")
        .on("mousemove", function() {
            const mouseX = d3.mouse(this)[0];
            const iteration = Math.round(x.invert(mouseX));
            
            // Находим ближайшую итерацию
            const closestIterationIndex = cpuData.iterations.reduce((closest, curr, idx) => {
                return Math.abs(curr - iteration) < Math.abs(cpuData.iterations[closest] - iteration) ? idx : closest;
            }, 0);
            
            const iterationData = stackData[closestIterationIndex];
            
            // Формируем текст для tooltip
            let tooltipText = `<strong>Итерация: ${iterationData.iteration}</strong><br>`;
            
            visiblePoolNames.forEach(poolName => {
                const value = iterationData[poolName];
                if (value !== undefined) {
                    tooltipText += `${poolName}: ${value.toFixed(5)}<br>`;
                }
            });
            
            // Отображаем tooltip
            tooltip.transition()
                .duration(200)
                .style("opacity", .9);
            
            tooltip.html(tooltipText)
                .style("left", (d3.event.pageX + 10) + "px")
                .style("top", (d3.event.pageY - 28) + "px");
            
            // Рисуем вертикальную линию
            g.selectAll(".mouseLine").remove();
            g.append("line")
                .attr("class", "mouseLine overlay-line")
                .attr("x1", x(iterationData.iteration))
                .attr("x2", x(iterationData.iteration))
                .attr("y1", 0)
                .attr("y2", height);
        })
        .on("mouseout", function() {
            // Убираем tooltip и линию
            tooltip.transition()
                .duration(500)
                .style("opacity", 0);
            
            g.selectAll(".mouseLine").remove();
        });
}

// Функция для отрисовки графика количества потоков
function renderThreadsChart(cpuData) {
    const svg = d3.select("#threadsChart");
    svg.selectAll("*").remove(); // Очищаем предыдущий график
    
    const margin = {top: 30, right: 80, bottom: 40, left: 60};
    const width = svg.node().getBoundingClientRect().width - margin.left - margin.right;
    const height = svg.node().getBoundingClientRect().height - margin.top - margin.bottom;
    
    const g = svg.append("g")
        .attr("transform", `translate(${margin.left},${margin.top})`);
    
    // Если нет данных, выходим
    if (cpuData.iterations.length === 0) {
        g.append("text")
            .attr("x", width / 2)
            .attr("y", height / 2)
            .attr("text-anchor", "middle")
            .text("Нет данных для построения графика");
        return;
    }
    
    // Создаем шкалы
    const x = d3.scaleLinear()
        .domain(d3.extent(cpuData.iterations))
        .range([0, width]);
    
    // Находим максимальное значение для шкалы Y
    const maxThreads = Math.max(
        d3.max(cpuData.totalThreadCount) || 0, 
        d3.max(cpuData.totalMaxThreadCount) || 0
    );
    
    const y = d3.scaleLinear()
        .domain([0, maxThreads * 1.1]) // Добавляем 10% отступа сверху
        .range([height, 0]);
    
    // Создаем оси
    g.append("g")
        .attr("class", "grid")
        .attr("transform", `translate(0,${height})`)
        .call(d3.axisBottom(x).ticks(10).tickSize(-height))
        .append("text")
        .attr("class", "axis-label")
        .attr("x", width / 2)
        .attr("y", 35)
        .text("Итерация");
    
    g.append("g")
        .attr("class", "grid")
        .call(d3.axisLeft(y).ticks(10).tickSize(-width))
        .append("text")
        .attr("class", "axis-label")
        .attr("transform", "rotate(-90)")
        .attr("x", -height / 2)
        .attr("y", -45)
        .text("Количество потоков");
    
    // Создаем области для стекирования потоков
    const stackData = [];
    cpuData.iterations.forEach((iteration, i) => {
        const point = { iteration };
        let cumulativeCurrent = 0;
        let cumulativeMax = 0;
        
        visiblePools.forEach(poolName => {
            if (cpuData.poolsThreadCount[poolName]) {
                const currentThreads = cpuData.poolsThreadCount[poolName].current[i] || 0;
                const maxThreads = cpuData.poolsThreadCount[poolName].potential[i] || 0;
                
                point[poolName + '_current_start'] = cumulativeCurrent;
                point[poolName + '_current_end'] = cumulativeCurrent + currentThreads;
                cumulativeCurrent += currentThreads;
                
                point[poolName + '_max_start'] = cumulativeMax;
                point[poolName + '_max_end'] = cumulativeMax + maxThreads;
                cumulativeMax += maxThreads;
            }
        });
        
        stackData.push(point);
    });
    
    // Рисуем области для текущих потоков
    visiblePools.forEach(poolName => {
        if (cpuData.poolsThreadCount[poolName]) {
            const area = d3.area()
                .x(d => x(d.iteration))
                .y0(d => y(d[poolName + '_current_start'] || 0))
                .y1(d => y(d[poolName + '_current_end'] || 0))
                .curve(d3.curveMonotoneX);
            
            g.append("path")
                .datum(stackData)
                .attr("class", "area")
                .attr("fill", poolColorMap[poolName] || "#000000")
                .attr("opacity", 0.7)
                .attr("d", area);
        }
    });
    
    // Рисуем линию для максимального общего количества потоков
    const maxThreadLine = d3.line()
        .x((d, i) => x(cpuData.iterations[i]))
        .y(d => y(d))
        .curve(d3.curveMonotoneX);
    
    g.append("path")
        .datum(cpuData.totalMaxThreadCount)
        .attr("class", "line")
        .attr("d", maxThreadLine)
        .attr("stroke", "#777")
        .attr("stroke-width", 1.5)
        .attr("stroke-dasharray", "5,5")
        .attr("fill", "none");
    
    // Создаем легенду
    const legend = svg.append("g")
        .attr("class", "chart-legend")
        .attr("transform", `translate(${margin.left}, 10)`);
    
    // Легенда для пулов
    visiblePools.forEach((poolName, i) => {
        const legendItem = legend.append("g")
            .attr("class", "legend-item")
            .attr("transform", `translate(${i * 100}, 0)`);
        
        legendItem.append("rect")
            .attr("class", "legend-color")
            .attr("width", 16)
            .attr("height", 10)
            .attr("fill", poolColorMap[poolName] || "#000000");
        
        legendItem.append("text")
            .attr("class", "legend-text")
            .attr("x", 20)
            .attr("y", 9)
            .text(poolName);
    });
    
    // Легенда для максимальных потоков
    const maxLegend = legend.append("g")
        .attr("class", "legend-item")
        .attr("transform", `translate(${visiblePools.size * 100}, 0)`);
    
    maxLegend.append("line")
        .attr("class", "legend-color")
        .attr("x1", 0)
        .attr("y1", 5)
        .attr("x2", 16)
        .attr("y2", 5)
        .attr("stroke", "#777")
        .attr("stroke-width", 1.5)
        .attr("stroke-dasharray", "5,5");
    
    maxLegend.append("text")
        .attr("class", "legend-text")
        .attr("x", 20)
        .attr("y", 9)
        .text("Max Threads");
    
    // Создаем tooltip
    const tooltip = d3.select("body").append("div")
        .attr("class", "tooltip")
        .style("opacity", 0);
    
    // Создаем невидимую область для отслеживания движения мыши
    g.append("rect")
        .attr("width", width)
        .attr("height", height)
        .attr("fill", "none")
        .attr("pointer-events", "all")
        .on("mousemove", function() {
            const mouseX = d3.mouse(this)[0];
            const iteration = Math.round(x.invert(mouseX));
            
            // Находим ближайшую итерацию
            const closestIterationIndex = cpuData.iterations.reduce((closest, curr, idx) => {
                return Math.abs(curr - iteration) < Math.abs(cpuData.iterations[closest] - iteration) ? idx : closest;
            }, 0);
            
            const iterationData = stackData[closestIterationIndex];
            
            // Формируем текст для tooltip
            let tooltipText = `<strong>Итерация: ${iterationData.iteration}</strong><br>`;
            tooltipText += `<strong>Всего потоков:</strong> ${cpuData.totalThreadCount[closestIterationIndex]}<br>`;
            tooltipText += `<strong>Макс. потоков:</strong> ${cpuData.totalMaxThreadCount[closestIterationIndex]}<br><br>`;
            
            visiblePools.forEach(poolName => {
                if (cpuData.poolsThreadCount[poolName]) {
                    const currentThreads = cpuData.poolsThreadCount[poolName].current[closestIterationIndex];
                    const maxThreads = cpuData.poolsThreadCount[poolName].potential[closestIterationIndex];
                    
                    if (currentThreads !== undefined) {
                        tooltipText += `${poolName}: ${currentThreads} / ${maxThreads}<br>`;
                    }
                }
            });
            
            // Отображаем tooltip
            tooltip.transition()
                .duration(200)
                .style("opacity", .9);
            
            tooltip.html(tooltipText)
                .style("left", (d3.event.pageX + 10) + "px")
                .style("top", (d3.event.pageY - 28) + "px");
            
            // Рисуем вертикальную линию
            g.selectAll(".mouseLine").remove();
            g.append("line")
                .attr("class", "mouseLine overlay-line")
                .attr("x1", x(iterationData.iteration))
                .attr("x2", x(iterationData.iteration))
                .attr("y1", 0)
                .attr("y2", height);
        })
        .on("mouseout", function() {
            // Убираем tooltip и линию
            tooltip.transition()
                .duration(500)
                .style("opacity", 0);
            
            g.selectAll(".mouseLine").remove();
        });
}

// Функция для отрисовки графика Budget и CPU
function renderBudgetChart(cpuData) {
    const svg = d3.select("#budgetChart");
    svg.selectAll("*").remove(); // Очищаем предыдущий график
    
    const margin = {top: 30, right: 80, bottom: 40, left: 60};
    const width = svg.node().getBoundingClientRect().width - margin.left - margin.right;
    const height = svg.node().getBoundingClientRect().height - margin.top - margin.bottom;
    
    const g = svg.append("g")
        .attr("transform", `translate(${margin.left},${margin.top})`);
    
    // Если нет данных, выходим
    if (cpuData.iterations.length === 0) {
        g.append("text")
            .attr("x", width / 2)
            .attr("y", height / 2)
            .attr("text-anchor", "middle")
            .text("Нет данных для построения графика");
        return;
    }
    
    // Выбираем метрику
    const metricType = cpuMetricSelect.val(); // elapsedCpu или usedCpu
    const valueType = cpuValueSelect.val();   // cpu или lastSecondCpu
    
    // Создаем шкалы
    const x = d3.scaleLinear()
        .domain(d3.extent(cpuData.iterations))
        .range([0, width]);
    
    // Находим максимальное значение для шкалы Y
    const maxBudget = d3.max(cpuData.budget) || 10;
    const maxCpu = metricType === 'elapsedCpu' ? 
        d3.max(cpuData.totalElapsedCpu) : 
        d3.max(cpuData.totalUsedCpu);
    
    const maxValue = Math.max(maxBudget, maxCpu || 0);
    
    const y = d3.scaleLinear()
        .domain([0, maxValue * 1.1]) // Добавляем 10% отступа сверху
        .range([height, 0]);
    
    // Создаем оси
    g.append("g")
        .attr("class", "grid")
        .attr("transform", `translate(0,${height})`)
        .call(d3.axisBottom(x).ticks(10).tickSize(-height))
        .append("text")
        .attr("class", "axis-label")
        .attr("x", width / 2)
        .attr("y", 35)
        .text("Итерация");
    
    g.append("g")
        .attr("class", "grid")
        .call(d3.axisLeft(y).ticks(10).tickSize(-width))
        .append("text")
        .attr("class", "axis-label")
        .attr("transform", "rotate(-90)")
        .attr("x", -height / 2)
        .attr("y", -45)
        .text("Значение");
    
    // Рисуем линию для Budget
    const budgetLine = d3.line()
        .x((d, i) => x(cpuData.iterations[i]))
        .y(d => y(d))
        .curve(d3.curveMonotoneX);
    
    g.append("path")
        .datum(cpuData.budget)
        .attr("class", "line")
        .attr("d", budgetLine)
        .attr("stroke", colorScheme.budget)
        .attr("stroke-width", 2)
        .attr("fill", "none");
    
    // Рисуем линию для CPU
    const cpuValues = metricType === 'elapsedCpu' ? 
        cpuData.totalElapsedCpu : 
        cpuData.totalUsedCpu;
    
    const cpuLine = d3.line()
        .x((d, i) => x(cpuData.iterations[i]))
        .y(d => y(d))
        .curve(d3.curveMonotoneX);
    
    g.append("path")
        .datum(cpuValues)
        .attr("class", "line")
        .attr("d", cpuLine)
        .attr("stroke", colorScheme.totalCpu)
        .attr("stroke-width", 2)
        .attr("fill", "none");
    
    // Создаем легенду
    const legend = svg.append("g")
        .attr("class", "chart-legend")
        .attr("transform", `translate(${margin.left}, 10)`);
    
    // Легенда для Budget
    const budgetLegend = legend.append("g")
        .attr("class", "legend-item")
        .attr("transform", "translate(0, 0)");
    
    budgetLegend.append("rect")
        .attr("class", "legend-color")
        .attr("width", 16)
        .attr("height", 10)
        .attr("fill", colorScheme.budget);
    
    budgetLegend.append("text")
        .attr("class", "legend-text")
        .attr("x", 20)
        .attr("y", 9)
        .text("Budget");
    
    // Легенда для CPU
    const cpuLegend = legend.append("g")
        .attr("class", "legend-item")
        .attr("transform", "translate(100, 0)");
    
    cpuLegend.append("rect")
        .attr("class", "legend-color")
        .attr("width", 16)
        .attr("height", 10)
        .attr("fill", colorScheme.totalCpu);
    
    cpuLegend.append("text")
        .attr("class", "legend-text")
        .attr("x", 20)
        .attr("y", 9)
        .text(metricType === 'elapsedCpu' ? 'Total Elapsed CPU' : 'Total Used CPU');
    
    // Создаем tooltip
    const tooltip = d3.select("body").append("div")
        .attr("class", "tooltip")
        .style("opacity", 0);
    
    // Создаем невидимую область для отслеживания движения мыши
    g.append("rect")
        .attr("width", width)
        .attr("height", height)
        .attr("fill", "none")
        .attr("pointer-events", "all")
        .on("mousemove", function() {
            const mouseX = d3.mouse(this)[0];
            const iteration = Math.round(x.invert(mouseX));
            
            // Находим ближайшую итерацию
            const closestIterationIndex = cpuData.iterations.reduce((closest, curr, idx) => {
                return Math.abs(curr - iteration) < Math.abs(cpuData.iterations[closest] - iteration) ? idx : closest;
            }, 0);
            
            // Формируем текст для tooltip
            const iterationNumber = cpuData.iterations[closestIterationIndex];
            const budgetValue = cpuData.budget[closestIterationIndex];
            const cpuValue = cpuValues[closestIterationIndex];
            
            let tooltipText = `<strong>Итерация: ${iterationNumber}</strong><br>`;
            tooltipText += `<strong>Budget:</strong> ${budgetValue.toFixed(5)}<br>`;
            tooltipText += `<strong>${metricType === 'elapsedCpu' ? 'Total Elapsed CPU' : 'Total Used CPU'}:</strong> ${cpuValue.toFixed(5)}<br>`;
            
            // Отображаем tooltip
            tooltip.transition()
                .duration(200)
                .style("opacity", .9);
            
            tooltip.html(tooltipText)
                .style("left", (d3.event.pageX + 10) + "px")
                .style("top", (d3.event.pageY - 28) + "px");
            
            // Рисуем вертикальную линию
            g.selectAll(".mouseLine").remove();
            g.append("line")
                .attr("class", "mouseLine overlay-line")
                .attr("x1", x(iterationNumber))
                .attr("x2", x(iterationNumber))
                .attr("y1", 0)
                .attr("y2", height);
        })
        .on("mouseout", function() {
            // Убираем tooltip и линию
            tooltip.transition()
                .duration(500)
                .style("opacity", 0);
            
            g.selectAll(".mouseLine").remove();
        });
} 