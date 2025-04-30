document.head.insertAdjacentHTML('beforeend', `
<style>
.brush .selection {
    fill: #3498db;
    fill-opacity: 0.2;
    stroke: #2980b9;
    stroke-width: 1px;
}
.tooltip {
    position: absolute;
    background-color: rgba(255, 255, 255, 0.95);
    border: 1px solid #ddd;
    border-radius: 4px;
    padding: 8px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.15);
    pointer-events: none;
    font-size: 12px;
    max-width: 300px;
    z-index: 1100;
}
.mouseLine {
    stroke: #555;
    stroke-width: 1px;
    stroke-dasharray: 3,3;
}
</style>
`);

function initChart(svgId, margin = {top: 20, right: 80, bottom: 40, left: 60}) {
    const svgElement = document.getElementById(svgId);
    if (!svgElement) {
        console.error(`initChart: не найден DOM элемент #${svgId}`);
        return null;
    }

    const svg = d3.select(`#${svgId}`);
    svg.selectAll("*").remove();
    
    const containerWidth = svg.node().getBoundingClientRect().width;
    const containerHeight = svg.node().getBoundingClientRect().height;
    
    if (containerWidth <= margin.left + margin.right || containerHeight <= margin.top + margin.bottom) {
        console.error(`initChart: слишком маленькие размеры контейнера для ${svgId}`, {
            containerWidth, containerHeight, margin
        });
        
        svg.append("text")
            .attr("x", 10)
            .attr("y", 30)
            .attr("fill", "red")
            .text("Ошибка: невозможно отобразить график (размеры контейнера слишком малы)");
        return null;
    }
    
    const width = containerWidth - margin.left - margin.right;
    const height = containerHeight - margin.top - margin.bottom;
    
    console.log(`initChart: размеры графика для ${svgId}:`, {
        containerWidth, containerHeight, width, height
    });
    
    const g = svg.append("g")
        .attr("transform", `translate(${margin.left},${margin.top})`)
        .attr("width", width)
        .attr("height", height);
    
    return { svg, g, width, height };
}

function createAxes(g, x, y, width, height, xLabel, yLabel) {
    g.append("g")
        .attr("class", "grid")
        .attr("transform", `translate(0,${height})`)
        .call(d3.axisBottom(x).ticks(10).tickSize(-height))
        .append("text")
        .attr("class", "axis-label")
        .attr("x", width / 2)
        .attr("y", 35)
        .text(xLabel);
    
    g.append("g")
        .attr("class", "grid")
        .call(d3.axisLeft(y).ticks(10).tickSize(-width))
        .append("text")
        .attr("class", "axis-label")
        .attr("transform", "rotate(-90)")
        .attr("x", -height / 2)
        .attr("y", -45)
        .text(yLabel);
}

function createTooltip() {
    let tooltip = d3.select("body > .tooltip");
    
    if (tooltip.empty()) {
        tooltip = d3.select("body").append("div")
            .attr("class", "tooltip")
            .style("opacity", 0);
    }
    
    return tooltip;
}

function showTooltip(tooltip, html, x, y) {
    tooltip.transition()
        .duration(200)
        .style("opacity", .9);
    
    tooltip.html(html)
        .style("left", (x + 10) + "px")
        .style("top", (y - 28) + "px");
}

function hideTooltip(tooltip) {
    tooltip.transition()
        .duration(500)
        .style("opacity", 0);
}

function findClosestIndex(array, value) {
    return array.reduce((closest, curr, idx) => {
        return Math.abs(curr - value) < Math.abs(array[closest] - value) ? idx : closest;
    }, 0);
}

function createMouseLine(g, x, xValue, height) {
    g.selectAll(".mouseLine").remove();
    g.append("line")
        .attr("class", "mouseLine overlay-line")
        .attr("x1", x(xValue))
        .attr("x2", x(xValue))
        .attr("y1", 0)
        .attr("y2", height);
}

function createLegend(svg, items, margin, clickHandler = null) {
    const legend = svg.append("g")
        .attr("class", "chart-legend")
        .attr("transform", `translate(${margin.left}, ${margin.top})`);
    
    items.forEach((item, i) => {
        const legendItem = legend.append("g")
            .attr("class", "legend-item")
            .attr("transform", `translate(${i * 120}, 0)`);
        
        if (clickHandler) {
            legendItem.on("click", () => clickHandler(item.name));
        }
        
        if (item.type === 'line') {
            legendItem.append("line")
                .attr("class", "legend-color")
                .attr("x1", 0)
                .attr("y1", 5)
                .attr("x2", 16)
                .attr("y2", 5)
                .attr("stroke", item.color)
                .attr("stroke-width", 2)
                .attr("stroke-dasharray", item.dashed ? "5,5" : null);
        } else {
            legendItem.append("rect")
                .attr("class", "legend-color")
                .attr("width", 16)
                .attr("height", 10)
                .attr("fill", item.color);
        }
        
        legendItem.append("text")
            .attr("class", "legend-text")
            .attr("x", 20)
            .attr("y", 9)
            .text(item.name);
    });
    
    return legend;
}

function renderMetricsChart() {
    if (!currentData || !currentData.history || currentData.history.length === 0) {
        console.log("renderMetricsChart: нет данных для отображения");
        return;
    }
    
    console.log("renderMetricsChart: начинаем отрисовку, история:", currentData.history.length);
    const selectedMetric = chartMetricSelect.val();
    console.log("renderMetricsChart: выбранная метрика:", selectedMetric);

    const margin = {top: 20, right: 80, bottom: 40, left: 60};
    const chart = initChart("metricsChart", margin);
    if (!chart) return;
    
    const { svg, g, width, height } = chart;
    
    let metricData = [...currentData.history].sort((a, b) => a.timestamp - b.timestamp);
    const allIterations = metricData.map(d => d.iteration);
    
    const x = d3.scaleLinear()
        .domain(d3.extent(metricData, d => d.iteration))
        .range([0, width]);
    
    const metricValues = metricData.map(d => d[selectedMetric]).filter(d => d !== undefined && d !== null);
    
    if (metricValues.length === 0) {
        g.append("text")
            .attr("x", width / 2)
            .attr("y", height / 2)
            .attr("text-anchor", "middle")
            .text("Нет данных для выбранной метрики");
        return;
    }
    
    const y = d3.scaleLinear()
        .domain([0, d3.max(metricValues) * 1.1])
        .range([height, 0]);
    
    createAxes(g, x, y, width, height, "Итерация", selectedMetric);
    
    const line = d3.line()
        .defined(d => d[selectedMetric] !== undefined && d[selectedMetric] !== null)
        .x(d => x(d.iteration))
        .y(d => y(d[selectedMetric]))
        .curve(d3.curveMonotoneX);
    
    g.append("path")
        .datum(metricData)
        .attr("class", `line line-${selectedMetric}`)
        .attr("d", line)
        .style("stroke", colorScheme[selectedMetric] || "#007bff");
    
    const dots = g.selectAll(".dot")
        .data(metricData.filter(d => d[selectedMetric] !== undefined && d[selectedMetric] !== null))
        .enter().append("circle")
        .attr("class", "dot")
        .attr("cx", d => x(d.iteration))
        .attr("cy", d => y(d[selectedMetric]))
        .attr("r", 3)
        .style("fill", colorScheme[selectedMetric] || "#007bff");
    
    const tooltip = createTooltip();

    const tooltipGenerator = (iteration, index) => {
        const dataPoint = metricData.find(d => d.iteration === iteration);
        if (!dataPoint) return null;
        return `<strong>Итерация: ${dataPoint.iteration}</strong><br>${selectedMetric}: ${dataPoint[selectedMetric].toFixed(5)}`;
    };
    addBrushToChart(g, x, height, allIterations, tooltip, tooltipGenerator, findClosestIndex, dots);
}

function renderPoolChart() {
    if (!currentData || !currentData.history || currentData.history.length === 0) {
        console.log("renderPoolChart: нет данных для отображения");
        return;
    }

    const selectedPool = poolSelect.val();
    const selectedMetric = poolMetricSelect.val();
    
    console.log("renderPoolChart: начинаем отрисовку, пул:", selectedPool, "метрика:", selectedMetric);
    
    if (!selectedPool || !selectedMetric) {
        console.log("renderPoolChart: не выбран пул или метрика");
        return;
    }
    
    const margin = {top: 20, right: 80, bottom: 40, left: 60};
    const chart = initChart("poolChart", margin);
    if (!chart) return;
    
    const { svg, g, width, height } = chart;
    
    let poolData = [];
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
    
    poolData.sort((a, b) => a.iteration - b.iteration);
    
    if (poolData.length === 0) {
        g.append("text")
            .attr("x", width / 2)
            .attr("y", height / 2)
            .attr("text-anchor", "middle")
            .text("Нет данных для выбранного пула и метрики");
        return;
    }
    
    const x = d3.scaleLinear()
        .domain(d3.extent(poolData, d => d.iteration))
        .range([0, width]);
    
    const y = d3.scaleLinear()
        .domain([0, d3.max(poolData, d => d.value) * 1.1])
        .range([height, 0]);
    
    createAxes(g, x, y, width, height, "Итерация", selectedMetric);
    
    const line = d3.line()
        .x(d => x(d.iteration))
        .y(d => y(d.value))
        .curve(d3.curveMonotoneX);
    
    g.append("path")
        .datum(poolData)
        .attr("class", "line")
        .attr("d", line)
        .style("stroke", poolColorMap[selectedPool] || colorScheme[selectedMetric] || "#20c997");
    
    const dots = g.selectAll(".dot")
        .data(poolData)
        .enter().append("circle")
        .attr("class", "dot")
        .attr("cx", d => x(d.iteration))
        .attr("cy", d => y(d.value))
        .attr("r", 3)
        .style("fill", poolColorMap[selectedPool] || colorScheme[selectedMetric] || "#20c997");
    
    const tooltip = createTooltip();
    
    g.append("rect")
        .attr("width", width)
        .attr("height", height)
        .attr("fill", "none")
        .attr("pointer-events", "all")
        .on("mousemove", function() {
            const mouseX = d3.mouse(this)[0];
            const iteration = Math.round(x.invert(mouseX));
            
            if (poolData.length === 0) return;
            
            const closestIterationIndex = findClosestIndex(poolData.map(d => d.iteration), iteration);
            const closestDataPoint = poolData[closestIterationIndex];
            
            const tooltipText = `<strong>Итерация: ${closestDataPoint.iteration}</strong><br>${selectedMetric}: ${closestDataPoint.value.toFixed(5)}`;
            
            showTooltip(tooltip, tooltipText, d3.event.pageX, d3.event.pageY);
            
            createMouseLine(g, x, closestDataPoint.iteration, height);
            
            dots.attr("r", 3);
            dots.filter(d => d.iteration === closestDataPoint.iteration)
                .attr("r", 5);
        })
        .on("mouseout", function() {
            hideTooltip(tooltip);
            g.selectAll(".mouseLine").remove();
            dots.attr("r", 3);
        });
}

function renderCpuCharts() {
    if (!currentData || !currentData.history || currentData.history.length === 0) {
        console.log("renderCpuCharts: нет данных для отображения");
        return;
    }
    
    if (!document.getElementById("cpuPoolsChart") || 
        !document.getElementById("threadsChart") || 
        !document.getElementById("budgetChart")) {
        console.error("renderCpuCharts: не найдены DOM элементы для графиков");
        return;
    }
    
    console.log("renderCpuCharts: начинаем отрисовку, история:", currentData.history.length);
    
    const cpuData = prepareCpuData();
    
    console.log("renderCpuCharts: данные подготовлены, итераций:", cpuData.iterations.length);
    
    renderCpuPoolsChart(cpuData);
    
    renderThreadsChart(cpuData);
    
    renderBudgetChart(cpuData);
}

function renderCpuPoolsChart(cpuData) {
    console.log("renderCpuPoolsChart: начинаем отрисовку");
    
    const margin = {top: 30, right: 80, bottom: 40, left: 60};
    const chart = initChart("cpuPoolsChart", margin);
    if (!chart) return;
    
    const { svg, g, width, height } = chart;
    
    if (cpuData.iterations.length === 0) {
        console.log("renderCpuPoolsChart: нет итераций в данных");
        g.append("text")
            .attr("x", width / 2)
            .attr("y", height / 2)
            .attr("text-anchor", "middle")
            .text("Нет данных для построения графика");
        return;
    }
    
    const metricType = cpuMetricSelect.val();
    const valueType = cpuValueSelect.val();  
    
    const x = d3.scaleLinear()
        .domain(d3.extent(cpuData.iterations))
        .range([0, width]);
    
    const areaData = [];
    
    const visiblePoolNames = Array.from(visiblePools);
    const visiblePoolData = {};
    
    visiblePoolNames.forEach(poolName => {
        if (cpuData.pools[poolName]) {
            const values = cpuData.pools[poolName][metricType][valueType];
                visiblePoolData[poolName] = values;
        }
    });
    
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
        .domain([0, maxValue * 1.1])
        .range([height, 0]);
    
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
    
    createAxes(g, x, y, width, height, "Итерация", metricType === 'elapsedCpu' ? 'Elapsed CPU' : 'Used CPU');
    
    const legendItems = visiblePoolNames.map(poolName => ({
        name: poolName,
        color: poolColorMap[poolName] || "#000000"
    }));
    
    createLegend(svg, legendItems, {left: margin.left, top: 10}, function(poolName) {
            if (visiblePools.has(poolName)) {
                visiblePools.delete(poolName);
                d3.select(this).classed("legend-item-disabled", true);
            } else {
                visiblePools.add(poolName);
                d3.select(this).classed("legend-item-disabled", false);
            }
            renderCpuCharts();
        });
    
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
    
    const tooltip = createTooltip();

    const tooltipGenerator = (iteration) => {
        const iterationData = stackData.find(d => d.iteration === iteration);
        if (!iterationData) {
            console.log("[CpuPoolsChart Tooltip] No data found for iteration:", iteration);
            return null;
        }
        
        let tooltipText = `<strong>Итерация: ${iterationData.iteration}</strong><br>`;
        visiblePoolNames.forEach(poolName => {
            const value = iterationData[poolName];
            if (value !== undefined) {
                tooltipText += `${poolName}: ${value.toFixed(5)}<br>`;
            }
        });
        console.log("[CpuPoolsChart Tooltip] Iteration:", iteration, "Generated text:", tooltipText);
        return tooltipText;
    };
    addBrushToChart(g, x, height, cpuData.iterations, tooltip, tooltipGenerator, findClosestIndex);
}

function renderThreadsChart(cpuData) {
    const margin = {top: 30, right: 80, bottom: 40, left: 60};
    const chart = initChart("threadsChart", margin);
    if (!chart) return;
    
    const { svg, g, width, height } = chart;
    
    if (cpuData.iterations.length === 0) {
        g.append("text")
            .attr("x", width / 2)
            .attr("y", height / 2)
            .attr("text-anchor", "middle")
            .text("Нет данных для построения графика");
        return;
    }
    
    const x = d3.scaleLinear()
        .domain(d3.extent(cpuData.iterations))
        .range([0, width]);
    
    let filteredTotalThreadCount = cpuData.totalThreadCount;
    let filteredMaxThreadCount = cpuData.totalMaxThreadCount;
    
    const maxThreads = Math.max(
        d3.max(filteredTotalThreadCount) || 0, 
        d3.max(filteredMaxThreadCount) || 0
    );
    
    const y = d3.scaleLinear()
        .domain([0, maxThreads * 1.1])
        .range([height, 0]);
    
    createAxes(g, x, y, width, height, "Итерация", "Количество потоков");
    
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
    
    const maxThreadLine = d3.line()
        .x((d, i) => x(cpuData.iterations[i]))
        .y(d => y(d))
        .curve(d3.curveMonotoneX);
    
    g.append("path")
        .datum(filteredMaxThreadCount)
        .attr("class", "line")
        .attr("d", maxThreadLine)
        .attr("stroke", "#777")
        .attr("stroke-width", 1.5)
        .attr("stroke-dasharray", "5,5")
        .attr("fill", "none");
    
    const poolLegendItems = Array.from(visiblePools)
        .filter(poolName => cpuData.poolsThreadCount[poolName])
        .map(poolName => ({
            name: poolName,
            color: poolColorMap[poolName] || "#000000"
        }));
    
    poolLegendItems.push({
        name: "Max Threads",
        color: "#777",
        type: "line",
        dashed: true
    });
    
    createLegend(svg, poolLegendItems, {left: margin.left, top: 10});
    
    const tooltip = createTooltip();

    const tooltipGenerator = (iteration) => {
        const index = cpuData.iterations.findIndex(it => it === iteration);
        if (index === -1) {
            console.log("[ThreadsChart Tooltip] Index not found for iteration:", iteration);
            return null;
        }

        let tooltipText = `<strong>Итерация: ${iteration}</strong><br>`;
        tooltipText += `<strong>Всего потоков:</strong> ${filteredTotalThreadCount[index]}<br>`;
        tooltipText += `<strong>Макс. потоков:</strong> ${filteredMaxThreadCount[index]}<br><br>`;
        
        visiblePools.forEach(poolName => {
            if (cpuData.poolsThreadCount[poolName]) {
                const currentThreads = cpuData.poolsThreadCount[poolName].current[index];
                const maxThreads = cpuData.poolsThreadCount[poolName].potential[index];
                
                if (currentThreads !== undefined) {
                    tooltipText += `${poolName}: ${currentThreads} / ${maxThreads}<br>`;
                }
            }
        });
        console.log("[ThreadsChart Tooltip] Iteration:", iteration, "Index:", index, "Generated text:", tooltipText);
        return tooltipText;
    };
    addBrushToChart(g, x, height, cpuData.iterations, tooltip, tooltipGenerator, findClosestIndex);
}

function renderBudgetChart(cpuData) {
    const margin = {top: 30, right: 80, bottom: 40, left: 60};
    const chart = initChart("budgetChart", margin);
    if (!chart) return;
    
    const { svg, g, width, height } = chart;
    
    if (cpuData.iterations.length === 0) {
        g.append("text")
            .attr("x", width / 2)
            .attr("y", height / 2)
            .attr("text-anchor", "middle")
            .text("Нет данных для построения графика");
        return;
    }
    
    const metricType = cpuMetricSelect.val();
    const valueType = cpuValueSelect.val();  
    
    const x = d3.scaleLinear()
        .domain(d3.extent(cpuData.iterations))
        .range([0, width]);
    
    const maxBudget = d3.max(cpuData.budget) || 10;
    const maxCpu = d3.max(cpuData.totalCpu) || 0;
    
    const maxValue = Math.max(maxBudget, maxCpu);
    
    const y = d3.scaleLinear()
        .domain([0, maxValue * 1.1])
        .range([height, 0]);
    
    createAxes(g, x, y, width, height, "Итерация", "Значение");
    
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
    
    const cpuLine = d3.line()
        .x((d, i) => x(cpuData.iterations[i]))
        .y(d => y(d))
        .curve(d3.curveMonotoneX);
    
    g.append("path")
        .datum(cpuData.totalCpu)
        .attr("class", "line")
        .attr("d", cpuLine)
        .attr("stroke", colorScheme.totalCpu)
        .attr("stroke-width", 2)
        .attr("fill", "none");
    
    const legendItems = [
        {
            name: "Budget",
            color: colorScheme.budget
        },
        {
            name: metricType === 'elapsedCpu' ? 'Total Elapsed CPU' : 'Total Used CPU',
            color: colorScheme.totalCpu
        }
    ];
    
    createLegend(svg, legendItems, {left: margin.left, top: 10});
    
    const tooltip = createTooltip();

    const tooltipGenerator = (iteration) => {
        const index = cpuData.iterations.findIndex(it => it === iteration);
        if (index === -1) {
            console.log("[BudgetChart Tooltip] Index not found for iteration:", iteration);
            return null;
        }
        
        const budgetValue = cpuData.budget[index];
        const cpuValue = cpuData.totalCpu[index];
        const iterationNumber = iteration;
        
        let tooltipText = `<strong>Итерация: ${iterationNumber}</strong><br>`;
        tooltipText += `<strong>Budget:</strong> ${budgetValue.toFixed(5)}<br>`;
        tooltipText += `<strong>${metricType === 'elapsedCpu' ? 'Total Elapsed CPU' : 'Total Used CPU'}:</strong> ${cpuValue.toFixed(5)}<br>`;
        console.log("[BudgetChart Tooltip] Iteration:", iteration, "Index:", index, "Generated text:", tooltipText);
        return tooltipText;
    };
    addBrushToChart(g, x, height, cpuData.iterations, tooltip, tooltipGenerator, findClosestIndex);
}

function addBrushToChart(g, x, height, allIterations, tooltip, tooltipContentGenerator, iterationFinder = findClosestIteration, dots = null) {
    if (!window.brushGroups) window.brushGroups = [];

    const brush = d3.brushX()
        .extent([[0, 0], [g.attr("width"), height]])
        .on("end", brushed);

    const brushGroup = g.append("g")
        .attr("class", "brush")
        .call(brush);

    window.brushGroups.push({ brushGroup, brush });

    const overlay = brushGroup.select(".overlay");
    if (!overlay.empty()) {
        overlay
            .on("mousemove.tooltip", function() {
                if (!tooltipContentGenerator) return;
                
            const mouseX = d3.mouse(this)[0];
            const iterationValue = x.invert(mouseX);
            
                const closestIterationIndex = iterationFinder(allIterations, iterationValue);
                const closestIteration = allIterations[closestIterationIndex];
                
                const tooltipText = tooltipContentGenerator(closestIteration, closestIterationIndex);
                
                if (tooltipText) {
                    showTooltip(tooltip, tooltipText, d3.event.pageX, d3.event.pageY);
            
                    createMouseLine(g, x, closestIteration, height);
                    
                    if (dots) {
                        dots.attr("r", 3);
                        dots.filter(d => d.iteration === closestIteration)
                            .attr("r", 5);
                    }
                }
            })
            .on("mouseout.tooltip", function() {
                hideTooltip(tooltip);
                g.selectAll(".mouseLine").remove();
                if (dots) {
                    dots.attr("r", 3);
                }
            });
    } else {
        console.warn("Не удалось найти .overlay в brush группе для добавления тултипов");
    }

    function brushed() {
        if (!d3.event.sourceEvent) return;

        if (d3.event.sourceEvent.type === 'dblclick') {
            clearAllBrushes();
            return;
        }
            
        if (!d3.event.selection) return;
            
        const [x0, x1] = d3.event.selection.map(x.invert);
            
        const startIteration = findClosestIteration(allIterations, x0);
        const endIteration = findClosestIteration(allIterations, x1);
            
        if (startIteration === endIteration) {
            return;
        }
            
        console.log("Выбран диапазон итераций через brush:", startIteration, endIteration);

        iterationFrom.val(startIteration);
        iterationTo.val(endIteration);

        iterationFrom.trigger('change');
        iterationTo.trigger('change');
    }
    
    return brushGroup;
}

function findClosestIteration(iterations, value) {
    if (!iterations || iterations.length === 0) return undefined;
    return iterations.reduce((closest, current) => {
        return Math.abs(current - value) < Math.abs(closest - value) ? current : closest;
    }, iterations[0]);
}

window.clearAllBrushes = function() {
    if (window.brushGroups) {
        window.brushGroups.forEach(({ brushGroup, brush }) => {
            if (brushGroup && brush) {
                brushGroup.call(brush.move, null);
            }
        });
    }
};
