// Добавляем CSS стили (если не добавлены в основной CSS)
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

// Вспомогательные функции для устранения дублирования кода

/**
 * Инициализирует график: создает SVG, вычисляет размеры, проверяет корректность
 * @param {string} svgId - ID SVG элемента
 * @param {Object} margin - отступы графика {top, right, bottom, left}
 * @returns {Object|null} - объект с элементами для построения графика или null при ошибке
 */
function initChart(svgId, margin = {top: 20, right: 80, bottom: 40, left: 60}) {
    // Проверяем наличие DOM элемента
    const svgElement = document.getElementById(svgId);
    if (!svgElement) {
        console.error(`initChart: не найден DOM элемент #${svgId}`);
        return null;
    }

    const svg = d3.select(`#${svgId}`);
    svg.selectAll("*").remove(); // Очищаем предыдущий график
    
    // Получаем размеры SVG контейнера
    const containerWidth = svg.node().getBoundingClientRect().width;
    const containerHeight = svg.node().getBoundingClientRect().height;
    
    // Проверяем, что размеры положительные и достаточно большие
    if (containerWidth <= margin.left + margin.right || containerHeight <= margin.top + margin.bottom) {
        console.error(`initChart: слишком маленькие размеры контейнера для ${svgId}`, {
            containerWidth, containerHeight, margin
        });
        
        // Добавляем сообщение об ошибке в SVG
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

/**
 * Создает оси графика
 * @param {Object} g - группа SVG для отрисовки
 * @param {Function} x - шкала X
 * @param {Function} y - шкала Y
 * @param {number} width - ширина графика
 * @param {number} height - высота графика
 * @param {string} xLabel - подпись оси X
 * @param {string} yLabel - подпись оси Y
 */
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

/**
 * Создает или возвращает существующий tooltip
 * @returns {Object} - D3 selection с tooltip
 */
function createTooltip() {
    // Проверяем, существует ли уже tooltip
    let tooltip = d3.select("body > .tooltip");
    
    // Если нет, создаем его
    if (tooltip.empty()) {
        tooltip = d3.select("body").append("div")
            .attr("class", "tooltip")
            .style("opacity", 0);
    }
    
    return tooltip;
}

/**
 * Показывает tooltip с указанным текстом
 * @param {Object} tooltip - D3 selection с tooltip
 * @param {string} html - HTML содержимое tooltip
 * @param {number} x - координата X
 * @param {number} y - координата Y
 */
function showTooltip(tooltip, html, x, y) {
    tooltip.transition()
        .duration(200)
        .style("opacity", .9);
    
    tooltip.html(html)
        .style("left", (x + 10) + "px")
        .style("top", (y - 28) + "px");
}

/**
 * Скрывает tooltip
 * @param {Object} tooltip - D3 selection с tooltip
 */
function hideTooltip(tooltip) {
    tooltip.transition()
        .duration(500)
        .style("opacity", 0);
}

/**
 * Находит индекс ближайшего элемента в массиве к указанному значению
 * @param {Array} array - исходный массив
 * @param {number} value - значение для поиска
 * @returns {number} - индекс ближайшего элемента
 */
function findClosestIndex(array, value) {
    return array.reduce((closest, curr, idx) => {
        return Math.abs(curr - value) < Math.abs(array[closest] - value) ? idx : closest;
    }, 0);
}

/**
 * Создает вертикальную линию для отображения позиции курсора
 * @param {Object} g - группа SVG для отрисовки
 * @param {Function} x - шкала X
 * @param {number} xValue - значение X для позиции линии
 * @param {number} height - высота графика
 */
function createMouseLine(g, x, xValue, height) {
    g.selectAll(".mouseLine").remove();
    g.append("line")
        .attr("class", "mouseLine overlay-line")
        .attr("x1", x(xValue))
        .attr("x2", x(xValue))
        .attr("y1", 0)
        .attr("y2", height);
}

/**
 * Создает легенду для графика
 * @param {Object} svg - SVG элемент
 * @param {Array} items - элементы легенды [{name, color}]
 * @param {Object} margin - отступы {left, top}
 * @param {Function} clickHandler - обработчик клика (опционально)
 */
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
        
        // Для линий используем line, для остальных rect
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

// Функция для отрисовки графика метрик
function renderMetricsChart() {
    if (!currentData || !currentData.history || currentData.history.length === 0) {
        console.log("renderMetricsChart: нет данных для отображения");
        return;
    }
    
    console.log("renderMetricsChart: начинаем отрисовку, история:", currentData.history.length);
    const selectedMetric = chartMetricSelect.val();
    console.log("renderMetricsChart: выбранная метрика:", selectedMetric);

    // Инициализируем график
    const margin = {top: 20, right: 80, bottom: 40, left: 60};
    const chart = initChart("metricsChart", margin);
    if (!chart) return;
    
    const { svg, g, width, height } = chart;
    
    // Получаем отсортированные по времени данные
    let metricData = [...currentData.history].sort((a, b) => a.timestamp - b.timestamp);
    const allIterations = metricData.map(d => d.iteration); // Получаем все итерации для brush
    
    // Создаем шкалы
    const x = d3.scaleLinear()
        .domain(d3.extent(metricData, d => d.iteration))
        .range([0, width]);
    
    // Находим минимальное и максимальное значение метрики
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
    createAxes(g, x, y, width, height, "Итерация", selectedMetric);
    
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
    const dots = g.selectAll(".dot")
        .data(metricData.filter(d => d[selectedMetric] !== undefined && d[selectedMetric] !== null))
        .enter().append("circle")
        .attr("class", "dot")
        .attr("cx", d => x(d.iteration))
        .attr("cy", d => y(d[selectedMetric]))
        .attr("r", 3)
        .style("fill", colorScheme[selectedMetric] || "#007bff");
    
    // Создаем tooltip
    const tooltip = createTooltip();

    // Добавляем возможность выделения диапазона и передаем параметры для тултипа
    const tooltipGenerator = (iteration, index) => {
        const dataPoint = metricData.find(d => d.iteration === iteration);
        if (!dataPoint) return null;
        return `<strong>Итерация: ${dataPoint.iteration}</strong><br>${selectedMetric}: ${dataPoint[selectedMetric].toFixed(5)}`;
    };
    addBrushToChart(g, x, height, allIterations, tooltip, tooltipGenerator, findClosestIndex, dots);
}

// Функция для отрисовки графика пула
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
    
    // Инициализируем график
    const margin = {top: 20, right: 80, bottom: 40, left: 60};
    const chart = initChart("poolChart", margin);
    if (!chart) return;
    
    const { svg, g, width, height } = chart;
    
    // Подготавливаем данные для графика
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
    createAxes(g, x, y, width, height, "Итерация", selectedMetric);
    
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
    const dots = g.selectAll(".dot")
        .data(poolData)
        .enter().append("circle")
        .attr("class", "dot")
        .attr("cx", d => x(d.iteration))
        .attr("cy", d => y(d.value))
        .attr("r", 3)
        .style("fill", poolColorMap[selectedPool] || colorScheme[selectedMetric] || "#20c997");
    
    // Создаем tooltip
    const tooltip = createTooltip();
    
    // Создаем невидимую область для отслеживания движения мыши
    g.append("rect")
        .attr("width", width)
        .attr("height", height)
        .attr("fill", "none")
        .attr("pointer-events", "all")
        .on("mousemove", function() {
            const mouseX = d3.mouse(this)[0];
            const iteration = Math.round(x.invert(mouseX));
            
            // Находим ближайшую итерацию в данных
            if (poolData.length === 0) return;
            
            const closestIterationIndex = findClosestIndex(poolData.map(d => d.iteration), iteration);
            const closestDataPoint = poolData[closestIterationIndex];
            
            // Формируем текст для tooltip
            const tooltipText = `<strong>Итерация: ${closestDataPoint.iteration}</strong><br>${selectedMetric}: ${closestDataPoint.value.toFixed(5)}`;
            
            // Отображаем tooltip
            showTooltip(tooltip, tooltipText, d3.event.pageX, d3.event.pageY);
            
            // Рисуем вертикальную линию
            createMouseLine(g, x, closestDataPoint.iteration, height);
            
            // Увеличиваем размер точки
            dots.attr("r", 3); // Сбрасываем размер для всех точек
            dots.filter(d => d.iteration === closestDataPoint.iteration)
                .attr("r", 5); // Увеличиваем нужную
        })
        .on("mouseout", function() {
            // Убираем tooltip и линию
            hideTooltip(tooltip);
            g.selectAll(".mouseLine").remove();
            dots.attr("r", 3); // Возвращаем размер точек
    });
}

// Функция для отрисовки всех CPU графиков
function renderCpuCharts() {
    if (!currentData || !currentData.history || currentData.history.length === 0) {
        console.log("renderCpuCharts: нет данных для отображения");
        return;
    }
    
    // Проверяем наличие DOM элементов перед рендерингом
    if (!document.getElementById("cpuPoolsChart") || 
        !document.getElementById("threadsChart") || 
        !document.getElementById("budgetChart")) {
        console.error("renderCpuCharts: не найдены DOM элементы для графиков");
        return;
    }
    
    console.log("renderCpuCharts: начинаем отрисовку, история:", currentData.history.length);
    
    // Подготавливаем данные для всех графиков
    const cpuData = prepareCpuData(); // Вызываем prepareCpuData из data.js
    
    console.log("renderCpuCharts: данные подготовлены, итераций:", cpuData.iterations.length);
    
    // Рисуем график CPU Pools
    renderCpuPoolsChart(cpuData);
    
    // Рисуем график количества потоков
    renderThreadsChart(cpuData);
    
    // Рисуем график Budget
    renderBudgetChart(cpuData);
}

// Функция для отрисовки графика потребления CPU по пулам
function renderCpuPoolsChart(cpuData) {
    console.log("renderCpuPoolsChart: начинаем отрисовку");
    
    // Инициализируем график
    const margin = {top: 30, right: 80, bottom: 40, left: 60};
    const chart = initChart("cpuPoolsChart", margin);
    if (!chart) return;
    
    const { svg, g, width, height } = chart;
    
    // Если нет данных, выходим
    if (cpuData.iterations.length === 0) {
        console.log("renderCpuPoolsChart: нет итераций в данных");
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
    createAxes(g, x, y, width, height, "Итерация", metricType === 'elapsedCpu' ? 'Elapsed CPU' : 'Used CPU');
    
    // Создаем элементы легенды
    const legendItems = visiblePoolNames.map(poolName => ({
        name: poolName,
        color: poolColorMap[poolName] || "#000000"
    }));
    
    // Создаем легенду
    createLegend(svg, legendItems, {left: margin.left, top: 10}, function(poolName) {
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
    const tooltip = createTooltip();

    // Добавляем возможность выделения диапазона
    const tooltipGenerator = (iteration) => { // Принимаем только iteration
        // Находим данные для нужной итерации в stackData
        const iterationData = stackData.find(d => d.iteration === iteration);
        if (!iterationData) {
            console.log("[CpuPoolsChart Tooltip] No data found for iteration:", iteration);
            return null; // Если данных для итерации нет
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
    // Передаем findClosestIteration как iterationFinder, чтобы addBrushToChart нашел нужную итерацию
    addBrushToChart(g, x, height, cpuData.iterations, tooltip, tooltipGenerator, findClosestIndex);
}

// Функция для отрисовки графика количества потоков
function renderThreadsChart(cpuData) {
    // Инициализируем график
    const margin = {top: 30, right: 80, bottom: 40, left: 60};
    const chart = initChart("threadsChart", margin);
    if (!chart) return;
    
    const { svg, g, width, height } = chart;
    
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
    
    let filteredTotalThreadCount = cpuData.totalThreadCount;
    let filteredMaxThreadCount = cpuData.totalMaxThreadCount;
    
    // Находим максимальное значение для шкалы Y
    const maxThreads = Math.max(
        d3.max(filteredTotalThreadCount) || 0, 
        d3.max(filteredMaxThreadCount) || 0
    );
    
    const y = d3.scaleLinear()
        .domain([0, maxThreads * 1.1]) // Добавляем 10% отступа сверху
        .range([height, 0]);
    
    // Создаем оси
    createAxes(g, x, y, width, height, "Итерация", "Количество потоков");
    
    // Создаем области для стекирования потоков
    const stackData = [];
    cpuData.iterations.forEach((iteration, i) => {
        const point = { iteration };
        let cumulativeCurrent = 0;
        let cumulativeMax = 0;
        
        visiblePools.forEach(poolName => {
            if (cpuData.poolsThreadCount[poolName]) {
                // Данные текущего пула
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
        .datum(filteredMaxThreadCount)
        .attr("class", "line")
        .attr("d", maxThreadLine)
        .attr("stroke", "#777")
        .attr("stroke-width", 1.5)
        .attr("stroke-dasharray", "5,5")
        .attr("fill", "none");
    
    // Создаем элементы легенды для пулов
    const poolLegendItems = Array.from(visiblePools)
        .filter(poolName => cpuData.poolsThreadCount[poolName])
        .map(poolName => ({
            name: poolName,
            color: poolColorMap[poolName] || "#000000"
        }));
    
    // Добавляем элемент для максимальных потоков
    poolLegendItems.push({
        name: "Max Threads",
        color: "#777",
        type: "line",
        dashed: true
    });
    
    // Создаем легенду
    createLegend(svg, poolLegendItems, {left: margin.left, top: 10});
    
    // Создаем tooltip
    const tooltip = createTooltip();

    // Добавляем возможность выделения диапазона
    const tooltipGenerator = (iteration) => { // Принимаем только iteration
        // Находим индекс этой итерации в исходных данных
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
    // Передаем null вместо dots
    addBrushToChart(g, x, height, cpuData.iterations, tooltip, tooltipGenerator, findClosestIndex);
}

// Функция для отрисовки графика Budget и CPU
function renderBudgetChart(cpuData) {
    // Инициализируем график
    const margin = {top: 30, right: 80, bottom: 40, left: 60};
    const chart = initChart("budgetChart", margin);
    if (!chart) return;
    
    const { svg, g, width, height } = chart;
    
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
    const maxCpu = d3.max(cpuData.totalCpu) || 0;
    
    const maxValue = Math.max(maxBudget, maxCpu);
    
    const y = d3.scaleLinear()
        .domain([0, maxValue * 1.1]) // Добавляем 10% отступа сверху
        .range([height, 0]);
    
    // Создаем оси
    createAxes(g, x, y, width, height, "Итерация", "Значение");
    
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
    
    // Создаем элементы легенды
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
    
    // Создаем легенду
    createLegend(svg, legendItems, {left: margin.left, top: 10});
    
    // Создаем tooltip
    const tooltip = createTooltip();

    // Добавляем возможность выделения диапазона
    const tooltipGenerator = (iteration) => { // Принимаем только iteration
        // Находим индекс этой итерации в исходных данных
        const index = cpuData.iterations.findIndex(it => it === iteration);
        if (index === -1) {
            console.log("[BudgetChart Tooltip] Index not found for iteration:", iteration);
            return null;
        }
        
        const budgetValue = cpuData.budget[index];
        const cpuValue = cpuData.totalCpu[index];
        const iterationNumber = iteration; // Используем найденную итерацию
        
        let tooltipText = `<strong>Итерация: ${iterationNumber}</strong><br>`;
        tooltipText += `<strong>Budget:</strong> ${budgetValue.toFixed(5)}<br>`;
        tooltipText += `<strong>${metricType === 'elapsedCpu' ? 'Total Elapsed CPU' : 'Total Used CPU'}:</strong> ${cpuValue.toFixed(5)}<br>`;
        console.log("[BudgetChart Tooltip] Iteration:", iteration, "Index:", index, "Generated text:", tooltipText);
        return tooltipText;
    };
    addBrushToChart(g, x, height, cpuData.iterations, tooltip, tooltipGenerator, findClosestIndex);
}

/**
 * Вспомогательная функция для добавления brush (выделения диапазона) к графику
 * @param {Object} g - группа SVG для отрисовки
 * @param {Function} x - шкала X
 * @param {number} height - высота графика
 * @param {Array} allIterations - массив всех доступных итераций
 * @param {Object} tooltip - D3 selection объекта tooltip
 * @param {Function} tooltipContentGenerator - Функция (iterationData) => "html string" для генерации контента tooltip
 * @param {Function} [iterationFinder=findClosestIndex] - Функция для поиска индекса итерации
 * @param {Object} [dots=null] - D3 selection точек для подсветки
 */
function addBrushToChart(g, x, height, allIterations, tooltip, tooltipContentGenerator, iterationFinder = findClosestIteration, dots = null) {
    // Сохраняем brush группы для возможности сброса
    if (!window.brushGroups) window.brushGroups = [];

    const brush = d3.brushX()
        .extent([[0, 0], [g.attr("width"), height]])
        .on("end", brushed); // Обработчик завершения выделения

    const brushGroup = g.append("g")
        .attr("class", "brush")
        .call(brush);

    // Сохраняем для сброса
    window.brushGroups.push({ brushGroup, brush });

    // --- Логика для тултипов --- 
    // Находим overlay элемент, созданный d3.brush
    const overlay = brushGroup.select(".overlay");
    if (!overlay.empty()) {
        overlay
            .on("mousemove.tooltip", function() { // Добавляем .tooltip к имени события
                if (!tooltipContentGenerator) return; // Если генератор не передан
                
            const mouseX = d3.mouse(this)[0];
            const iterationValue = x.invert(mouseX);
            
            // Находим ближайшую итерацию
                // Используем переданный iterationFinder
                const closestIterationIndex = iterationFinder(allIterations, iterationValue);
                const closestIteration = allIterations[closestIterationIndex];
                
                // Генерируем контент тултипа
                // Предполагаем, что generator ожидает саму итерацию или индекс
                // Адаптируем, если нужно передавать весь объект данных
                const tooltipText = tooltipContentGenerator(closestIteration, closestIterationIndex);
                
                if (tooltipText) {
            // Отображаем tooltip
                    showTooltip(tooltip, tooltipText, d3.event.pageX, d3.event.pageY);
            
            // Рисуем вертикальную линию
                    createMouseLine(g, x, closestIteration, height);
                    
                    // Подсвечиваем точку, если dots переданы
                    if (dots) {
                        dots.attr("r", 3); // Сбрасываем размер для всех точек
                        dots.filter(d => d.iteration === closestIteration)
                            .attr("r", 5); // Увеличиваем нужную
                    }
                }
            })
            .on("mouseout.tooltip", function() { // Добавляем .tooltip к имени события
            // Убираем tooltip и линию
                hideTooltip(tooltip);
            g.selectAll(".mouseLine").remove();
                // Возвращаем размер точек
                if (dots) {
                    dots.attr("r", 3);
                }
            });
    } else {
        console.warn("Не удалось найти .overlay в brush группе для добавления тултипов");
    }
    // --- Конец логики для тултипов ---

    function brushed() { // Обработчик завершения выделения brush
        if (!d3.event.sourceEvent) return; // Игнорируем программные события brush

        // Сброс по двойному клику
        if (d3.event.sourceEvent.type === 'dblclick') {
            clearAllBrushes();
            // Возможно, стоит сбросить поля ввода к значениям по умолчанию или полному диапазону?
            // iterationFrom.val(-30); // Например
            // iterationTo.val("");
            // iterationFrom.trigger('change'); // Вызовет fetchData
                return;
            }
            
        // Если выделение снято (клик без протягивания)
        if (!d3.event.selection) return;
            
            const [x0, x1] = d3.event.selection.map(x.invert);
            
        // Находим ближайшие итерации к границам выделения
            const startIteration = findClosestIteration(allIterations, x0);
            const endIteration = findClosestIteration(allIterations, x1);
            
        // Если начало и конец совпадают, ничего не делаем
            if (startIteration === endIteration) {
                return;
            }
            
        console.log("Выбран диапазон итераций через brush:", startIteration, endIteration);

        // Обновляем поля ввода
        iterationFrom.val(startIteration);
        iterationTo.val(endIteration);

        // Триггерим событие change для обновления URL и загрузки данных
        // Важно триггерить оба, чтобы URL обновился корректно
        iterationFrom.trigger('change');
        iterationTo.trigger('change');
        
        // Очищаем само выделение brush после обновления полей
        // brushGroup.call(brush.move, null); // Можно раскомментировать, если нужно убирать выделение
    }
    
    return brushGroup;
}

/**
 * Вспомогательная функция для поиска ближайшей итерации к значению
 * Используется в addBrushToChart
 */
function findClosestIteration(iterations, value) {
    if (!iterations || iterations.length === 0) return undefined;
    return iterations.reduce((closest, current) => {
        return Math.abs(current - value) < Math.abs(closest - value) ? current : closest;
    }, iterations[0]);
}

/**
 * Функция для сброса выделения brush на всех графиках
 */
window.clearAllBrushes = function() {
    if (window.brushGroups) {
        window.brushGroups.forEach(({ brushGroup, brush }) => {
            if (brushGroup && brush) {
                brushGroup.call(brush.move, null);
            }
        });
    }
};

// Вызываем функцию инициализации при загрузке страницы
$(document).ready(function() {
    // Удаляем вызов initIterationRangeSync
}); 