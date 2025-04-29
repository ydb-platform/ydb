// Добавляем CSS стили для brush (если не добавлены в основной CSS)
document.head.insertAdjacentHTML('beforeend', `
<style>
.brush .selection {
    fill: #3498db;
    fill-opacity: 0.2;
    stroke: #2980b9;
    stroke-width: 1px;
}
.brush-instruction {
    font-style: italic;
    opacity: 0.7;
}
.chart-reset-button, .reset-button {
    cursor: pointer;
}
.chart-reset-button:hover rect, .reset-button:hover rect {
    fill: #ff4444;
}
.chart-range-info {
    font-weight: bold;
}
#range-info-container {
    position: fixed;
    top: 10px;
    left: 50%;
    transform: translateX(-50%);
    background-color: rgba(255, 255, 255, 0.95);
    border: 1px solid #ddd;
    border-radius: 5px;
    padding: 8px 12px;
    box-shadow: 0 2px 10px rgba(0,0,0,0.15);
    z-index: 1000;
    display: flex;
    align-items: center;
    gap: 12px;
    font-size: 14px;
    max-width: 90%;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}
#range-info-container button {
    background-color: #ff6b6b;
    color: white;
    border: none;
    border-radius: 4px;
    padding: 4px 10px;
    cursor: pointer;
    transition: background-color 0.2s;
    font-weight: bold;
}
#range-info-container button:hover {
    background-color: #ff4444;
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

// Функция для отрисовки графика метрик
function renderMetricsChart() {
    if (!currentData || !currentData.history || currentData.history.length === 0) {
        console.log("renderMetricsChart: нет данных для отображения");
        return;
    }
    
    const svgElement = document.getElementById("metricsChart");
    if (!svgElement) {
        console.error("renderMetricsChart: не найден DOM элемент #metricsChart");
        return;
    }

    console.log("renderMetricsChart: начинаем отрисовку, история:", currentData.history.length);
    const selectedMetric = chartMetricSelect.val();
    console.log("renderMetricsChart: выбранная метрика:", selectedMetric);

    const svg = d3.select("#metricsChart");
    svg.selectAll("*").remove(); // Очищаем предыдущий график
    
    const margin = {top: 20, right: 80, bottom: 40, left: 60};
    
    // Получаем размеры SVG контейнера
    const containerWidth = svg.node().getBoundingClientRect().width;
    const containerHeight = svg.node().getBoundingClientRect().height;
    
    // Проверяем, что размеры положительные и достаточно большие
    if (containerWidth <= margin.left + margin.right || containerHeight <= margin.top + margin.bottom) {
        console.error("renderMetricsChart: слишком маленькие размеры контейнера", {
            containerWidth,
            containerHeight,
            margin
        });
        
        // Добавляем сообщение об ошибке в SVG
        svg.append("text")
            .attr("x", 10)
            .attr("y", 30)
            .attr("fill", "red")
            .text("Ошибка: невозможно отобразить график (размеры контейнера слишком малы)");
        return;
    }
    
    const width = containerWidth - margin.left - margin.right;
    const height = containerHeight - margin.top - margin.bottom;
    
    console.log("renderMetricsChart: размеры графика:", {
        containerWidth,
        containerHeight,
        width,
        height
    });
    
    const g = svg.append("g")
        .attr("transform", `translate(${margin.left},${margin.top})`)
        .attr("width", width)
        .attr("height", height);
    
    // Получаем отсортированные по времени данные
    let metricData = [...currentData.history].sort((a, b) => a.timestamp - b.timestamp);
    
    // Применяем фильтр по выбранному диапазону итераций, если он установлен
    if (window.selectedIterationRange) {
        metricData = metricData.filter(d => 
            d.iteration >= window.selectedIterationRange.start && 
            d.iteration <= window.selectedIterationRange.end
        );
    }
    
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
    
    // Получаем все итерации для всего приложения
    const allIterations = currentData.history.map(item => item.iteration).sort((a, b) => a - b);
    
    // Добавляем возможность выделения диапазона (brush)
    addBrushToChart(g, x, height, {iterations: allIterations}, function(start, end) {
        // Обновляем все графики после выбора диапазона
        renderCpuCharts();
        renderMetricsChart();
        renderPoolChart();
    });
}

// Функция для отрисовки графика пула
function renderPoolChart() {
    if (!currentData || !currentData.history || currentData.history.length === 0) {
        console.log("renderPoolChart: нет данных для отображения");
        return;
    }
    
    const svgElement = document.getElementById("poolChart");
    if (!svgElement) {
        console.error("renderPoolChart: не найден DOM элемент #poolChart");
        return;
    }

    const selectedPool = poolSelect.val();
    const selectedMetric = poolMetricSelect.val();
    
    console.log("renderPoolChart: начинаем отрисовку, пул:", selectedPool, "метрика:", selectedMetric);
    
    if (!selectedPool || !selectedMetric) {
        console.log("renderPoolChart: не выбран пул или метрика");
        return;
    }
    
    const svg = d3.select("#poolChart");
    svg.selectAll("*").remove(); // Очищаем предыдущий график
    
    const margin = {top: 20, right: 80, bottom: 40, left: 60};
    
    // Получаем размеры SVG контейнера
    const containerWidth = svg.node().getBoundingClientRect().width;
    const containerHeight = svg.node().getBoundingClientRect().height;
    
    // Проверяем, что размеры положительные и достаточно большие
    if (containerWidth <= margin.left + margin.right || containerHeight <= margin.top + margin.bottom) {
        console.error("renderPoolChart: слишком маленькие размеры контейнера", {
            containerWidth,
            containerHeight,
            margin
        });
        
        // Добавляем сообщение об ошибке в SVG
        svg.append("text")
            .attr("x", 10)
            .attr("y", 30)
            .attr("fill", "red")
            .text("Ошибка: невозможно отобразить график (размеры контейнера слишком малы)");
        return;
    }
    
    const width = containerWidth - margin.left - margin.right;
    const height = containerHeight - margin.top - margin.bottom;
    
    console.log("renderPoolChart: размеры графика:", {
        containerWidth,
        containerHeight,
        width,
        height
    });
    
    const g = svg.append("g")
        .attr("transform", `translate(${margin.left},${margin.top})`)
        .attr("width", width)
        .attr("height", height);
    
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
    
    // Применяем фильтр по выбранному диапазону итераций, если он установлен
    if (window.selectedIterationRange) {
        poolData = poolData.filter(d => 
            d.iteration >= window.selectedIterationRange.start && 
            d.iteration <= window.selectedIterationRange.end
        );
    }
    
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
    
    // Получаем все итерации для всего приложения
    const allIterations = currentData.history.map(item => item.iteration).sort((a, b) => a - b);
    
    // Добавляем возможность выделения диапазона (brush)
    addBrushToChart(g, x, height, {iterations: allIterations}, function(start, end) {
        // Обновляем все графики после выбора диапазона
        renderPoolChart();
        renderCpuCharts();
        renderMetricsChart();
    });
}

// Функция для инициализации синхронизации полей итераций с выбранным диапазоном
function initIterationRangeSync() {
    // Инициализация полей ввода при загрузке страницы
    $(document).ready(function() {
        // Устанавливаем обработчики событий после загрузки данных
        $(document).on('data-loaded', function() {
            // Получаем все доступные итерации и сортируем их
            const allIterations = currentData.history.map(item => item.iteration).sort((a, b) => a - b);
            
            if (allIterations.length > 0) {
                // Устанавливаем начальные значения полей ввода
                const firstIteration = allIterations[0];
                $("#iterationFrom").val(firstIteration);
                $("#iterationTo").val("");
                
                // Устанавливаем начальный выбранный диапазон
                window.selectedIterationRange = {
                    start: firstIteration,
                    end: null
                };
            }
        });
    });

    // Добавим обработчики изменений для полей ввода
    $("#iterationFrom, #iterationTo").on("change", function() {
        const fromVal = parseInt($("#iterationFrom").val());
        const toVal = parseInt($("#iterationTo").val());
        
        // Проверяем валидность значений
        if (isNaN(fromVal)) {
            alert("Неверное значение начала диапазона!");
            return;
        }
        
        // Если указан конец диапазона, проверяем его валидность
        if (!isNaN(toVal) && toVal < fromVal) {
            alert("Конец диапазона должен быть больше или равен началу!");
            return;
        }
        
        // Обновляем глобальный выбранный диапазон
        window.selectedIterationRange = {
            start: fromVal,
            end: isNaN(toVal) ? null : toVal
        };
        
        // Отображаем информацию о выбранном диапазоне
        showRangeInfo(fromVal, isNaN(toVal) ? null : toVal);
        
        // Обновляем графики
        renderCpuCharts();
        renderMetricsChart();
        renderPoolChart();
    });
    
    // Добавим обработчик кнопки обновления
    $("#iterationRefresh").on("click", function() {
        const fromVal = parseInt($("#iterationFrom").val());
        const toVal = parseInt($("#iterationTo").val());
        
        // Проверяем валидность значений
        if (isNaN(fromVal)) {
            alert("Неверное значение начала диапазона!");
            return;
        }
        
        // Если указан конец диапазона, проверяем его валидность
        if (!isNaN(toVal) && toVal < fromVal) {
            alert("Конец диапазона должен быть больше или равен началу!");
            return;
        }
        
        // Обновляем глобальный выбранный диапазон
        window.selectedIterationRange = {
            start: fromVal,
            end: isNaN(toVal) ? null : toVal
        };
        
        // Отображаем информацию о выбранном диапазоне
        showRangeInfo(fromVal, isNaN(toVal) ? null : toVal);
        
        // Обновляем графики
        renderCpuCharts();
        renderMetricsChart();
        renderPoolChart();
    });

    // После загрузки данных
    $.getJSON("/actor_system_mon/json/iterations").done(function(data) {
        if (data.length === 0) {
            $("#iterationFrom").attr("disabled", "disabled");
            $("#iterationTo").attr("disabled", "disabled");
            $("#iterationRefresh").attr("disabled", "disabled");
            return;
        }
        
        // Сохраняем доступные итерации глобально
        window.availableIterations = data;
        
        // Сортируем итерации по возрастанию
        const sortedIterations = [...data].sort((a, b) => a - b);
        
        // Заполняем поля ввода начальными значениями
        $("#iterationFrom").val(sortedIterations[0]);
        $("#iterationTo").val("");
        
        // Устанавливаем глобальный выбранный диапазон
        window.selectedIterationRange = {
            start: sortedIterations[0],
            end: null
        };
        
        // Показываем информацию о выбранном диапазоне
        showRangeInfo(sortedIterations[0], null);
        
        // Обновляем графики
        renderCpuCharts();
        renderMetricsChart();
        renderPoolChart();
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
    
    // Если нет выбранного диапазона, инициализируем его полным диапазоном
    if (!window.selectedIterationRange) {
        const allIterations = currentData.history.map(item => item.iteration).sort((a, b) => a - b);
        if (allIterations.length > 0) {
            console.log("Инициализируем диапазон итераций полным диапазоном:", allIterations[0], allIterations[allIterations.length - 1]);
        }
    }
    
    // Если диапазон указан, но end равен null, заменяем на последнюю итерацию
    if (window.selectedIterationRange && window.selectedIterationRange.end === null) {
        const allIterations = currentData.history.map(item => item.iteration).sort((a, b) => a - b);
        if (allIterations.length > 0) {
            window.selectedIterationRange.end = allIterations[allIterations.length - 1];
            console.log("Установлена последняя итерация:", window.selectedIterationRange.end);
        }
    }
    
    // Устанавливаем или обновляем информацию о выбранном диапазоне
    if (window.selectedIterationRange) {
        // Добавляем единообразное отображение информации о диапазоне
        showRangeInfo(window.selectedIterationRange.start, window.selectedIterationRange.end);
    }
    
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
    
    // Проверяем наличие DOM элемента
    if (!document.getElementById("cpuPoolsChart")) {
        console.error("renderCpuPoolsChart: не найден DOM элемент #cpuPoolsChart");
        return;
    }
    
    const svg = d3.select("#cpuPoolsChart");
    svg.selectAll("*").remove(); // Очищаем предыдущий график
    
    const margin = {top: 30, right: 80, bottom: 40, left: 60};
    
    // Получаем размеры SVG контейнера
    const containerWidth = svg.node().getBoundingClientRect().width;
    const containerHeight = svg.node().getBoundingClientRect().height;
    
    // Проверяем, что размеры положительные и достаточно большие
    if (containerWidth <= margin.left + margin.right || containerHeight <= margin.top + margin.bottom) {
        console.error("renderCpuPoolsChart: слишком маленькие размеры контейнера", {
            containerWidth,
            containerHeight,
            margin
        });
        
        // Добавляем сообщение об ошибке в SVG
        svg.append("text")
            .attr("x", 10)
            .attr("y", 30)
            .attr("fill", "red")
            .text("Ошибка: невозможно отобразить график (размеры контейнера слишком малы)");
        return;
    }
    
    const width = containerWidth - margin.left - margin.right;
    const height = containerHeight - margin.top - margin.bottom;
    
    console.log("renderCpuPoolsChart: размеры графика:", {
        containerWidth,
        containerHeight,
        width,
        height
    });
    
    const g = svg.append("g")
        .attr("transform", `translate(${margin.left},${margin.top})`)
        .attr("width", width)
        .attr("height", height);
    
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
    
    // Используем выбранный диапазон итераций, если он установлен
    let filteredIterations = cpuData.iterations;
    let startIdx = 0;
    let endIdx = cpuData.iterations.length;
    
    if (window.selectedIterationRange) {
        startIdx = cpuData.iterations.findIndex(it => it >= window.selectedIterationRange.start);
        const tempEndIdx = cpuData.iterations.findIndex(it => it > window.selectedIterationRange.end);
        endIdx = tempEndIdx === -1 ? cpuData.iterations.length : tempEndIdx;
        
        if (startIdx !== -1) {
            filteredIterations = cpuData.iterations.slice(startIdx, endIdx);
        }
    }
    
    // Создаем шкалы
    const x = d3.scaleLinear()
        .domain(d3.extent(filteredIterations))
        .range([0, width]);
    
    // Создаем массив данных для формирования области
    const areaData = [];
    
    // Проходим по всем пулам и добавляем только видимые
    const visiblePoolNames = Array.from(visiblePools);
    const visiblePoolData = {};
    
    visiblePoolNames.forEach(poolName => {
        if (cpuData.pools[poolName]) {
            const values = cpuData.pools[poolName][metricType][valueType];
            // Фильтруем значения по выбранному диапазону
            if (window.selectedIterationRange && startIdx !== -1) {
                visiblePoolData[poolName] = values.slice(startIdx, endIdx);
            } else {
                visiblePoolData[poolName] = values;
            }
        }
    });
    
    // Находим максимальные значения для шкалы Y
    let maxValue = 0;
    filteredIterations.forEach((_, i) => {
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
    filteredIterations.forEach((iteration, i) => {
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
            const closestIterationIndex = filteredIterations.reduce((closest, curr, idx) => {
                return Math.abs(curr - iteration) < Math.abs(filteredIterations[closest] - iteration) ? idx : closest;
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
    
    // Добавляем возможность выделения диапазона (brush)
    addBrushToChart(g, x, height, cpuData, function(start, end) {
        // Обновляем все графики после выбора диапазона
        renderCpuCharts();
        renderMetricsChart();
        renderPoolChart();
    });
}

// Функция для отрисовки графика количества потоков
function renderThreadsChart(cpuData) {
    // Проверяем наличие DOM элемента
    if (!document.getElementById("threadsChart")) {
        console.error("renderThreadsChart: не найден DOM элемент #threadsChart");
        return;
    }
    
    const svg = d3.select("#threadsChart");
    svg.selectAll("*").remove(); // Очищаем предыдущий график
    
    const margin = {top: 30, right: 80, bottom: 40, left: 60};
    
    // Получаем размеры SVG контейнера
    const containerWidth = svg.node().getBoundingClientRect().width;
    const containerHeight = svg.node().getBoundingClientRect().height;
    
    // Проверяем, что размеры положительные и достаточно большие
    if (containerWidth <= margin.left + margin.right || containerHeight <= margin.top + margin.bottom) {
        console.error("renderThreadsChart: слишком маленькие размеры контейнера", {
            containerWidth,
            containerHeight,
            margin
        });
        
        // Добавляем сообщение об ошибке в SVG
        svg.append("text")
            .attr("x", 10)
            .attr("y", 30)
            .attr("fill", "red")
            .text("Ошибка: невозможно отобразить график (размеры контейнера слишком малы)");
        return;
    }
    
    const width = containerWidth - margin.left - margin.right;
    const height = containerHeight - margin.top - margin.bottom;
    
    const g = svg.append("g")
        .attr("transform", `translate(${margin.left},${margin.top})`)
        .attr("width", width)
        .attr("height", height);
    
    // Если нет данных, выходим
    if (cpuData.iterations.length === 0) {
        g.append("text")
            .attr("x", width / 2)
            .attr("y", height / 2)
            .attr("text-anchor", "middle")
            .text("Нет данных для построения графика");
        return;
    }
    
    // Используем выбранный диапазон итераций, если он установлен
    let filteredIterations = cpuData.iterations;
    let startIdx = 0;
    let endIdx = cpuData.iterations.length;
    
    if (window.selectedIterationRange) {
        startIdx = cpuData.iterations.findIndex(it => it >= window.selectedIterationRange.start);
        const tempEndIdx = cpuData.iterations.findIndex(it => it > window.selectedIterationRange.end);
        endIdx = tempEndIdx === -1 ? cpuData.iterations.length : tempEndIdx;
        
        if (startIdx !== -1) {
            filteredIterations = cpuData.iterations.slice(startIdx, endIdx);
        }
    }
    
    // Создаем шкалы
    const x = d3.scaleLinear()
        .domain(d3.extent(filteredIterations))
        .range([0, width]);
    
    // Фильтруем данные по выбранному диапазону
    let filteredTotalThreadCount = cpuData.totalThreadCount;
    let filteredMaxThreadCount = cpuData.totalMaxThreadCount;
    
    if (window.selectedIterationRange && startIdx !== -1) {
        filteredTotalThreadCount = cpuData.totalThreadCount.slice(startIdx, endIdx);
        filteredMaxThreadCount = cpuData.totalMaxThreadCount.slice(startIdx, endIdx);
    }
    
    // Находим максимальное значение для шкалы Y
    const maxThreads = Math.max(
        d3.max(filteredTotalThreadCount) || 0, 
        d3.max(filteredMaxThreadCount) || 0
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
    filteredIterations.forEach((iteration, i) => {
        const point = { iteration };
        let cumulativeCurrent = 0;
        let cumulativeMax = 0;
        
        visiblePools.forEach(poolName => {
            if (cpuData.poolsThreadCount[poolName]) {
                let currentThreads, maxThreads;
                
                // Фильтруем данные потоков по выбранному диапазону
                if (window.selectedIterationRange && startIdx !== -1) {
                    const currentSliced = cpuData.poolsThreadCount[poolName].current.slice(startIdx, endIdx);
                    const maxSliced = cpuData.poolsThreadCount[poolName].potential.slice(startIdx, endIdx);
                    currentThreads = currentSliced[i] || 0;
                    maxThreads = maxSliced[i] || 0;
                } else {
                    currentThreads = cpuData.poolsThreadCount[poolName].current[i] || 0;
                    maxThreads = cpuData.poolsThreadCount[poolName].potential[i] || 0;
                }
                
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
        .x((d, i) => x(filteredIterations[i]))
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
            const closestIterationIndex = filteredIterations.reduce((closest, curr, idx) => {
                return Math.abs(curr - iteration) < Math.abs(filteredIterations[closest] - iteration) ? idx : closest;
            }, 0);
            
            const iterationData = stackData[closestIterationIndex];
            
            // Формируем текст для tooltip
            let tooltipText = `<strong>Итерация: ${iterationData.iteration}</strong><br>`;
            tooltipText += `<strong>Всего потоков:</strong> ${filteredTotalThreadCount[closestIterationIndex]}<br>`;
            tooltipText += `<strong>Макс. потоков:</strong> ${filteredMaxThreadCount[closestIterationIndex]}<br><br>`;
            
            visiblePools.forEach(poolName => {
                if (cpuData.poolsThreadCount[poolName]) {
                    let currentThreads, maxThreads;
                    
                    if (window.selectedIterationRange && startIdx !== -1) {
                        const currentSliced = cpuData.poolsThreadCount[poolName].current.slice(startIdx, endIdx);
                        const maxSliced = cpuData.poolsThreadCount[poolName].potential.slice(startIdx, endIdx);
                        currentThreads = currentSliced[closestIterationIndex];
                        maxThreads = maxSliced[closestIterationIndex];
                    } else {
                        currentThreads = cpuData.poolsThreadCount[poolName].current[closestIterationIndex];
                        maxThreads = cpuData.poolsThreadCount[poolName].potential[closestIterationIndex];
                    }
                    
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
    
    // Добавляем возможность выделения диапазона (brush)
    addBrushToChart(g, x, height, cpuData, function(start, end) {
        // Обновляем все графики после выбора диапазона
        renderCpuCharts();
        renderMetricsChart();
        renderPoolChart();
    });
}

// Функция для отрисовки графика Budget и CPU
function renderBudgetChart(cpuData) {
    // Проверяем наличие DOM элемента
    if (!document.getElementById("budgetChart")) {
        console.error("renderBudgetChart: не найден DOM элемент #budgetChart");
        return;
    }
    
    const svg = d3.select("#budgetChart");
    svg.selectAll("*").remove(); // Очищаем предыдущий график
    
    const margin = {top: 30, right: 80, bottom: 40, left: 60};
    
    // Получаем размеры SVG контейнера
    const containerWidth = svg.node().getBoundingClientRect().width;
    const containerHeight = svg.node().getBoundingClientRect().height;
    
    // Проверяем, что размеры положительные и достаточно большие
    if (containerWidth <= margin.left + margin.right || containerHeight <= margin.top + margin.bottom) {
        console.error("renderBudgetChart: слишком маленькие размеры контейнера", {
            containerWidth,
            containerHeight,
            margin
        });
        
        // Добавляем сообщение об ошибке в SVG
        svg.append("text")
            .attr("x", 10)
            .attr("y", 30)
            .attr("fill", "red")
            .text("Ошибка: невозможно отобразить график (размеры контейнера слишком малы)");
        return;
    }
    
    const width = containerWidth - margin.left - margin.right;
    const height = containerHeight - margin.top - margin.bottom;
    
    const g = svg.append("g")
        .attr("transform", `translate(${margin.left},${margin.top})`)
        .attr("width", width)
        .attr("height", height);
    
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
    
    // Используем выбранный диапазон итераций, если он установлен
    let filteredIterations = cpuData.iterations;
    let filteredBudget = cpuData.budget;
    let filteredCpuValues;
    
    if (window.selectedIterationRange) {
        const startIdx = cpuData.iterations.findIndex(it => it >= window.selectedIterationRange.start);
        const endIdx = cpuData.iterations.findIndex(it => it > window.selectedIterationRange.end);
        const realEndIdx = endIdx === -1 ? cpuData.iterations.length : endIdx;
        
        if (startIdx !== -1) {
            filteredIterations = cpuData.iterations.slice(startIdx, realEndIdx);
            filteredBudget = cpuData.budget.slice(startIdx, realEndIdx);
            
            if (metricType === 'elapsedCpu') {
                filteredCpuValues = cpuData.totalElapsedCpu.slice(startIdx, realEndIdx);
            } else {
                filteredCpuValues = cpuData.totalUsedCpu.slice(startIdx, realEndIdx);
            }
        } else {
            // Если не нашли начальный индекс, используем все данные
            if (metricType === 'elapsedCpu') {
                filteredCpuValues = cpuData.totalElapsedCpu;
            } else {
                filteredCpuValues = cpuData.totalUsedCpu;
            }
        }
    } else {
        // Используем все данные
        if (metricType === 'elapsedCpu') {
            filteredCpuValues = cpuData.totalElapsedCpu;
        } else {
            filteredCpuValues = cpuData.totalUsedCpu;
        }
    }
    
    // Создаем шкалы
    const x = d3.scaleLinear()
        .domain(d3.extent(filteredIterations))
        .range([0, width]);
    
    // Находим максимальное значение для шкалы Y
    const maxBudget = d3.max(filteredBudget) || 10;
    const maxCpu = d3.max(filteredCpuValues) || 0;
    
    const maxValue = Math.max(maxBudget, maxCpu);
    
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
        .x((d, i) => x(filteredIterations[i]))
        .y(d => y(d))
        .curve(d3.curveMonotoneX);
    
    g.append("path")
        .datum(filteredBudget)
        .attr("class", "line")
        .attr("d", budgetLine)
        .attr("stroke", colorScheme.budget)
        .attr("stroke-width", 2)
        .attr("fill", "none");
    
    // Рисуем линию для CPU
    const cpuLine = d3.line()
        .x((d, i) => x(filteredIterations[i]))
        .y(d => y(d))
        .curve(d3.curveMonotoneX);
    
    g.append("path")
        .datum(filteredCpuValues)
        .attr("class", "line")
        .attr("d", cpuLine)
        .attr("stroke", colorScheme.totalCpu)
        .attr("stroke-width", 2)
        .attr("fill", "none");
    
    // Создаем легенду
    const legend = svg.append("g")
        .attr("class", "chart-legend")
        .attr("transform", `translate(${margin.left}, 10)`);
    
    // Используем функцию для вычисления ширины текста
    function getTextWidth(text) {
        // Примерная ширина текста (можно настроить в зависимости от шрифта)
        return text.length * 7 + 30; // 7px на символ + дополнительное пространство
    }
    
    // Легенда для Budget
    const budgetText = "Budget";
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
        .text(budgetText);
    
    // Вычисляем отступ для второго элемента легенды
    const budgetWidth = getTextWidth(budgetText);
    const legendItemSpacing = 20; // дополнительный отступ между элементами
    
    // Легенда для CPU
    const cpuText = metricType === 'elapsedCpu' ? 'Total Elapsed CPU' : 'Total Used CPU';
    const cpuLegend = legend.append("g")
        .attr("class", "legend-item")
        .attr("transform", `translate(${budgetWidth + legendItemSpacing}, 0)`);
    
    cpuLegend.append("rect")
        .attr("class", "legend-color")
        .attr("width", 16)
        .attr("height", 10)
        .attr("fill", colorScheme.totalCpu);
    
    cpuLegend.append("text")
        .attr("class", "legend-text")
        .attr("x", 20)
        .attr("y", 9)
        .text(cpuText);
    
    // Создаем tooltip
    const tooltip = d3.select("body").append("div")
        .attr("class", "tooltip")
        .style("opacity", 0);
    
    // Создаем невидимую область для отслеживания движения мыши
    const overlay = g.append("rect")
        .attr("width", width)
        .attr("height", height)
        .attr("fill", "none")
        .attr("pointer-events", "all")
        .on("mousemove", function() {
            const mouseX = d3.mouse(this)[0];
            const iteration = Math.round(x.invert(mouseX));
            
            // Находим ближайшую итерацию
            const closestIterationIndex = filteredIterations.reduce((closest, curr, idx) => {
                return Math.abs(curr - iteration) < Math.abs(filteredIterations[closest] - iteration) ? idx : closest;
            }, 0);
            
            // Формируем текст для tooltip
            const iterationNumber = filteredIterations[closestIterationIndex];
            const budgetValue = filteredBudget[closestIterationIndex];
            const cpuValue = filteredCpuValues[closestIterationIndex];
            
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
    
    // Добавляем возможность выделения диапазона (brush)
    addBrushToChart(g, x, height, cpuData, function(start, end) {
        // Обновляем все графики после выбора диапазона
        renderCpuCharts();
        renderMetricsChart();
        renderPoolChart();
    });
}

// Функция для отображения информации о выбранном диапазоне
function showRangeInfo(start, end) {
    // Создаем контейнер для информации о диапазоне, если он еще не существует
    let rangeInfo = d3.select("#rangeInfo");
    if (rangeInfo.empty()) {
        rangeInfo = d3.select("body")
            .insert("div", ":first-child")
            .attr("id", "rangeInfo")
            .style("position", "fixed")
            .style("top", "10px")
            .style("left", "10px")
            .style("background-color", "rgba(255, 255, 255, 0.9)")
            .style("padding", "10px")
            .style("border-radius", "5px")
            .style("box-shadow", "0 0 10px rgba(0, 0, 0, 0.2)")
            .style("z-index", "1000")
            .style("font-family", "Arial, sans-serif");
    }
    
    // Если конец диапазона не указан, отображаем только начало
    if (end === null || end === undefined) {
        rangeInfo
            .html(`<strong>Выбрана итерация:</strong> ${start} 
                   <button id="resetRangeBtn" style="margin-left: 10px;">Сбросить</button>`)
            .style("display", "block");
    } else {
        rangeInfo
            .html(`<strong>Выбран диапазон:</strong> ${start} - ${end} 
                   <button id="resetRangeBtn" style="margin-left: 10px;">Сбросить</button>`)
            .style("display", "block");
    }
    
    // Добавляем обработчик клика на кнопку сброса
    d3.select("#resetRangeBtn").on("click", resetRange);

    // Синхронизируем значения полей ввода диапазона итераций
    if (typeof $ === 'function') { // убеждаемся, что jQuery доступен
        if (end === null || end === undefined) {
            $("#iterationFrom").val(start);
            $("#iterationTo").val("");
        } else {
            $("#iterationFrom").val(start);
            $("#iterationTo").val(end);
        }
        // Тригерим событие обновления (например, change) для обновления данных
        $("#iterationFrom").trigger('change');
        $("#iterationTo").trigger('change');
    }

    // Убираем выделение brush на всех графиках
    if (window.clearAllBrushes) {
        window.clearAllBrushes();
    }
}

// Функция для сброса выбранного диапазона
function resetRange() {
    // Устанавливаем начальные значения
    if (window.availableIterations && window.availableIterations.length > 0) {
        // Сортируем итерации по возрастанию
        const sortedIterations = [...window.availableIterations].sort((a, b) => a - b);
        
        // Устанавливаем начальное значение в первую итерацию
        $("#iterationFrom").val(sortedIterations[0]);
        
        // Очищаем поле конца диапазона
        $("#iterationTo").val("");
        
        // Обновляем глобальный выбранный диапазон
        window.selectedIterationRange = {
            start: sortedIterations[0],
            end: null
        };
        
        // Скрываем информацию о диапазоне
        d3.select("#rangeInfo").style("display", "none");
        
        // Обновляем графики
        renderCpuCharts();
        renderMetricsChart();
        renderPoolChart();
    }
}

// Вспомогательная функция для добавления brush (выделения диапазона) к графику
function addBrushToChart(g, x, height, cpuData, onBrushEnd) {
    // Создаем brush компонент
    const brush = d3.brushX()
        .extent([[0, 0], [g.attr("width"), height]])
        .on("end", function() {
            // Получаем выделенную область
            if (!d3.event.sourceEvent) return; // Только для событий пользователя
            
            // Если двойной клик, сбрасываем выделение
            if (d3.event.sourceEvent && d3.event.sourceEvent.type === 'dblclick') {
                resetRange();
                return;
            }
            
            // Если нет выделения или оно было отменено
            if (!d3.event.selection) {
                return;
            }
            
            const [x0, x1] = d3.event.selection.map(x.invert);
            
            // Находим ближайшие итерации к границам выделения в общих данных
            // Для этого используем итерации из истории, а не из фильтрованных данных
            const allIterations = currentData.history.map(item => item.iteration).sort((a, b) => a - b);
            
            // Находим ближайшие итерации в исходных данных
            const startIteration = findClosestIteration(allIterations, x0);
            const endIteration = findClosestIteration(allIterations, x1);
            
            // Если начало и конец совпадают, это считаем кликом (отсутствием выделения)
            if (startIteration === endIteration) {
                return;
            }
            
            console.log("Выбран диапазон итераций:", startIteration, endIteration);
            
            // Сохраняем выделенный диапазон
            window.selectedIterationRange = {
                start: startIteration,
                end: endIteration
            };
            
            // Обновляем информацию о выбранном диапазоне и поля ввода
            showRangeInfo(startIteration, endIteration);
            
            // Вызываем колбэк, если он предоставлен
            if (onBrushEnd) {
                onBrushEnd(startIteration, endIteration);
            }
            
            // Обновляем все графики с новым диапазоном, но не вызываем API
            renderCpuCharts();
            renderMetricsChart();
            renderPoolChart();
        });
    
    // Добавляем элемент brush на график
    const brushGroup = g.append("g")
        .attr("class", "brush")
        .call(brush);
    
    // --- ДОБАВЛЕНО: сохраняем brushGroup и brush для глобального сброса ---
    if (!window.brushGroups) window.brushGroups = [];
    window.brushGroups.push({ brushGroup, brush });
    // ---
    
    // Если уже есть выделенный диапазон, показываем его
    if (window.selectedIterationRange) {
        const startX = x(window.selectedIterationRange.start);
        const endX = x(window.selectedIterationRange.end);
        if (!isNaN(startX) && !isNaN(endX)) {
            brushGroup.call(brush.move, [startX, endX]);
        }
    }
    
    // Добавляем инструкцию по использованию brush
    g.append("text")
        .attr("class", "brush-instruction")
        .attr("x", g.attr("width") / 2)
        .attr("y", 20)
        .attr("text-anchor", "middle")
        .attr("font-size", "12px")
        .attr("fill", "#666")
        .text("Выделите область для зума. Двойной клик или кнопка 'Сбросить' для возврата к полному диапазону.");
    
    return brushGroup;
}

// Вспомогательная функция для поиска ближайшей итерации к значению
function findClosestIteration(iterations, value) {
    return iterations.reduce((closest, current) => {
        return Math.abs(current - value) < Math.abs(closest - value) ? current : closest;
    }, iterations[0]);
}

// --- ДОБАВЛЕНО: функция для сброса выделения brush на всех графиках ---
window.clearAllBrushes = function() {
    if (window.brushGroups) {
        window.brushGroups.forEach(({ brushGroup, brush }) => {
            if (brushGroup && brush) {
                brushGroup.call(brush.move, null);
            }
        });
        // Очищаем массив после сброса
        window.brushGroups = [];
    }
};

// Вызываем функцию инициализации при загрузке страницы
$(document).ready(function() {
    initIterationRangeSync();
}); 