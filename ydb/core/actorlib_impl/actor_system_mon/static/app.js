// Проверка на наличие jQuery
if (typeof jQuery === 'undefined') {
    console.error('jQuery не загружен! Проверьте подключение библиотеки.');
    document.body.innerHTML = '<div style="padding: 20px; color: red; font-weight: bold;">Ошибка: jQuery не загружен. Проверьте консоль браузера для получения дополнительной информации.</div>';
} else {
    console.log('jQuery версия:', jQuery.fn.jquery);
}

// Глобальные переменные и константы
let currentData = null;

// Подготовка палитры цветов для пулов
const poolColors = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", 
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
    "#aec7e8", "#ffbb78", "#98df8a", "#ff9896", "#c5b0d5",
    "#c49c94", "#f7b6d2", "#c7c7c7", "#dbdb8d", "#9edae5"
];

// Создаем цветовую мапу для пулов
const poolColorMap = {};

// Настройки цветов для графиков
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

// Состояние визуализации пулов
let selectedPools = new Set();
let visiblePools = new Set();

// Глобальные ссылки на элементы DOM для удобства доступа из разных файлов
let dataContainer, loadingIndicator, errorContainer, refreshButton, levelSelect, iterationFrom, iterationTo, timeMode, timeFrom, timeTo, sortNewestFirstCheckbox;
let chartMetricSelect, poolSelect, poolMetricSelect, refreshChartButton;
let cpuMetricSelect, cpuValueSelect, refreshCpuButton, showAllPoolsCheckbox, poolCheckboxesContainer, poolSelectorContainer;

// Функция для форсированного пересчета размеров графиков
function recalculateChartSizes() {
    console.log("Форсированный пересчет размеров графиков...");
    
    // Получаем все SVG элементы для графиков
    const chartElements = document.querySelectorAll('svg[id$="Chart"]');
    
    chartElements.forEach(element => {
        // Устанавливаем явно стили, чтобы гарантировать видимость
        element.style.display = 'block';
        element.style.visibility = 'visible';
        
        // Вызовем getBoundingClientRect(), чтобы форсировать пересчет размеров
        const rect = element.getBoundingClientRect();
        console.log(`Размеры элемента ${element.id}:`, {
            width: rect.width,
            height: rect.height
        });
    });
    
    // После пересчета размеров вызываем перерисовку графиков, если данные уже загружены
    if (currentData) {
        // Определяем активную вкладку
        const activeTabId = document.querySelector('#mainTabs .nav-link.active').getAttribute('data-bs-target');
        
        // Перерисовываем графики в зависимости от активной вкладки
        if (activeTabId === '#chartsTab') {
            console.log('Перерисовка графиков на вкладке Метрики после изменения размера');
            renderMetricsChart();
            renderPoolChart();
        } else if (activeTabId === '#cpuTab') {
            console.log('Перерисовка графиков на вкладке CPU после изменения размера');
            renderCpuCharts();
        }
    }
}

$(document).ready(function() {
    console.log("Инициализация приложения...");
    
    // Проверка на наличие Bootstrap
    if (typeof bootstrap === 'undefined') {
        console.error('Bootstrap не загружен! Проверьте подключение библиотеки bootstrap.js.');
        $('#mainTabsContent').before('<div class="alert alert-danger">Ошибка: Bootstrap JS не загружен. Проверьте консоль браузера для получения дополнительной информации.</div>');
    } else {
        console.log('Bootstrap доступен');
        
        // Инициализация вкладок
        const tabElements = document.querySelectorAll('#mainTabs button[data-bs-toggle="tab"]');
        if (tabElements.length > 0) {
            tabElements.forEach(tabEl => {
                tabEl.addEventListener('click', function(event) {
                    event.preventDefault();
                    const tabId = this.getAttribute('data-bs-target');
                    console.log('Переключение на вкладку:', tabId);
                    
                    // Удаляем активный класс со всех вкладок
                    document.querySelectorAll('#mainTabs .nav-link').forEach(t => {
                        t.classList.remove('active');
                        t.setAttribute('aria-selected', 'false');
                    });
                    
                    // Удаляем активный класс со всех панелей содержимого
                    document.querySelectorAll('#mainTabsContent .tab-pane').forEach(p => {
                        p.classList.remove('show', 'active');
                        p.classList.remove('fade'); // Удаляем класс fade
                    });
                    
                    // Добавляем активный класс выбранной вкладке
                    this.classList.add('active');
                    this.setAttribute('aria-selected', 'true');
                    
                    // Активируем соответствующую панель содержимого
                    const targetPane = document.querySelector(tabId);
                    if (targetPane) {
                        // Делаем активной с отключенной анимацией
                        targetPane.classList.add('show', 'active');
                        
                        // Форсируем пересчет размеров после переключения
                        setTimeout(recalculateChartSizes, 50);
                        
                        // Обновляем графики для активной вкладки
                        if (currentData) {
                            if (tabId === '#chartsTab') {
                                console.log('Обновляем графики на вкладке Метрики');
                                setTimeout(() => {
                                    renderMetricsChart(); // Вызов из charts.js
                                    renderPoolChart();    // Вызов из charts.js
                                }, 100); // Небольшая задержка для перерисовки DOM
                            } else if (tabId === '#cpuTab') {
                                console.log('Обновляем графики на вкладке CPU');
                                setTimeout(() => {
                                    renderCpuCharts();    // Вызов из charts.js
                                }, 100); // Небольшая задержка для перерисовки DOM
                            }
                        }
                    }
                });
            });
            
            console.log('Вкладки инициализированы');
        }
    }
    
    // Инициализируем глобальные ссылки на элементы DOM
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

    // Проверяем наличие элементов
    console.log("Проверка элементов формы:", {
        refreshButton: refreshButton.length > 0,
        levelSelect: levelSelect.length > 0,
        iterationFrom: iterationFrom.length > 0,
        iterationTo: iterationTo.length > 0,
        chartMetricSelect: chartMetricSelect.length > 0,
        cpuMetricSelect: cpuMetricSelect.length > 0
    });

    // Обработчики событий
    refreshButton.on('click', function() {
        console.log("Нажата кнопка 'Обновить'");
        fetchData(); // Вызов из data.js
    });
    
    levelSelect.on('change', function() {
        console.log("Изменен уровень детализации:", levelSelect.val());
        fetchData(); // Вызов из data.js
    });
    
    iterationFrom.on('change', function() {
        console.log("Изменено значение 'От итерации':", iterationFrom.val());
        fetchData(); // Вызов из data.js
    });
    
    iterationTo.on('change', function() {
        console.log("Изменено значение 'До итерации':", iterationTo.val());
        fetchData(); // Вызов из data.js
    });
    
    timeMode.on('change', function() {
        const isTimeMode = $(this).is(':checked');
        $('#iterationMode').toggle(!isTimeMode);
        $('#timeMode').toggle(isTimeMode);
        fetchData(); // Вызов из data.js
    });
    
    timeFrom.on('change', function() {
        console.log("Изменено значение 'От времени':", timeFrom.val());
        fetchData(); // Вызов из data.js
    });
    
    timeTo.on('change', function() {
        console.log("Изменено значение 'До времени':", timeTo.val());
        fetchData(); // Вызов из data.js
    });
    
    sortNewestFirstCheckbox.on('change', function() {
        console.log("Изменен порядок сортировки, новые сверху:", sortNewestFirstCheckbox.prop('checked'));
        // Если есть данные, перерисовываем без запроса
        if (currentData) {
            renderData(currentData); // Вызов из ui.js
        }
    });
    
    // Обработчики для графиков метрик
    chartMetricSelect.on('change', function() {
        console.log("Выбрана метрика:", chartMetricSelect.val());
        renderMetricsChart(); // Вызов из charts.js
    });
    
    poolSelect.on('change', function() {
        console.log("Выбран пул:", poolSelect.val());
        renderPoolChart(); // Вызов из charts.js
    });
    
    poolMetricSelect.on('change', function() {
        console.log("Выбрана метрика пула:", poolMetricSelect.val());
        renderPoolChart(); // Вызов из charts.js
    });
    
    refreshChartButton.on('click', function() {
        console.log("Нажата кнопка 'Обновить график'");
        fetchData(); // Вызов из data.js
    });
    
    // Обработчики для графиков CPU
    cpuMetricSelect.on('change', function() {
        console.log("Выбран тип CPU метрики:", cpuMetricSelect.val());
        renderCpuCharts(); // Вызов из charts.js
    });
    
    cpuValueSelect.on('change', function() {
        console.log("Выбрано значение CPU:", cpuValueSelect.val());
        renderCpuCharts(); // Вызов из charts.js
    });
    
    refreshCpuButton.on('click', function() {
        console.log("Нажата кнопка 'Обновить' для CPU графиков");
        fetchData(); // Вызов из data.js
    });
    
    // Обработчик для переключения режима отображения всех пулов
    showAllPoolsCheckbox.on('change', function() {
        console.log("Изменен режим отображения всех пулов:", showAllPoolsCheckbox.prop('checked'));
        if ($(this).is(':checked')) {
            poolSelectorContainer.hide();
        } else {
            poolSelectorContainer.show();
        }
        
        renderCpuCharts(); // Вызов из charts.js
    });

    // Обработчик для чекбоксов пулов (делегирование событий)
    poolCheckboxesContainer.on('change', '.pool-checkbox', function() {
        const poolName = $(this).data('pool');
        
        if ($(this).is(':checked')) {
            selectedPools.add(poolName);
        } else {
            selectedPools.delete(poolName);
        }
        
        renderCpuCharts(); // Вызов из charts.js
    });
    
    // Обработчик изменения размера окна
    let resizeTimeout;
    $(window).on('resize', function() {
        // Используем задержку, чтобы не вызывать функцию слишком часто
        clearTimeout(resizeTimeout);
        resizeTimeout = setTimeout(function() {
            console.log("Изменен размер окна, пересчитываем графики");
            recalculateChartSizes();
        }, 250);
    });
    
    // Загружаем данные при запуске
    console.log("Загружаем начальные данные...");
    fetchData(); // Вызов из data.js
}); 