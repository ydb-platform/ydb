#include "actor_system_mon.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/library/actors/core/actorsystem.h>

#include <ydb/core/mon/crossref.h>
#include <ydb/core/mon/mon.h>
#include <ydb/library/actors/core/mon.h>
#include <library/cpp/json/json_writer.h>

using namespace NActors;

namespace NKikimr {

class TActorSystemMonQueryProcessor : public TActorBootstrapped<TActorSystemMonQueryProcessor> {
    IHarmonizer* Harmonizer;
    NMon::TEvHttpInfo::TPtr Ev;

public:
    TActorSystemMonQueryProcessor(IHarmonizer* harmonizer, NMon::TEvHttpInfo::TPtr ev)
        : Harmonizer(harmonizer)
        , Ev(ev)
    {}

    void TryToReadHistory() {
        const TCgiParameters& cgi = Ev->Get()->Request.GetParams();
        bool isJsonRequested = cgi.Has("type") && cgi.Get("type") == "json";

        TStringStream str;
        bool success = Harmonizer->InvokeReadHistory([&](const TIterableDoubleRange<THarmonizerIterationState>& history) {
            i64 size = history.size();
            i64 begin = size - 10;
            if (size < 10) {
                begin = 0;
            }

            if (isJsonRequested) {
                // Создаем JSON-ответ
                NJson::TJsonWriter writer(&str, false);
                writer.OpenMap();
                
                // Добавляем статистику
                auto stats = Harmonizer->GetStats();
                writer.WriteKey("stats");
                writer.OpenMap();
                writer.WriteKey("maxUsedCpu");
                writer.Write(stats.MaxUsedCpu);
                writer.WriteKey("minUsedCpu");
                writer.Write(stats.MinUsedCpu);
                writer.WriteKey("maxElapsedCpu");
                writer.Write(stats.MaxElapsedCpu);
                writer.WriteKey("minElapsedCpu");
                writer.Write(stats.MinElapsedCpu);
                writer.WriteKey("avgAwakeningTimeUs");
                writer.Write(stats.AvgAwakeningTimeUs);
                writer.WriteKey("avgWakingUpTimeUs");
                writer.Write(stats.AvgWakingUpTimeUs);
                writer.CloseMap();
                
                // Добавляем данные истории
                writer.WriteKey("history");
                writer.OpenArray();
                for (i64 idx = size - 1; idx >= begin; --idx) {
                    writer.OpenMap();
                    writer.WriteKey("iteration");
                    writer.Write(history[idx].Iteration);
                    writer.WriteKey("timestamp");
                    writer.Write(history[idx].Ts);
                    writer.WriteKey("budget");
                    writer.Write(history[idx].Budget);
                    writer.WriteKey("lostCpu");
                    writer.Write(history[idx].LostCpu);
                    writer.WriteKey("freeSharedCpu");
                    writer.Write(history[idx].FreeSharedCpu);
                    writer.CloseMap();
                }
                writer.CloseArray();
                writer.CloseMap();
            } else {
                // HTML-ответ
                HTML(str) {
                    DIV_CLASS("panel panel-info") {
                        DIV_CLASS("panel-heading") {
                            str << "Harmonizer";
                        }
                        DIV_CLASS("panel-body") {
                            auto stats = Harmonizer->GetStats();
                            DIV() {
                                str << "Harmonizer stats: " << stats.ToString() << "<br/>";
                                str << "Maximum CPU usage: " << stats.MaxUsedCpu << "<br/>";
                                str << "Minimum CPU usage: " << stats.MinUsedCpu << "<br/>";
                                str << "Maximum CPU time: " << stats.MaxElapsedCpu << "<br/>";
                                str << "Minimum CPU time: " << stats.MinElapsedCpu << "<br/>";
                                str << "Average awakening time (us): " << stats.AvgAwakeningTimeUs << "<br/>";
                                str << "Average waking up time (us): " << stats.AvgWakingUpTimeUs;
                            }
                        }
                    }

                    // Добавляем график с использованием d3.js
                    DIV_CLASS("panel panel-info") {
                        DIV_CLASS("panel-heading") {
                            str << "Harmonizer Metrics Chart";
                        }
                        DIV_CLASS("panel-body") {
                            // Добавляем кнопки для переключения между графиками
                            str << "<div class='btn-group' style='margin-bottom: 15px;'>";
                            str << "  <button id='mainMetricsBtn' class='btn btn-primary active'>Overview</button>";
                            str << "  <button id='poolsBtn' class='btn btn-default'>Pools</button>";
                            str << "</div>";
                            
                            str << "<div id=\"chart\" style=\"width:100%; height:400px;\"></div>";
                            str << "<script src=\"/lwtrace/mon/static/js/d3.v4.min.js\"></script>";
                            str << "<script>";
                            
                            // Данные для основного графика
                            str << "var rawData = [";
                            for (i64 idx = begin; idx < size; ++idx) {
                                if (idx > begin) {
                                    str << ",";
                                }
                                str << "{\"iteration\": " << history[idx].Iteration 
                                    << ", \"budget\": " << history[idx].Budget
                                    << ", \"lostCpu\": " << history[idx].LostCpu
                                    << ", \"freeSharedCpu\": " << history[idx].FreeSharedCpu
                                    << "}";
                            }
                            str << "];";
                            
                            // Данные для графика пулов (демо)
                            str << "var poolData = [";
                            for (i64 idx = begin; idx < size; ++idx) {
                                if (idx > begin) {
                                    str << ",";
                                }
                                str << "{\"iteration\": " << history[idx].Iteration;
                                
                                // CPU usage по пулам (демо)
                                for (int poolIdx = 0; poolIdx < 5; poolIdx++) {
                                    float elapsedCpu = 0;
                                    for (auto &thread : history[idx].Pools[poolIdx].Threads) {
                                        elapsedCpu += thread.ElapsedCpu.Cpu;
                                    }
                                    str << ", \"pool" << poolIdx << "_usage\": " << elapsedCpu;
                                }
                                
                                // Текущее количество потоков по пулам (демо)
                                for (int poolIdx = 0; poolIdx < 5; poolIdx++) {
                                    float threadCount = history[idx].Pools[poolIdx].CurrentThreadCount;
                                    str << ", \"pool" << poolIdx << "_threads\": " << threadCount;
                                }
                                
                                // Максимальное количество потоков по пулам (демо)
                                for (int poolIdx = 0; poolIdx < 5; poolIdx++) {
                                    float maxThreads = history[idx].Pools[poolIdx].PotentialMaxThreadCount;
                                    str << ", \"pool" << poolIdx << "_max_threads\": " << maxThreads;
                                }
                                
                                str << "}";
                            }
                            str << "];";
                            
                            str << "var data = rawData.sort(function(a, b) { return a.iteration - b.iteration; });";
                            str << "var poolDataSorted = poolData.sort(function(a, b) { return a.iteration - b.iteration; });";
                            
                            // Определяем цвета для пулов
                            str << "var poolColors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd'];";
                            str << "var currentChartType = 'main';";
                            
                            // Функция для создания основного графика
                            str << "function createMainChart() {";
                            str << "    var chartDiv = document.getElementById('chart');";
                            str << "    var width = chartDiv.clientWidth;";
                            str << "    var height = chartDiv.clientHeight;";
                            str << "    var margin = {top: 20, right: 80, bottom: 40, left: 60};";
                            str << "    var innerWidth = width - margin.left - margin.right;";
                            str << "    var innerHeight = height - margin.top - margin.bottom;";
                            
                            str << "    d3.select('#chart svg').remove();";
                            
                            str << "    var svg = d3.select('#chart').append('svg')";
                            str << "        .attr('width', width)";
                            str << "        .attr('height', height)";
                            str << "        .append('g')";
                            str << "        .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');";
                            
                            str << "    var x = d3.scaleLinear().range([0, innerWidth]);";
                            str << "    var y = d3.scaleLinear().range([innerHeight, 0]);";
                            
                            str << "    var budgetLine = d3.line()";
                            str << "        .x(function(d) { return x(d.iteration); })";
                            str << "        .y(function(d) { return y(d.budget); });";
                            
                            str << "    var lostCpuLine = d3.line()";
                            str << "        .x(function(d) { return x(d.iteration); })";
                            str << "        .y(function(d) { return y(d.lostCpu); });";
                            
                            str << "    var freeSharedCpuLine = d3.line()";
                            str << "        .x(function(d) { return x(d.iteration); })";
                            str << "        .y(function(d) { return y(d.freeSharedCpu); });";
                            
                            str << "    if (data.length === 0) {";
                            str << "        svg.append('text')";
                            str << "            .attr('x', innerWidth / 2)";
                            str << "            .attr('y', innerHeight / 2)";
                            str << "            .attr('text-anchor', 'middle')";
                            str << "            .text('Данные отсутствуют');";
                            str << "        return;";
                            str << "    }";
                            
                            str << "    var xExtent = d3.extent(data, function(d) { return d.iteration; });";
                            str << "    var yMax = d3.max(data, function(d) {";
                            str << "        return Math.max(d.budget, d.lostCpu, d.freeSharedCpu);";
                            str << "    });";
                            
                            str << "    x.domain([xExtent[0], xExtent[1] + (xExtent[1] - xExtent[0]) * 0.05]);";
                            str << "    y.domain([0, yMax * 1.1]);";
                            
                            str << "    svg.append('g')";
                            str << "        .attr('class', 'x-axis')";
                            str << "        .attr('transform', 'translate(0,' + innerHeight + ')')";
                            str << "        .call(d3.axisBottom(x).ticks(5));";
                            
                            str << "    svg.append('text')";
                            str << "        .attr('x', innerWidth / 2)";
                            str << "        .attr('y', innerHeight + 35)";
                            str << "        .attr('text-anchor', 'middle')";
                            str << "        .text('Iteration');";
                            
                            str << "    svg.append('g')";
                            str << "        .attr('class', 'y-axis')";
                            str << "        .call(d3.axisLeft(y).ticks(5));";
                            
                            str << "    svg.append('text')";
                            str << "        .attr('transform', 'rotate(-90)')";
                            str << "        .attr('y', -45)";
                            str << "        .attr('x', -(innerHeight / 2))";
                            str << "        .attr('text-anchor', 'middle')";
                            str << "        .text('Values');";
                            
                            str << "    svg.append('path')";
                            str << "        .datum(data)";
                            str << "        .attr('class', 'line')";
                            str << "        .attr('d', budgetLine)";
                            str << "        .style('fill', 'none')";
                            str << "        .style('stroke', 'steelblue')";
                            str << "        .style('stroke-width', '2px');";
                            
                            str << "    svg.append('path')";
                            str << "        .datum(data)";
                            str << "        .attr('class', 'line')";
                            str << "        .attr('d', lostCpuLine)";
                            str << "        .style('fill', 'none')";
                            str << "        .style('stroke', 'red')";
                            str << "        .style('stroke-width', '2px');";
                            
                            str << "    svg.append('path')";
                            str << "        .datum(data)";
                            str << "        .attr('class', 'line')";
                            str << "        .attr('d', freeSharedCpuLine)";
                            str << "        .style('fill', 'none')";
                            str << "        .style('stroke', 'green')";
                            str << "        .style('stroke-width', '2px');";
                            
                            str << "    var legend = svg.append('g')";
                            str << "        .attr('class', 'legend')";
                            str << "        .attr('transform', 'translate(' + (innerWidth - 120) + ', 0)');";
                            
                            str << "    legend.append('rect')";
                            str << "        .attr('x', 0)";
                            str << "        .attr('y', 0)";
                            str << "        .attr('width', 12)";
                            str << "        .attr('height', 12)";
                            str << "        .style('fill', 'steelblue');";
                            str << "    legend.append('text')";
                            str << "        .attr('x', 18)";
                            str << "        .attr('y', 10)";
                            str << "        .text('Budget')";
                            str << "        .style('font-size', '12px');";
                            
                            str << "    legend.append('rect')";
                            str << "        .attr('x', 0)";
                            str << "        .attr('y', 20)";
                            str << "        .attr('width', 12)";
                            str << "        .attr('height', 12)";
                            str << "        .style('fill', 'red');";
                            str << "    legend.append('text')";
                            str << "        .attr('x', 18)";
                            str << "        .attr('y', 30)";
                            str << "        .text('Lost CPU')";
                            str << "        .style('font-size', '12px');";
                            
                            str << "    legend.append('rect')";
                            str << "        .attr('x', 0)";
                            str << "        .attr('y', 40)";
                            str << "        .attr('width', 12)";
                            str << "        .attr('height', 12)";
                            str << "        .style('fill', 'green');";
                            str << "    legend.append('text')";
                            str << "        .attr('x', 18)";
                            str << "        .attr('y', 50)";
                            str << "        .text('Free Shared CPU')";
                            str << "        .style('font-size', '12px');";
                            str << "}";
                            
                            // Функция для создания графика пулов
                            str << "function createPoolsChart() {";
                            str << "    var chartDiv = document.getElementById('chart');";
                            str << "    var width = chartDiv.clientWidth;";
                            str << "    var height = chartDiv.clientHeight;";
                            str << "    var margin = {top: 20, right: 150, bottom: 40, left: 60};";
                            str << "    var innerWidth = width - margin.left - margin.right;";
                            str << "    var innerHeight = height - margin.top - margin.bottom;";
                            
                            str << "    d3.select('#chart svg').remove();";
                            
                            str << "    var svg = d3.select('#chart').append('svg')";
                            str << "        .attr('width', width)";
                            str << "        .attr('height', height)";
                            str << "        .append('g')";
                            str << "        .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');";
                            
                            str << "    var x = d3.scaleLinear().range([0, innerWidth]);";
                            str << "    var y = d3.scaleLinear().range([innerHeight, 0]);";
                            
                            str << "    if (poolDataSorted.length === 0) {";
                            str << "        svg.append('text')";
                            str << "            .attr('x', innerWidth / 2)";
                            str << "            .attr('y', innerHeight / 2)";
                            str << "            .attr('text-anchor', 'middle')";
                            str << "            .text('Данные отсутствуют');";
                            str << "        return;";
                            str << "    }";
                            
                            // Определяем линии для использования CPU по пулам
                            str << "    var poolUsageLines = [];";
                            str << "    for (var i = 0; i < 5; i++) {";
                            str << "        poolUsageLines.push(d3.line()";
                            str << "            .x(function(d) { return x(d.iteration); })";
                            str << "            .y(function(d) { return y(d['pool' + i + '_usage']); }));";
                            str << "    }";
                            
                            // Определяем линии для текущего количества потоков (пунктир)
                            str << "    var poolThreadsLines = [];";
                            str << "    for (var i = 0; i < 5; i++) {";
                            str << "        poolThreadsLines.push(d3.line()";
                            str << "            .x(function(d) { return x(d.iteration); })";
                            str << "            .y(function(d) { return y(d['pool' + i + '_threads']); }));";
                            str << "    }";
                            
                            // Определяем линии для максимального количества потоков (другой пунктир)
                            str << "    var poolMaxThreadsLines = [];";
                            str << "    for (var i = 0; i < 5; i++) {";
                            str << "        poolMaxThreadsLines.push(d3.line()";
                            str << "            .x(function(d) { return x(d.iteration); })";
                            str << "            .y(function(d) { return y(d['pool' + i + '_max_threads']); }));";
                            str << "    }";
                            
                            // Определяем максимальное значение для оси Y
                            str << "    var xExtent = d3.extent(poolDataSorted, function(d) { return d.iteration; });";
                            str << "    var yMax = 0;";
                            str << "    for (var i = 0; i < 5; i++) {";
                            str << "        yMax = Math.max(yMax, d3.max(poolDataSorted, function(d) {";
                            str << "            return Math.max(d['pool' + i + '_usage'], d['pool' + i + '_threads'], d['pool' + i + '_max_threads']);";
                            str << "        }));";
                            str << "    }";
                            
                            str << "    x.domain([xExtent[0], xExtent[1] + (xExtent[1] - xExtent[0]) * 0.05]);";
                            str << "    y.domain([0, yMax * 1.1]);";
                            
                            // Создаем оси
                            str << "    svg.append('g')";
                            str << "        .attr('class', 'x-axis')";
                            str << "        .attr('transform', 'translate(0,' + innerHeight + ')')";
                            str << "        .call(d3.axisBottom(x).ticks(5));";
                            
                            str << "    svg.append('text')";
                            str << "        .attr('x', innerWidth / 2)";
                            str << "        .attr('y', innerHeight + 35)";
                            str << "        .attr('text-anchor', 'middle')";
                            str << "        .text('Iteration');";
                            
                            str << "    svg.append('g')";
                            str << "        .attr('class', 'y-axis')";
                            str << "        .call(d3.axisLeft(y).ticks(5));";
                            
                            str << "    svg.append('text')";
                            str << "        .attr('transform', 'rotate(-90)')";
                            str << "        .attr('y', -45)";
                            str << "        .attr('x', -(innerHeight / 2))";
                            str << "        .attr('text-anchor', 'middle')";
                            str << "        .text('Values');";
                            
                            // Отрисовываем линии использования CPU для каждого пула
                            str << "    for (var i = 0; i < 5; i++) {";
                            str << "        svg.append('path')";
                            str << "            .datum(poolDataSorted)";
                            str << "            .attr('class', 'line')";
                            str << "            .attr('d', poolUsageLines[i])";
                            str << "            .style('fill', 'none')";
                            str << "            .style('stroke', poolColors[i])";
                            str << "            .style('stroke-width', '2px');";
                            str << "    }";
                            
                            // Отрисовываем линии текущих потоков (пунктир)
                            str << "    for (var i = 0; i < 5; i++) {";
                            str << "        svg.append('path')";
                            str << "            .datum(poolDataSorted)";
                            str << "            .attr('class', 'line')";
                            str << "            .attr('d', poolThreadsLines[i])";
                            str << "            .style('fill', 'none')";
                            str << "            .style('stroke', poolColors[i])";
                            str << "            .style('stroke-width', '1.5px')";
                            str << "            .style('stroke-dasharray', '3,3');";
                            str << "    }";
                            
                            // Отрисовываем линии максимальных потоков (другой пунктир)
                            str << "    for (var i = 0; i < 5; i++) {";
                            str << "        svg.append('path')";
                            str << "            .datum(poolDataSorted)";
                            str << "            .attr('class', 'line')";
                            str << "            .attr('d', poolMaxThreadsLines[i])";
                            str << "            .style('fill', 'none')";
                            str << "            .style('stroke', poolColors[i])";
                            str << "            .style('stroke-width', '1.5px')";
                            str << "            .style('stroke-dasharray', '5,5');";
                            str << "    }";
                            
                            // Создаем легенду для пулов
                            str << "    var legend = svg.append('g')";
                            str << "        .attr('class', 'legend')";
                            str << "        .attr('transform', 'translate(' + (innerWidth + 5) + ', 0)');";
                            
                            str << "    for (var i = 0; i < 5; i++) {";
                            // Легенда для использования CPU
                            str << "        legend.append('rect')";
                            str << "            .attr('x', 0)";
                            str << "            .attr('y', i * 60)";
                            str << "            .attr('width', 12)";
                            str << "            .attr('height', 12)";
                            str << "            .style('fill', poolColors[i]);";
                            str << "        legend.append('text')";
                            str << "            .attr('x', 18)";
                            str << "            .attr('y', i * 60 + 10)";
                            str << "            .text('Pool ' + i + ' CPU')";
                            str << "            .style('font-size', '12px');";
                            
                            // Легенда для текущих потоков
                            str << "        legend.append('line')";
                            str << "            .attr('x1', 0)";
                            str << "            .attr('y1', i * 60 + 20)";
                            str << "            .attr('x2', 12)";
                            str << "            .attr('y2', i * 60 + 20)";
                            str << "            .style('stroke', poolColors[i])";
                            str << "            .style('stroke-width', '1.5px')";
                            str << "            .style('stroke-dasharray', '3,3');";
                            str << "        legend.append('text')";
                            str << "            .attr('x', 18)";
                            str << "            .attr('y', i * 60 + 25)";
                            str << "            .text('Pool ' + i + ' Threads')";
                            str << "            .style('font-size', '12px');";
                            
                            // Легенда для максимальных потоков
                            str << "        legend.append('line')";
                            str << "            .attr('x1', 0)";
                            str << "            .attr('y1', i * 60 + 35)";
                            str << "            .attr('x2', 12)";
                            str << "            .attr('y2', i * 60 + 35)";
                            str << "            .style('stroke', poolColors[i])";
                            str << "            .style('stroke-width', '1.5px')";
                            str << "            .style('stroke-dasharray', '5,5');";
                            str << "        legend.append('text')";
                            str << "            .attr('x', 18)";
                            str << "            .attr('y', i * 60 + 40)";
                            str << "            .text('Pool ' + i + ' Max')";
                            str << "            .style('font-size', '12px');";
                            str << "    }";
                            
                            str << "}";
                            
                            // Функция для переключения графика
                            str << "function switchChart(chartType) {";
                            str << "    currentChartType = chartType;";
                            str << "    if (chartType === 'main') {";
                            str << "        document.getElementById('mainMetricsBtn').classList.add('active');";
                            str << "        document.getElementById('poolsBtn').classList.remove('active');";
                            str << "        createMainChart();";
                            str << "    } else if (chartType === 'pools') {";
                            str << "        document.getElementById('mainMetricsBtn').classList.remove('active');";
                            str << "        document.getElementById('poolsBtn').classList.add('active');";
                            str << "        createPoolsChart();";
                            str << "    }";
                            str << "}";
                            
                            // Инициализация основного графика
                            str << "createMainChart();";
                            
                            // Обработчики нажатия на кнопки
                            str << "document.getElementById('mainMetricsBtn').addEventListener('click', function() {";
                            str << "    switchChart('main');";
                            str << "});";
                            
                            str << "document.getElementById('poolsBtn').addEventListener('click', function() {";
                            str << "    switchChart('pools');";
                            str << "});";
                            
                            // Адаптивность при изменении размера окна
                            str << "window.addEventListener('resize', function() {";
                            str << "    if (currentChartType === 'main') {";
                            str << "        createMainChart();";
                            str << "    } else if (currentChartType === 'pools') {";
                            str << "        createPoolsChart();";
                            str << "    }";
                            str << "});";
                            
                            str << "</script>";
                        }
                    }

                    DIV_CLASS("panel-body") {
                        TABLE_CLASS("table table-bordered table-condensed") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() { str << "Iteration"; }
                                    TABLEH() { str << "Timestamp"; }
                                    TABLEH() { str << "Budget"; }
                                    TABLEH() { str << "Lost CPU"; }
                                    TABLEH() { str << "Free shared CPU"; }
                                }
                            }
                            TABLEBODY() {
                                for (i64 idx = size - 1; idx >= begin; --idx) {
                                    TABLER() {
                                        TABLED() { str << history[idx].Iteration; }
                                        TABLED() { str << history[idx].Ts; }
                                        TABLED() { str << history[idx].Budget; }
                                        TABLED() { str << history[idx].LostCpu; }
                                        TABLED() { str << history[idx].FreeSharedCpu; }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
        
        if (!success) {
            Schedule(TDuration::MicroSeconds(100), new TEvents::TEvWakeup());
            return;
        }

        if (isJsonRequested) {
            Send(Ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
        } else {
            Send(Ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
        }

        PassAway();
    }

    void Bootstrap() {
        Become(&TThis::StateReadHistory);
        TryToReadHistory();
    }

    STATEFN(StateReadHistory) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TSystem::Poison, PassAway);
            sFunc(TEvents::TEvWakeup, TryToReadHistory);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TActorSystemMonActor
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TActorSystemMonActor : public TActorBootstrapped<TActorSystemMonActor> {
    IHarmonizer* Harmonizer = nullptr;

public:
    TActorSystemMonActor()
    {}

    void Bootstrap() {
        Harmonizer = TActivationContext::ActorSystem()->GetHarmonizer();
        Y_ABORT_UNLESS(Harmonizer);
        NActors::TMon* mon = AppData()->Mon;
        if (mon) {
            NMonitoring::TIndexMonPage *actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(TMon::TRegisterActorPageFields{
                .Title = "Actor System",
                .RelPath = "actor_system",
                .ActorSystem = TActivationContext::ActorSystem(),
                .Index = actorsMonPage, 
                .PreTag = false, 
                .ActorId = SelfId(),
                .MonServiceName = "actor_system_mon"
            });
        }

        Become(&TThis::StateOnline);
    }

    STATEFN(StateOnline) {
        switch (ev->GetTypeRewrite()) {
            cFunc(NActors::TEvents::TSystem::Poison, PassAway);
            hFunc(NMon::TEvHttpInfo, Handle);
        default:
            break;
        }
    }

    void Handle(NMon::TEvHttpInfo::TPtr &ev) {
        if (!Harmonizer) {
            Send(ev->Sender, new NMon::TEvHttpInfoRes("Actor System Harmonizer is not initialized"));
            return;
        }
        Register(new TActorSystemMonQueryProcessor(Harmonizer, ev));
    }
};

TActorId MakeActorSystemMonId() {
    return TActorId(0, TStringBuf("ActorSystemMon", 12));
}

IActor* CreateActorSystemMon() {
    return new TActorSystemMonActor();
}

} // NKikimr
