from __future__ import annotations
import allure
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.results_processor import ResultsProcessor
from urllib.parse import urlencode
from datetime import datetime
from copy import deepcopy
from pytz import timezone
import logging


class NodeErrors:
    def __init__(self, node: YdbCluster.Node, message: str):
        self.node = node
        self.core_hashes: list[tuple[str, str]] = []    # id, aggregated hash
        self.was_oom: bool = False
        self.message: str = message


def _set_monitoring(test_info: dict[str, str], start_time: float, end_time: float) -> None:
    monitoring_start = int((start_time) * 1000)
    monitoring_end = int((end_time) * 1000)
    database = '/' + test_info.get('database', '*')
    # monitoring does not show intervals less 1 minute.
    monitoring_addition = 60000 - (monitoring_end - monitoring_start)
    if monitoring_addition > 0:
        monitoring_start -= monitoring_addition
        monitoring_end += monitoring_addition

    if len(YdbCluster.get_monitoring_urls()) > 0:
        test_info['monitoring'] = ', '.join([
            f"<a target='_blank' href='{monitoring.url.format(
                database=database,
                start_time=monitoring_start,
                end_time=monitoring_end
            )}'>{monitoring.caption}</a>"
            for monitoring in YdbCluster.get_monitoring_urls()
        ])


def _set_coredumps(test_info: dict[str, str], start_time: float, end_time: float) -> None:
    tz = timezone('Europe/Moscow')
    params = urlencode([
        ('filter', f'program_type=kikimr; @cluster_name={test_info["name"]}'),
        ('since_ts', datetime.fromtimestamp(start_time, tz).isoformat()),
        ('till_ts', datetime.fromtimestamp(end_time, tz).isoformat()),
    ])
    test_info['coredumps'] = f"<a target='_blank' href='https://coredumps.yandex-team.ru/v3/cores?{params}'>link</a>"


def _set_node_errors(test_info: dict[str, str], node_errors: list[NodeErrors]) -> None:
    if len(node_errors) == 0:
        return
    html = '<ul>'
    for node in node_errors:
        html += f'<li>{node.node.slot}'
        if node.message:
            html += f'<p>Node {node.message}</p>'
        if node.was_oom:
            html += '<p>Node was killed by OOM</p>'
        for core_id, core_hash in node.core_hashes:
            color = hex(0xFF0000 + hash(str(core_hash)) % 0xFFFF).split('x')[-1]
            html += f'<p>There was coredump <a target="_blank" href="https://coredumps.yandex-team.ru/core_trace?core_id={core_id}" style="background-color: #{color}">{core_hash}</a></p>'
        html += '</li>'
    html += '</ul>'
    test_info['<span style="background-color: #FF8888">node errors</span>'] = html


def _set_results_plot(test_info: dict[str, str], suite: str, test: str, refference_set: str) -> None:
    if not ResultsProcessor.send_results:
        return
    params = urlencode({
        'tab': 'o8',
        'suite_b2rp': suite,
        'test_ehyw': test,
        'db_fmdl': ResultsProcessor.get_cluster_id(),
        'cluster_dufr': refference_set
    })
    test_info['results_plot'] = f"<a target='_blank' href='https://datalens.yandex-team.ru/iqnd4b1miaz27-testy-ydb?{params}'>link</a>"


def _set_logs_command(test_info: dict[str, str], start_time: float, end_time: float):
    hosts = []
    for node in YdbCluster.get_cluster_nodes():
        if node.role == YdbCluster.Node.Role.STORAGE:
            hosts.append(node.host)
    hosts_cmd = ' '.join([f'-H {h}' for h in hosts])
    tz = timezone('Europe/Moscow')
    start = datetime.fromtimestamp(start_time, tz)
    end = datetime.fromtimestamp(end_time, tz)
    start_short = start.strftime('%Y-%m-%d %H:%M:%S')
    end_short = end.strftime('%Y-%m-%d %H:%M:%S')
    start_iso = start.isoformat()
    end_iso = end.isoformat()
    time_cmd = f'-S "{start_short}" -U "{end_short}"'
    time_cmd_iso = f'-S "{start_iso}" -U "{end_iso}"'
    not_ask_for_key = '-x \"-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no\"'

    cmd = f"parallel-ssh {hosts_cmd} -i {not_ask_for_key} 'ulimit -n 100500;unified_agent select {time_cmd_iso} -s kikimr' 2>/dev/null 1> kikimr.log"
    dmesg_cmd = f"parallel-ssh {hosts_cmd} -i {not_ask_for_key} 'sudo journalctl -k {time_cmd} --grep ydb' 2>/dev/null 1> journalctrl.log"
    test_info['kikimr_log'] = f'<details><code>{cmd}</code></details>'
    test_info['kernel_log'] = f'<details><code>{dmesg_cmd}</code></details>'


def _create_iterations_table(result, node_errors: list[NodeErrors] = [], workload_params: dict = None) -> str:
    """
    Создает HTML таблицу с информацией об итерациях workload
    
    Args:
        result: WorkloadRunResult с информацией об итерациях
        node_errors: Список ошибок нод для подсчета cores и OOM
        workload_params: Параметры запуска workload для отображения в заголовке
        
    Returns:
        str: HTML таблица
    """
    logging.info(f"_create_iterations_table called with result: {result}")
    if result:
        logging.info(f"result has iterations: {hasattr(result, 'iterations')}")
        if hasattr(result, 'iterations'):
            logging.info(f"iterations content: {result.iterations}")
    
    # Собираем информацию о нодах
    node_info_map = {}  # slot -> NodeErrors
    for node_error in node_errors:
        node_info_map[node_error.node.slot] = node_error
    
    # Получаем уникальные ноды для колонок
    unique_nodes = sorted(node_info_map.keys()) if node_info_map else []
    
    # Формируем информацию о параметрах workload для заголовка
    params_info = ""
    if workload_params:
        params_list = []
        for key, value in workload_params.items():
            params_list.append(f"{key}: {value}")
        if params_list:
            params_info = f"<div style='margin-bottom: 10px; padding: 5px; background-color: #f5f5f5; border: 1px solid #ddd;'><strong>Workload Parameters:</strong> {', '.join(params_list)}</div>"
    
    # Создаем заголовок таблицы (убираем дублирующие колонки Cores/OOM)
    table_html = f"""
    {params_info}
    <table border='1' cellpadding='2px' style='border-collapse: collapse; font-size: 12px;'>
        <tr style='background-color: #f0f0f0;'>
            <th>Iter</th><th>Status</th><th>Dur(s)</th>"""
    
    # Добавляем колонки для каждой ноды
    for node_slot in unique_nodes:
        table_html += f"<th>{node_slot}</th>"
    
    table_html += """
        </tr>
    """
    
    # Если result None или нет iterations, создаем пустую таблицу с информацией
    if not result or not hasattr(result, 'iterations') or not result.iterations:
        # Создаем строку с placeholder
        table_html += f"""
            <tr>
                <td>-</td>
                <td style='background-color: #f0f0f0;'>no data</td>
                <td style='background-color: #f0f0f0;'>N/A</td>"""
        
        # Добавляем значения для каждой ноды
        for node_slot in unique_nodes:
            node_error = node_info_map[node_slot]
            
            # Собираем все проблемы ноды
            issues = []
            has_issues = False
            has_critical_issues = False  # Отслеживаем критичные проблемы (cores/oom)
            
            # Проверяем основные проблемы (рестарт, падение)
            if node_error.message and node_error.message not in ['diagnostic info collected']:
                issues.append(node_error.message.replace('was ', '').replace('is ', ''))
                has_issues = True
                
            # Добавляем cores если есть (критичная проблема)
            if node_error.core_hashes:
                issues.append(f"cores:{len(node_error.core_hashes)}")
                has_issues = True
                has_critical_issues = True
                
            # Добавляем oom если есть (критичная проблема)
            if node_error.was_oom:
                issues.append("oom")
                has_issues = True
                has_critical_issues = True
            
            if has_issues:
                # Красный только для критичных проблем (cores/oom)
                # Зеленый для обычных проблем (restarted/down)
                node_color = "#ffcccc" if has_critical_issues else "#ccffcc"
                node_value = ", ".join(issues)
            else:
                node_color = "#ccffcc"  # Зеленый
                node_value = "ok"
            
            table_html += f"<td style='background-color: {node_color};'>{node_value}</td>"
        
        table_html += """
            </tr>
        </table>
        """
        
        return table_html
    
    # Добавляем строки для каждой итерации
    iterations = sorted(result.iterations.keys())
    nodes_shown = set()  # Отслеживаем, для каких нод уже показали проблемы
    
    for i, iteration_num in enumerate(iterations):
        iteration = result.iterations[iteration_num]
        
        # Упрощенная логика статуса workload
        if hasattr(iteration, 'error_message') and iteration.error_message:
            # Проверяем, содержит ли ошибка информацию о timeout
            error_msg_lower = iteration.error_message.lower()
            if ("timeout" in error_msg_lower or "timed out" in error_msg_lower or 
                "command timed out" in error_msg_lower):
                workload_color = "#ffffcc"  # Светло-желтый 
                workload_value = "timeout"
            else:
                workload_color = "#ffffcc"  # Светло-желтый
                workload_value = "warning"
        else:
            workload_color = "#ccffcc"  # Светло-зеленый
            workload_value = "ok"
        
        # Получаем продолжительность итерации
        duration = getattr(iteration, 'time', 0)
        if duration:
            duration_str = f"{duration:.1f}"
            duration_color = "#f0f0f0"  # Нейтральный серый
        else:
            duration_str = "N/A"
            duration_color = "#ffffcc"  # Светло-желтый для неизвестных значений
        
        # Добавляем строку таблицы (убираем колонки Cores/OOM)
        table_html += f"""
            <tr>
                <td>{iteration_num}</td>
                <td style='background-color: {workload_color};'>{workload_value}</td>
                <td style='background-color: {duration_color};'>{duration_str}</td>"""
        
        # Добавляем значения для каждой ноды
        for node_slot in unique_nodes:
            node_error = node_info_map[node_slot]
            
            # Показываем проблемы с нодой только в первой итерации
            show_node_issues = node_slot not in nodes_shown
            
            if show_node_issues:
                # Собираем все проблемы ноды
                issues = []
                has_issues = False
                has_critical_issues = False  # Отслеживаем критичные проблемы (cores/oom)
                
                # Проверяем основные проблемы (рестарт, падение)
                if node_error.message and node_error.message not in ['diagnostic info collected']:
                    issues.append(node_error.message.replace('was ', '').replace('is ', ''))
                    has_issues = True
                    
                # Добавляем cores если есть (критичная проблема)
                if node_error.core_hashes:
                    issues.append(f"cores:{len(node_error.core_hashes)}")
                    has_issues = True
                    has_critical_issues = True
                    
                # Добавляем oom если есть (критичная проблема)
                if node_error.was_oom:
                    issues.append("oom")
                    has_issues = True
                    has_critical_issues = True
                
                if has_issues:
                    # Красный только для критичных проблем (cores/oom)
                    # Зеленый для обычных проблем (restarted/down)
                    node_color = "#ffcccc" if has_critical_issues else "#ccffcc"
                    node_value = ", ".join(issues)
                    nodes_shown.add(node_slot)
                else:
                    node_color = "#ccffcc"  # Зеленый
                    node_value = "ok"
            else:
                node_color = "#ccffcc"  # Зеленый
                node_value = "ok"
            
            table_html += f"<td style='background-color: {node_color};'>{node_value}</td>"
        
        table_html += """
            </tr>"""
    
    table_html += """
    </table>
    """
    
    return table_html


def allure_test_description(
    suite: str,
    test: str,
    start_time: float,
    end_time: float,
    addition_table_strings: dict[str, any] = {},
    attachments: tuple[str, str, allure.attachment_type] = [],
    refference_set: str = '',
    node_errors: list[NodeErrors] = [],
    workload_result = None,
    workload_params: dict = None,
):
    def _pretty_str(s):
        return ' '.join(s.split('_')).capitalize()

    allure.dynamic.title(f'{suite}.{test}')
    for body, name, type in attachments:
        allure.attach(body, name, type)

    test_info = deepcopy(YdbCluster.get_cluster_info())
    test_info.update(addition_table_strings)

    _set_monitoring(test_info, start_time, end_time)
    _set_coredumps(test_info, start_time, end_time)
    _set_node_errors(test_info, node_errors)
    _set_results_plot(test_info, suite, test, refference_set)
    _set_logs_command(test_info, start_time, end_time)

    service_url = YdbCluster._get_service_url()
    db = test_info['database']
    test_info.update(
        {
            'table_path': YdbCluster.tables_path,
            'db_admin': (
                f"<a target='_blank' href='{service_url}/monitoring/tenant?"
                f"schema=/{db}/{YdbCluster.tables_path}&tenantPage=query"
                f"&diagnosticsTab=nodes&name=/{db}'>{service_url}</a>"
            ),
            'time': f"{datetime.fromtimestamp(start_time).strftime('%a %d %b %y %H:%M:%S')} - {datetime.fromtimestamp(end_time).strftime('%H:%M:%S')}",
        }
    )
    table_strings = '\n'.join([f'<tr><td>{_pretty_str(k)}</td><td>{v}</td></tr>' for k, v in test_info.items()])
    html = f'''<table border='1' cellpadding='4px'><tbody>
        {table_strings}
        </tbody></table>
    '''
    
    # Добавляем компактную таблицу итераций прямо в description
    logging.info(f"allure_test_description called with workload_result: {workload_result}")
    if workload_result:
        logging.info(f"workload_result is not None, calling _create_iterations_table")
        iterations_table = _create_iterations_table(workload_result, node_errors, workload_params)
        logging.info(f"iterations_table created, length: {len(iterations_table) if iterations_table else 0}")
        if iterations_table:
            html += f'''
            <h3>Workload Iterations</h3>
            {iterations_table}
            '''
            logging.info("Added iterations table to description HTML")
        else:
            logging.warning("iterations_table is empty, not adding to HTML")
    else:
        logging.warning("workload_result is None, not creating iterations table")
        # Для отладки - показываем таблицу с параметрами, даже если нет workload_result
        if workload_params:
            logging.info("Creating empty table with just workload_params for debugging")
            empty_table = _create_iterations_table(None, node_errors, workload_params)
            if empty_table:
                html += f'''
                <h3>Workload Iterations (Debug)</h3>
                {empty_table}
                '''
                logging.info("Added debug iterations table to HTML")
    
    allure.dynamic.description_html(html)
    allure.attach(html, "description.html", allure.attachment_type.HTML)
