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
    
    # Если result None или нет iterations, создаем пустую таблицу с информацией
    if not result or not hasattr(result, 'iterations') or not result.iterations:
        # Все равно показываем таблицу с параметрами, даже если нет итераций
        
        # Подсчитываем cores и OOM
        total_cores = sum(len(node_error.core_hashes) for node_error in node_errors)
        total_ooms = sum(1 for node_error in node_errors if node_error.was_oom)
        
        # Формируем информацию о параметрах workload для заголовка
        params_info = ""
        if workload_params:
            params_list = []
            for key, value in workload_params.items():
                params_list.append(f"{key}: {value}")
            if params_list:
                params_info = f"<div style='margin-bottom: 10px; padding: 5px; background-color: #f5f5f5; border: 1px solid #ddd;'><strong>Workload Parameters:</strong> {', '.join(params_list)}</div>"
        
        # Определяем значения и цвета для cores и OOM
        cores_color = "#ffcccc" if total_cores > 0 else "#ccffcc"
        cores_value = str(total_cores) if total_cores > 0 else "ok"
        
        oom_color = "#ffcccc" if total_ooms > 0 else "#ccffcc"
        oom_value = str(total_ooms) if total_ooms > 0 else "ok"
        
        # Создаем таблицу с placeholder
        table_html = f"""
        {params_info}
        <table border='1' cellpadding='2px' style='border-collapse: collapse; font-size: 12px;'>
            <tr style='background-color: #f0f0f0;'>
                <th>Iter</th><th>Status</th><th>Dur(s)</th><th>Cores</th><th>OOM</th>
            </tr>
            <tr style='font-size: 10px; color: #666;'>
                <td>#</td><td>🟢ok 🟨warn/timeout</td><td>sec</td><td>🟢ok 🔴cnt</td><td>🟢ok 🔴cnt</td>
            </tr>
            <tr>
                <td>-</td>
                <td style='background-color: #f0f0f0;'>no data</td>
                <td style='background-color: #f0f0f0;'>N/A</td>
                <td style='background-color: {cores_color};'>{cores_value}</td>
                <td style='background-color: {oom_color};'>{oom_value}</td>
            </tr>
        </table>
        """
        
        return table_html
    
    # Подсчитываем cores и OOM по итерациям
    total_cores = sum(len(node_error.core_hashes) for node_error in node_errors)
    total_ooms = sum(1 for node_error in node_errors if node_error.was_oom)
    
    # Формируем информацию о параметрах workload для заголовка
    params_info = ""
    if workload_params:
        params_list = []
        for key, value in workload_params.items():
            params_list.append(f"{key}: {value}")
        if params_list:
            params_info = f"<div style='margin-bottom: 10px; padding: 5px; background-color: #f5f5f5; border: 1px solid #ddd;'><strong>Workload Parameters:</strong> {', '.join(params_list)}</div>"
    
    # Создаем заголовок таблицы
    table_html = f"""
    {params_info}
    <table border='1' cellpadding='2px' style='border-collapse: collapse; font-size: 12px;'>
        <tr style='background-color: #f0f0f0;'>
            <th>Iter</th><th>Status</th><th>Dur(s)</th><th>Cores</th><th>OOM</th>
        </tr>
        <tr style='font-size: 10px; color: #666;'>
            <td>#</td><td>🟢ok 🟨warn/timeout</td><td>sec</td><td>🟢ok 🔴cnt</td><td>🟢ok 🔴cnt</td>
        </tr>
    """
    
    # Добавляем строки для каждой итерации
    iterations = sorted(result.iterations.keys())
    cores_shown = False
    ooms_shown = False
    
    for i, iteration_num in enumerate(iterations):
        iteration = result.iterations[iteration_num]
        
        # Определяем статус workload - добавляем проверку на timeout
        if hasattr(iteration, 'error_message') and iteration.error_message:
            # Проверяем, содержит ли ошибка информацию о timeout (различные форматы)
            error_msg_lower = iteration.error_message.lower()
            if ("timeout" in error_msg_lower or "timed out" in error_msg_lower or 
                "command timed out" in error_msg_lower):
                workload_color = "#ffffcc"  # Светло-желтый (warning для timeout)
                workload_value = "timeout"
            else:
                workload_color = "#ffffcc"  # Светло-желтый (warning)
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
        
        # Показываем cores и OOM только в первой итерации или если есть ошибки
        # Если ошибок нет в workload, но есть cores/OOM - показываем в первой итерации
        show_cores = False
        show_ooms = False
        
        if not cores_shown and total_cores > 0:
            show_cores = True
            cores_shown = True
            
        if not ooms_shown and total_ooms > 0:
            show_ooms = True
            ooms_shown = True
        
        # Определяем значения и цвета для cores и OOM
        if show_cores and total_cores > 0:
            cores_color = "#ffcccc"
            cores_value = str(total_cores)
        else:
            cores_color = "#ccffcc"
            cores_value = "ok"
            
        if show_ooms and total_ooms > 0:
            oom_color = "#ffcccc"
            oom_value = str(total_ooms)
        else:
            oom_color = "#ccffcc"
            oom_value = "ok"
        
        # Добавляем строку таблицы
        table_html += f"""
            <tr>
                <td>{iteration_num}</td>
                <td style='background-color: {workload_color};'>{workload_value}</td>
                <td style='background-color: {duration_color};'>{duration_str}</td>
                <td style='background-color: {cores_color};'>{cores_value}</td>
                <td style='background-color: {oom_color};'>{oom_value}</td>
            </tr>
        """
    
    table_html += "</table>"
    
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
