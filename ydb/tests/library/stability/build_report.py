from collections import defaultdict
from copy import deepcopy
import logging
import allure
from datetime import datetime
from ydb.tests.library.stability.aggregate_results import StressUtilTestResults
from ydb.tests.olap.lib.allure_utils import NodeErrors, _attach_sanitizer_outputs, _produce_sanitizer_report, _produce_verify_report, _set_coredumps, _set_logs_command, _set_monitoring, _set_node_errors, _set_results_plot
from ydb.tests.olap.lib.utils import external_param_is_true, get_ci_version, get_self_version
from ydb.tests.olap.lib.ydb_cluster import YdbCluster


def create_parallel_allure_report(result: StressUtilTestResults, workload_name, node_errors, verify_errors):
    """Формирует allure-отчёт по результатам workload"""
    additional_table_strings = {}
    parallel_allure_test_description(
        suite="Parallel tests",
        test=workload_name,
        start_time=result.start_time,
        end_time=result.end_time,
        addition_table_strings=additional_table_strings,
        node_errors=node_errors,
        verify_errors=verify_errors,
        execution_result=result
    )


def parallel_allure_test_description(
    suite: str,
    test: str,
    start_time: float,
    end_time: float,
    addition_table_strings: dict[str, any] = None,
    attachments: tuple[str, str, allure.attachment_type] = None,
    refference_set: str = '',
    node_errors: list[NodeErrors] = None,
    verify_errors=None,
    addition_blocks: list[str] = [],
    execution_result: StressUtilTestResults = None
):
    if addition_table_strings is None:
        addition_table_strings = {}
    if attachments is None:
        attachments = []
    if node_errors is None:
        node_errors = []

    def _pretty_str(s):
        return ' '.join(s.split('_')).capitalize()

    for body, name, type in attachments:
        allure.attach(body, name, type)

    test_info = deepcopy(YdbCluster.get_cluster_info())
    test_info['ci_version'] = get_ci_version()
    test_info['test_tools_version'] = get_self_version()
    test_info.update(addition_table_strings)

    _set_monitoring(test_info, start_time, end_time)
    _set_coredumps(test_info, start_time, end_time)
    _set_results_plot(test_info, suite, test, refference_set)
    _set_logs_command(test_info, start_time, end_time)

    service_url = YdbCluster._get_service_url()
    db = test_info['database']
    test_info.update(
        {
            'table_path': YdbCluster.get_tables_path(),
            'db_admin': (
                f"<a target='_blank' href='{service_url}/monitoring/tenant?"
                f"schema=/{db}/{YdbCluster.get_tables_path()}&tenantPage=query"
                f"&diagnosticsTab=nodes&name=/{db}'>{service_url}</a>"
            ),
            'time': (
                f"{datetime.fromtimestamp(start_time).strftime('%a %d %b %y %H:%M:%S')} - "
                f"{datetime.fromtimestamp(end_time).strftime('%H:%M:%S')}"),
        }
    )
    table_strings = '\n'.join([f'<tr><td>{_pretty_str(k)}</td><td>{v}</td></tr>' for k, v in test_info.items()])
    html = f'''<table border='1' cellpadding='4px'><tbody>
        {table_strings}
        </tbody></table>
    '''

    html += _set_node_errors(node_errors)
    html += _produce_verify_report(verify_errors)
    logs_in_html = external_param_is_true('save_san_logs_in_html')
    if logs_in_html:
        html += _produce_sanitizer_report(node_errors)
    else:
        if node_errors and len(node_errors) > 0:
            html += '<h4>Sanitizer Errors</h4>'
            html += '<p>Sanitizer output was saved as allure attachment in the last step</p>'
            _attach_sanitizer_outputs(node_errors)
    html += '\n'.join([f'<div>\n{b}\n</div>\n\n' for b in addition_blocks])

    iterations_table = __create_parallel_test_table(execution_result)
    logging.info(f"iterations_table created, length: {len(iterations_table) if iterations_table else 0}")
    if iterations_table:
        html += f'''
        <h3>Workload Iterations</h3>
        {iterations_table}
        '''
        logging.info("Added iterations table to description HTML")
    else:
        logging.warning("iterations_table is empty, not adding to HTML")

    allure.dynamic.description_html(html)
    allure.attach(html, "description.html", allure.attachment_type.HTML)


def __create_parallel_test_table(execution_result):
    # Создаем заголовок таблицы с подколонками для каждой ноды
    table_html = """
    <table border='1' cellpadding='2px' style='border-collapse: collapse; font-size: 12px;'>
        <tr style='background-color: #f0f0f0;'>
            <th>Stress type</th>
    """
    # Группируем ноды по хостам
    all_cluster_nodes = YdbCluster.get_cluster_nodes(db_only=True)
    hosts_to_nodes = {}
    for node in all_cluster_nodes:
        if node.host not in hosts_to_nodes:
            hosts_to_nodes[node.host] = []
        hosts_to_nodes[node.host].append(node)

    # Для каждого хоста берем первую ноду в качестве представителя
    unique_hosts = sorted(hosts_to_nodes.keys())
    # Добавляем объединенные колонки для каждого хоста
    if unique_hosts:
        for host in unique_hosts:
            table_html += f'<th>{host.split('.')[0]}</th>'
    else:
        # Если нет нод, добавляем агрегированные колонки
        table_html += '<th>Aggregated</th>'
    logging.info(f"unique_hosts: {unique_hosts}")

    table_html += """
        </tr>
    """

    stress_grouped_results = defaultdict(lambda: [])
    for stress_name, node_result in execution_result.stress_util_runs.items():
        stress_grouped_results[stress_name] = node_result

    for stress_name, stress_result in stress_grouped_results.items():
        table_html += '<tr>'
        stress_color = '#ccffcc'
        stress_sucesses_for_all_hosts = sum(map(lambda res: res.successful_runs, stress_result)) == sum(map(lambda res: res.total_runs, stress_result))
        if stress_sucesses_for_all_hosts == 0:
            stress_color = "#ffcccc"
        elif stress_sucesses_for_all_hosts < len(unique_hosts):
            stress_color = "#fff4cc"
        table_html += f'<td style="background-color: {stress_color};">{stress_name}</td>'

        hosts_stress_result = dict(map(lambda res: (res.host, res), stress_result))
        logging.info(f"hosts_stress_result: {hosts_stress_result}")
        for host in unique_hosts:
            color = '#ccffcc'
            if host not in hosts_stress_result:
                color = "#ffcccc"
                table_html += f'<td style="background-color: {color};">Not deployed</td>'
            else:
                host_successes = sum(map(lambda res: res.successful_runs, filter(lambda res: res.host == host, stress_result)))
                host_total = sum(map(lambda res: res.total_runs, filter(lambda res: res.host == host, stress_result)))
                if host_successes == 0:
                    color = "#ffcccc"
                elif host_successes != host_total:
                    color = "#fff4cc"
                table_html += f'<td style="background-color: {color};">{host_successes}/{host_total}</td>'
        table_html += '</tr>'

    return table_html
