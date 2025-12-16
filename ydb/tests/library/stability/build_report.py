from copy import deepcopy
import logging
import allure
from datetime import datetime
from ydb.tests.library.stability.utils.results_models import StressUtilTestResults
from ydb.tests.olap.lib.allure_utils import (
    NodeErrors,
    _attach_sanitizer_outputs,
    _produce_sanitizer_report,
    _produce_verify_report,
    _set_coredumps,
    _set_logs_command,
    _set_monitoring,
    _set_node_errors
)
from ydb.tests.library.stability.utils.utils import external_param_is_true, get_ci_version, get_external_param, get_self_version
from ydb.tests.olap.lib.ydb_cluster import YdbCluster


def create_parallel_allure_report(result: StressUtilTestResults, node_errors, verify_errors):
    """Creates an Allure report for workload test results"""
    additional_table_strings = {}
    end_time = result.end_time if result.recoverability_result is None else result.recoverability_result.end_time
    parallel_allure_test_description(
        start_time=result.start_time,
        end_time=end_time,
        addition_table_strings=additional_table_strings,
        node_errors=node_errors,
        verify_errors=verify_errors,
        execution_result=result
    )


def parallel_allure_test_description(
    start_time: float,
    end_time: float,
    addition_table_strings: dict[str, any] = None,
    attachments: tuple[str, str, allure.attachment_type] = None,
    node_errors: list[NodeErrors] = None,
    verify_errors=None,
    addition_blocks: list[str] = [],
    execution_result: StressUtilTestResults = None
):
    """Creates a detailed Allure test description for parallel tests

    Args:
        suite: Test suite name
        test: Test name
        start_time: Test start timestamp
        end_time: Test end timestamp
        addition_table_strings: Additional key-value pairs for info table
        attachments: Tuple of (body, name, type) for attachments
        node_errors: List of node error objects
        verify_errors: Verification errors
        addition_blocks: Additional HTML blocks to include
        execution_result: Test execution results
    """
    if addition_table_strings is None:
        addition_table_strings = {}
    if attachments is None:
        attachments = []
    if node_errors is None:
        node_errors = []

    def _pretty_str(s):
        """Convert snake_case strings to pretty display format"""
        return ' '.join(s.split('_')).capitalize()

    for body, name, type in attachments:
        allure.attach(body, name, type)

    test_info = deepcopy(YdbCluster.get_cluster_info())
    test_info['ci_version'] = get_ci_version()
    test_info['test_tools_version'] = get_self_version()
    test_info.update(addition_table_strings)

    __set_nemesis_dashboard(test_info, start_time, end_time)
    _set_monitoring(test_info, start_time, end_time)
    _set_coredumps(test_info, start_time, end_time)
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


def __create_parallel_test_table(execution_result: StressUtilTestResults) -> str:
    """Creates an HTML table showing workload iteration results per host

    Args:
        execution_result: Stress test execution results containing node run data

    Returns:
        str: HTML string containing the formatted results table
    """
    table_html = """
    <div>
    <table border='1' cellpadding='2px' style='border-collapse: collapse; font-size: 12px;'>
        <tr style='background-color: #f0f0f0;'>
            <th>Stress type</th>
    """
    # Group nodes by host

    all_nodes = set([node_host for run_result in execution_result.stress_util_runs.values() for node_host in run_result.node_runs.keys()])
    all_cluster_nodes = YdbCluster.get_cluster_nodes(db_only=True)
    hosts_to_nodes = {}
    for node in all_cluster_nodes:
        if node.host not in hosts_to_nodes:
            hosts_to_nodes[node.host] = []
        hosts_to_nodes[node.host].append(node)

    # For each host, take the first node as representative
    unique_hosts = sorted(all_nodes)
    for host in unique_hosts:
        table_html += f'<th>{host.split('.')[0]}</th>'
    if execution_result.recoverability_result:
        table_html += '<th>Recovered</th>'

    logging.info(f"unique_hosts: {unique_hosts}")

    table_html += """
        </tr>
    """

    for stress_name, stress_result in execution_result.stress_util_runs.items():
        table_html += '<tr>'
        stress_color = '#ccffcc'
        if stress_result.get_successful_runs() == 0:
            stress_color = "#ffcccc"
        elif len(list(filter(lambda x: x.is_all_success(), stress_result.node_runs.values()))) < len(unique_hosts):
            stress_color = "#fff4cc"
        table_html += f'<td style="background-color: {stress_color};">{stress_name}</td>'

        for host in unique_hosts:
            color = '#ccffcc'
            if host not in stress_result.node_runs:
                color = "#ffcccc"
                table_html += f'<td style="background-color: {color};">Not deployed</td>'
            else:
                host_successes = stress_result.node_runs[host].get_successful_runs()
                host_total = stress_result.node_runs[host].get_total_runs()
                if host_successes == 0:
                    color = "#ffcccc"
                elif host_successes != host_total:
                    color = "#fff4cc"
                table_html += f'<td style="background-color: {color};">{host_successes}/{host_total}</td>'

        if execution_result.recoverability_result:
            result_for_util = execution_result.recoverability_result.stress_util_runs[stress_name]
            color = '#ccffcc'
            text = 'Recovered'
            if result_for_util.get_successful_runs() == 0:
                color = "#ffcccc"
                text = 'All failed'
            else:
                color = "#fff4cc"
                text = 'Some failed'
            table_html += f'<td style="background-color: {color};">{text}</td>'

        table_html += '</tr>'
    table_html += '</table></div>'

    return table_html


def __set_nemesis_dashboard(test_info: dict[str, str], start_time: float, end_time: float) -> None:
    monitoring_start = int((start_time - 120) * 1000)
    monitoring_end = int((end_time + 120) * 1000)
    # monitoring does not show intervals less 1 minute.
    monitoring_addition = 60000 - (monitoring_end - monitoring_start)
    if monitoring_addition > 0:
        monitoring_start -= monitoring_addition
        monitoring_end += monitoring_addition

    nemesis_monitoring = get_external_param('nemesis_dashboard', None)
    if not nemesis_monitoring:
        return
    test_info['nemesis_dashboard'] = f"<a target='_blank' href='https://{nemesis_monitoring.format(
        start_time=monitoring_start,
        end_time=monitoring_end
    )}'>Nemesis Dashboard</a>"
