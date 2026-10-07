from copy import deepcopy
import html
import json
import logging
import allure
from datetime import datetime
from ydb.tests.library.stability.utils.results_models import StressUtilTestResults
from ydb.tests.library.stability.utils.collect_errors import WardenResults
from ydb.tests.olap.lib.allure_utils import (
    NodeErrors,
    _attach_sanitizer_outputs,
    _produce_sanitizer_report,
    _set_coredumps,
    _set_logs_command,
    _set_monitoring,
    _set_node_errors
)
from ydb.tests.library.stability.utils.utils import external_param_is_true, get_ci_version, get_external_param, get_self_version
from ydb.tests.olap.lib.ydb_cluster import YdbCluster


def create_parallel_allure_report(
    result: StressUtilTestResults,
    node_errors,
    warden_results: WardenResults = None,
):
    """Creates an Allure report for workload test results.

    Args:
        result: Stress test execution results
        node_errors: List of NodeErrors with diagnostic info
        warden_results: Results from nemesis orchestrator warden checks
    """
    additional_table_strings = {}
    end_time = result.end_time if result.recoverability_result is None else result.recoverability_result.end_time
    parallel_allure_test_description(
        start_time=result.start_time,
        end_time=end_time,
        addition_table_strings=additional_table_strings,
        node_errors=node_errors,
        execution_result=result,
        warden_results=warden_results,
    )


def parallel_allure_test_description(
    start_time: float,
    end_time: float,
    addition_table_strings: dict[str, any] = None,
    attachments: tuple[str, str, allure.attachment_type] = None,
    node_errors: list[NodeErrors] = None,
    addition_blocks: list[str] = [],
    execution_result: StressUtilTestResults = None,
    warden_results: WardenResults = None,
):
    """Creates a detailed Allure test description for parallel tests

    Args:
        start_time: Test start timestamp
        end_time: Test end timestamp
        addition_table_strings: Additional key-value pairs for info table
        attachments: Tuple of (body, name, type) for attachments
        node_errors: List of node error objects
        addition_blocks: Additional HTML blocks to include
        execution_result: Test execution results
        warden_results: Results from nemesis orchestrator warden checks
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

    # Add warden checks status table and violations from orchestrator
    if warden_results is not None:
        html += _produce_warden_checks_report(warden_results)

    logs_in_html = external_param_is_true('save_san_logs_in_html')
    if logs_in_html:
        html += _produce_sanitizer_report(node_errors)
    else:
        if node_errors and len(node_errors) > 0:
            html += '<h4>Sanitizer Errors</h4>'
            html += '<p>Sanitizer output was saved as allure attachment in the last step</p>'
            _attach_sanitizer_outputs(node_errors)
    html += '\n'.join([f'<div>\n{b}\n</div>\n\n' for b in addition_blocks])

    iterations_html = __create_workload_iterations_report(execution_result)
    logging.info(f"iterations report created, length: {len(iterations_html) if iterations_html else 0}")
    if iterations_html:
        html += iterations_html
        logging.info("Added iterations report to description HTML")
    else:
        logging.warning("iterations report is empty, not adding to HTML")

    allure.dynamic.description_html(html)
    allure.attach(html, "description.html", allure.attachment_type.HTML)


def _produce_warden_checks_report(warden_results: WardenResults) -> str:
    """Produce HTML report section for warden check results from the nemesis orchestrator.

    Includes:
    - A status table showing all checks with their status (ok/failed)
    - Full text of all violations if any were found

    Args:
        warden_results: WardenResults from the orchestrator

    Returns:
        str: HTML string with the warden checks report
    """
    if warden_results is None:
        return ''

    html = '<h3>Nemesis Orchestrator Warden Checks</h3>'

    # Show polling status
    if not warden_results.poll_success:
        if warden_results.error_message:
            html += (
                f'<p style="color: orange; font-weight: bold;">'
                f'⚠ Warden checks polling did not complete successfully: '
                f'{_escape_html(warden_results.error_message)}</p>'
            )
        else:
            html += (
                '<p style="color: orange; font-weight: bold;">'
                '⚠ Warden checks were not executed</p>'
            )
        if not warden_results.checks:
            return html

    # --- Checks status table ---
    html += '<h4>Checks Status</h4>'
    html += (
        '<table border="1" cellpadding="4px" style="border-collapse: collapse; font-size: 13px;">'
        '<tr style="background-color: #f0f0f0;">'
        '<th>Check Name</th>'
        '<th>Type</th>'
        '<th>Status</th>'
        '<th>Affected Hosts</th>'
        '<th>Violations Count</th>'
        '</tr>'
    )

    # Sort checks: safety first, then liveness, then orchestrator variants
    type_order = {
        'safety': 0,
        'liveness': 1,
        'orchestrator_safety': 2,
        'orchestrator_liveness': 3,
        'orchestrator': 4,
    }

    sorted_checks = sorted(
        warden_results.checks.items(),
        key=lambda x: (type_order.get(x[1].check_type, 5), x[0]),
    )
    for name, check in sorted_checks:
        if check.is_ok():
            status_color = '#ccffcc'
            status_text = '✅ ok'
        elif check.is_violation():
            status_color = '#ffcccc'
            status_text = f'⚠ Violation ({len(check.violations)})'
        elif check.is_error():
            status_color = '#fff3cd'
            status_text = '❌ Error'
        else:
            status_color = '#ffcccc'
            status_text = f'❌ {_escape_html(check.status)}'

        affected_hosts_str = ', '.join(sorted(check.affected_hosts)) if check.affected_hosts else '—'
        violations_count = len(check.violations)

        # Show full check name — it contains important information
        display_name = name

        # Type badge with color
        check_type = getattr(check, 'check_type', 'safety')
        if check_type == 'liveness':
            type_badge = '<span style="color: #0066cc; font-weight: bold;">liveness</span>'
        elif check_type == 'orchestrator_liveness':
            type_badge = '<span style="color: #0066cc; font-weight: bold;">liveness (cluster)</span>'
        elif check_type == 'orchestrator_safety':
            type_badge = '<span style="color: #006600; font-weight: bold;">safety (cluster)</span>'
        elif check_type == 'orchestrator':
            type_badge = '<span style="color: #666666; font-weight: bold;">orchestrator</span>'
        else:
            type_badge = '<span style="color: #006600; font-weight: bold;">safety</span>'

        html += (
            f'<tr>'
            f'<td>{_escape_html(display_name)}</td>'
            f'<td style="text-align: center;">{type_badge}</td>'
            f'<td style="background-color: {status_color}; text-align: center;">{status_text}</td>'
            f'<td>{_escape_html(affected_hosts_str)}</td>'
            f'<td style="text-align: center;">{violations_count}</td>'
            f'</tr>'
        )

    html += '</table>'

    # --- Violations text ---
    all_violations = warden_results.get_all_violations()
    if all_violations:
        html += '<h4>Violations Details</h4>'
        for name, check in sorted(warden_results.checks.items()):
            if not check.violations:
                continue
            html += (
                f'<details style="margin-bottom: 10px;">'
                f'<summary style="display: list-item; font-weight: bold; color: #cc0000;">'
                f'{_escape_html(name)} ({len(check.violations)} violation(s))'
                f'</summary>'
                f'<div style="margin: 5px 0; padding: 8px; background-color: #fff5f5; '
                f'border: 1px solid #ffcccc; border-radius: 4px;">'
            )
            for violation in check.violations:
                html += f'<pre style="white-space: pre-wrap; margin: 4px 0;">{_escape_html(str(violation))}</pre>'
            html += '</div></details>'

    # Attach raw warden response as JSON for debugging
    if warden_results.raw_response:
        try:
            raw_json = json.dumps(warden_results.raw_response, indent=2, ensure_ascii=False, default=str)
            allure.attach(raw_json, "Warden Raw Response", allure.attachment_type.JSON)
        except Exception:
            pass

    return html


# _pretty_check_name removed: full check names are shown as-is per user request
# (check names contain important information that should not be truncated)


def _escape_html(text: str) -> str:
    """Escape HTML special characters in text.

    Args:
        text: Raw text string

    Returns:
        str: HTML-escaped text
    """
    if not text:
        return ''
    return html.escape(text, quote=True)


# Colors for workload status cells
_COLOR_OK = '#ccffcc'
_COLOR_PARTIAL = '#fff3cd'
_COLOR_FAIL = '#ffcccc'
_COLOR_EMPTY = '#fafafa'
_COLOR_HEADER = '#f0f0f0'


def _cell_status_color(successful: int, total: int) -> str:
    """Return background color for ok/total cell."""
    if total <= 0:
        return _COLOR_EMPTY
    if successful == total:
        return _COLOR_OK
    if successful == 0:
        return _COLOR_FAIL
    return _COLOR_PARTIAL


def __create_workload_iterations_report(execution_result: StressUtilTestResults) -> str:
    """Create a summary table of workload success per stress type.

    Shows overall ok/total, success rate, host count, and recoverability
    status when present.
    """
    if not execution_result or not execution_result.stress_util_runs:
        return ''

    has_recoverability = execution_result.recoverability_result is not None
    html_parts = ['<div>']

    html_parts.append('<h3>Workload Summary</h3>')
    html_parts.append(
        '<table border="1" cellpadding="4px" style="border-collapse: collapse; font-size: 13px;">'
        f'<tr style="background-color: {_COLOR_HEADER};">'
        '<th>Stress type</th>'
        '<th>Main ok/total</th>'
        '<th>Success rate</th>'
        '<th>Hosts</th>'
    )
    if has_recoverability:
        html_parts.append('<th>Recoverability</th>')
    html_parts.append('</tr>')

    for stress_name, stress_result in execution_result.stress_util_runs.items():
        ok = stress_result.get_successful_runs()
        total = stress_result.get_total_runs()
        rate = f'{(ok / total):.0%}' if total else '—'
        hosts_count = len(stress_result.node_runs)
        color = _cell_status_color(ok, total)
        html_parts.append('<tr>')
        html_parts.append(f'<td>{_escape_html(stress_name)}</td>')
        html_parts.append(
            f'<td style="background-color: {color}; text-align: center;">{ok}/{total}</td>'
        )
        html_parts.append(f'<td style="text-align: center;">{rate}</td>')
        html_parts.append(f'<td style="text-align: center;">{hosts_count}</td>')
        if has_recoverability:
            rec = execution_result.recoverability_result.stress_util_runs.get(stress_name)
            if rec is None:
                html_parts.append(f'<td style="background-color: {_COLOR_EMPTY};">—</td>')
            else:
                rec_ok = rec.get_successful_runs()
                rec_total = rec.get_total_runs()
                rec_color = _cell_status_color(rec_ok, rec_total)
                if rec_total == 0:
                    rec_text = '—'
                elif rec_ok == rec_total:
                    rec_text = f'Recovered ({rec_ok}/{rec_total})'
                elif rec_ok == 0:
                    rec_text = f'All failed ({rec_ok}/{rec_total})'
                else:
                    rec_text = f'Some failed ({rec_ok}/{rec_total})'
                html_parts.append(
                    f'<td style="background-color: {rec_color}; text-align: center;">{rec_text}</td>'
                )
        html_parts.append('</tr>')

    html_parts.append('</table></div>')
    return '\n'.join(html_parts)


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
    nemesis_monitoring = nemesis_monitoring.replace('{{', '{').replace('}}', '}')
    test_info['nemesis_dashboard'] = f"<a target='_blank' href='https://{nemesis_monitoring.format(
        start_time=monitoring_start,
        end_time=monitoring_end
    )}'>Nemesis Dashboard</a>"
