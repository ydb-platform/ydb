from datetime import datetime, timedelta
from pathlib import Path

from django import forms
from django.contrib import messages
from django.template.defaultfilters import filesizeformat
from django.utils.translation import gettext_lazy as _

from .base import OnePageAdmin


class SummaryAdmin(OnePageAdmin):

    def contribute_onepage_context(self, request, context):
        from uwsgiconf.runtime.platform import uwsgi
        from uwsgiconf.runtime.logging import get_current_log_size
        from uwsgiconf.runtime.rpc import get_rpc_list
        from uwsgiconf.runtime.signals import registry_signals
        from uwsgiconf.runtime.spooler import Spooler
        from uwsgiconf.runtime.mules import Farm

        def get_func_name(func) -> str:
            """Returns a distinctive name for a given function."""
            module_path = func.__module__

            if module_path.startswith('uwsgi_file'):
                module_path = module_path.replace('uwsgi_file__', 'uwsgi://', 1).replace('_', '/')

            return f'{module_path}.{func.__name__}'

        def get_signals_info(signals):
            info = []

            for signal in signals:
                info.append(f'{signal.num} - {signal.target}: {get_func_name(signal.func)}')

            return info

        time_started = datetime.fromtimestamp(uwsgi.started_on)
        rss, vsz = uwsgi.memory
        config = uwsgi.config

        info_basic = {
            _('Version'): uwsgi.get_version(),
            _('Hostname'): uwsgi.hostname,
            _('Serving since'): time_started,
            _('Serving for'): datetime.now() - time_started,
            _('Clock'): uwsgi.clock,
            _('Master PID'): uwsgi.master_pid,
            _('Memory (RSS, VSZ)'): '\n'.join((filesizeformat(rss), filesizeformat(vsz))),
            _('Buffer size'): uwsgi.buffer_size,
            _('Cores'): uwsgi.cores_count,
            _('Workers'): uwsgi.workers_count,
            _('Mules'): config.get('mules', 0),
            _('Farms'): '\n'.join(map(str, Farm.get_farms())),
            _('Threads support'): '+' if uwsgi.threads_enabled else '-',
            _('Current worker'): uwsgi.worker_id,
            _('Requests by worker'): uwsgi.request.id,
            _('Requests total'): uwsgi.request.total_count,
            _('Socket queue size'): uwsgi.get_listen_queue(),
            _('Log size'): get_current_log_size(),
            _('RPC'): '\n'.join(get_rpc_list()),
            _('Post fork hooks'): '\n'.join(map(get_func_name, uwsgi.postfork_hooks.funcs)),
            _('Signals'): '\n'.join(get_signals_info(registry_signals)),
            _('Spoolers'): '\n'.join(map(str, Spooler.get_spoolers())),
        }

        context.update({
            'panels': {
                '': {
                    'rows': dict((key, [val]) for key, val in info_basic.items()),
                }
            },
        })


class ConfigurationAdmin(OnePageAdmin):

    def contribute_onepage_context(self, request, context):
        from uwsgiconf.runtime.platform import uwsgi

        context.update({
            'panels': {
                '': {'rows': dict((key, [val]) for key, val in uwsgi.config.items())},
            },
        })


class WorkersAdmin(OnePageAdmin):

    def contribute_onepage_context(self, request, context):
        from uwsgiconf.runtime.platform import uwsgi

        fromts = datetime.fromtimestamp

        info_worker_map = {
            'id': (_('ID'), None),
            'pid': (_('PID'), None),
            'status': (_('Status'), None),
            'running_time': (_('Running for'), lambda val: timedelta(microseconds=val)),
            'last_spawn': (_('Spawned at'), lambda val: fromts(val)),
            'respawn_count': (_('Respawns'), None),
            'requests': (_('Requests'), None),
            'delta_requests': (_('Delta requests'), None),  # Used alongside with MAX_REQUESTS
            'exceptions': (_('Exceptions'), None),
            'signals': (_('Signals'), None),
            'rss': (_('RSS'), lambda val: filesizeformat(val)),
            'vsz': (_('VSZ'), lambda val: filesizeformat(val)),
            'tx': (_('Transmitted'), lambda val: filesizeformat(val)),
            'avg_rt': (_('Avg. response'), lambda val: timedelta(microseconds=val)),
            'apps': (None, lambda val: iter_items(val, info_app_map)),
        }

        info_app_map = {
            'id': (_('ID'), None),
            'startup_time': (_('Serving since'), None),
            'interpreter': (_('Interpreter'), None),
            'modifier1': (_('Modifier 1'), None),
            'mountpoint': (_('Mountpoint'), None),
            'callable': (_('Callable'), None),
            'chdir': (_('Directory'), None),
            'requests': (_('Requests'), None),
            'exceptions': (_('Exceptions'), None),
        }

        panels = {}
        info_workers = {}
        panels[''] = {'rows': info_workers}

        info_apps = {}

        def iter_items(info, mapping):

            unknown = set()

            for idx, info_item in enumerate(info):
                for keyname, (name, func) in mapping.items():

                    value = info_item.get(keyname, unknown)
                    if value is unknown:
                        continue

                    if func is not None:
                        value = func(value)

                    yield idx, keyname, name, value

        for idx_worker, keyname_worker, name_worker, value_worker in iter_items(uwsgi.workers_info, info_worker_map):

            if keyname_worker == 'apps':
                # Get info about applications served by worker,
                for idx_app, keyname_app, name_app, value_app in value_worker:
                    app_key = f'%s {idx_worker + 1}. %s {idx_app}' % (_('Worker'), _('Application'))
                    info_apps.setdefault(app_key, {})[name_app] = [value_app]

            else:
                info_workers.setdefault(name_worker, []).append(value_worker)

        # Add panel for every app on every worker.
        for title, info in info_apps.items():
            panels[title] = {'rows': info}

        context.update({'panels': panels})


class MaintenanceForm(forms.Form):

    confirm_word = 'maintenance'

    confirm = forms.CharField(
        label=_('Confirmation'),
        help_text=_('Enter the word "%(word)s".') % {'word': confirm_word})

    def clean_confirm(self):

        value = self.cleaned_data['confirm']
        confirm_word = self.confirm_word

        if value != confirm_word:
            raise forms.ValidationError(
                _('You need to enter "%(word)s" to proceed.') % {'word': confirm_word})

        return value


class MaintenanceAdmin(OnePageAdmin):

    def contribute_onepage_context(self, request, context):

        from uwsgiconf.settings import get_maintenance_path
        maintenance = get_maintenance_path()

        if not maintenance:
            self.message_user(
                request,
                _('Maintenance mode is not supported in this setup.'),
                level=messages.WARNING
            )
            return

        form_kwargs = {}
        is_submitted = request.method == 'POST'

        if is_submitted:
            form_kwargs['data'] = request.POST

        form = MaintenanceForm(**form_kwargs)

        if is_submitted and form.is_valid():

            try:
                Path(maintenance).touch()
                self.message_user(request, _('Maintenance mode is scheduled.'))

            except Exception as e:
                self.message_user(
                    request,
                    _('Unable to schedule maintenance: %(error)s.') % {'error': f'{e}'})

        else:

            self.message_user(
                request,
                _('Remember: maintenance mode can only be turned off from the filesystem. '
                  'If the process is scheduled, your application, including this administration interface, '
                  'will only become available after filesystem manipulation and uWSGI restart.'),
                level=messages.WARNING
            )

            context.update({
                'panels': {'': {'rows': {'': [form]}}},
                'show_save': True,
            })
