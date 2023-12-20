from pathlib import Path
import subprocess
from typing import Final

import jinja2

import yatest.common

import json

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EProtocol
from ydb.library.yql.providers.generic.connector.api.service.protos.connector_pb2 import EDateTimeFormat

from ydb.library.yql.providers.generic.connector.tests.utils.runner import Result, Runner
from ydb.library.yql.providers.generic.connector.tests.utils.log import make_logger
from ydb.library.yql.providers.generic.connector.tests.utils.schema import Schema
from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings, GenericSettings

LOGGER = make_logger(__name__)


class SchemeRenderer:
    template_: Final = '''

{% macro create_data_source(kind, data_source, host, port, login, password, protocol, database, schema) -%}

CREATE OBJECT {{data_source}}_local_password (TYPE SECRET) WITH (value = "{{password}}");

CREATE EXTERNAL DATA SOURCE {{data_source}} WITH (
    SOURCE_TYPE="{{kind}}",
    LOCATION="{{host}}:{{port}}",
    DATABASE_NAME="{{database}}",
    AUTH_METHOD="BASIC",
    LOGIN="{{login}}",
    PASSWORD_SECRET_NAME="{{data_source}}_local_password",
    USE_TLS="FALSE",
    PROTOCOL="{{protocol}}"

    {% if kind == POSTGRESQL and schema %}
        ,SCHEMA="{{schema}}"
    {% endif %}
);

{%- endmacro -%}

{% set CLICKHOUSE = 'ClickHouse' %}
{% set POSTGRESQL = 'PostgreSQL' %}

{% set NATIVE = 'NATIVE' %}
{% set HTTP = 'HTTP' %}

{% for cluster in generic_settings.clickhouse_clusters %}

{% if cluster.protocol == EProtocol.NATIVE %}
{% set CLICKHOUSE_PORT = settings.clickhouse.native_port %}
{% set CLICKHOUSE_PROTOCOL = NATIVE %}
{% elif cluster.protocol == EProtocol.HTTP %}
{% set CLICKHOUSE_PORT = settings.clickhouse.http_port %}
{% set CLICKHOUSE_PROTOCOL = HTTP %}
{% endif %}

{{ create_data_source(
    CLICKHOUSE,
    settings.clickhouse.cluster_name,
    settings.clickhouse.host,
    CLICKHOUSE_PORT,
    settings.clickhouse.username,
    settings.clickhouse.password,
    CLICKHOUSE_PROTOCOL,
    cluster.database,
    NONE)
}}
{% endfor %}

{% for cluster in generic_settings.postgresql_clusters %}
{{ create_data_source(
    POSTGRESQL,
    settings.postgresql.cluster_name,
    settings.postgresql.host,
    settings.postgresql.port,
    settings.postgresql.username,
    settings.postgresql.password,
    NATIVE,
    cluster.database,
    cluster.schema)
}}
{% endfor %}

'''

    def __init__(self):
        self.template = jinja2.Environment(loader=jinja2.BaseLoader, undefined=jinja2.DebugUndefined).from_string(
            self.template_
        )
        self.template.globals['EProtocol'] = EProtocol

    def render(self, file_path: Path, settings: Settings, generic_settings: GenericSettings) -> None:
        content = self.template.render(dict(settings=settings, generic_settings=generic_settings))
        with open(file_path, 'w') as f:
            f.write(content)


class AppConfigRenderer:
    template_: Final = '''

FeatureFlags {
  EnableExternalDataSources: true
  EnableScriptExecutionOperations: true
}

QueryServiceConfig {
  Generic {
    Connector {
        Endpoint {
            host: "{{settings.connector.grpc_host}}"
            port: {{settings.connector.grpc_port}}
        }
        UseSsl: false
    }

    DefaultSettings {
        Name: "DateTimeFormat"
        {% if generic_settings.date_time_format == EDateTimeFormat.STRING_FORMAT %}
        Value: "string"
        {% elif generic_settings.date_time_format == EDateTimeFormat.YQL_FORMAT %}
        Value: "YQL"
        {% endif %}
    }
  }
}

'''

    def __init__(self):
        self.template = jinja2.Environment(loader=jinja2.BaseLoader, undefined=jinja2.DebugUndefined).from_string(
            self.template_
        )
        self.template.globals['EDateTimeFormat'] = EDateTimeFormat

    def render(self, file_path: Path, settings: Settings, generic_settings: GenericSettings) -> None:
        content = self.template.render(dict(settings=settings, generic_settings=generic_settings))
        with open(file_path, 'w') as f:
            f.write(content)


class KqpRunner(Runner):
    def __init__(
        self,
        kqprun_path: Path,
        settings: Settings,
    ):
        self.scheme_renderer = SchemeRenderer()
        self.app_conf_renderer = AppConfigRenderer()
        self.kqprun_path = kqprun_path
        self.settings = settings

    def run(self, test_dir: Path, script: str, generic_settings: GenericSettings) -> Result:
        LOGGER.debug(script)

        # Create file with YQL script
        script_path = test_dir.joinpath('script.yql')
        with open(script_path, "w") as f:
            f.write(script)

        # Create config
        app_conf_path = test_dir.joinpath('app_confif.conf')
        self.app_conf_renderer.render(app_conf_path, settings=self.settings, generic_settings=generic_settings)

        # Create scheme
        scheme_path = test_dir.joinpath('scheme.txt')
        self.scheme_renderer.render(scheme_path, settings=self.settings, generic_settings=generic_settings)

        # Run kqprun
        result_path = test_dir.joinpath('result.json')

        # For debug add option --trace-opt to args
        cmd = f'{self.kqprun_path} -s {scheme_path} -p {script_path} --app-config={app_conf_path} --result-file={result_path} --result-format=full'
        out = subprocess.run(cmd, shell=True, capture_output=True)

        data_out = None
        data_out_with_types = None
        schema = None
        unique_suffix = test_dir.name

        if out.returncode == 0:
            # Parse output
            with open(result_path, 'r') as f:
                result = json.loads(f.read().encode('ascii'), strict=False)

            # Kqprun's data output is missing type information (everything is a string),
            # so we have to recover schema and transform the results to make them comparable with the inputs
            data_out = result["rows"]
            schema = Schema.from_json(result["columns"])

            data_out_with_types = []
            for row in data_out:
                row_values = []
                for item in row['items']:
                    name, value = list(item.items())[0]
                    if name == 'null_flag_value':
                        row_values.append(None)
                    else:
                        row_values.append(value)

                data_out_with_types.append(schema.cast_row(row_values))

            LOGGER.debug('Schema: %s', schema)
            LOGGER.debug('Data out: %s', data_out)
            LOGGER.debug('Data out with types: %s', data_out_with_types)

        else:
            LOGGER.error('STDOUT: ')
            for line in out.stdout.decode('utf-8').splitlines():
                LOGGER.error(line)
            LOGGER.error('STDERR: ')
            for line in out.stderr.decode('utf-8').splitlines():
                LOGGER.error(line)

        err_file = yatest.common.output_path(f'kqprun-{unique_suffix}.err')
        with open(err_file, "w") as f:
            f.write(out.stderr.decode('utf-8'))

        out_file = yatest.common.output_path(f'kqprun-{unique_suffix}.out')
        with open(out_file, "w") as f:
            f.write(out.stdout.decode('utf-8'))

        return Result(
            data_out=data_out,
            data_out_with_types=data_out_with_types,
            schema=schema,
            stdout=out.stdout.decode('utf-8'),
            stderr=out.stderr.decode('utf-8'),
            returncode=out.returncode,
        )
