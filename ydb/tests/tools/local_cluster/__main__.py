#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Utility to start local YDB cluster (ydbd) on default ports.
Uses harness from ydb/tests/library/harness to start cluster with MIRROR_3DC configuration.
First node starts on default ports (grpc=2135, mon=8765, ic=19001).
"""
import argparse
import json
import logging
import os
import signal
import socket
import sys
import threading
import time
import urllib.request
import urllib.error
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse

from ydb.tests.library.harness import kikimr_config
from ydb.tests.library.harness import kikimr_runner
from ydb.tests.library.harness.kikimr_port_allocator import (
    DefaultFirstNodePortAllocator,
)
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.nemesis.nemesis_core import NemesisProcess
from ydb.tests.tools.nemesis.library.node import KillNodeNemesis


class DualStackHTTPServer(HTTPServer):
    """HTTP Server that supports both IPv4 and IPv6."""
    address_family = socket.AF_INET6
    
    def __init__(self, address, *args, **kwargs):
        # Try IPv6 first, fallback to IPv4 if not supported
        try:
            super().__init__(address, *args, **kwargs)
            # Enable dual-stack: allow IPv4 connections on IPv6 socket
            if self.address_family == socket.AF_INET6 and hasattr(socket, 'IPV6_V6ONLY'):
                try:
                    self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
                except (OSError, AttributeError):
                    # IPV6_V6ONLY not supported on this system, continue without dual-stack
                    pass
        except OSError as e:
            if e.errno in (socket.EAFNOSUPPORT, socket.EPROTONOSUPPORT):
                # IPv6 not supported, fallback to IPv4
                self.address_family = socket.AF_INET
                # Convert :: to 0.0.0.0 for IPv4
                if address[0] == '::':
                    address = ('0.0.0.0', address[1])
                super().__init__(address, *args, **kwargs)
            else:
                raise

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def download_binary_from_s3(version, working_dir):
    """
    Downloads ydbd binary from S3 storage if not already downloaded.
    Each version is stored in a separate directory: binary/<version>/ydbd

    Args:
        version: Git ref/version (e.g., '25.3.1.21')
        working_dir: Base working directory

    Returns:
        Path to downloaded binary
    """
    binary_dir = os.path.join(working_dir, "binary", version)
    binary_path = os.path.join(binary_dir, "ydbd")

    if os.path.exists(binary_path) and os.access(binary_path, os.X_OK):
        logger.info("Binary already exists for version %s: %s", version, binary_path)
        return binary_path

    url = f"https://storage.yandexcloud.net/ydb-builds/{version}/release/ydbd"

    logger.info("Downloading ydbd binary from S3...")
    logger.info("Version: %s", version)
    logger.info("URL: %s", url)
    logger.info("Destination: %s", binary_path)

    try:
        os.makedirs(binary_dir, exist_ok=True)
        urllib.request.urlretrieve(url, binary_path)
        os.chmod(binary_path, 0o755)

        logger.info("Binary downloaded successfully")
        return binary_path
    except Exception as e:
        logger.error("Unexpected error during download: %s", e)
        raise RuntimeError(f"Failed to download binary: {e}") from e


class LocalCluster:
    def __init__(self, working_dir, binary_path=None, http_port=8080):
        self.cluster = None
        self.working_dir = working_dir
        self.binary_path = binary_path
        self.http_port = http_port
        self.nemesis_process = None
        self.nemesis_lock = threading.Lock()
        self.http_thread = None

    def start(self):
        """Starts local YDB cluster with MIRROR_3DC configuration on default ports."""
        logger.info("Initializing local YDB cluster with MIRROR_3DC configuration...")

        if not self.binary_path:
            raise RuntimeError("binary_path must be set before calling start()")

        logger.info("Using ydbd binary: %s", self.binary_path)

        port_allocator = DefaultFirstNodePortAllocator()

        configurator = kikimr_config.KikimrConfigGenerator(
            erasure=Erasure.MIRROR_3_DC,
            port_allocator=port_allocator,
            output_path=self.working_dir,
            binary_paths=[self.binary_path],
        )

        self.cluster = kikimr_runner.KiKiMR(
            configurator=configurator,
            cluster_name='local_cluster'
        )

        logger.info("Starting cluster...")
        try:
            self.cluster.start()
        except Exception as e:
            error_msg = "Failed to start cluster.\n\n"
            error_msg += "\nOriginal error:\n"
            error_msg += str(e)
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e

        logger.info("Cluster started successfully!")
        logger.info("Total nodes: %d", len(self.cluster.nodes))
        for node_id, node in sorted(self.cluster.nodes.items()):
            logger.info(
                "Node %d: GRPC=%s, MON=%s, IC=%s, Endpoint=%s",
                node_id, node.grpc_port, node.mon_port, node.ic_port, node.endpoint
            )

        first_node = self.cluster.nodes[1]
        logger.info("First node working directory: %s", first_node.cwd)

        # Start HTTP server for nemesis control
        self._start_http_server()

        return self.cluster

    def stop(self):
        """Stops the cluster."""
        # Stop nemesis if running
        self._stop_nemesis()
        
        if self.cluster:
            logger.info("Stopping cluster...")
            self.cluster.stop()
            logger.info("Cluster stopped")
            self.cluster = None

    def wait(self):
        """Waits for cluster termination (until signal is received)."""
        try:
            while True:
                time.sleep(1)
                if self.cluster and self.cluster.nodes:
                    for node_id, node in self.cluster.nodes.items():
                        if node.daemon.process.poll() is not None:
                            logger.error("ydbd process for node %d terminated unexpectedly", node_id)
                            logger.error("Stopping cluster due to unexpected node termination")
                            self.stop()
                            return
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")

    def _start_http_server(self):
        """Starts HTTP server for nemesis control."""
        class NemesisHTTPHandler(BaseHTTPRequestHandler):
            def __init__(self, cluster_manager, *args, **kwargs):
                self.cluster_manager = cluster_manager
                super().__init__(*args, **kwargs)

            def do_GET(self):
                parsed_path = urlparse(self.path)
                if parsed_path.path == '/':
                    self._send_html_response(self.cluster_manager._get_html_interface())
                elif parsed_path.path == '/api/nemesis/status':
                    self._send_json_response(self.cluster_manager._api_nemesis_status())
                else:
                    self._send_error(404, "Not Found")

            def do_POST(self):
                parsed_path = urlparse(self.path)
                if parsed_path.path == '/api/nemesis/start':
                    self._send_json_response(self.cluster_manager._api_start_nemesis())
                elif parsed_path.path == '/api/nemesis/stop':
                    self._send_json_response(self.cluster_manager._api_stop_nemesis())
                elif parsed_path.path.startswith('/api/nemesis/trigger/'):
                    nemesis_type = parsed_path.path.split('/')[-1]
                    self._send_json_response(self.cluster_manager._api_trigger_nemesis(nemesis_type))
                else:
                    self._send_error(404, "Not Found")

            def _send_html_response(self, html):
                self.send_response(200)
                self.send_header('Content-type', 'text/html; charset=utf-8')
                self.end_headers()
                self.wfile.write(html.encode('utf-8'))

            def _send_json_response(self, data):
                response = json.dumps(data, ensure_ascii=False).encode('utf-8')
                self.send_response(200)
                self.send_header('Content-type', 'application/json; charset=utf-8')
                self.send_header('Content-Length', str(len(response)))
                self.end_headers()
                self.wfile.write(response)

            def _send_error(self, code, message):
                self.send_response(code)
                self.send_header('Content-type', 'text/plain; charset=utf-8')
                self.end_headers()
                self.wfile.write(message.encode('utf-8'))

            def log_message(self, format, *args):
                # Suppress default logging
                pass

        def create_handler(*args, **kwargs):
            return NemesisHTTPHandler(self, *args, **kwargs)

        def run_server():
            try:
                # Try IPv6 dual-stack first (:: listens on both IPv4 and IPv6)
                server = DualStackHTTPServer(('::', self.http_port), create_handler)
                logger.info("Starting HTTP server for nemesis control on port %d (IPv4 and IPv6, accessible from all interfaces)", self.http_port)
                server.serve_forever()
            except Exception as e:
                logger.error("Failed to start HTTP server: %s", e)

        self.http_thread = threading.Thread(target=run_server, daemon=True)
        self.http_thread.start()
        
        # Get local IP addresses for logging
        local_ipv4 = None
        local_ipv6 = None
        
        try:
            # Get IPv4 address
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            local_ipv4 = s.getsockname()[0]
            s.close()
        except Exception:
            pass
        
        try:
            # Get IPv6 address
            s = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
            s.connect(("2001:4860:4860::8888", 80))
            local_ipv6 = s.getsockname()[0]
            s.close()
        except Exception:
            pass
        
        logger.info("HTTP server started and accessible at:")
        logger.info("  - http://127.0.0.1:%d (localhost IPv4)", self.http_port)
        logger.info("  - http://[::1]:%d (localhost IPv6)", self.http_port)
        if local_ipv4:
            logger.info("  - http://%s:%d (local network IPv4)", local_ipv4, self.http_port)
        if local_ipv6:
            logger.info("  - http://[%s]:%d (local network IPv6)", local_ipv6, self.http_port)
        logger.info("  - http://0.0.0.0:%d or http://[::]:%d (all interfaces)", self.http_port, self.http_port)

    def _get_html_interface(self):
        """Returns HTML interface for nemesis control."""
        html = """
<!DOCTYPE html>
<html>
<head>
    <title>YDB Local Cluster - Nemesis Control</title>
    <meta charset="utf-8">
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {

        
            background: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        h1 {
            color: #333;
            border-bottom: 3px solid #4CAF50;
            padding-bottom: 10px;
        }
        .status {
            padding: 15px;
            margin: 20px 0;
            border-radius: 5px;
            font-weight: bold;
        }
        .status.running {
            background-color: #d4edda;
            color: #155724;
            border: 1px solid #c3e6cb;
        }
        .status.stopped {
            background-color: #f8d7da;
            color: #721c24;
            border: 1px solid #f5c6cb;
        }
        .button-group {
            margin: 20px 0;
        }
        button {
            padding: 12px 24px;
            margin: 5px;
            font-size: 16px;
            border: none;
            border-radius: 5px;
            cursor: pointer;
            transition: background-color 0.3s;
        }
        .btn-primary {
            background-color: #4CAF50;
            color: white;
        }
        .btn-primary:hover {
            background-color: #45a049;
        }
        .btn-danger {
            background-color: #f44336;
            color: white;
        }
        .btn-danger:hover {
            background-color: #da190b;
        }
        .btn-action {
            background-color: #2196F3;
            color: white;
        }
        .btn-action:hover {
            background-color: #0b7dda;
        }
        .nemesis-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
            gap: 15px;
            margin-top: 20px;
        }
        .nemesis-card {
            border: 1px solid #ddd;
            border-radius: 5px;
            padding: 15px;
            background-color: #f9f9f9;
        }
        .nemesis-card h3 {
            margin-top: 0;
            color: #333;
        }
        .nemesis-card p {
            color: #666;
            font-size: 14px;
        }
        .log {
            background-color: #1e1e1e;
            color: #d4d4d4;
            padding: 15px;
            border-radius: 5px;
            font-family: 'Courier New', monospace;
            font-size: 12px;
            max-height: 300px;
            overflow-y: auto;
            margin-top: 20px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🎯 YDB Local Cluster - Nemesis Control</h1>
        
        <div id="status" class="status stopped">Nemesis: Остановлен</div>
        
        <div class="button-group">
            <button class="btn-primary" onclick="startNemesis()">▶ Запустить Nemesis</button>
            <button class="btn-danger" onclick="stopNemesis()">⏹ Остановить Nemesis</button>
            <button class="btn-action" onclick="updateStatus()">🔄 Обновить статус</button>
        </div>

        <h2>Быстрое действие</h2>
        <div class="nemesis-grid">
            <div class="nemesis-card">
                <h3>Убить ноду</h3>
                <p>Немедленно останавливает случайную ноду кластера. Кластер должен автоматически перезапустить её.</p>
                <button class="btn-action" onclick="triggerNemesis('kill_node')">Запустить шатание</button>
            </div>
        </div>

        <div class="log" id="log"></div>
    </div>

    <script>
        function log(message) {
            const logDiv = document.getElementById('log');
            const time = new Date().toLocaleTimeString();
            logDiv.innerHTML += '[' + time + '] ' + message + '\\n';
            logDiv.scrollTop = logDiv.scrollHeight;
        }

        function updateStatus() {
            fetch('/api/nemesis/status')
                .then(response => response.json())
                .then(data => {
                    const statusDiv = document.getElementById('status');
                    if (data.running) {
                        statusDiv.className = 'status running';
                        statusDiv.textContent = 'Nemesis: Запущен';
                    } else {
                        statusDiv.className = 'status stopped';
                        statusDiv.textContent = 'Nemesis: Остановлен';
                    }
                    log('Статус обновлен: ' + (data.running ? 'Запущен' : 'Остановлен'));
                })
                .catch(error => {
                    log('Ошибка при получении статуса: ' + error);
                });
        }

        function startNemesis() {
            log('Запуск nemesis...');
            fetch('/api/nemesis/start', {method: 'POST'})
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        log('Nemesis успешно запущен');
                        updateStatus();
                    } else {
                        log('Ошибка при запуске nemesis: ' + (data.error || 'Неизвестная ошибка'));
                    }
                })
                .catch(error => {
                    log('Ошибка при запуске nemesis: ' + error);
                });
        }

        function stopNemesis() {
            log('Остановка nemesis...');
            fetch('/api/nemesis/stop', {method: 'POST'})
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        log('Nemesis успешно остановлен');
                        updateStatus();
                    } else {
                        log('Ошибка при остановке nemesis: ' + (data.error || 'Неизвестная ошибка'));
                    }
                })
                .catch(error => {
                    log('Ошибка при остановке nemesis: ' + error);
                });
        }

        function triggerNemesis(nemesisType) {
            log('Запуск шатания: ' + nemesisType);
            fetch('/api/nemesis/trigger/' + nemesisType, {method: 'POST'})
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        log('Шатание ' + nemesisType + ' успешно запущено');
                    } else {
                        log('Ошибка при запуске шатания: ' + (data.error || 'Неизвестная ошибка'));
                    }
                })
                .catch(error => {
                    log('Ошибка при запуске шатания: ' + error);
                });
        }

        // Auto-update status every 2 seconds
        setInterval(updateStatus, 2000);
        updateStatus();
    </script>
</body>
</html>
        """
        return html

    def _api_start_nemesis(self):
        """API endpoint to start nemesis."""
        with self.nemesis_lock:
            if self.nemesis_process is not None and self.nemesis_process.is_alive():
                return {'success': False, 'error': 'Nemesis уже запущен'}

            try:
                if not self.cluster:
                    return {'success': False, 'error': 'Кластер не запущен'}

                logger.info("Creating KillNodeNemesis for local cluster")
                # Используем только KillNodeNemesis - простую операцию, которая работает с локальным кластером
                # Создаем nemesis с интервалом 60-120 секунд между убийствами
                nemesis = KillNodeNemesis(self.cluster, schedule=(60, 120))
                
                self.nemesis_process = NemesisProcess([nemesis], initial_sleep=10)
                self.nemesis_process.start()
                logger.info("Nemesis process started (KillNodeNemesis)")
                return {'success': True}
            except Exception as e:
                logger.exception("Failed to start nemesis: %s", e)
                return {'success': False, 'error': str(e)}

    def _api_stop_nemesis(self):
        """API endpoint to stop nemesis."""
        with self.nemesis_lock:
            if self.nemesis_process is None or not self.nemesis_process.is_alive():
                return {'success': False, 'error': 'Nemesis не запущен'}

            try:
                self.nemesis_process.stop()
                self.nemesis_process = None
                logger.info("Nemesis process stopped")
                return {'success': True}
            except Exception as e:
                logger.exception("Failed to stop nemesis: %s", e)
                return {'success': False, 'error': str(e)}

    def _api_nemesis_status(self):
        """API endpoint to get nemesis status."""
        with self.nemesis_lock:
            running = self.nemesis_process is not None and self.nemesis_process.is_alive()
            return {'running': running}

    def _api_trigger_nemesis(self, nemesis_type):
        """API endpoint to trigger specific nemesis action."""
        if not self.cluster:
            return {'success': False, 'error': 'Кластер не запущен'}

        # Поддерживаем только kill_node для локального кластера
        if nemesis_type != 'kill_node':
            return {'success': False, 'error': f'Тип nemesis "{nemesis_type}" не поддерживается для локального кластера. Используйте "kill_node".'}

        try:
            # Создаем nemesis с schedule=0 для немедленного выполнения
            nemesis = KillNodeNemesis(self.cluster, schedule=0)
            nemesis.prepare_state()
            nemesis.inject_fault()
            
            logger.info("Triggered nemesis: kill_node")
            return {
                'success': True,
                'message': 'Нода успешно остановлена. Кластер должен автоматически перезапустить её.'
            }
        except Exception as e:
            logger.exception("Failed to trigger nemesis kill_node: %s", e)
            return {'success': False, 'error': str(e)}

    def _stop_nemesis(self):
        """Stops nemesis if running."""
        with self.nemesis_lock:
            if self.nemesis_process is not None and self.nemesis_process.is_alive():
                logger.info("Stopping nemesis...")
                try:
                    self.nemesis_process.stop()
                except Exception as e:
                    logger.error("Error stopping nemesis: %s", e)
                self.nemesis_process = None


def setup_signal_handlers(cluster):
    """Sets up signal handlers for graceful shutdown."""
    def signal_handler(signum, frame):
        logger.info("Received signal %s, stopping cluster...", signum)
        cluster.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def main():
    parser = argparse.ArgumentParser(
        description='Utility to start local YDB cluster (ydbd) with MIRROR_3DC configuration on default ports'
    )
    parser.add_argument(
        '--working-dir',
        type=str,
        default='.ydbd_working_dir',
        help='Working directory for cluster data storage (default: .ydbd_working_dir in current directory)'
    )
    parser.add_argument(
        '--binary-path',
        type=str,
        default=None,
        help='Path to ydbd binary (overrides --version if specified)'
    )
    parser.add_argument(
        '--version',
        type=str,
        default="main",
        help='Git ref/version to download from S3 (e.g., 25.3.1.21). Binary will be downloaded to working-dir. Ignored if --binary-path is specified'
    )
    parser.add_argument(
        '--http-port',
        type=int,
        default=8080,
        help='HTTP port for nemesis control interface (default: 8080)'
    )

    args = parser.parse_args()

    working_dir = os.path.abspath(args.working_dir)

    if args.binary_path is not None:
        binary_path = args.binary_path
        if not os.path.exists(binary_path):
            logger.error("Binary path does not exist: %s", binary_path)
            sys.exit(1)
        if not os.access(binary_path, os.X_OK):
            logger.error("Binary path is not executable: %s", binary_path)
            sys.exit(1)
    else:
        binary_path = download_binary_from_s3(args.version, working_dir)

    cluster_manager = LocalCluster(
        working_dir=working_dir,
        binary_path=binary_path,
        http_port=args.http_port
    )

    try:
        cluster_manager.start()
        setup_signal_handlers(cluster_manager)

        logger.info("Cluster is running. Press Ctrl+C to stop.")
        
        # Get local IP addresses for logging
        local_ipv4 = None
        local_ipv6 = None
        
        try:
            # Get IPv4 address
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            local_ipv4 = s.getsockname()[0]
            s.close()
        except Exception:
            pass
        
        try:
            # Get IPv6 address
            s = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
            s.connect(("2001:4860:4860::8888", 80))
            local_ipv6 = s.getsockname()[0]
            s.close()
        except Exception:
            pass
        
        logger.info("Nemesis control interface:")
        logger.info("  - http://127.0.0.1:%d (localhost IPv4)", args.http_port)
        logger.info("  - http://[::1]:%d (localhost IPv6)", args.http_port)
        if local_ipv4:
            logger.info("  - http://%s:%d (local network IPv4)", local_ipv4, args.http_port)
        if local_ipv6:
            logger.info("  - http://[%s]:%d (local network IPv6)", local_ipv6, args.http_port)
        cluster_manager.wait()

    except Exception as e:
        logger.exception("Error during cluster operation: %s", e)

        cluster_manager.stop()
        sys.exit(1)


if __name__ == '__main__':
    main()
