from yatest.common.network import PortManager
import yql_utils

port_manager = None


def get_yql_port(service='unknown'):
    global port_manager

    if port_manager is None:
        port_manager = PortManager()

    port = port_manager.get_port()
    yql_utils.log('get port for service %s: %d' % (service, port))
    return port


def release_yql_port(port):
    if port is None:
        return

    global port_manager
    port_manager.release_port(port)


def get_yql_port_range(service, count):
    global port_manager

    if port_manager is None:
        port_manager = PortManager()

    port = port_manager.get_port_range(None, count)
    yql_utils.log('get port range for service %s: start_port: %d, count: %d' % (service, port, count))
    return port


def release_yql_port_range(start_port, count):
    if start_port is None:
        return

    global port_manager
    for port in range(start_port, start_port + count):
        port_manager.release_port(port)
