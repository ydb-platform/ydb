"""Serves a list of stations found via multicast.

Run with:
    python -m openhtf.output.servers.dashboard_server
"""

import argparse
import collections
import json
import logging
import six
import socket
import threading
import time

import sockjs.tornado
import tornado.web

from openhtf.output.servers import station_server
from openhtf.output.servers import pub_sub
from openhtf.output.servers import web_gui_server
from openhtf.output.web_gui import web_launcher
from openhtf.util import data
from openhtf.util import multicast

_LOG = logging.getLogger(__name__)

DASHBOARD_SERVER_TYPE = 'dashboard'

StationInfo = collections.namedtuple(
    'StationInfo',
    'cell host port station_id status test_description test_name')


def _discover(discover_socket, **kwargs):
  """Yields info about station servers announcing themselves via multicast."""
  query = station_server.MULTICAST_QUERY
  for host, response in discover_socket.send(query, **kwargs):
    try:
      result = json.loads(response)
    except ValueError:
      _LOG.warn('Received bad JSON over multicast from %s: %s', host, response)
    try:
      yield StationInfo(result['cell'], host, result['port'],
                        result['station_id'], 'ONLINE',
                        result.get('test_description'),
                        result['test_name'])
    except KeyError:
      if 'last_activity_time_millis' in result:
        _LOG.debug('Received old station API response on multicast. Ignoring.')
      else:
        _LOG.warn('Received bad multicast response from %s: %s', host, response)


class StationListHandler(tornado.web.RequestHandler):
  """GET endpoint for the list of available stations.

  Sends the same message provided by DashboardPubSub.
  """

  def get(self):
    self.write(DashboardPubSub.make_message())


class DashboardPubSub(pub_sub.PubSub):
  """WebSocket endpoint for the list of available stations."""
  _lock = threading.Lock()  # Required by pub_sub.PubSub.
  subscribers = set()  # Required by pub_sub.PubSub.
  last_message = None
  station_map = {}
  station_map_lock = threading.Lock()

  def on_subscribe(self, unused_info):
    """Called by the base class when a client connects."""
    if self.last_message is not None:
      self.send(self.last_message)

  @classmethod
  def update_stations(cls, station_info_list):
    """Called by the station discovery loop to update the station map."""
    with cls.station_map_lock:

      # By default, assume old stations are unreachable.
      for host_port, station_info in six.iteritems(cls.station_map):
        cls.station_map[host_port] = station_info._replace(status='UNREACHABLE')

      for station_info in station_info_list:
        host_port = '%s:%s' % (station_info.host, station_info.port)
        cls.station_map[host_port] = station_info

  @classmethod
  def publish_if_new(cls):
    """If the station map has changed, publish the new information."""
    message = cls.make_message()
    if message != cls.last_message:
      super(DashboardPubSub, cls).publish(message)
      cls.last_message = message

  @classmethod
  def make_message(cls):
    with cls.station_map_lock:
      return data.convert_to_base_types(cls.station_map)


class DashboardServer(web_gui_server.WebGuiServer):
  """Serves a list of known stations and an Angular frontend."""

  def __init__(self, port, static_files_root=None):
    dash_router = sockjs.tornado.SockJSRouter(DashboardPubSub, '/sub/dashboard')
    routes = dash_router.urls + [
        ('/station_list', StationListHandler),
    ]
    super(DashboardServer, self).__init__(routes, port, static_files_root=static_files_root)

  def _get_config(self):
    return {
        'server_type': DASHBOARD_SERVER_TYPE,
    }

  def run(self):
    _LOG.info(
        'Starting dashboard server at:\n'
        '  Local: http://localhost:{port}\n'
        '  Remote: http://{host}:{port}'
        .format(host=socket.gethostname(), port=self.port))
    super(DashboardServer, self).run()

  def stop(self):
    _LOG.info('Stopping dashboard server.')
    super(DashboardServer, self).stop()


def main(dashboard_server_class=DashboardServer):
  logging.basicConfig(level=logging.INFO)

  parser = argparse.ArgumentParser(
      description='Serves web GUI for interacting with multiple OpenHTF '
                  'stations.')
  parser.add_argument('--discovery-interval-s', type=int, default=1,
                      help='Seconds between station discovery attempts.')
  parser.add_argument('--launch-web-gui', default=True, action="store_true",
                      help='Whether to automatically open web GUI.')
  parser.add_argument('--no-launch-web-gui', dest="launch_web_gui",
                      action="store_false",
                      help='Whether to automatically open web GUI.')
  parser.add_argument('--dashboard-server-port', type=int, default=12000,
                      help='Port on which to serve the dashboard server.')

  # These have default values in openhtf.util.multicast.py.
  parser.add_argument('--station-discovery-address', type=str)
  parser.add_argument('--station-discovery-port', type=int)
  parser.add_argument('--station-discovery-ttl', type=int)
  parser.add_argument(
      '--no-local-only',
      action='store_false',
      default=True,
      dest='station_discovery_local_only',
      help=('Whether to discover only local stations.'))

  args = parser.parse_args()

  with dashboard_server_class(args.dashboard_server_port) as server:

    if args.launch_web_gui:
      url = 'http://localhost:%s' % (server.port,)
      try:
        web_launcher.launch(url)
      except Exception:  # pylint: disable=broad-except
        _LOG.exception('Problem launching web gui')

    # Make kwargs from command line arguments.
    multicast_kwargs = {
        attr: getattr(args, 'station_discovery_%s' % attr)
        for attr in ('address', 'port', 'ttl', 'local_only')
        if getattr(args, 'station_discovery_%s' % attr) is not None
    }

    _LOG.info('Starting station discovery.')

    # Exit on CTRL+C.
    discover_socket = multicast.MulticastSendSocket()
    while True:
      stations = _discover(discover_socket, **multicast_kwargs)
      DashboardPubSub.update_stations(list(stations))
      DashboardPubSub.publish_if_new()
      time.sleep(args.discovery_interval_s)


if __name__ == '__main__':
  main()
