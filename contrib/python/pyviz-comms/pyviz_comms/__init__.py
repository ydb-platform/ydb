import os
import pkgutil
import sys
import uuid
import traceback
import json
import builtins

try:
    from StringIO import StringIO
except:
    from io import StringIO

import param

from ._version import __version__

data_str = pkgutil.get_data('pyviz_comms', 'labextension/package.json')
data = json.loads(data_str)

def _jupyter_labextension_paths():
    return [{
        'src': 'labextension',
        'dest': data['name']
    }]

_in_ipython = hasattr(builtins, 'get_ipython')

# Setting this so we can check the launched jupyter has pyviz_comms installed
if not (_in_ipython and sys.argv[0].endswith('ipykernel_launcher.py')):
    os.environ['_PYVIZ_COMMS_INSTALLED'] = '1'

# nb_mime_js is used to enable the necessary mime type support in classic notebook
notebook_js_data = pkgutil.get_data('pyviz_comms', 'notebook.js')
nb_mime_js = nb_mime_js = '\n\n' + notebook_js_data.decode()


class extension(param.ParameterizedFunction):
    """
    Base class for pyviz extensions, which allow defining shared cleanup
    utilities.
    """

    # A registry of actions to perform when a delete event is received
    _delete_actions = []

    # A registry of actions to perform when a server delete event is received
    _server_delete_actions = []

    # Records the execution_count at each execution of an extension
    _last_execution_count = None
    _repeat_execution_in_cell = False

    def __new__(cls, *args, **kwargs):
        if _in_ipython:
            from IPython import get_ipython

            exec_count = get_ipython().execution_count  # noqa: F821
            extension._repeat_execution_in_cell = (exec_count == cls._last_execution_count)
            # Update the last count on this base class only so that every new instance
            # creation obtains the updated count.
            extension._last_execution_count = exec_count
        return param.ParameterizedFunction.__new__(cls, *args, **kwargs)

    @classmethod
    def add_delete_action(cls, action):
        cls._delete_actions.append(action)

    @classmethod
    def add_server_delete_action(cls, action):
        cls._server_delete_actions.append(action)

    @classmethod
    def _process_comm_msg(cls, msg):
        """
        Processes comm messages to handle global actions such as
        cleaning up plots.
        """
        event_type = msg['event_type']
        if event_type == 'delete':
            for action in cls._delete_actions:
                action(msg['id'])
        elif event_type == 'server_delete':
            for action in cls._server_delete_actions:
                action(msg['id'])


PYVIZ_PROXY = """
if ((window.PyViz === undefined) || (window.PyViz instanceof HTMLElement)) {
  window.PyViz = {comms: {}, comm_status:{}, kernels:{}, receivers: {}, plot_index: []}
}
"""

ABORT_JS = """
if (!window.PyViz) {{
  return;
}}
var events = [];
var receiver = window.PyViz.receivers['{plot_id}'];
if (receiver &&
        receiver._partial &&
        receiver._partial.content &&
        receiver._partial.content.events) {{
    events = receiver._partial.content.events;
}}

var value = cb_obj['{change}'];

{transform}

for (var event of events) {{
  if ((event.kind === 'ModelChanged') && (event.attr === '{change}') &&
      (cb_obj.id === event.model.id) &&
      (JSON.stringify(value) === JSON.stringify(event.new))) {{
    return;
  }}
}}
"""

# Following JS block becomes body of the message handler callback
bokeh_msg_handler = """
var plot_id = "{plot_id}";

if ((plot_id in window.PyViz.plot_index) && (window.PyViz.plot_index[plot_id] != null)) {{
  var plot = window.PyViz.plot_index[plot_id];
}} else if ((Bokeh !== undefined) && (plot_id in Bokeh.index)) {{
  var plot = Bokeh.index[plot_id];
}}

if (plot == null) {{
  return
}}

if (plot_id in window.PyViz.receivers) {{
  var receiver = window.PyViz.receivers[plot_id];
}} else {{
  var receiver = new Bokeh.protocol.Receiver();
  window.PyViz.receivers[plot_id] = receiver;
}}

if ((buffers != undefined) && (buffers.length > 0)) {{
  receiver.consume(buffers[0].buffer)
}} else {{
  receiver.consume(msg)
}}

const comm_msg = receiver.message;
if ((comm_msg != null) && (Object.keys(comm_msg.content).length > 0)) {{
  plot.model.document.apply_json_patch(comm_msg.content, comm_msg.buffers)
}}
"""

JS_CALLBACK = """
function unique_events(events) {{
  // Processes the event queue ignoring duplicate events
  // of the same type
  var unique = [];
  var unique_events = [];
  for (var i=0; i<events.length; i++) {{
    var _tmpevent = events[i];
    event = _tmpevent[0];
    data = _tmpevent[1];
    if (unique_events.indexOf(event)===-1) {{
      unique.unshift(data);
      unique_events.push(event);
      }}
  }}
  return unique;
}}

function process_events(comm_status) {{
  // Iterates over event queue and sends events via Comm
  var events = unique_events(comm_status.event_buffer);
  for (var i=0; i<events.length; i++) {{
    var data = events[i];
    var comm = window.PyViz.comms[data["comm_id"]];
    comm.send(data);
  }}
  comm_status.event_buffer = [];
}}

function on_msg(msg) {{
  // Receives acknowledgement from Python, processing event
  // and unblocking Comm if event queue empty
  var metadata = msg.metadata;
  var comm_id = metadata.comm_id
  var comm_status = window.PyViz.comm_status[comm_id];
  if (comm_status.event_buffer.length) {{
    process_events(comm_status);
    comm_status.blocked = true;
    comm_status.time = Date.now()+{debounce};
  }} else {{
    comm_status.blocked = false;
  }}
  comm_status.event_buffer = [];
  if ((metadata.msg_type == "Ready") && metadata.content) {{
    console.log("Python callback returned following output:", metadata.content);
  }} else if (metadata.msg_type == "Error") {{
    console.log("Python failed with the following traceback:", metadata.traceback)
  }}
}}

// Initialize Comm
if ((window.PyViz == undefined) || (window.PyViz.comm_manager == undefined)) {{ return }}
var comm = window.PyViz.comm_manager.get_client_comm("{plot_id}", "{comm_id}", on_msg);
if (!comm) {{
  return
}}

// Initialize event queue and timeouts for Comm
var comm_status = window.PyViz.comm_status["{comm_id}"];
if (comm_status === undefined) {{
  comm_status = {{event_buffer: [], blocked: false, time: Date.now()}}
  window.PyViz.comm_status["{comm_id}"] = comm_status
}}

// Add current event to queue and process queue if not blocked
var event_name = cb_obj.event_name;
if (event_name === undefined) {{
  // we are a widget not an event... fake a key.
  event_name = Object.keys(data).join(',');
}}
data['comm_id'] = "{comm_id}";
var timeout = comm_status.time + {timeout};
if ((comm_status.blocked && (Date.now() < timeout))) {{
  comm_status.event_buffer.unshift([event_name, data]);
}} else {{
  comm_status.event_buffer.unshift([event_name, data]);
  setTimeout(function() {{ process_events(comm_status); }}, {debounce});
  comm_status.blocked = true;
  comm_status.time = Date.now()+{debounce};
}}
"""


class StandardOutput(list):
    """
    Context manager to capture standard output for any code it
    is wrapping and make it available as a list, e.g.:

    >>> with StandardOutput() as stdout:
    ...   print('This gets captured')
    >>> print(stdout[0])
    This gets captured
    """

    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self

    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        sys.stdout = self._stdout


class Comm(param.Parameterized):
    """
    Comm encompasses any uni- or bi-directional connection between
    a python process and a frontend allowing passing of messages
    between the two. A Comms class must implement methods
    send data and handle received message events.

    If the Comm has to be set up on the frontend a template to
    handle the creation of the comms channel along with a message
    handler to process incoming messages must be supplied.

    The template must accept three arguments:

    * id          -  A unique id to register to register the comm under.
    * msg_handler -  JS code which has the msg variable in scope and
                     performs appropriate action for the supplied message.
    * init_frame  -  The initial frame to render on the frontend.
    """

    html_template = """
    <div id="fig_{plot_id}">
      {init_frame}
    </div>
    """

    id = param.String(doc="Unique identifier of this Comm instance")

    js_template = ''

    def __init__(self, id=None, on_msg=None, on_error=None, on_stdout=None, on_open=None):
        """
        Initializes a Comms object
        """
        self._on_msg = on_msg
        self._on_error = on_error
        self._on_stdout = on_stdout
        self._on_open = on_open
        self._comm = None
        super(Comm, self).__init__(id = id if id else uuid.uuid4().hex)

    def init(self, on_msg=None):
        """
        Initializes comms channel.
        """

    def close(self):
        """
        Closes the comm connection
        """

    def send(self, data=None, metadata=None, buffers=[]):
        """
        Sends data to the frontend
        """

    @classmethod
    def decode(cls, msg):
        """
        Decode incoming message, e.g. by parsing json.
        """
        return msg

    @property
    def comm(self):
        if not self._comm:
            raise ValueError('Comm has not been initialized')
        return self._comm

    def _handle_msg(self, msg):
        """
        Decode received message before passing it to on_msg callback
        if it has been defined.
        """
        comm_id = None
        try:
            stdout = []
            msg = self.decode(msg)
            comm_id = msg.pop('comm_id', None)
            if self._on_msg:
                # Comm swallows standard output so we need to capture
                # it and then send it to the frontend
                with StandardOutput() as stdout:
                    self._on_msg(msg)
                if stdout:
                    try:
                        self._on_stdout(stdout)
                    except:
                        pass
        except Exception as e:
            try:
                self._on_error(e)
            except:
                pass
            error = '\n'
            frames = traceback.extract_tb(sys.exc_info()[2])
            for frame in frames[-20:]:
                fname,lineno,fn,text = frame
                error_kwargs = dict(fn=fn, fname=fname, line=lineno)
                error += '{fname} {fn} L{line}\n'.format(**error_kwargs)
            error += '\t{type}: {error}'.format(type=type(e).__name__, error=str(e))
            if stdout:
                stdout = '\n\t'+'\n\t'.join(stdout)
                error = '\n'.join([stdout, error])
            reply = {'msg_type': "Error", 'traceback': error}
        else:
            stdout = '\n\t'+'\n\t'.join(stdout) if stdout else ''
            reply = {'msg_type': "Ready", 'content': stdout}

        # Returning the comm_id in an ACK message ensures that
        # the correct comms handle is unblocked
        if comm_id:
            reply['comm_id'] = comm_id
        self.send(metadata=reply)


class JupyterComm(Comm):
    """
    JupyterComm provides a Comm for the notebook which is initialized
    the first time data is pushed to the frontend.
    """

    js_template = """
    function msg_handler(msg) {{
      var metadata = msg.metadata;
      var buffers = msg.buffers;
      var msg = msg.content.data;
      if ((metadata.msg_type == "Ready")) {{
        if (metadata.content) {{
          console.log("Python callback returned following output:", metadata.content);
        }}
      }} else if (metadata.msg_type == "Error") {{
        console.log("Python failed with the following traceback:", metadata.traceback)
      }} else {{
        {msg_handler}
      }}
    }}
    if ((window.PyViz == undefined) || (!window.PyViz.comm_manager)) {{
      console.log("Could not find comm manager")
    }} else {{
      window.PyViz.comm_manager.register_target('{plot_id}', '{comm_id}', msg_handler);
    }}
    """

    def init(self):
        from ipykernel.comm import Comm as IPyComm
        self._comm = IPyComm(target_name=self.id, data={})
        self._comm.on_msg(self._handle_msg)
        if self._on_open:
            self._on_open({})

    @classmethod
    def decode(cls, msg):
        """
        Decodes messages following Jupyter messaging protocol.
        If JSON decoding fails data is assumed to be a regular string.
        """
        return msg['content']['data']

    def close(self):
        """
        Closes the comm connection
        """
        if self._comm:
            self._comm.close()

    def send(self, data=None, metadata=None, buffers=[]):
        """
        Pushes data across comm socket.
        """
        if not self._comm:
            self.init()
        self.comm.send(data, metadata=metadata, buffers=buffers)


class JupyterCommJS(JupyterComm):
    """
    JupyterCommJS provides a comms channel for the Jupyter notebook,
    which is initialized on the frontend. This allows sending events
    initiated on the frontend to python.
    """

    js_template = """
    <script>
      function msg_handler(msg) {{
        var msg = msg.content.data;
        var buffers = msg.buffers
        {msg_handler}
      }}
      var comm = window.PyViz.comm_manager.get_client_comm("{comm_id}");
      comm.on_msg(msg_handler);
    </script>
    """

    @classmethod
    def decode(cls, msg):
        decoded = dict(msg['content']['data'])
        if 'buffers' in msg:
            decoded['_buffers'] = {i: v for i, v in enumerate(msg['buffers'])}
        return decoded

    def __init__(self, id=None, on_msg=None, on_error=None, on_stdout=None, on_open=None):
        """
        Initializes a Comms object
        """
        from IPython import get_ipython
        super(JupyterCommJS, self).__init__(id, on_msg, on_error, on_stdout, on_open)
        self.manager = get_ipython().kernel.comm_manager
        self.manager.register_target(self.id, self._handle_open)

    def close(self):
        """
        Closes the comm connection
        """
        if self._comm:
            self._comm.close()
        else:
            if self.id in self.manager.targets:
                del self.manager.targets[self.id]
            else:
                raise AssertionError('JupyterCommJS %s is already closed' % self.id)

    def _handle_open(self, comm, msg):
        self._comm = comm
        self._comm.on_msg(self._handle_msg)
        if self._on_open:
            self._on_open(msg)

    def send(self, data=None, metadata=None, buffers=[]):
        """
        Pushes data across comm socket.
        """
        self.comm.send(data, metadata=metadata, buffers=buffers)



class CommManager(object):
    """
    The CommManager is an abstract baseclass for establishing
    websocket comms on the client and the server.
    """

    js_manager = """
    function CommManager() {
    }

    CommManager.prototype.register_target = function() {
    }

    CommManager.prototype.get_client_comm = function() {
    }

    window.PyViz.comm_manager = CommManager()
    """

    _comms = {}

    server_comm = Comm

    client_comm = Comm

    @classmethod
    def get_server_comm(cls, on_msg=None, id=None, on_error=None, on_stdout=None, on_open=None):
        comm = cls.server_comm(id, on_msg, on_error, on_stdout, on_open)
        cls._comms[comm.id] = comm
        return comm

    @classmethod
    def get_client_comm(cls, on_msg=None, id=None, on_error=None, on_stdout=None, on_open=None):
        comm = cls.client_comm(id, on_msg, on_error, on_stdout, on_open)
        cls._comms[comm.id] = comm
        return comm



class JupyterCommManager(CommManager):
    """
    The JupyterCommManager is used to establishing websocket comms on
    the client and the server via the Jupyter comms interface.

    There are two cases for both the register_target and get_client_comm
    methods: one to handle the classic notebook frontend and one to
    handle JupyterLab. The latter case uses the globally available PyViz
    object which is made available by each PyViz project requiring the
    use of comms. This object is handled in turn by the JupyterLab
    extension which keeps track of the kernels associated with each
    plot, ensuring the corresponding comms can be accessed.
    """

    js_manager = """
    function JupyterCommManager() {
    }

    JupyterCommManager.prototype.register_target = function(plot_id, comm_id, msg_handler) {
      if (window.comm_manager || ((window.Jupyter !== undefined) && (Jupyter.notebook.kernel != null))) {
        var comm_manager = window.comm_manager || Jupyter.notebook.kernel.comm_manager;
        comm_manager.register_target(comm_id, function(comm) {
          comm.on_msg(msg_handler);
        });
      } else if ((plot_id in window.PyViz.kernels) && (window.PyViz.kernels[plot_id])) {
        window.PyViz.kernels[plot_id].registerCommTarget(comm_id, function(comm) {
          comm.onMsg = msg_handler;
        });
      } else if (typeof google != 'undefined' && google.colab.kernel != null) {
        google.colab.kernel.comms.registerTarget(comm_id, (comm) => {
          var messages = comm.messages[Symbol.asyncIterator]();
          function processIteratorResult(result) {
            var message = result.value;
            var content = {data: message.data, comm_id};
            var buffers = []
            for (var buffer of message.buffers || []) {
              buffers.push(new DataView(buffer))
            }
            var metadata = message.metadata || {};
            var msg = {content, buffers, metadata}
            msg_handler(msg);
            return messages.next().then(processIteratorResult);
          }
          return messages.next().then(processIteratorResult);
        })
      }
    }

    JupyterCommManager.prototype.get_client_comm = function(plot_id, comm_id, msg_handler) {
      if (comm_id in window.PyViz.comms) {
        return window.PyViz.comms[comm_id];
      } else if (window.comm_manager || ((window.Jupyter !== undefined) && (Jupyter.notebook.kernel != null))) {
        var comm_manager = window.comm_manager || Jupyter.notebook.kernel.comm_manager;
        var comm = comm_manager.new_comm(comm_id, {}, {}, {}, comm_id);
        if (msg_handler) {
          comm.on_msg(msg_handler);
        }
      } else if ((plot_id in window.PyViz.kernels) && (window.PyViz.kernels[plot_id])) {
        var comm = window.PyViz.kernels[plot_id].connectToComm(comm_id);
        let retries = 0;
        const open = () => {
          if (comm.active) {
            comm.open();
          } else if (retries > 3) {
            console.warn('Comm target never activated')
          } else {
            retries += 1
            setTimeout(open, 500)
          }
        }
        if (comm.active) {
          comm.open();
        } else {
          setTimeout(open, 500)
        }
        if (msg_handler) {
          comm.onMsg = msg_handler;
        }
      } else if (typeof google != 'undefined' && google.colab.kernel != null) {
        var comm_promise = google.colab.kernel.comms.open(comm_id)
        comm_promise.then((comm) => {
          window.PyViz.comms[comm_id] = comm;
          if (msg_handler) {
            var messages = comm.messages[Symbol.asyncIterator]();
            function processIteratorResult(result) {
              var message = result.value;
              var content = {data: message.data};
              var metadata = message.metadata || {comm_id};
              var msg = {content, metadata}
              msg_handler(msg);
              return messages.next().then(processIteratorResult);
            }
            return messages.next().then(processIteratorResult);
          }
        })
        var sendClosure = (data, metadata, buffers, disposeOnDone) => {
          return comm_promise.then((comm) => {
            comm.send(data, metadata, buffers, disposeOnDone);
          });
        };
        var comm = {
          send: sendClosure
        };
      }
      window.PyViz.comms[comm_id] = comm;
      return comm;
    }
    window.PyViz.comm_manager = new JupyterCommManager();
    """

    server_comm = JupyterComm

    client_comm = JupyterCommJS
