var JS_MIME_TYPE = 'application/javascript';
var HTML_MIME_TYPE = 'text/html';
var EXEC_MIME_TYPE = 'application/vnd.holoviews_exec.v0+json';
var CLASS_NAME = 'output';

/**
 * Render data to the DOM node
 */
function render(props, node) {
  var div = document.createElement("div");
  var script = document.createElement("script");
  node.appendChild(div);
  node.appendChild(script);
}

/**
 * Handle when a new output is added
 */
function handle_add_output(event, handle) {
  var output_area = handle.output_area;
  var output = handle.output;
  if ((output.data == undefined) || (!output.data.hasOwnProperty(EXEC_MIME_TYPE))) {
    return
  }
  var id = output.metadata[EXEC_MIME_TYPE]["id"];
  var toinsert = output_area.element.find("." + CLASS_NAME.split(' ')[0]);
  if (id !== undefined) {
    var nchildren = toinsert.length;
    var html_node = toinsert[nchildren-1].children[0];
    html_node.innerHTML = output.data[HTML_MIME_TYPE];
    var scripts = [];
    var nodelist = html_node.querySelectorAll("script");
    for (var i in nodelist) {
      if (nodelist.hasOwnProperty(i)) {
        scripts.push(nodelist[i])
      }
    }

    scripts.forEach( function (oldScript) {
      var newScript = document.createElement("script");
      var attrs = [];
      var nodemap = oldScript.attributes;
      for (var j in nodemap) {
        if (nodemap.hasOwnProperty(j)) {
          attrs.push(nodemap[j])
        }
      }
      attrs.forEach(function(attr) { newScript.setAttribute(attr.name, attr.value) });
      newScript.appendChild(document.createTextNode(oldScript.innerHTML));
      oldScript.parentNode.replaceChild(newScript, oldScript);
    });
    if (JS_MIME_TYPE in output.data) {
      toinsert[nchildren-1].children[1].textContent = output.data[JS_MIME_TYPE];
    }
    output_area._hv_plot_id = id;
    if ((window.Bokeh !== undefined) && (id in Bokeh.index)) {
      window.PyViz.plot_index[id] = Bokeh.index[id];
    } else {
      window.PyViz.plot_index[id] = null;
    }
  } else if (output.metadata[EXEC_MIME_TYPE]["server_id"] !== undefined) {
    var bk_div = document.createElement("div");
    bk_div.innerHTML = output.data[HTML_MIME_TYPE];
    var script_attrs = bk_div.children[0].attributes;
    for (var i = 0; i < script_attrs.length; i++) {
      toinsert[toinsert.length - 1].childNodes[1].setAttribute(script_attrs[i].name, script_attrs[i].value);
    }
    // store reference to server id on output_area
    output_area._bokeh_server_id = output.metadata[EXEC_MIME_TYPE]["server_id"];
  }
}

/**
 * Handle when an output is cleared or removed
 */
function handle_clear_output(event, handle) {
  var id = handle.cell.output_area._hv_plot_id;
  var server_id = handle.cell.output_area._bokeh_server_id;
  if (((id === undefined) || !(id in PyViz.plot_index)) && (server_id !== undefined)) { return; }
  var comm = window.PyViz.comm_manager.get_client_comm("hv-extension-comm", "hv-extension-comm", function () {});
  if (server_id !== null) {
    comm.send({event_type: 'server_delete', 'id': server_id});
    return;
  } else if (comm !== null) {
    comm.send({event_type: 'delete', 'id': id});
  }
  delete PyViz.plot_index[id];
  if ((window.Bokeh !== undefined) & (id in window.Bokeh.index)) {
    var doc = window.Bokeh.index[id].model.document
    doc.clear();
    const i = window.Bokeh.documents.indexOf(doc);
    if (i > -1) {
      window.Bokeh.documents.splice(i, 1);
    }
  }
}

/**
 * Handle kernel restart event
 */
function handle_kernel_cleanup(event, handle) {
  delete PyViz.comms["hv-extension-comm"];
  window.PyViz.plot_index = {}
}

/**
 * Handle update_display_data messages
 */
function handle_update_output(event, handle) {
  handle_clear_output(event, {cell: {output_area: handle.output_area}})
  handle_add_output(event, handle)
}

function register_renderer(events, OutputArea) {
  function append_mime(data, metadata, element) {
    // create a DOM node to render to
    var toinsert = this.create_output_subarea(
    metadata,
    CLASS_NAME,
    EXEC_MIME_TYPE
    );
    this.keyboard_manager.register_events(toinsert);
    // Render to node
    var props = {data: data, metadata: metadata[EXEC_MIME_TYPE]};
    render(props, toinsert[0]);
    element.append(toinsert);
    return toinsert
  }

  events.on('output_added.OutputArea', handle_add_output);
  events.on('output_updated.OutputArea', handle_update_output);
  events.on('clear_output.CodeCell', handle_clear_output);
  events.on('delete.Cell', handle_clear_output);
  events.on('kernel_ready.Kernel', handle_kernel_cleanup);

  OutputArea.prototype.register_mime_type(EXEC_MIME_TYPE, append_mime, {
    safe: true,
    index: 0
  });
}

if (window.Jupyter !== undefined) {
  try {
    var events = require('base/js/events');
    var OutputArea = require('notebook/js/outputarea').OutputArea;
    if (OutputArea.prototype.mime_types().indexOf(EXEC_MIME_TYPE) == -1) {
      register_renderer(events, OutputArea);
    }
  } catch(err) {
  }
}
