from __future__ import annotations

import inspect
import sys

from jinja2 import Template

from .wrappers import Response

TEMPLATE = """
<style>
pre {
  margin: 0;
}

.traceback, .locals {
  display: table;
  width: 100%;
  margin: 5px;
}

.traceback>div, .locals>div {
  display: table-row;
}

.traceback>div>div, .locals>div>div {
  display: table-cell;
}

.locals>div>div {
  border-top: 1px solid lightgrey;
}

.header {
  background-color: #ececec;
  margin-bottom: 5px;
}

.highlight {
  background-color: #ececec;
}

.info {
  font-weight: bold;
}

li {
  border: 1px solid lightgrey;
  border-radius: 5px;
  padding: 5px;
  list-style-type: none;
  margin-bottom: 5px;
}

h1>span {
  font-weight: lighter;
}
</style>

<h1>{{ name }} <span>{{ value }}</span></h1>
<ul>
  {% for frame in frames %}
    <li>
      <div class="header">
        File <span class="info">{{ frame.file }}</span>,
        line <span class="info">{{ frame.line }}</span>, in
      </div>
      <div class="traceback">
        {% for line in frame.code[0] %}
          <div {% if frame.line == loop.index + frame.code[1] %}class="highlight"{% endif %}>
            <div>{{ loop.index + frame.code[1] }}</div>
            <div><pre>{{ line }}</pre></div>
          </div>
        {% endfor %}
      </div>
      <div class="locals">
        {% for name, repr in frame.locals.items() %}
          <div>
            <div>{{ name }}</div>
            <div>{{ repr }}</div>
          </div>
        {% endfor %}
      </div>
    </li>
  {% endfor %}
</ul>
"""


async def traceback_response() -> Response:
    type_, value, tb = sys.exc_info()
    frames = []
    while tb:
        frame = tb.tb_frame
        try:
            code = inspect.getsourcelines(frame)
        except OSError:
            code = None

        frames.append(
            {
                "file": inspect.getfile(frame),
                "line": frame.f_lineno,
                "locals": frame.f_locals,
                "code": code,
            }
        )
        tb = tb.tb_next

    name = type_.__name__
    template = Template(TEMPLATE)
    html = template.render(frames=reversed(frames), name=name, value=value)
    return Response(html, 500)
