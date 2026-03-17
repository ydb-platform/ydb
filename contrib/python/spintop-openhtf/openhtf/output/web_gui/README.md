# Spintop-OpenHTF web GUI client
This is a copy of the web gui in the openhtf repository, adapted for spintop.

## Development

First, start the dashboard server on the default port by running the following
from the project root directory:

```
python -m openhtf.output.servers.dashboard_server --no-launch-web-gui
```

Then start the Webpack watcher by running the following from the
`openhtf/output/web_gui` directory in another tab:

```
npm install
npm run start
```

You can then visit the app at http://localhost:8080/. The app should connect to
the dashboard server and should reload automatically when changes are made to
frontend files.

With the dashboard running, you can run OpenHTF tests separately, and they
should be picked up over multicast and appear on the dashboard.
