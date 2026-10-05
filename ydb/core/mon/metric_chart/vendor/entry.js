import React from 'react';
import {createRoot} from 'react-dom/client';
import {flushSync} from 'react-dom';
import ChartKit, {settings} from '@gravity-ui/chartkit';
import {YagrPlugin} from '@gravity-ui/chartkit/yagr';
import {ThemeProvider} from '@gravity-ui/uikit';
import '@gravity-ui/uikit/styles/styles.css';

settings.set({plugins: [YagrPlugin]});

// Keep React and the chart engine behind the plain JavaScript embedding API.
export function mountChartKit(host, data, onChartLoad, onError) {
    const root = createRoot(host);
    flushSync(() => root.render(React.createElement(ThemeProvider, {theme: 'light'},
        React.createElement(ChartKit, {
            type: 'yagr', data,
            onChartLoad: ({widget}) => {if (widget) onChartLoad(widget);},
            onError,
        }))));
    return () => root.unmount();
}
