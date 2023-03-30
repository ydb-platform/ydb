requirejs.config({
    baseUrl: 'js/lib',
    paths: {
        jquery: 'ext/jquery.min'
    }
});

var createEditor;

let run = () => {
  require.config({
    paths: { vs: "https://cdn.jsdelivr.net/npm/monaco-editor@0.27.0/min/vs" }
  });

  require(["vs/editor/editor.main"], function () {
    createEditor = (container, readOnly, width) => {
        var editor;
        container.style.border = '1px solid #eee';
        container.style.borderRadius = '8px';
        container.style.overflow = 'hidden';
        editor = monaco.editor.create(container, {
            language: "yaml",
            scrollBeyondLastLine: false,
            wrappingStrategy: 'advanced',
            minimap: {
                enabled: false,
            },
            overviewRulerLanes: 0,
            automaticLayout: true,
            readOnly: readOnly,
            scrollbar: {
                alwaysConsumeMouseWheel: false,
            },
        });
        let ignoreEvent = false;
        const updateHeight = () => {
            const contentHeight = editor.getContentHeight();
            container.style.width = `100%`;
            container.style.height = `${contentHeight + 2}px`;
            try {
                ignoreEvent = true;
                editor.layout({ width, height: contentHeight });
            } finally {
                ignoreEvent = false;
            }
        };
        editor.onDidContentSizeChange(updateHeight);
        updateHeight();
        return editor;
    }

    $(document).ready(main);
  });
};

run();
