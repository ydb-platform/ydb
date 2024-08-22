requirejs.config({
    baseUrl: 'js/lib',
    paths: {
        jquery: 'ext/jquery.min'
    },
    waitSeconds: 60 // Since editor is quite large
});

var createEditorPromise = new $.Deferred();

let run = () => {
  require.config({
    urlArgs: "v=0.27.0.2",
    paths: { vs: "../../cms/ext/monaco-editor/vs" }
  });

  require(["vs/editor/editor.main"], function () {
    createEditorPromise.resolve((container, readOnly, width) => {
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
    });

    $(document).ready(main);
  });
};

run();
