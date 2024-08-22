var createEditorPromise = new $.Deferred();

var codeMirror;
var codeMirrorResolved;

function replaceWithEditor(selector) {
    var container = $(selector);
    var value = container.text();
    container.text("");
    var editorPromise = createEditorPromise.then(function(createEditor) {
        var editor = createEditor(container.get(0), true, 1068)
        editor.setValue(value);
        return editor;
    });
    return editorPromise;
}

function main() {
    $("#nodePicker").fuzzyComplete(nodeNames);
    $('#nodePicker').on('keyup blur', function() {
        if (window.location.pathname === "/actors/configs_dispatcher") {
            $('#nodesGo').attr("href", window.location.protocol + "//" + $(this).parent().find("select").val() + ":8765/actors/configs_dispatcher");
        } else {
            $('#nodesGo').attr("href", "/" + $(this).parent().find("select").val() + ":8765/actors/configs_dispatcher");
        }
    });

    replaceWithEditor("#yaml-config-item").then(function(codeMirror) {
        codeMirror.trigger('fold', 'editor.foldLevel2');
    
        $("#fold-yaml-config").click(function() {
            codeMirror.trigger('fold', 'editor.foldLevel2');
        });
    
        $("#unfold-yaml-config").click(function() {
            codeMirror.trigger('fold', 'editor.unfoldAll');
        });
    
        $("#copy-yaml-config").click(function() {
            copyToClipboard(codeMirror.getValue());
        });
    });

    replaceWithEditor("#resolved-yaml-config-item").then(function(codeMirrorResolved) {
        codeMirrorResolved.trigger('fold', 'editor.foldLevel1');
    
        $("#fold-resolved-yaml-config").click(function() {
            codeMirrorResolved.trigger('fold', 'editor.foldLevel1');
        });
    
        $("#unfold-resolved-yaml-config").click(function() {
            codeMirrorResolved.trigger('fold', 'editor.unfoldAll');
        });
    
        $("#copy-resolved-yaml-config").click(function() {
            copyToClipboard(codeMirrorResolved.getValue());
        });
    });

    $("#host-ref").text("YDB Developer UI - " + window.location.hostname);

    $(".yaml-config-item").each(function() {
        replaceWithEditor(this).then(function(editor) {
            $(this).parent().find('.fold-yaml-config').click(function() {
                editor.trigger('fold', 'editor.foldLevel2');
            });
    
            $(this).parent().find('.unfold-yaml-config').click(function() {
                editor.trigger('fold', 'editor.unfoldAll');
            });
    
            $(this).parent().find('.copy-yaml-config').click(function() {
                copyToClipboard(editor.getValue());
            });
        });
    });
}

let run = () => {
  require.config({
    urlArgs: "v=0.27.0.2",
    paths: { vs: "../cms/ext/monaco-editor/vs" },
    waitSeconds: 60 // Since editor is quite large
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
