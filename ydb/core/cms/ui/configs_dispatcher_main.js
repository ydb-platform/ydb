var createEditor;

var codeMirror;
var codeMirrorResolved;

function replaceWithEditor(selector) {
    var container = $(selector);
    var value = container.text();
    container.text("");
    var editor = createEditor(container.get(0), true, 1068)
    editor.setValue(value);
    return editor;
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

    codeMirror = replaceWithEditor("#yaml-config-item");
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

    codeMirrorResolved = replaceWithEditor("#resolved-yaml-config-item");
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

    $("#host-ref").text("YDB Developer UI - " + window.location.hostname);

    $(".yaml-config-item").each(function() {
        let editor = replaceWithEditor(this);

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
}

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
