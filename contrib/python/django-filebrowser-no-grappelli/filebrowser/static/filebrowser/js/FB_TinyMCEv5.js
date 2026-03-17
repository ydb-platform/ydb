var FileBrowserDialogue = {
    fileSubmit : function (FileURL) {
        window.parent.postMessage({
            mceAction: 'FileSelected',
            content: FileURL
        }, window);
    }
}