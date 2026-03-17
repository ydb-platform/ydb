function FileBrowserPopup(callback, value, type) {
    var fbURL = '/admin/filebrowser/browse/?pop=5';
    fbURL = fbURL + "&type=" + type.filetype;
    if(value)
        fbURL += '&input=';
    const instanceApi = tinyMCE.activeEditor.windowManager.openUrl({
        title: 'Filebrowser image/media/file picker',
        url: fbURL,
        width: 850,
        height: 500,
        onMessage: function(dialogApi, details) {
            callback(details.content);
            instanceApi.close();
        }
    });
    return false;
}

tinyMCE.init({
    // Initialise TinyMCE with using file_picker_callback to call FileBrowser
    // see https://www.tiny.cloud/docs/demo/basic-example/
    selector:'textarea',
    height: 500,
    menubar: false,
    plugins: [
        'advlist autolink lists link image charmap print preview anchor',
        'searchreplace visualblocks code fullscreen',
        'insertdatetime media table paste code help wordcount'
    ],
    toolbar: 'undo redo | formatselect | ' +
    'bold italic backcolor | alignleft aligncenter ' +
    'alignright alignjustify | bullist numlist outdent indent | ' +
    'removeformat | help',
    content_css: '//www.tiny.cloud/css/codepen.min.css',
    image_advtab: true,

    file_picker_callback: FileBrowserPopup,
});