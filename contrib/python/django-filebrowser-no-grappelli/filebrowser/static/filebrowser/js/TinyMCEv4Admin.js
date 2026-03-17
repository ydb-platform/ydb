/**
 * Created by sune on 04/02/2016.
 */
function CustomFileBrowser(input_id, input_value, type, win){
    var cmsURL = '/admin/filebrowser/browse/?pop=4';
    cmsURL = cmsURL + '&type=' + type;

    tinymce.activeEditor.windowManager.open({
        file: cmsURL,
        width: 800,  // Your dimensions may differ - toy around with them!
        height: 500,
        resizable: 'yes',
        scrollbars: 'yes',
        inline: 'yes',  // This parameter only has an effect if you use the inlinepopups plugin!
        close_previous: 'no'
    }, {
        window: win,
        input: input_id,
    });
    return false;
}

tinymce.init({
    mode: 'textareas',
    theme: "modern",
    width: 940,
    height: 755,
    //content_css: "example.css",
    plugins: [
        "advlist autolink lists link image charmap hr anchor pagebreak",
        "searchreplace wordcount visualblocks visualchars code fullscreen",
        "insertdatetime media nonbreaking save table contextmenu directionality",
        "paste textpattern imagetools"
    ],
    toolbar1: "insertfile undo redo | styleselect | bold italic | alignleft aligncenter alignright | bullist numlist | link image media",
    image_advtab: false,
    extended_valid_elements: 'iframe[src|title|width|height|allowfullscreen|frameborder|class|id],object[classid|width|height|codebase|*],param[name|value|_value|*],embed[type|width|height|src|*]"',
    file_browser_callback: CustomFileBrowser,
    convert_urls : false
});