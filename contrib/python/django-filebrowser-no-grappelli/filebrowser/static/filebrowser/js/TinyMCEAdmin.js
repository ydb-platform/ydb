
function CustomFileBrowser(field_name, url, type, win) {
    
    var cmsURL = '/admin/filebrowser/browse/?pop=2';
    cmsURL = cmsURL + '&type=' + type;
    
    tinyMCE.activeEditor.windowManager.open({
        file: cmsURL,
        width: 980,  // Your dimensions may differ - toy around with them!
        height: 500,
        resizable: 'yes',
        scrollbars: 'yes',
        inline: 'no',  // This parameter only has an effect if you use the inlinepopups plugin!
        close_previous: 'no'
    }, {
        window: win,
        input: field_name,
        editor_id: tinyMCE.selectedInstance.editorId
    });
    return false;
}

tinyMCE.init({
    
    // see
    // http://wiki.moxiecode.com/index.php/TinyMCE:Configuration
    
    // Init
    mode: 'textareas',
    theme: 'advanced',
    skin: 'grappelli',
    
    // General
    //accessibility_warnings: false,
    browsers: 'gecko,msie,safari,opera',
    dialog_type: 'window',
    editor_deselector: 'mceNoEditor',
    keep_styles: false,
    language: 'en',
    object_resizing: false,
    media_strict: true,
    
    // Callbackss
    file_browser_callback: 'CustomFileBrowser',
    
    // Layout
    width: 758,
    height: 300,
    indentation: '10px',
    
    // Cleanup
    cleanup: true,
    cleanup_on_startup: true,
    element_format: 'xhtml',
    fix_list_elements: true,
    fix_table_elements: true,
    fix_nesting: true,
    forced_root_block : 'p',
    
    // URL
    relative_urls: false,
    remove_script_host: true,
    
    // Content CSS
    // content_css : "css/example.css",
    
    // Plugins
    plugins: 'advimage,advlink,fullscreen,paste,media,searchreplace,grappelli,grappelli_contextmenu,template',
    
    // Theme Advanced
    theme_advanced_toolbar_location: 'top',
    theme_advanced_toolbar_align: 'left',
    theme_advanced_statusbar_location: 'bottom',
    theme_advanced_buttons1: 'formatselect,styleselect,|,bold,italic,underline,|,bullist,numlist,blockquote,|,undo,redo,|,link,unlink,|,image,|,fullscreen,|,grappelli_adv',
    theme_advanced_buttons2: 'search,|,pasteword,template,media,charmap,|,code,|,table,cleanup,grappelli_documentstructure',
    theme_advanced_buttons3: '',
    theme_advanced_path: false,
    theme_advanced_blockformats: 'p,h2,h3,h4,pre',
    theme_advanced_resizing: true,
    theme_advanced_resize_horizontal: false,
    theme_advanced_resizing_use_cookie: true,
    theme_advanced_styles: 'Image Left=img_left;Image Right=img_right;Image Block=img_block',
    
    // Style formats
    // see http://wiki.moxiecode.com/index.php/TinyMCE:Configuration/style_formats
    style_formats : [
        {title : 'Paragraph Small', block : 'p', classes: 'p_small'},
        {title : 'Paragraph ImageCaption', block : 'p', classes: 'p_caption'},
        {title : 'Clearfix', block : 'p', classes: 'clearfix'},
        {title : 'Code', block : 'p', classes: 'code'}
    ],
    
    // Templates
    // see http://wiki.moxiecode.com/index.php/TinyMCE:Plugins/template
    // please note that you need to add the URLs (src) to your url-patterns
    // with django.views.generic.simple.direct_to_template
    template_templates : [
        {
            title : '2 Columns',
            src : '/path/to/your/template/',
            description : '2 Columns.'
        },
        {
            title : '4 Columns',
            src : '/path/to/your/template/',
            description : '4 Columns.'
        }
    ],
    
    // Adv
    advlink_styles: 'Internal Link=internal;External Link=external',
    advimage_update_dimensions_onchange: true,
    
    // Grappelli
    grappelli_adv_hidden: false,
    grappelli_show_documentstructure: 'on'
    
    // Elements
    // valid_elements: '@[id|class|style|title|dir<ltr?rtl|lang|xml::lang|onclick|ondblclick|'
    // + 'onmousedown|onmouseup|onmouseover|onmousemove|onmouseout|onkeypress|'
    // + 'onkeydown|onkeyup],a[rel|rev|charset|hreflang|tabindex|accesskey|type|'
    // + 'name|href|target|title|class|onfocus|onblur],strong/b,em/i,strike,u,'
    // + '#p,-ol[type|compact],-ul[type|compact],-li,br,img[longdesc|usemap|'
    // + 'src|border|alt=|title|hspace|vspace|width|height|align],-sub,-sup,'
    // + '-blockquote,-table[border=0|cellspacing|cellpadding|width|frame|rules|'
    // + 'height|align|summary|bgcolor|background|bordercolor],-tr[rowspan|width|'
    // + 'height|align|valign|bgcolor|background|bordercolor],tbody,thead,tfoot,'
    // + '#td[colspan|rowspan|width|height|align|valign|bgcolor|background|bordercolor'
    // + '|scope],#th[colspan|rowspan|width|height|align|valign|scope],caption,-div,'
    // + '-span,-code,-pre,address,-h1,-h2,-h3,-h4,-h5,-h6,hr[size|noshade],-font[face'
    // + '|size|color],dd,dl,dt,cite,abbr,acronym,del[datetime|cite],ins[datetime|cite],'
    // + 'object[classid|width|height|codebase|*],param[name|value|_value],embed[type|width'
    // + '|height|src|*],script[src|type],map[name],area[shape|coords|href|alt|target],bdo,'
    // + 'button,col[align|char|charoff|span|valign|width],colgroup[align|char|charoff|span|'
    // + 'valign|width],dfn,fieldset,form[action|accept|accept-charset|enctype|method],'
    // + 'input[accept|alt|checked|disabled|maxlength|name|readonly|size|src|type|value],'
    // + 'kbd,label[for],legend,noscript,optgroup[label|disabled],option[disabled|label|selected|value],'
    // + 'q[cite],samp,select[disabled|multiple|name|size],small,'
    // + 'textarea[cols|rows|disabled|name|readonly],tt,var,big',
    // extended_valid_elements : 'embed[width|height|name|flashvars|src|bgcolor|align|play|'
    // + 'loop|quality|allowscriptaccess|type|pluginspage]'
    
});

