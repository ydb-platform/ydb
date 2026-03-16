/**
 * Created by Sune Kjærgård on 04/02/2016.
 * Originaly authored by Alan Hicks
 * http://p-o.co.uk/tech-articles/howto-use-tinymce-with-django-filebrowser-media-manager/
 */
var FileBrowserDialogue = {
    fileSubmit : function (FileURL) {
        parentWin = (!window.frameElement && window.dialogArguments) || opener || parent || top;
        tinymce = tinyMCE = parentWin.tinymce;
        self.editor = tinymce.EditorManager.activeEditor;
        self.params = self.editor.windowManager.getParams();
        parentWin.document.getElementById(self.params.input).value = FileURL;
        self.editor.windowManager.close(parentWin);
    },
}