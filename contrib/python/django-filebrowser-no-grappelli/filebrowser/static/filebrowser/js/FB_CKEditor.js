function ProtectPath(path) {
    path = path.replace( /\\/g,'\\\\');
    path = path.replace( /'/g,'\\\'');
    return path ;
}

function gup( name ) {
  name = name.replace(/[\[]/,"\\\[").replace(/[\]]/,"\\\]");
  var regexS = "[\\?&]"+name+"=([^&#]*)";
  var regex = new RegExp(regexS);
  var results = regex.exec(window.location.href);
  if(results == null)
    return "";
  else
    return results[1];
}

function OpenFile(fileUrl) {
    var CKEditorFuncNum = gup('CKEditorFuncNum');
    window.top.opener.CKEDITOR.tools.callFunction(CKEditorFuncNum, fileUrl);
    window.top.close();
    window.top.opener.focus();
}
