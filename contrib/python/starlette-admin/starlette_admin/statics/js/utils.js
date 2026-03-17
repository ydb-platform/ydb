function get_file_icon(mimeType) {
  mapping = {
    image: "fa-file-image",
    audio: "fa-file-audio",
    video: "fa-file-video",
    "application/pdf": "fa-file-pdf",
    "application/msword": "fa-file-word",
    "application/vnd.ms-word": "fa-file-word",
    "application/vnd.oasis.opendocument.text": "fa-file-word",
    "application/vnd.openxmlformatsfficedocument.wordprocessingml":
      "fa-file-word",
    "application/vnd.ms-excel": "fa-file-excel",
    "application/vnd.openxmlformatsfficedocument.spreadsheetml":
      "fa-file-excel",
    "application/vnd.oasis.opendocument.spreadsheet": "fa-file-excel",
    "application/vnd.ms-powerpoint": "fa-file-powerpoint",
    "application/vnd.openxmlformatsfficedocument.presentationml":
      "fa-file-powerpoint",
    "application/vnd.oasis.opendocument.presentation": "fa-file-powerpoint",
    "text/plain": "fa-file-text",
    "text/html": "fa-file-code",
    "text/csv": "fa-file-csv",
    "application/json": "fa-file-code",
    "application/gzip": "fa-file-archive",
    "application/zip": "fa-file-archive",
  };
  if (mimeType)
    for (var key in mapping) {
      if (mimeType.search(key) === 0) {
        return mapping[key];
      }
    }
  return "fa-file";
}

function pretty_print_json(data) {
  var jsonLine = /^( *)("[\w]+": )?("[^"]*"|[\w.+-]*)?([,[{])?$/gm;
  var replacer = function (match, pIndent, pKey, pVal, pEnd) {
    var key = '<span class="json-key" style="color: brown">',
      val = '<span class="json-value" style="color: navy">',
      str = '<span class="json-string" style="color: olive">',
      r = pIndent || "";
    if (pKey) r = r + key + pKey.replace(/[": ]/g, "") + "</span>: ";
    if (pVal) r = r + (pVal[0] == '"' ? str : val) + pVal + "</span>";
    return r + (pEnd || "");
  };

  return JSON.stringify(data, null, 3)
    .replace(/&/g, "&amp;")
    .replace(/\\"/g, "&quot;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(jsonLine, replacer);
}

if (typeof structuredClone === 'undefined') {
  // Simple (non-performant) replacement of `structuredClone` for old browsers
  structuredClone = function (value) {
    return JSON.parse(JSON.stringify(value));
  }
}
