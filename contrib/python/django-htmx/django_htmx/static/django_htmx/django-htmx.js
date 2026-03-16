{
  const data = document.currentScript.dataset;
  const isDebug = data.debug === "True";

  if (isDebug) {
    document.addEventListener("htmx:beforeOnLoad", function (event) {
      const xhr = event.detail.xhr;
      if (xhr.status == 400 || xhr.status == 403 || xhr.status == 404 || xhr.status == 500 ) {
        // Tell htmx to stop processing this response
        event.stopPropagation();

        document.children[0].innerHTML = xhr.response;

        // Run inline scripts, which Django’s error pages use
        for (const script of document.scripts) {
          // (1, eval) wtf - see https://stackoverflow.com/questions/9107240/1-evalthis-vs-evalthis-in-javascript
          (1, eval)(script.innerText);
        }

        // Run window.onload function if defined, which Django’s error pages use
        if (typeof window.onload === "function") {
          window.onload();
        }
      }
    });
  }
}
