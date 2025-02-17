window.onload = function() {
  window.ui = SwaggerUIBundle({
    url: "viewer.yaml",
    dom_id: '#swagger-ui',
    deepLinking: true,
    presets: [
      SwaggerUIBundle.presets.apis,
      SwaggerUIStandalonePreset
    ],
    plugins: [
      SwaggerUIBundle.plugins.DownloadUrl
    ],
    layout: "StandaloneLayout",
    validatorUrl: null,
    displayRequestDuration: true,
    docExpansion: "none"
  });
};
