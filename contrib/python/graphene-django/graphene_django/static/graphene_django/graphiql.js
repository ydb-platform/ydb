(function (
  document,

  GRAPHENE_SETTINGS,
  GraphiQL,
  React,
  ReactDOM,
  graphqlWs,
  GraphiQLPluginExplorer,
  fetch,
  history,
  location,
) {

  // Collect the URL parameters
  var parameters = {};
  location.hash
    .substr(1)
    .split("&")
    .forEach(function (entry) {
      var eq = entry.indexOf("=");
      if (eq >= 0) {
        parameters[decodeURIComponent(entry.slice(0, eq))] = decodeURIComponent(
          entry.slice(eq + 1),
        );
      }
    });
  // Produce a Location fragment string from a parameter object.
  function locationQuery(params) {
    return (
      "#" +
      Object.keys(params)
        .map(function (key) {
          return (
            encodeURIComponent(key) + "=" + encodeURIComponent(params[key])
          );
        })
        .join("&")
    );
  }
  // Derive a fetch URL from the current URL, sans the GraphQL parameters.
  var graphqlParamNames = {
    query: true,
    variables: true,
    operationName: true,
  };
  var otherParams = {};
  for (var k in parameters) {
    if (parameters.hasOwnProperty(k) && graphqlParamNames[k] !== true) {
      otherParams[k] = parameters[k];
    }
  }

  var fetchURL = locationQuery(otherParams);

  // Derive the subscription URL. If the SUBSCRIPTION_URL setting is specified, uses that value. Otherwise
  // assumes the current window location with an appropriate websocket protocol.
  var subscribeURL =
    location.origin.replace(/^http/, "ws") +
    (GRAPHENE_SETTINGS.subscriptionPath || location.pathname);

  function trueLambda() { return true; };

  var headers = {};
  var cookies = ("; " + document.cookie).split("; csrftoken=");
  if (cookies.length == 2) {
    csrftoken = cookies.pop().split(";").shift();
  } else {
    csrftoken = document.querySelector("[name=csrfmiddlewaretoken]").value;
  }
  if (csrftoken) {
    headers['X-CSRFToken'] = csrftoken
  }

  var graphQLFetcher = GraphiQL.createFetcher({
    url: fetchURL,
    wsClient: graphqlWs.createClient({
      url: subscribeURL,
      shouldRetry: trueLambda,
      lazy: true,
    }),
    headers: headers
  })

  // When the query and variables string is edited, update the URL bar so
  // that it can be easily shared.
  function onEditQuery(newQuery) {
    parameters.query = newQuery;
    updateURL();
  }
  function onEditVariables(newVariables) {
    parameters.variables = newVariables;
    updateURL();
  }
  function onEditOperationName(newOperationName) {
    parameters.operationName = newOperationName;
    updateURL();
  }
  function updateURL() {
    history.replaceState(null, null, locationQuery(parameters));
  }

  function GraphiQLWithExplorer() {
    var [query, setQuery] = React.useState(parameters.query);

    function handleQuery(query) {
      setQuery(query);
      onEditQuery(query);
    }

    var explorerPlugin = GraphiQLPluginExplorer.useExplorerPlugin({
      query: query,
      onEdit: handleQuery,
    });

    var options = {
      fetcher: graphQLFetcher,
      plugins: [explorerPlugin],
      defaultEditorToolsVisibility: true,
      onEditQuery: handleQuery,
      onEditVariables: onEditVariables,
      onEditOperationName: onEditOperationName,
      isHeadersEditorEnabled: GRAPHENE_SETTINGS.graphiqlHeaderEditorEnabled,
      shouldPersistHeaders: GRAPHENE_SETTINGS.graphiqlShouldPersistHeaders,
      inputValueDeprecation: GRAPHENE_SETTINGS.graphiqlInputValueDeprecation,
      query: query,
    };
    if (parameters.variables) {
      options.variables = parameters.variables;
    }
    if (parameters.operation_name) {
      options.operationName = parameters.operation_name;
    }

    return React.createElement(GraphiQL, options);
  }

  // Render <GraphiQL /> into the body.
  ReactDOM.render(
    React.createElement(GraphiQLWithExplorer),
    document.getElementById("editor"),
  );
})(
  document,

  window.GRAPHENE_SETTINGS,
  window.GraphiQL,
  window.React,
  window.ReactDOM,
  window.graphqlWs,
  window.GraphiQLPluginExplorer,
  window.fetch,
  window.history,
  window.location,
);
