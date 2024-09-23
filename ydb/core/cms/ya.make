LIBRARY()

SRCS(
    api_adapters.cpp
    audit_log.cpp
    base_handler.h
    cluster_info.cpp
    cluster_info.h
    cms.cpp
    cms.h
    cms_impl.h
    cms_state.h
    cms_tx_get_log_tail.cpp
    cms_tx_init_scheme.cpp
    cms_tx_load_state.cpp
    cms_tx_log_and_send.cpp
    cms_tx_log_cleanup.cpp
    cms_tx_process_notification.cpp
    cms_tx_reject_notification.cpp
    cms_tx_remove_expired_notifications.cpp
    cms_tx_remove_permissions.cpp
    cms_tx_remove_request.cpp
    cms_tx_remove_task.cpp
    cms_tx_store_permissions.cpp
    cms_tx_store_walle_task.cpp
    cms_tx_update_config.cpp
    cms_tx_update_downtimes.cpp
    defs.h
    downtime.h
    downtime.cpp
    erasure_checkers.h
    erasure_checkers.cpp
    http.cpp
    http.h
    info_collector.cpp
    info_collector.h
    json_proxy.h
    json_proxy_config_items.h
    json_proxy_config_updates.h
    json_proxy_config_validators.h
    json_proxy_console_log.h
    json_proxy_log.h
    json_proxy_operations.h
    json_proxy_proto.h
    json_proxy_sentinel.h
    json_proxy_toggle_config_validator.h
    logger.cpp
    logger.h
    node_checkers.cpp
    node_checkers.h
    log_formatter.h
    pdiskid.h
    scheme.h
    sentinel.cpp
    services.cpp
    walle.h
    walle_api_handler.cpp
    walle_check_task_adapter.cpp
    walle_create_task_adapter.cpp
    walle_list_tasks_adapter.cpp
    walle_remove_task_adapter.cpp
)

RESOURCE(
    ui/index.html cms/ui/index.html
    ui/cms.css cms/ui/cms.css
    ui/cms.js cms/ui/cms.js
    ui/config_dispatcher.css cms/ui/config_dispatcher.css
    ui/cms_log.js cms/ui/cms_log.js
    ui/console_log.js cms/ui/console_log.js
    ui/common.css cms/ui/common.css
    ui/common.js cms/ui/common.js
    ui/configs.js cms/ui/configs.js
    ui/yaml_config.js cms/ui/yaml_config.js
    ui/config_forms.js cms/ui/config_forms.js
    ui/datashard.css cms/ui/datashard.css
    ui/datashard.js cms/ui/datashard.js
    ui/datashard_hist.js cms/ui/datashard_hist.js
    ui/datashard_info.js cms/ui/datashard_info.js
    ui/datashard_op.js cms/ui/datashard_op.js
    ui/datashard_ops_list.js cms/ui/datashard_ops_list.js
    ui/datashard_rs.js cms/ui/datashard_rs.js
    ui/datashard_slow_ops.js cms/ui/datashard_slow_ops.js
    ui/enums.js cms/ui/enums.js
    ui/ext/myers-diff/myers_diff.js cms/ui/ext/myers-diff/myers_diff.js
    ui/ext/bootstrap.min.css cms/ui/ext/bootstrap.min.css
    ui/ext/fuzzycomplete.min.css cms/ui/ext/fuzzycomplete.min.css
    ui/ext/fuzzycomplete.min.js cms/ui/ext/fuzzycomplete.min.js
    ui/ext/fuse.min.js cms/ui/ext/fuse.min.js
    ui/ext/document-copy.svg cms/ui/ext/document-copy.svg
    ui/ext/unfold-less.svg cms/ui/ext/unfold-less.svg
    ui/ext/unfold-more.svg cms/ui/ext/unfold-more.svg
    ui/ext/1-circle.svg cms/ui/ext/1-circle.svg
    ui/ext/link.svg cms/ui/ext/link.svg
    ui/ext/gear.svg cms/ui/ext/gear.svg
    ui/ext/require.min.js cms/ui/ext/require.min.js
    ui/ext/jquery.min.js cms/ui/ext/jquery.min.js
    ui/ext/monaco-editor/vs/base/browser/ui/codicons/codicon/codicon.ttf cms/ui/ext/monaco-editor/vs/base/browser/ui/codicons/codicon/codicon.ttf
    ui/ext/monaco-editor/vs/base/worker/workerMain.js cms/ui/ext/monaco-editor/vs/base/worker/workerMain.js
    ui/ext/monaco-editor/vs/basic-languages/abap/abap.js cms/ui/ext/monaco-editor/vs/basic-languages/abap/abap.js
    ui/ext/monaco-editor/vs/basic-languages/apex/apex.js cms/ui/ext/monaco-editor/vs/basic-languages/apex/apex.js
    ui/ext/monaco-editor/vs/basic-languages/azcli/azcli.js cms/ui/ext/monaco-editor/vs/basic-languages/azcli/azcli.js
    ui/ext/monaco-editor/vs/basic-languages/bat/bat.js cms/ui/ext/monaco-editor/vs/basic-languages/bat/bat.js
    ui/ext/monaco-editor/vs/basic-languages/bicep/bicep.js cms/ui/ext/monaco-editor/vs/basic-languages/bicep/bicep.js
    ui/ext/monaco-editor/vs/basic-languages/cameligo/cameligo.js cms/ui/ext/monaco-editor/vs/basic-languages/cameligo/cameligo.js
    ui/ext/monaco-editor/vs/basic-languages/clojure/clojure.js cms/ui/ext/monaco-editor/vs/basic-languages/clojure/clojure.js
    ui/ext/monaco-editor/vs/basic-languages/coffee/coffee.js cms/ui/ext/monaco-editor/vs/basic-languages/coffee/coffee.js
    ui/ext/monaco-editor/vs/basic-languages/cpp/cpp.js cms/ui/ext/monaco-editor/vs/basic-languages/cpp/cpp.js
    ui/ext/monaco-editor/vs/basic-languages/csharp/csharp.js cms/ui/ext/monaco-editor/vs/basic-languages/csharp/csharp.js
    ui/ext/monaco-editor/vs/basic-languages/csp/csp.js cms/ui/ext/monaco-editor/vs/basic-languages/csp/csp.js
    ui/ext/monaco-editor/vs/basic-languages/css/css.js cms/ui/ext/monaco-editor/vs/basic-languages/css/css.js
    ui/ext/monaco-editor/vs/basic-languages/dart/dart.js cms/ui/ext/monaco-editor/vs/basic-languages/dart/dart.js
    ui/ext/monaco-editor/vs/basic-languages/dockerfile/dockerfile.js cms/ui/ext/monaco-editor/vs/basic-languages/dockerfile/dockerfile.js
    ui/ext/monaco-editor/vs/basic-languages/ecl/ecl.js cms/ui/ext/monaco-editor/vs/basic-languages/ecl/ecl.js
    ui/ext/monaco-editor/vs/basic-languages/elixir/elixir.js cms/ui/ext/monaco-editor/vs/basic-languages/elixir/elixir.js
    ui/ext/monaco-editor/vs/basic-languages/fsharp/fsharp.js cms/ui/ext/monaco-editor/vs/basic-languages/fsharp/fsharp.js
    ui/ext/monaco-editor/vs/basic-languages/go/go.js cms/ui/ext/monaco-editor/vs/basic-languages/go/go.js
    ui/ext/monaco-editor/vs/basic-languages/graphql/graphql.js cms/ui/ext/monaco-editor/vs/basic-languages/graphql/graphql.js
    ui/ext/monaco-editor/vs/basic-languages/handlebars/handlebars.js cms/ui/ext/monaco-editor/vs/basic-languages/handlebars/handlebars.js
    ui/ext/monaco-editor/vs/basic-languages/hcl/hcl.js cms/ui/ext/monaco-editor/vs/basic-languages/hcl/hcl.js
    ui/ext/monaco-editor/vs/basic-languages/html/html.js cms/ui/ext/monaco-editor/vs/basic-languages/html/html.js
    ui/ext/monaco-editor/vs/basic-languages/ini/ini.js cms/ui/ext/monaco-editor/vs/basic-languages/ini/ini.js
    ui/ext/monaco-editor/vs/basic-languages/java/java.js cms/ui/ext/monaco-editor/vs/basic-languages/java/java.js
    ui/ext/monaco-editor/vs/basic-languages/javascript/javascript.js cms/ui/ext/monaco-editor/vs/basic-languages/javascript/javascript.js
    ui/ext/monaco-editor/vs/basic-languages/julia/julia.js cms/ui/ext/monaco-editor/vs/basic-languages/julia/julia.js
    ui/ext/monaco-editor/vs/basic-languages/kotlin/kotlin.js cms/ui/ext/monaco-editor/vs/basic-languages/kotlin/kotlin.js
    ui/ext/monaco-editor/vs/basic-languages/less/less.js cms/ui/ext/monaco-editor/vs/basic-languages/less/less.js
    ui/ext/monaco-editor/vs/basic-languages/lexon/lexon.js cms/ui/ext/monaco-editor/vs/basic-languages/lexon/lexon.js
    ui/ext/monaco-editor/vs/basic-languages/liquid/liquid.js cms/ui/ext/monaco-editor/vs/basic-languages/liquid/liquid.js
    ui/ext/monaco-editor/vs/basic-languages/lua/lua.js cms/ui/ext/monaco-editor/vs/basic-languages/lua/lua.js
    ui/ext/monaco-editor/vs/basic-languages/m3/m3.js cms/ui/ext/monaco-editor/vs/basic-languages/m3/m3.js
    ui/ext/monaco-editor/vs/basic-languages/markdown/markdown.js cms/ui/ext/monaco-editor/vs/basic-languages/markdown/markdown.js
    ui/ext/monaco-editor/vs/basic-languages/mips/mips.js cms/ui/ext/monaco-editor/vs/basic-languages/mips/mips.js
    ui/ext/monaco-editor/vs/basic-languages/msdax/msdax.js cms/ui/ext/monaco-editor/vs/basic-languages/msdax/msdax.js
    ui/ext/monaco-editor/vs/basic-languages/mysql/mysql.js cms/ui/ext/monaco-editor/vs/basic-languages/mysql/mysql.js
    ui/ext/monaco-editor/vs/basic-languages/objective-c/objective-c.js cms/ui/ext/monaco-editor/vs/basic-languages/objective-c/objective-c.js
    ui/ext/monaco-editor/vs/basic-languages/pascal/pascal.js cms/ui/ext/monaco-editor/vs/basic-languages/pascal/pascal.js
    ui/ext/monaco-editor/vs/basic-languages/pascaligo/pascaligo.js cms/ui/ext/monaco-editor/vs/basic-languages/pascaligo/pascaligo.js
    ui/ext/monaco-editor/vs/basic-languages/perl/perl.js cms/ui/ext/monaco-editor/vs/basic-languages/perl/perl.js
    ui/ext/monaco-editor/vs/basic-languages/pgsql/pgsql.js cms/ui/ext/monaco-editor/vs/basic-languages/pgsql/pgsql.js
    ui/ext/monaco-editor/vs/basic-languages/php/php.js cms/ui/ext/monaco-editor/vs/basic-languages/php/php.js
    ui/ext/monaco-editor/vs/basic-languages/postiats/postiats.js cms/ui/ext/monaco-editor/vs/basic-languages/postiats/postiats.js
    ui/ext/monaco-editor/vs/basic-languages/powerquery/powerquery.js cms/ui/ext/monaco-editor/vs/basic-languages/powerquery/powerquery.js
    ui/ext/monaco-editor/vs/basic-languages/powershell/powershell.js cms/ui/ext/monaco-editor/vs/basic-languages/powershell/powershell.js
    ui/ext/monaco-editor/vs/basic-languages/pug/pug.js cms/ui/ext/monaco-editor/vs/basic-languages/pug/pug.js
    ui/ext/monaco-editor/vs/basic-languages/python/python.js cms/ui/ext/monaco-editor/vs/basic-languages/python/python.js
    ui/ext/monaco-editor/vs/basic-languages/qsharp/qsharp.js cms/ui/ext/monaco-editor/vs/basic-languages/qsharp/qsharp.js
    ui/ext/monaco-editor/vs/basic-languages/r/r.js cms/ui/ext/monaco-editor/vs/basic-languages/r/r.js
    ui/ext/monaco-editor/vs/basic-languages/razor/razor.js cms/ui/ext/monaco-editor/vs/basic-languages/razor/razor.js
    ui/ext/monaco-editor/vs/basic-languages/redis/redis.js cms/ui/ext/monaco-editor/vs/basic-languages/redis/redis.js
    ui/ext/monaco-editor/vs/basic-languages/redshift/redshift.js cms/ui/ext/monaco-editor/vs/basic-languages/redshift/redshift.js
    ui/ext/monaco-editor/vs/basic-languages/restructuredtext/restructuredtext.js cms/ui/ext/monaco-editor/vs/basic-languages/restructuredtext/restructuredtext.js
    ui/ext/monaco-editor/vs/basic-languages/ruby/ruby.js cms/ui/ext/monaco-editor/vs/basic-languages/ruby/ruby.js
    ui/ext/monaco-editor/vs/basic-languages/rust/rust.js cms/ui/ext/monaco-editor/vs/basic-languages/rust/rust.js
    ui/ext/monaco-editor/vs/basic-languages/sb/sb.js cms/ui/ext/monaco-editor/vs/basic-languages/sb/sb.js
    ui/ext/monaco-editor/vs/basic-languages/scala/scala.js cms/ui/ext/monaco-editor/vs/basic-languages/scala/scala.js
    ui/ext/monaco-editor/vs/basic-languages/scheme/scheme.js cms/ui/ext/monaco-editor/vs/basic-languages/scheme/scheme.js
    ui/ext/monaco-editor/vs/basic-languages/scss/scss.js cms/ui/ext/monaco-editor/vs/basic-languages/scss/scss.js
    ui/ext/monaco-editor/vs/basic-languages/shell/shell.js cms/ui/ext/monaco-editor/vs/basic-languages/shell/shell.js
    ui/ext/monaco-editor/vs/basic-languages/solidity/solidity.js cms/ui/ext/monaco-editor/vs/basic-languages/solidity/solidity.js
    ui/ext/monaco-editor/vs/basic-languages/sophia/sophia.js cms/ui/ext/monaco-editor/vs/basic-languages/sophia/sophia.js
    ui/ext/monaco-editor/vs/basic-languages/sparql/sparql.js cms/ui/ext/monaco-editor/vs/basic-languages/sparql/sparql.js
    ui/ext/monaco-editor/vs/basic-languages/sql/sql.js cms/ui/ext/monaco-editor/vs/basic-languages/sql/sql.js
    ui/ext/monaco-editor/vs/basic-languages/st/st.js cms/ui/ext/monaco-editor/vs/basic-languages/st/st.js
    ui/ext/monaco-editor/vs/basic-languages/swift/swift.js cms/ui/ext/monaco-editor/vs/basic-languages/swift/swift.js
    ui/ext/monaco-editor/vs/basic-languages/systemverilog/systemverilog.js cms/ui/ext/monaco-editor/vs/basic-languages/systemverilog/systemverilog.js
    ui/ext/monaco-editor/vs/basic-languages/tcl/tcl.js cms/ui/ext/monaco-editor/vs/basic-languages/tcl/tcl.js
    ui/ext/monaco-editor/vs/basic-languages/twig/twig.js cms/ui/ext/monaco-editor/vs/basic-languages/twig/twig.js
    ui/ext/monaco-editor/vs/basic-languages/typescript/typescript.js cms/ui/ext/monaco-editor/vs/basic-languages/typescript/typescript.js
    ui/ext/monaco-editor/vs/basic-languages/vb/vb.js cms/ui/ext/monaco-editor/vs/basic-languages/vb/vb.js
    ui/ext/monaco-editor/vs/basic-languages/xml/xml.js cms/ui/ext/monaco-editor/vs/basic-languages/xml/xml.js
    ui/ext/monaco-editor/vs/basic-languages/yaml/yaml.js cms/ui/ext/monaco-editor/vs/basic-languages/yaml/yaml.js
    ui/ext/monaco-editor/vs/editor/editor.main.css cms/ui/ext/monaco-editor/vs/editor/editor.main.css
    ui/ext/monaco-editor/vs/editor/editor.main.js cms/ui/ext/monaco-editor/vs/editor/editor.main.js
    ui/ext/monaco-editor/vs/editor/editor.main.nls.de.js cms/ui/ext/monaco-editor/vs/editor/editor.main.nls.de.js
    ui/ext/monaco-editor/vs/editor/editor.main.nls.es.js cms/ui/ext/monaco-editor/vs/editor/editor.main.nls.es.js
    ui/ext/monaco-editor/vs/editor/editor.main.nls.fr.js cms/ui/ext/monaco-editor/vs/editor/editor.main.nls.fr.js
    ui/ext/monaco-editor/vs/editor/editor.main.nls.it.js cms/ui/ext/monaco-editor/vs/editor/editor.main.nls.it.js
    ui/ext/monaco-editor/vs/editor/editor.main.nls.ja.js cms/ui/ext/monaco-editor/vs/editor/editor.main.nls.ja.js
    ui/ext/monaco-editor/vs/editor/editor.main.nls.js cms/ui/ext/monaco-editor/vs/editor/editor.main.nls.js
    ui/ext/monaco-editor/vs/editor/editor.main.nls.ko.js cms/ui/ext/monaco-editor/vs/editor/editor.main.nls.ko.js
    ui/ext/monaco-editor/vs/editor/editor.main.nls.ru.js cms/ui/ext/monaco-editor/vs/editor/editor.main.nls.ru.js
    ui/ext/monaco-editor/vs/editor/editor.main.nls.zh-cn.js cms/ui/ext/monaco-editor/vs/editor/editor.main.nls.zh-cn.js
    ui/ext/monaco-editor/vs/editor/editor.main.nls.zh-tw.js cms/ui/ext/monaco-editor/vs/editor/editor.main.nls.zh-tw.js
    ui/ext/monaco-editor/vs/language/css/cssMode.js cms/ui/ext/monaco-editor/vs/language/css/cssMode.js
    ui/ext/monaco-editor/vs/language/css/cssWorker.js cms/ui/ext/monaco-editor/vs/language/css/cssWorker.js
    ui/ext/monaco-editor/vs/language/html/htmlMode.js cms/ui/ext/monaco-editor/vs/language/html/htmlMode.js
    ui/ext/monaco-editor/vs/language/html/htmlWorker.js cms/ui/ext/monaco-editor/vs/language/html/htmlWorker.js
    ui/ext/monaco-editor/vs/language/json/jsonMode.js cms/ui/ext/monaco-editor/vs/language/json/jsonMode.js
    ui/ext/monaco-editor/vs/language/json/jsonWorker.js cms/ui/ext/monaco-editor/vs/language/json/jsonWorker.js
    ui/ext/monaco-editor/vs/language/typescript/tsMode.js cms/ui/ext/monaco-editor/vs/language/typescript/tsMode.js
    ui/ext/monaco-editor/vs/language/typescript/tsWorker.js cms/ui/ext/monaco-editor/vs/language/typescript/tsWorker.js
    ui/ext/monaco-editor/vs/loader.js cms/ui/ext/monaco-editor/vs/loader.js
    ui/main.js cms/ui/main.js
    ui/configs_dispatcher_main.js cms/ui/configs_dispatcher_main.js
    ui/ext/question-circle.svg cms/ui/ext/question-circle.svg
    ui/ext/bootstrap.bundle.min.js cms/ui/ext/bootstrap.bundle.min.js
    ui/ext/theme.blue.css cms/ui/ext/theme.blue.css
    ui/proto_types.js cms/ui/proto_types.js
    ui/res/edit.png cms/ui/res/edit.png
    ui/res/help.png cms/ui/res/help.png
    ui/res/remove.png cms/ui/res/remove.png
    ui/validators.js cms/ui/validators.js
    ui/sentinel_state.js cms/ui/sentinel_state.js
    ui/nanotable.js cms/ui/nanotable.js
    ui/sentinel.css cms/ui/sentinel.css
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/blobstorage
    ydb/core/blobstorage/base
    ydb/core/blobstorage/crypto
    ydb/core/engine/minikql
    ydb/core/mind
    ydb/core/mind/bscontroller
    ydb/core/node_whiteboard
    ydb/core/protos
    ydb/core/protos/out
    ydb/core/tablet_flat
    ydb/core/tx/datashard
    ydb/library/aclib
    ydb/library/services
)

GENERATE_ENUM_SERIALIZATION(services.h)
GENERATE_ENUM_SERIALIZATION(node_checkers.h)


END()

RECURSE(
    console
)

RECURSE_FOR_TESTS(
    ut
    ut_sentinel
    ut_sentinel_unstable
)
