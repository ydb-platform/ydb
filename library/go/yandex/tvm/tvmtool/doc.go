// Pure Go implementation of tvm-interface based on TVMTool client.
//
// https://wiki.yandex-team.ru/passport/tvm2/tvm-daemon/.
// Package allows you to get service/user TVM-tickets, as well as check them.
// This package can provide fast getting of service tickets (from cache), other cases lead to http request to localhost.
// Also this package provides TVM client for Qloud (NewQloudClient) and Yandex.Deploy (NewDeployClient) environments.
package tvmtool
