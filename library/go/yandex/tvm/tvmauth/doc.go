// CGO implementation of tvm-interface based on ticket_parser2.
//
// Package allows you to get service/user TVM-tickets, as well as check them.
// This package provides client via tvm-api or tvmtool.
// Also this package provides the most efficient way for checking tickets regardless of the client construction way.
// All scenerios are provided without any request after construction.
//
// You should create client with NewAPIClient() or NewToolClient().
// Also you need to check status of client with GetStatus().
package tvmauth
