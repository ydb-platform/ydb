$(VERBOSE).SILENT:
############################# Main targets #############################
ci-build: install proto

# Install dependencies.
install: buf-install grpc-install openapiv2-install

# Run all linters and compile proto files.
proto: grpc
########################################################################

##### Variables ######
ifndef GOPATH
GOPATH := $(shell go env GOPATH)
endif

GOBIN := $(if $(shell go env GOBIN),$(shell go env GOBIN),$(GOPATH)/bin)
SHELL := PATH=$(GOBIN):$(PATH) /bin/sh

COLOR := "\e[1;36m%s\e[0m\n"

PROTO_OUT := .gen
$(PROTO_OUT):
	mkdir $(PROTO_OUT)

##### Compile proto files for go #####
grpc: buf-lint buf-breaking go-grpc

go-grpc: clean $(PROTO_OUT)
	printf $(COLOR) "Compile for go-gRPC..."
	buf generate --output $(PROTO_OUT)

##### Plugins & tools #####
buf-install:
	printf $(COLOR) "Install/update buf..."
	go install github.com/bufbuild/buf/cmd/buf@v1.25.1

grpc-install:
	printf $(COLOR) "Install/update go and grpc protoc gen ..."
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.31
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3

openapiv2-install:
	printf $(COLOR) "Install/update openapiv2 protoc gen..."
	go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@v2.16.2

##### Linters #####
buf-lint:
	printf $(COLOR) "Run buf linter..."
	buf lint

buf-breaking:
	@printf $(COLOR) "Run buf breaking changes check against main branch..."
	buf breaking --against 'https://github.com/temporalio/api-cloud.git#branch=main'

##### Clean #####
clean:
	printf $(COLOR) "Delete generated go files..."
	rm -rf $(PROTO_OUT)
