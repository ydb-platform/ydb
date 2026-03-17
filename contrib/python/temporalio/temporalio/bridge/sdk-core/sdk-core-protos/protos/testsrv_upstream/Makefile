$(VERBOSE).SILENT:
############################# Main targets #############################
ci-build: install proto

# Install dependencies.
install: grpc-install api-linter-install buf-install

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

PROTO_ROOT := .
PROTO_FILES = $(shell find $(PROTO_ROOT) -name "*.proto")
PROTO_DIRS = $(sort $(dir $(PROTO_FILES)))
PROTO_OUT := .gen
PROTO_IMPORTS := -I=$(PROTO_ROOT)

$(PROTO_OUT):
	mkdir $(PROTO_OUT)

##### Compile proto files for go #####
grpc: buf-lint api-linter buf-breaking fix-path

go-grpc: clean $(PROTO_OUT)
	printf $(COLOR) "Compile for go-gRPC..."
	$(foreach PROTO_DIR,$(PROTO_DIRS),protoc $(PROTO_IMPORTS) --go_out=plugins=grpc,paths=source_relative:$(PROTO_OUT) $(PROTO_DIR)*.proto;)

fix-path:
	mv -f $(PROTO_OUT)/temporal/api/* $(PROTO_OUT) && rm -rf $(PROTO_OUT)/temporal

##### Plugins & tools #####
grpc-install:
	printf $(COLOR) "Install/update gRPC plugins..."
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.1.0

go-protobuf-install:
	go install github.com/golang/protobuf/protoc-gen-go@v1.4.3

api-linter-install:
	printf $(COLOR) "Install/update api-linter..."
	go install github.com/googleapis/api-linter/cmd/api-linter@v1.22.0

buf-install:
	printf $(COLOR) "Install/update buf..."
	go install github.com/bufbuild/buf/cmd/buf@v0.43.2

##### Linters #####
api-linter:
	printf $(COLOR) "Run api-linter..."
	api-linter --set-exit-status $(PROTO_IMPORTS) --config $(PROTO_ROOT)/api-linter.yaml $(PROTO_FILES)

buf-lint:
	printf $(COLOR) "Run buf linter..."
	(cd $(PROTO_ROOT) && buf lint)

buf-breaking:
#	@printf $(COLOR) "Run buf breaking changes check against master branch..."
#	@(cd $(PROTO_ROOT) && buf breaking --against '../../../../.git#branch=master')

##### Clean #####
clean:
	printf $(COLOR) "Delete generated go files..."
	rm -rf $(PROTO_OUT)
