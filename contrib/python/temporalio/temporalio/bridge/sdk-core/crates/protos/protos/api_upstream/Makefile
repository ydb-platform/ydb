SHELL=bash -o pipefail

$(VERBOSE).SILENT:
############################# Main targets #############################
ci-build: install proto http-api-docs

# Install dependencies.
install: grpc-install api-linter-install buf-install

# Run all linters and compile proto files.
proto: grpc http-api-docs nexus-rpc-yaml system-nexus-wit
########################################################################

##### Variables ######
ifndef GOPATH
GOPATH := $(shell go env GOPATH)
endif

GOBIN := $(if $(shell go env GOBIN),$(shell go env GOBIN),$(GOPATH)/bin)
CARGO_HOME ?= $(HOME)/.cargo
CARGO_BIN := $(CARGO_HOME)/bin
PATH := $(GOBIN):$(CARGO_BIN):$(PATH)
STAMPDIR := .stamp

COLOR := "\e[1;36m%s\e[0m\n"

PROTO_ROOT := .
PROTO_FILES = $(shell find temporal -name "*.proto")
PROTO_DIRS = $(sort $(dir $(PROTO_FILES)))
PROTO_OUT := .gen
PROTO_IMPORTS = \
	-I=$(PROTO_ROOT)
PROTO_PATHS = paths=source_relative:$(PROTO_OUT)

OAPI_OUT := openapi
OAPI3_PATH := .components.schemas.Payload

NEX_GEN ?= nex-gen

$(PROTO_OUT):
	mkdir $(PROTO_OUT)

##### Compile proto files for go #####
grpc: buf-lint api-linter buf-breaking clean go-grpc fix-path

go-grpc: clean $(PROTO_OUT)
	printf $(COLOR) "Compile for go-gRPC..."
	protogen \
		--root=$(PROTO_ROOT) \
		--output=$(PROTO_OUT) \
		--exclude=internal \
		--exclude=proto/api/google \
		-I $(PROTO_ROOT) \
		-p go-grpc_out=$(PROTO_PATHS) \
		-p grpc-gateway_out=allow_patch_feature=false,$(PROTO_PATHS) \
		-p doc_out=html,index.html,source_relative:$(PROTO_OUT)

fix-path:
	mv -f $(PROTO_OUT)/temporal/api/* $(PROTO_OUT) && rm -rf $(PROTO_OUT)/temporal

# We need to rewrite bits of this to support our shorthand payload format
# We use both yq and jq here as they preserve comments and the ordering of the original
# document
http-api-docs:
	protoc -I $(PROTO_ROOT) \
		--openapi_out=$(OAPI_OUT) \
		--openapi_opt=enum_type=string \
		--openapiv2_out=openapi \
        --openapiv2_opt=allow_merge=true,merge_file_name=openapiv2,simple_operation_ids=true \
		temporal/api/workflowservice/v1/* \
		temporal/api/operatorservice/v1/*

	jq --rawfile desc $(OAPI_OUT)/payload_description.txt < $(OAPI_OUT)/openapiv2.swagger.json '.definitions.v1Payload={description: $$desc}' > $(OAPI_OUT)/v2.tmp
	mv -f $(OAPI_OUT)/v2.tmp $(OAPI_OUT)/openapiv2.json
	rm -f $(OAPI_OUT)/openapiv2.swagger.json
	DESC=$$(cat $(OAPI_OUT)/payload_description.txt) yq e -i '$(OAPIV3_PATH).description = strenv(DESC) | del($(OAPI3_PATH).type) | del($(OAPI3_PATH).properties)' $(OAPI_OUT)/openapi.yaml
	yq e -i '(.paths[] | .[] | .operationId) |= sub("\w+_(.*)", "$$1")' $(OAPI_OUT)/openapi.yaml
	mv -f $(OAPI_OUT)/openapi.yaml $(OAPI_OUT)/openapiv3.yaml

##### Plugins & tools #####
grpc-install:
	@printf $(COLOR) "Install/update protoc and plugins..."
	@go install go.temporal.io/api/cmd/protogen@main
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest
	@go install github.com/pseudomuto/protoc-gen-doc/cmd/protoc-gen-doc@latest
	# v2.27.8 errors with --openapiv2_out: can't resolve OpenAPI name from ".temporal.api.protocol.v1.Message"
	@go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@v2.27.7
	@go install github.com/google/gnostic/cmd/protoc-gen-openapi@latest
	@go install github.com/mikefarah/yq/v4@latest

api-linter-install:
	printf $(COLOR) "Install/update api-linter..."
	go install github.com/googleapis/api-linter/cmd/api-linter@v1.32.3
	go install github.com/itchyny/gojq/cmd/gojq@v0.12.14

buf-install:
	printf $(COLOR) "Install/update buf..."
	go install github.com/bufbuild/buf/cmd/buf@v1.27.0

##### Sync external proto dependencies #####
sync-nexus-annotations:
	printf $(COLOR) "Sync nexusannotations from buf.build/temporalio/nexus-annotations..."
	buf export buf.build/temporalio/nexus-annotations --output .

##### Linters #####
api-linter:
	printf $(COLOR) "Run api-linter..."
	@api-linter --set-exit-status $(PROTO_IMPORTS) --config $(PROTO_ROOT)/api-linter.yaml --output-format json $(PROTO_FILES) | gojq -r 'map(select(.problems != []) | . as $$file | .problems[] | {rule: .rule_doc_uri, location: "\($$file.file_path):\(.location.start_position.line_number)"}) | group_by(.rule) | .[] | .[0].rule + ":\n" + (map("\t" + .location) | join("\n"))'

$(STAMPDIR):
	mkdir $@

$(STAMPDIR)/buf-mod-prune: $(STAMPDIR) buf.yaml
	printf $(COLOR) "Pruning buf module"
	buf mod prune
	touch $@

buf-lint: $(STAMPDIR)/buf-mod-prune
	printf $(COLOR) "Run buf linter..."
	(cd $(PROTO_ROOT) && buf lint)

buf-breaking:
	@printf $(COLOR) "Run buf breaking changes check against main branch..."
	@(cd $(PROTO_ROOT) && buf breaking --against 'https://github.com/temporalio/api.git#branch=main')

nexus-rpc-yaml: nexus-rpc-yaml-install
	printf $(COLOR) "Generate nexus/temporal-proto-models-nexusrpc.yaml..."
	mkdir -p nexus
	protoc -I $(PROTO_ROOT) \
		--nexus-rpc-yaml_opt=nexus-rpc_langs_out=nexus/temporal-proto-models-nexusrpc.yaml \
		--nexus-rpc-yaml_opt=python_package_prefix=temporalio.api \
		--nexus-rpc-yaml_opt=typescript_package_prefix=@temporalio/api \
		--nexus-rpc-yaml_opt=include_operation_tags=exposed \
		--nexus-rpc-yaml_out=. \
		temporal/api/workflowservice/v1/* \
		temporal/api/operatorservice/v1/*

nexus-rpc-yaml-install:
	printf $(COLOR) "Build and install protoc-gen-nexus-rpc-yaml..."
	@cd cmd/protoc-gen-nexus-rpc-yaml && go install .

##### Compile system Nexus WIT files #####
system-nexus-wit: system-nexus-wit-install nex-gen-install
	printf $(COLOR) "Generate system Nexus WIT..."
	protoc -I $(PROTO_ROOT) \
		--system-nexus-wit_opt=output=nexus/workflow-service.wit \
		--system-nexus-wit_opt=nex_gen=$(NEX_GEN) \
		--system-nexus-wit_opt=linked_input=nexus/deps \
		--system-nexus-wit_out=. \
		temporal/api/workflowservice/v1/service.proto

system-nexus-wit-install:
	printf $(COLOR) "Build and install protoc-gen-system-nexus-wit..."
	@cd cmd/protoc-gen-system-nexus-wit && go install .

nex-gen-install:
	printf $(COLOR) "Install nex-gen if missing..."
	command -v $(NEX_GEN) >/dev/null || cargo install nex-gen --version ^0.1

##### Clean #####
clean:
	printf $(COLOR) "Delete generated go files..."
	rm -rf $(PROTO_OUT) $(BUF_DEPS)
