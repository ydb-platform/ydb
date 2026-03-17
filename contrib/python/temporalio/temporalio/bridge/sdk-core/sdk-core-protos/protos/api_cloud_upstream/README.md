# Temporal Cloud Operations API (Public Preview)

> aka the Cloud Ops API  
> These apis (proto files) are currently offered as a Public Preview. While they are production worthy, they are subject to change. Please reach out to Temporal Support if you have questions.

## How to use

To use the Cloud Ops API in your project, preform the following 4 steps:
1. Copy over the protobuf files under [temporal](temporal) directory to your desired project directory
2. Use [gRPC](https://grpc.io/docs/) to compile and generate code in your desired programming language, typically handled as a part of your code build process
3. Create a client connection in your code using a Temporal Cloud API Key (see [Samples](#samples) below)
4. Use the Cloud Operations API services to automate Cloud Operations, such as creating users or namespaces

### API Version

The client is expected to pass in a `temporal-cloud-api-version` header with the api version identifier with every request it makes to the apis. The backend will use the version to safely mutate resources. The `temporal:versioning:min_version` label specifies the minimum version of the API that supports the field.

Current Version `v0.7.1`

### URL

The grpc URL the clients should connect to:
```
saas-api.tmprl.cloud:443
```

## Samples

Refer to the [cloud-samples-go](https://github.com/temporalio/cloud-samples-go/blob/main/cmd/worker/README.md) sample repository for how to use the cloud ops api in Go.
> This sample demonstrates how to automate Temporal Cloud operations using Temporal Workflows that make Cloud Ops API requests within Workflow Activities ([Worker Sample README](https://github.com/temporalio/cloud-samples-go/tree/main/cmd/worker)).  
> See [here](https://github.com/temporalio/cloud-samples-go/blob/60d5cbca8696c87fb184efc56f5ae117561213d2/client/api/client.go#L16) for a quick reference showing you how to connect to Temporal Cloud with an API Key for the Cloud Ops API in Go.

Refer to the [temporal-cloud-api-client-typescript](https://github.com/steveandroulakis/temporal-cloud-api-client-typescript) sample repository for how to use the cloud ops api in Typescript.  
Refer to the [temporal-cloud-api-client-java](https://github.com/steveandroulakis/temporal-cloud-api-client-java) sample repository for how to use the cloud ops api in Java.  
Refer to the [temporal-cloud-api-client-kotlin](https://github.com/steveandroulakis/temporal-cloud-api-client-kotlin) sample repository for how to use the cloud ops api in Kotlin.
> The Java, Typescript, and Kotlin sample apps all provide a simple HTML UI that demonstrates how to use the Cloud Ops API to CRUD Namespaces and Users.
