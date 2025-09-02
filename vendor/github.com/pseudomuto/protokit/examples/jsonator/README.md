# jsonator

Quite possibly the most useless protoc-gen plugin out there.

This is just a demo to show you how to use `protokit`. All this does, is generate a single output file (`output.json`)
which contains the full name and description of each of the proto files to generate as well as any services and
associated methods.

**Running this example**

* `go generate && cat output.json`
* :rofl:

You'll see something like this (assuming you made no changes to the proto files here):

```json
[
  {
    "name":"com.jsonator.v1.sample.proto",
    "description":"This is just a sample proto for demonstrating how to use this library.\n\nThere's nothing \"fancy\" here.",
    "services":[
      {
        "name":"SampleService",
        "methods":[
          "RandomSample"
        ]
      }
    ]
  },
  {
    "name":"com.jsonator.v2.sample2.proto",
    "description":"This is just another sample proto for demonstrating how to use this library.\n\nThere's also nothing \"fancy\" here.",
    "services":[
      {
        "name":"SampleService",
        "methods":[
          "RandomSample"
        ]
      }
    ]
  }
]
```
