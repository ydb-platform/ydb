package utils

import (
	"fmt"
	"io"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func DumpProtoMessageToJSON(msg proto.Message, stream io.Writer) error {
	opts := protojson.MarshalOptions{
		Indent: "    ",
	}

	data, err := opts.Marshal(msg)
	if err != nil {
		return fmt.Errorf("protojson marshal: %w", err)
	}

	if _, err := stream.Write(data); err != nil {
		return fmt.Errorf("stream write: %w", err)
	}

	return nil
}
