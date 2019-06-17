package decoding

import (
	"encoding/json"
	goavro "github.com/linkedin/goavro/v2"
	"go.mongodb.org/mongo-driver/bson"
)

type MessageDecoder interface {
	Decode([]byte) (interface{}, error)
}

type JSONMessageDecoder struct{}

func (jmd *JSONMessageDecoder) Decode(val []byte) (interface{}, error) {
	var err error
	doc := make(map[string]interface{})
	if err = json.Unmarshal(val, &doc); err != nil {
		return nil, err
	}
	return doc, nil
}

type JSONExtMessageDecoder struct{}

func (jmd *JSONExtMessageDecoder) Decode(val []byte) (interface{}, error) {
	var err error
	doc := make(map[string]interface{})
	if err = bson.UnmarshalExtJSON(val, true, &doc); err != nil {
		return nil, err
	}
	return doc, nil
}

type AvroMessageDecoder struct {
	SchemaSpec string
	Binary     bool
}

func (amd *AvroMessageDecoder) Decode(val []byte) (interface{}, error) {
	codec, err := goavro.NewCodec(amd.SchemaSpec)
	if err != nil {
		return nil, err
	}
	if amd.Binary {
		native, _, err := codec.NativeFromBinary(val)
		if err != nil {
			return nil, err
		}
		return native, nil
	} else {
		native, _, err := codec.NativeFromTextual(val)
		if err != nil {
			return nil, err
		}
		return native, nil
	}
}
