package encoding

import (
	"encoding/json"
	"fmt"
	goavro "github.com/linkedin/goavro/v2"
	"go.mongodb.org/mongo-driver/bson"
	"math"
	"time"
)

type MessageEncoder interface {
	Encode(interface{}) ([]byte, error)
}

type JSONMessageEncoder struct{}

func (jme *JSONMessageEncoder) Encode(val interface{}) ([]byte, error) {
	if m, ok := val.(map[string]interface{}); ok {
		val = ConvertMapForJSON(m)
	}
	b, err := json.Marshal(val)
	if err != nil {
		return nil, err
	}
	return b, nil
}

type JSONExtMessageEncoder struct{}

func (jme *JSONExtMessageEncoder) Encode(val interface{}) ([]byte, error) {
	b, err := bson.MarshalExtJSON(val, true, true)
	if err != nil {
		return nil, err
	}
	return b, nil
}

type AvroMessageEncoder struct {
	SchemaSpec string
	Binary     bool
}

func (ame *AvroMessageEncoder) Encode(val interface{}) ([]byte, error) {
	codec, err := goavro.NewCodec(ame.SchemaSpec)
	if err != nil {
		return nil, err
	}
	if ame.Binary {
		b, err := codec.BinaryFromNative(nil, val)
		if err != nil {
			return nil, err
		}
		return b, nil
	} else {
		b, err := codec.TextualFromNative(nil, val)
		if err != nil {
			return nil, err
		}
		return b, nil
	}
}

const timeJsonFormat = "2006-01-02T15:04:05.000Z07:00"

type jsonTime time.Time

type jsonFloat float64

func (jt jsonTime) MarshalJSON() ([]byte, error) {
	t := time.Time(jt)
	if y := t.Year(); y < 0 || y >= 10000 {
		return []byte("null"), nil
	}
	b := make([]byte, 0, len(timeJsonFormat)+2)
	b = append(b, '"')
	b = t.AppendFormat(b, timeJsonFormat)
	b = append(b, '"')
	return b, nil
}

func (jf jsonFloat) MarshalJSON() ([]byte, error) {
	f := float64(jf)
	if math.IsNaN(f) {
		return []byte(`"nan"`), nil
	} else if math.IsInf(f, 1) {
		return []byte(`"+inf"`), nil
	} else if math.IsInf(f, -1) {
		return []byte(`"-inf"`), nil
	} else {
		return []byte(fmt.Sprintf("%v", f)), nil
	}
}

func convertSliceForJSON(a []interface{}) []interface{} {
	var avs []interface{}
	for _, av := range a {
		var avc interface{}
		switch achild := av.(type) {
		case map[string]interface{}:
			avc = ConvertMapForJSON(achild)
		case []interface{}:
			avc = convertSliceForJSON(achild)
		case time.Time:
			avc = jsonTime(achild)
		case float64:
			avc = jsonFloat(achild)
		default:
			avc = av
		}
		avs = append(avs, avc)
	}
	return avs
}

func ConvertMapForJSON(m map[string]interface{}) map[string]interface{} {
	o := map[string]interface{}{}
	for k, v := range m {
		switch child := v.(type) {
		case map[string]interface{}:
			o[k] = ConvertMapForJSON(child)
		case []interface{}:
			o[k] = convertSliceForJSON(child)
		case time.Time:
			o[k] = jsonTime(child)
		case float64:
			o[k] = jsonFloat(child)
		default:
			o[k] = v
		}
	}
	return o
}
