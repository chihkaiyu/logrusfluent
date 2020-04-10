package logrusfluent

import (
	"fmt"

	"github.com/chihkaiyu/logrus"
	"github.com/fluent/fluent-logger-golang/fluent"
)

const (
	// minFieldSizeLimit is the min value of fieldSizeLimit to avoid unexpected setting of FluentHook
	minFieldSizeLimit = 1024
)

// FluentHook to send logs via fluentd.
type FluentHook struct {
	Fluent         *fluent.Fluent
	DefaultTag     string
	fieldSizeLimit int
}

// NewFluentHook creates a new hook to send to fluentd.
func NewFluentHook(config fluent.Config, fieldSizeLimit int) (*FluentHook, error) {
	logger, err := fluent.New(config)
	if err != nil {
		return nil, err
	}
	if fieldSizeLimit < minFieldSizeLimit {
		return nil, fmt.Errorf("fieldSizeLimit:%d can't be smaller than '%d'", fieldSizeLimit, minFieldSizeLimit)
	}
	return &FluentHook{
		Fluent:         logger,
		DefaultTag:     "app",
		fieldSizeLimit: fieldSizeLimit,
	}, nil
}

// Fire implements logrus.Hook interface Fire method.
func (f *FluentHook) Fire(entry *logrus.Entry) error {
	msg := f.buildMessage(entry)
	tag := f.DefaultTag
	rawTag, ok := entry.Data["tag"]
	if ok {
		tag = fmt.Sprint(rawTag)
	}
	f.Fluent.Post(tag, msg)
	return nil
}

// Levels implements logrus.Hook interface Levels method.
func (f *FluentHook) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	}
}

func (f *FluentHook) buildMessage(entry *logrus.Entry) map[string]interface{} {
	data := make(map[string]interface{})
	for k, intf := range entry.Data {
		if k == "tag" {
			continue
		}
		switch v := intf.(type) {
		// add prefix to separate logs for protecting elasticsearch
		case uint8, uint16, uint32, uint64, int8, int16, int32, int64, uint, int:
			data["i_"+k] = v
		case float32, float64:
			data["f_"+k] = v
		case string:
			if len(v) > f.fieldSizeLimit {
				data["i_"+k+"_size"] = len(v)
				data["b_truncated"] = true
				v = v[:f.fieldSizeLimit]
			}
			data["s_"+k] = v
		case complex64, complex128:
			data["c_"+k] = v
		case bool:
			data["b_"+k] = v
		default:
			s := fmt.Sprintf("%+v", v)
			if len(s) > f.fieldSizeLimit {
				data["i_"+k+"_size"] = len(s)
				data["b_truncated"] = true
				s = s[:f.fieldSizeLimit]
			}
			data["t_"+k] = s
		}
	}
	data["msg"] = entry.Message
	data["level"] = entry.Level.String()

	return data
}
