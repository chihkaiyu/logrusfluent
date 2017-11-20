package logrusfluent

import (
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/fluent/fluent-logger-golang/fluent"
)

// FluentHook to send logs via fluentd.
type FluentHook struct {
	Fluent     *fluent.Fluent
	DefaultTag string
}

// NewFluentHook creates a new hook to send to fluentd.
func NewFluentHook(config fluent.Config) (*FluentHook, error) {
	logger, err := fluent.New(config)
	if err != nil {
		return nil, err
	}
	return &FluentHook{Fluent: logger, DefaultTag: "app"}, nil
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

	for k, v := range entry.Data {
		if k == "tag" {
			continue
		}
		switch v.(type) {
		// add prefix to separate logs for protecting elasticsearch
		case uint8, uint16, uint32, uint64, int8, int16, int32, int64, uint, int:
			data["i_"+k] = v
		case float32, float64:
			data["f_"+k] = v
		case string:
			data["s_"+k] = v
		case complex64, complex128:
			data["c_"+k] = v
		default:
			data["t_"+k] = fmt.Sprintf("%+v", v)
		}
	}
	data["msg"] = entry.Message
	data["level"] = entry.Level.String()

	return data
}
