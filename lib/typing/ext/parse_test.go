package ext

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestParseFromInterface(t *testing.T) {
	var vals []interface{}
	vals = append(vals, &ExtendedTime{
		Time: time.Now().UTC(),
		NestedKind: NestedKind{
			Type:   DateKindType,
			Format: PostgresDateFormat,
		},
	}, &ExtendedTime{
		Time: time.Now().UTC(),
		NestedKind: NestedKind{
			Type:   DateTimeKindType,
			Format: ISO8601,
		},
	}, &ExtendedTime{
		Time: time.Now().UTC(),
		NestedKind: NestedKind{
			Type:   TimeKindType,
			Format: PostgresTimeFormat,
		},
	})

	for _, val := range vals {
		extTime, err := ParseFromInterface(val)
		assert.NoError(t, err)
		assert.Equal(t, val, extTime)
	}

	invalidVals := []interface{}{
		nil,
		true,
		false,
	}
	for _, invalidVal := range invalidVals {
		_, err := ParseFromInterface(invalidVal)
		assert.Error(t, err)
	}
}

func TestParseFromInterfaceDateTime(t *testing.T) {
	now := time.Now().In(time.UTC)
	for _, supportedDateTimeLayout := range supportedDateTimeLayouts {
		et, err := ParseFromInterface(now.Format(supportedDateTimeLayout))
		assert.NoError(t, err)
		assert.Equal(t, et.NestedKind.Type, DateTimeKindType)

		// There's a known edge case between Ruby and Unix Date for single digit day formats and both layouts conform.
		var layout string
		if supportedDateTimeLayout == time.UnixDate || supportedDateTimeLayout == time.RubyDate {
			layout = supportedDateTimeLayout
		}

		// Without passing an override format, this should return the same preserved dt format.
		assert.Equal(t, et.String(layout), now.Format(supportedDateTimeLayout))
	}
}

func TestParseFromInterfaceTime(t *testing.T) {
	now := time.Now()
	for _, supportedTimeFormat := range supportedTimeFormats {
		et, err := ParseFromInterface(now.Format(supportedTimeFormat))
		assert.NoError(t, err)
		assert.Equal(t, et.NestedKind.Type, TimeKindType)
		// Without passing an override format, this should return the same preserved dt format.
		assert.Equal(t, et.String(""), now.Format(supportedTimeFormat))
	}
}

func TestParseFromInterfaceDate(t *testing.T) {
	now := time.Now()
	for _, supportedDateFormat := range supportedDateFormats {
		et, err := ParseFromInterface(now.Format(supportedDateFormat))
		assert.NoError(t, err)
		assert.Equal(t, et.NestedKind.Type, DateKindType)

		// Without passing an override format, this should return the same preserved dt format.
		assert.Equal(t, et.String(""), now.Format(supportedDateFormat))
	}
}
