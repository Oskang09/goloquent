package goloquent

import (
	"reflect"
	"regexp"
	"strings"
)

type tag struct {
	name    string
	options map[string]bool
	others  map[string]string
}

// TODO: Eager loading tag

func newTag(sf reflect.StructField) tag {
	name := sf.Name

	t := strings.TrimSpace(sf.Tag.Get("goloquent"))
	paths := strings.Split(t, ",")
	if strings.TrimSpace(paths[0]) != "" {
		name = paths[0]
	}

	options := map[string]bool{
		"omitempty": false,
		"flatten":   false,
		"unsigned":  false,
		"longtext":  false,
	}

	others := make(map[string]string)
	paths = paths[1:]
	for _, k := range paths {
		k = strings.ToLower(k)
		if _, isValid := options[k]; isValid {
			options[k] = true
		} else {
			rgx := regexp.MustCompile(`(datatype|charset|collate)\=.+`)
			if rgx.MatchString(k) {
				rgx = regexp.MustCompile(`(\w+)=(.+)`)
				result := rgx.FindStringSubmatch(k)
				others[result[1]] = result[2]
			}
		}
	}

	return tag{
		name:    name,
		options: options,
		others:  others,
	}
}

func (t tag) Get(k string) string {
	return t.others[k]
}

func (t tag) isPrimaryKey() bool {
	return t.name == keyFieldName
}

func (t tag) isSkip() bool {
	return t.name == "-"
}

func (t tag) isFlatten() bool {
	return t.options["flatten"]
}

func (t tag) isOmitEmpty() bool {
	return t.options["omitempty"]
}

func (t tag) isUnsigned() bool {
	return t.options["unsigned"]
}

func (t tag) isLongText() bool {
	return t.options["longtext"]
}
