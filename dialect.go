package goloquent

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
)

// Dialect :
type Dialect interface {
	Open(c Config) (*sql.DB, error)
	SetDB(db Client)
	GetTable(ns string) string
	Version(ctx context.Context) (ver string)
	CurrentDB(ctx context.Context) (n string)
	Quote(n string) string
	Bind(i uint) string
	FilterJSON(f Filter) (s string, args []interface{}, err error)
	JSONMarshal(i interface{}) (b json.RawMessage)
	Value(v interface{}) string
	GetSchema(c Column) []Schema
	DataType(s Schema) string
	HasTable(ctx context.Context, tb string) bool
	HasIndex(ctx context.Context, tb, idx string) bool
	GetColumns(ctx context.Context, tb string) (cols []string)
	GetIndexes(ctx context.Context, tb string) (idxs []string)
	CreateTable(ctx context.Context, tb string, cols []Column) error
	AlterTable(ctx context.Context, tb string, cols []Column, unsafe bool) error
	OnConflictUpdate(tb string, cols []string) string
	UpdateWithLimit() bool
	ReplaceInto(ctx context.Context, src, dst string) error
}

var (
	dialects = make(map[string]Dialect)
)

// RegisterDialect :
func RegisterDialect(driver string, d Dialect) {
	dialects[driver] = d
}

// GetDialect :
func GetDialect(driver string) (d Dialect, isValid bool) {
	d, isValid = dialects[driver]
	if isValid {
		// Clone a new dialect
		d = reflect.New(reflect.TypeOf(d).Elem()).Interface().(Dialect)
	}
	return
}
