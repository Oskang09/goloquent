package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"cloud.google.com/go/datastore"
	"github.com/Oskang09/goloquent"
)

// Connection :
func Connection(driver string) *goloquent.DB {
	driver = strings.TrimSpace(driver)
	paths := strings.SplitN(driver, ":", 2)
	if len(paths) != 2 {
		panic(fmt.Errorf("goloquent: invalid connection name %q", driver))
	}
	x, isOk := connPool.Load(driver)
	if !isOk {
		panic(fmt.Errorf("goloquent: connection not found"))
	}
	pool := x.(map[string]*goloquent.DB)
	for k, v := range pool {
		if k == paths[1] {
			return v
		}
		// return v
	}
	return nil
}

// Query :
func Query(ctx context.Context, stmt string, args ...interface{}) (*sql.Rows, error) {
	return defaultDB.Query(ctx, stmt, args...)
}

// Exec :
func Exec(ctx context.Context, stmt string, args ...interface{}) (sql.Result, error) {
	return defaultDB.Exec(ctx, stmt, args...)
}

// Table :
func Table(name string) *goloquent.Table {
	return defaultDB.Table(name)
}

// Migrate :
func Migrate(ctx context.Context, model ...interface{}) error {
	return defaultDB.Migrate(ctx, model...)
}

// Omit :
func Omit(fields ...string) goloquent.Replacer {
	return defaultDB.Omit(fields...)
}

// Create :
func Create(ctx context.Context, model interface{}, parentKey ...*datastore.Key) error {
	if parentKey == nil {
		return defaultDB.Create(ctx, model)
	}
	return defaultDB.Create(ctx, model, parentKey...)
}

// Upsert :
func Upsert(ctx context.Context, model interface{}, parentKey ...*datastore.Key) error {
	if parentKey == nil {
		return defaultDB.Upsert(ctx, model)
	}
	return defaultDB.Upsert(ctx, model, parentKey...)
}

// Delete :
func Delete(ctx context.Context, model interface{}) error {
	return defaultDB.Delete(ctx, model)
}

// Destroy :
func Destroy(ctx context.Context, model interface{}) error {
	return defaultDB.Destroy(ctx, model)
}

// Save :
func Save(ctx context.Context, model interface{}) error {
	return defaultDB.Save(ctx, model)
}

// Find :
func Find(ctx context.Context, key *datastore.Key, model interface{}) error {
	return defaultDB.Find(ctx, key, model)
}

// First :
func First(ctx context.Context, model interface{}) error {
	return defaultDB.First(ctx, model)
}

// Get :
func Get(ctx context.Context, model interface{}) error {
	return defaultDB.Get(ctx, model)
}

// Paginate :
func Paginate(ctx context.Context, p *goloquent.Pagination, model interface{}) error {
	return defaultDB.Paginate(ctx, p, model)
}

// NewQuery :
func NewQuery() *goloquent.Query {
	return defaultDB.NewQuery()
}

// Select :
func Select(fields ...string) *goloquent.Query {
	return defaultDB.Select(fields...)
}

// Ancestor :
func Ancestor(ancestor *datastore.Key) *goloquent.Query {
	return defaultDB.NewQuery().Ancestor(ancestor)
}

// AnyOfAncestor :
func AnyOfAncestor(ancestors ...*datastore.Key) *goloquent.Query {
	return defaultDB.NewQuery().AnyOfAncestor(ancestors...)
}

// Unscoped :
func Unscoped() *goloquent.Query {
	return defaultDB.NewQuery().Unscoped()
}

// DistinctOn :
func DistinctOn(fields ...string) *goloquent.Query {
	return defaultDB.NewQuery().DistinctOn(fields...)
}

// Where :
func Where(field string, operator string, value interface{}) *goloquent.Query {
	return defaultDB.Where(field, operator, value)
}

// WhereEqual :
func WhereEqual(field string, value interface{}) *goloquent.Query {
	return defaultDB.NewQuery().WhereEqual(field, value)
}

// WhereNotEqual :
func WhereNotEqual(field string, value interface{}) *goloquent.Query {
	return defaultDB.NewQuery().WhereNotEqual(field, value)
}

// WhereNull :
func WhereNull(field string) *goloquent.Query {
	return defaultDB.NewQuery().WhereNull(field)
}

// WhereNotNull :
func WhereNotNull(field string) *goloquent.Query {
	return defaultDB.NewQuery().WhereNotNull(field)
}

// WhereJSON :
func WhereJSON(field string, operator string, value interface{}) *goloquent.Query {
	return defaultDB.NewQuery().WhereJSON(field, operator, value)
}

// MatchAgainst :
func MatchAgainst(fields []string, value ...string) *goloquent.Query {
	return defaultDB.NewQuery().MatchAgainst(fields, value...)
}

// OrderBy :
func OrderBy(fields ...interface{}) *goloquent.Query {
	return defaultDB.NewQuery().OrderBy(fields...)
}

// Limit :
func Limit(limit int) *goloquent.Query {
	return defaultDB.NewQuery().Limit(limit)
}

// Offset :
func Offset(offset int) *goloquent.Query {
	return defaultDB.NewQuery().Offset(offset)
}

// RunInTransaction :
func RunInTransaction(cb goloquent.TransactionHandler) error {
	return defaultDB.RunInTransaction(cb)
}

// Truncate :
func Truncate(ctx context.Context, model ...interface{}) error {
	return defaultDB.Truncate(ctx, model...)
}
