package goloquent

import (
	"context"

	"cloud.google.com/go/datastore"
)

// Table :
type Table struct {
	name string
	db   *DB
}

func (t *Table) newQuery() *Query {
	q := t.db.NewQuery()
	q.table = t.name
	return q
}

// Create :
func (t *Table) Create(ctx context.Context, model interface{}, parentKey ...*datastore.Key) error {
	return newBuilder(t.newQuery()).put(ctx, model, parentKey)
}

// Upsert :
func (t *Table) Upsert(ctx context.Context, model interface{}, parentKey ...*datastore.Key) error {
	return newBuilder(t.newQuery()).upsert(ctx, model, parentKey)
}

// Migrate :
func (t *Table) Migrate(ctx context.Context, model interface{}) error {
	return newBuilder(t.newQuery()).migrate(ctx, model)
}

// Exists :
func (t *Table) Exists(ctx context.Context) bool {
	return t.db.dialect.HasTable(ctx, t.name)
}

// DropIfExists :
func (t *Table) DropIfExists(ctx context.Context) error {
	return newBuilder(t.newQuery()).dropTableIfExists(ctx, t.name)
}

// Truncate :
func (t *Table) Truncate(ctx context.Context) error {
	return newBuilder(t.newQuery()).truncate(ctx, t.name)
}

// // Rename :
// func (t *Table) Rename(name string) error {
// 	return newBuilder(t.newQuery()).renameTable(t.name, name)
// }

// AddIndex :
func (t *Table) AddIndex(ctx context.Context, fields ...string) error {
	return newBuilder(t.newQuery()).addIndex(ctx, fields, bTreeIdx)
}

// AddUniqueIndex :
func (t *Table) AddUniqueIndex(ctx context.Context, fields ...string) error {
	return newBuilder(t.newQuery()).addIndex(ctx, fields, uniqueIdx)
}

// Select :
func (t *Table) Select(fields ...string) *Query {
	return t.newQuery().Select(fields...)
}

// DistinctOn :
func (t *Table) DistinctOn(fields ...string) *Query {
	return t.newQuery().DistinctOn(fields...)
}

// Omit :
func (t *Table) Omit(fields ...string) *Query {
	return t.newQuery().Omit(fields...)
}

// Unscoped :
func (t *Table) Unscoped() *Query {
	return t.newQuery().Unscoped()
}

// Find :
func (t *Table) Find(ctx context.Context, key *datastore.Key, model interface{}) error {
	return t.newQuery().Find(ctx, key, model)
}

// First :
func (t *Table) First(ctx context.Context, model interface{}) error {
	return t.newQuery().First(ctx, model)
}

// Get :
func (t *Table) Get(ctx context.Context, model interface{}) error {
	return t.newQuery().Get(ctx, model)
}

// Paginate :
func (t *Table) Paginate(ctx context.Context, p *Pagination, model interface{}) error {
	return t.newQuery().Paginate(ctx, p, model)
}

// AnyOfAncestor :
func (t *Table) AnyOfAncestor(ancestors ...*datastore.Key) *Query {
	return t.newQuery().AnyOfAncestor(ancestors...)
}

// Ancestor :
func (t *Table) Ancestor(ancestor *datastore.Key) *Query {
	return t.newQuery().Ancestor(ancestor)
}

// Where :
func (t *Table) Where(field, op string, value interface{}) *Query {
	return t.newQuery().Where(field, op, value)
}

// WhereEqual :
func (t *Table) WhereEqual(field string, v interface{}) *Query {
	return t.newQuery().WhereEqual(field, v)
}

// WhereNotEqual :
func (t *Table) WhereNotEqual(field string, v interface{}) *Query {
	return t.newQuery().WhereNotEqual(field, v)
}

// WhereNull :
func (t *Table) WhereNull(field string) *Query {
	return t.newQuery().WhereNull(field)
}

// WhereNotNull :
func (t *Table) WhereNotNull(field string) *Query {
	return t.newQuery().WhereNotNull(field)
}

// WhereIn :
func (t *Table) WhereIn(field string, v []interface{}) *Query {
	return t.newQuery().WhereIn(field, v)
}

// WhereNotIn :
func (t *Table) WhereNotIn(field string, v []interface{}) *Query {
	return t.newQuery().WhereNotIn(field, v)
}

// WhereLike :
func (t *Table) WhereLike(field, v string) *Query {
	return t.newQuery().WhereLike(field, v)
}

// WhereNotLike :
func (t *Table) WhereNotLike(field, v string) *Query {
	return t.newQuery().WhereNotLike(field, v)
}

// WhereJSONEqual :
func (t *Table) WhereJSONEqual(field string, v interface{}) *Query {
	return t.newQuery().WhereJSONEqual(field, v)
}

// Lock :
func (t *Table) Lock(mode locked) *Query {
	return t.newQuery().Lock(mode)
}

// WLock :
func (t *Table) WLock() *Query {
	return t.newQuery().WLock()
}

// RLock :
func (t *Table) RLock() *Query {
	return t.newQuery().RLock()
}

// OrderBy :
func (t *Table) OrderBy(fields ...interface{}) *Query {
	return t.newQuery().OrderBy(fields...)
}

// Limit :
func (t *Table) Limit(limit int) *Query {
	return t.newQuery().Limit(limit)
}

// Offset :
func (t *Table) Offset(offset int) *Query {
	return t.newQuery().Offset(offset)
}

// ReplaceInto :
func (t *Table) ReplaceInto(ctx context.Context, table string) error {
	return t.newQuery().ReplaceInto(ctx, table)
}

// InsertInto :
func (t *Table) InsertInto(ctx context.Context, table string) error {
	return t.newQuery().InsertInto(ctx, table)
}

// Update :
func (t *Table) Update(ctx context.Context, v interface{}) error {
	return t.newQuery().Update(ctx, v)
}

// Save :
func (t *Table) Save(ctx context.Context, model interface{}) error {
	return newBuilder(t.newQuery()).save(ctx, model)
}

// Scan :
func (t *Table) Scan(ctx context.Context, dest ...interface{}) error {
	return t.newQuery().Scan(ctx, dest...)
}
