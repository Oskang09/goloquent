package test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/Oskang09/goloquent"
	"github.com/Oskang09/goloquent/db"
	_ "github.com/go-sql-driver/mysql"
)

var (
	ctx = context.Background()
)

func TestMySQLConn(t *testing.T) {
	conn, err := db.Open(ctx, "mysql", db.Config{
		Username: "root",
		Password: "abcd1234",
		Database: "goloquent",
		Logger: func(ctx context.Context, stmt *goloquent.Stmt) {
			log.Println(fmt.Sprintf("[%.3fms] %s", stmt.TimeElapse().Seconds()*1000, stmt.String()))
		},
	})
	if err != nil {
		panic(err)
	}
	my = conn
}

func TestMySQLDropTableIfExists(t *testing.T) {
	if err := my.Table("User").DropIfExists(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLMigration(t *testing.T) {
	if err := my.Migrate(ctx, new(User), new(TempUser)); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLTableExists(t *testing.T) {
	if isExist := my.Table("User").Exists(ctx); isExist != true {
		t.Fatal(fmt.Errorf("Unexpected error, table %q should exists", "User"))
	}
}

func TestMySQLTruncate(t *testing.T) {
	if err := my.Truncate(ctx, new(User), TempUser{}); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLAddIndex(t *testing.T) {
	if err := my.Table("User").
		AddUniqueIndex(ctx, "Username"); err != nil {
		t.Fatal(err)
	}
	if err := my.Table("User").
		AddIndex(ctx, "Age"); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLEmptyInsertOrUpsert(t *testing.T) {
	var users []User
	if err := my.Create(ctx, &users); err != nil {
		t.Fatal(err)
	}

	if err := my.Upsert(ctx, &users); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLCreate(t *testing.T) {
	u := getFakeUser()
	if err := my.Create(ctx, u); err != nil {
		t.Fatal(err)
	}

	u = getFakeUser()
	if err := my.Create(ctx, u, nameKey); err != nil {
		t.Fatal(err)
	}

	u = getFakeUser()
	if err := my.Create(ctx, u, idKey); err != nil {
		t.Fatal(err)
	}

	uu := []User{*getFakeUser(), *getFakeUser()}
	if err := my.Create(ctx, &uu); err != nil {
		t.Fatal(err)
	}

	users := []*User{getFakeUser(), getFakeUser()}
	if err := my.Create(ctx, &users); err != nil {
		t.Fatal(err)
	}

	var i *User
	if err := my.Create(ctx, i); err == nil {
		t.Fatal(err)
	}

	users = []*User{nil, nil}
	if err := my.Create(ctx, &users); err == nil {
		t.Fatal(err)
	}

	users = []*User{getFakeUser(), getFakeUser()}
	if err := my.Create(ctx, &users, symbolKey); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLReplaceInto(t *testing.T) {
	if err := my.Table("User").
		AnyOfAncestor(nameKey, idKey).
		ReplaceInto(ctx, "TempUser"); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLInsertInto(t *testing.T) {
	if err := my.Table("ArchiveUser").
		Migrate(ctx, new(User)); err != nil {
		t.Fatal(err)
	}
	if err := my.Table("User").
		AnyOfAncestor(nameKey, idKey).
		InsertInto(ctx, "ArchiveUser"); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLSave(t *testing.T) {
	var u User
	if err := my.Save(ctx, u); err == nil {
		t.Fatal(errors.New("`Save` func must addressable"))
	}
	if err := my.Save(ctx, nil); err == nil {
		t.Fatal(errors.New("nil entity suppose not allow in `Save` func"))
	}

	if err := my.Create(ctx, &u); err != nil {
		t.Fatal(err)
	}
	u.Name = "Something"
	if err := my.Save(ctx, &u); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLSelect(t *testing.T) {
	u := new(User)
	if err := my.Select("*", "Name").First(ctx, u); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLSubQuery(t *testing.T) {
	users := new([]User)
	if err := my.Where("PrimaryEmail", "in",
		db.Table("TestUser").
			Select("Email").
			WhereNotNull("Email").
			WhereIn("Email", []string{
				"DgHlUKz@pYEXo.ru",
				"sianloong@hotmail.com",
			})).Get(ctx, users); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLDistinctOn(t *testing.T) {
	u := new(User)
	if err := my.NewQuery().
		DistinctOn("*").First(ctx, u); err == nil {
		t.Fatal("Expected `DistinctOn` cannot allow *")
	}

	if err := my.NewQuery().
		DistinctOn("").First(ctx, u); err == nil {
		t.Fatal("Expected `DistinctOn` cannot have empty")
	}

	if err := my.NewQuery().
		DistinctOn("Name", "Password").First(ctx, u); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLGet(t *testing.T) {
	type NewUser struct {
		User
		Arr    []string
		Struct struct {
			Name string
		}
		Data json.RawMessage
		Geo  datastore.GeoPoint
	}

	nu := new(NewUser)
	if err := my.Table("User").Migrate(ctx, nu); err != nil {
		t.Fatal(err)
	}

	// json null shouldn't run panic when retrieve out
	o := new(NewUser)
	if err := db.Table("User").First(ctx, o); err != nil {
		t.Fatal(err)
	}

	if o.Key == nil || len(o.Arr) != 0 {
		t.Fatal(errors.New("unexpected result"))
	}

	u := new(User)
	// restore back to original structure
	if err := my.Migrate(ctx, u); err != nil {
		t.Fatal(err)
	}
	if err := my.First(ctx, u); err != nil {
		t.Fatal(err)
	}

	if err := my.Find(ctx, u.Key, u); err != nil {
		t.Fatal(err)
	}

	users := new([]User)
	if err := my.Get(ctx, users); err != nil {
		t.Fatal(err)
	}

	if err := my.NewQuery().Unscoped().Get(ctx, users); err != nil {
		t.Fatal(err)
	}

	u2 := getFakeUser()
	u2.Key = symbolKey
	if err := my.Create(ctx, u2); err != nil {
		t.Fatal(err)
	}

	if err := my.Find(ctx, u2.Key, u2); err != nil {
		t.Fatal(err)
	}

	if err := my.Where("$Key", "=", u2.Key).First(ctx, u); err != nil {
		t.Fatal(err)
	}
	if u.Key == nil {
		t.Fatal("unexpected result")
	}
}

func TestMySQLAncestor(t *testing.T) {
	users := new([]User)
	if err := my.Ancestor(idKey).
		Get(ctx, users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal(`Unexpected result from filter "Ancestor" using id key`)
	}

	if err := my.Ancestor(nameKey).
		Get(ctx, users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal(`Unexpected result from filter "Ancestor" using name key`)
	}

	if err := my.AnyOfAncestor(idKey, nameKey).Get(ctx, users); err != nil {
		t.Fatal(err)
	}

	if err := my.Ancestor(symbolKey).Get(ctx, users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal(`Unexpected result from filter "Ancestor" using name key with symbol`)
	}
}

func TestMySQLWhereFilter(t *testing.T) {
	age := uint8(85)
	creditLimit := float64(100.015)
	dob, _ := time.Parse("2006-01-02", "1900-10-01")

	u := getFakeUser()
	u.Age = age
	u.Nickname = nil
	u.CreditLimit = creditLimit
	u.Birthdate = goloquent.Date(dob)

	my.Create(ctx, u)

	users := new([]User)
	if err := my.Where("Age", "=", &age).
		Get(ctx, users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal(`Unexpected result from filter using "Where"`)
	}

	if err := my.Where("Birthdate", "=", goloquent.Date(dob)).
		Get(ctx, users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal(`Unexpected result from filter using "Where"`)
	}

	var nilNickname *string
	if err := my.Where("Nickname", "=", nilNickname).
		Get(ctx, users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal(`Unexpected result from filter using "Where"`)
	}

	if err := my.Where("CreditLimit", "=", &creditLimit).
		Get(ctx, users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal(`Unexpected result from filter using "Where"`)
	}
}

func TestMySQLWhereAnyLike(t *testing.T) {
	users := new([]User)

	u := getFakeUser()
	u.PrimaryEmail = "sianloong@hotmail.com"
	if err := my.Create(ctx, u); err != nil {
		t.Fatal(err)
	}

	if err := my.NewQuery().
		WhereAnyLike("PrimaryEmail", []string{
			"lzPskFb@OOxzA.net",
			"sianloong%",
		}).Get(ctx, users); err != nil {
		t.Fatal(err)
	}

	if len(*users) <= 0 {
		t.Fatal(`Unexpected result from filter using "WhereAnyLike"`)
	}
}

func TestMySQLJSONRawMessage(t *testing.T) {
	u := getFakeUser()
	if err := my.Upsert(ctx, u); err != nil {
		t.Fatal(err)
	}
	u.Information = nil
	if err := my.Upsert(ctx, u); err != nil {
		t.Fatal(err)
	}
	u.Information = json.RawMessage(`[]`)
	if err := my.Upsert(ctx, u); err != nil {
		t.Fatal(err)
	}
	u.Information = json.RawMessage(`{}`)
	if err := my.Upsert(ctx, u); err != nil {
		t.Fatal(err)
	}
	u.Information = json.RawMessage(`null`)
	if err := my.Upsert(ctx, u); err != nil {
		t.Fatal(err)
	}
	u.Information = json.RawMessage(`notvalid`)
	if err := my.Upsert(ctx, u); err == nil {
		t.Fatal(err)
	}
}

func TestMySQLEmptySliceInJSON(t *testing.T) {
	u := new(User)
	if err := my.First(ctx, u); err != nil {
		t.Fatal(err)
	}
	if u.Emails == nil {
		t.Fatal(fmt.Errorf("empty slice should init on any `Get` func"))
	}

	u2 := getFakeUser()
	u2.Emails = nil
	u2.PrimaryEmail = "sianloong@hotmail.com"
	if err := my.Create(ctx, u2); err != nil {
		t.Fatal(err)
	}
	if u2.Emails == nil {
		t.Fatal(fmt.Errorf("empty slice should init on any `Create` func"))
	}
}

func TestMySQLJSONEqual(t *testing.T) {
	users := new([]User)
	if err := my.NewQuery().
		WhereJSONEqual("Address>PostCode", int32(85)).
		Get(ctx, users); err != nil {
		t.Fatal(err)
	}

	if err := my.NewQuery().
		WhereJSONEqual("Address>PostCode", uint32(85)).
		Get(ctx, users); err != nil {
		t.Fatal(err)
	}

	postCode := uint32(85)
	if err := my.NewQuery().
		WhereJSONEqual("Address>PostCode", &postCode).
		Get(ctx, users); err != nil {
		t.Fatal(err)
	}

	if err := my.NewQuery().
		WhereJSONEqual("Address>Line1", "7812, Jalan Section 22").
		Get(ctx, users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal("JSON equal has unexpected result")
	}

	var emptyStr string
	if err := my.NewQuery().
		WhereJSONEqual("Address>Line2", emptyStr).
		Get(ctx, users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal("JSON equal has unexpected result")
	}

	timeZone := new(time.Time)
	if err := my.NewQuery().
		WhereJSONEqual("Address>region.TimeZone", timeZone).
		Get(ctx, users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal("JSON equal has unexpected result")
	}
}

func TestMySQLJSONNotEqual(t *testing.T) {
	var timeZone *time.Time
	users := new([]User)
	if err := my.NewQuery().
		WhereJSONNotEqual("Address>region.TimeZone", timeZone).
		Get(ctx, users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal("JSON equal has unexpected result")
	}

	if err := my.NewQuery().
		WhereJSONNotEqual("Address>Country", "").
		Get(ctx, users); err != nil {
		t.Fatal(err)
	}
	if len(*users) > 0 {
		t.Fatal("JSON equal has unexpected result")
	}
}

func TestMySQLJSONIn(t *testing.T) {
	users := new([]User)
	if err := my.NewQuery().
		WhereJSONIn("Address>PostCode", []interface{}{0, 10, 20}).
		Get(ctx, users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal("JSON in has unexpected result")
	}
}

func TestMySQLJSONNotIn(t *testing.T) {
	users := new([]User)
	if err := my.NewQuery().
		WhereJSONNotIn("Address>Line1", []interface{}{"PJ", "KL", "Cheras"}).
		Get(ctx, users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal("JSON contain any has unexpected result")
	}
}

func TestMySQLJSONContainAny(t *testing.T) {
	users := new([]User)
	if err := my.NewQuery().
		WhereJSONContainAny("Emails", []Email{
			"support@hotmail.com",
			"invalid@gmail.com",
		}).Get(ctx, users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal("JSON contain any has unexpected result")
	}

	if err := my.NewQuery().
		WhereJSONContainAny("Emails", []Email{
			"invalid@gmail.com",
			"invalid@hotmail.com",
		}).Get(ctx, users); err != nil {
		t.Fatal(err)
	}
	if len(*users) > 0 {
		t.Fatal("JSON contain any has unexpected result")
	}
}

func TestMySQLJSONType(t *testing.T) {
	users := new([]User)
	if err := my.NewQuery().
		WhereJSONType("Address>region", "OBJECT").
		Get(ctx, users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal("JSON isObject has unexpected result")
	}
}

func TestMySQLJSONIsObject(t *testing.T) {
	users := new([]User)
	if err := my.NewQuery().
		WhereJSONIsObject("Address>region").
		Get(ctx, users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal("JSON isObject has unexpected result")
	}
}

func TestMySQLJSONIsArray(t *testing.T) {
	users := new([]User)
	if err := my.NewQuery().
		WhereJSONIsArray("Address>region.keys").
		Get(ctx, users); err != nil {
		t.Fatal(err)
	}
	if len(*users) <= 0 {
		t.Fatal("JSON isArray has unexpected result")
	}
}

func TestMySQLPaginate(t *testing.T) {
	users := new([]User)
	p := &goloquent.Pagination{
		Limit: 1,
	}
	if err := my.Paginate(ctx, p, users); err != nil {
		t.Fatal(err)
	}
	if len(*(users)) <= 0 {
		t.Fatal(fmt.Errorf("paginate record set shouldn't empty"))
	}

	p.Cursor = p.NextCursor()
	if err := my.Paginate(ctx, p, users); err != nil {
		t.Fatal(err)
	}
	if len(*(users)) <= 0 {
		t.Fatal(fmt.Errorf("paginate record set shouldn't empty"))
	}

	p2 := &goloquent.Pagination{
		Limit: 1,
	}
	if err := my.Ancestor(nameKey).
		Paginate(ctx, p2, users); err != nil {
		t.Fatal(err)
	}
	if len(*(users)) <= 0 {
		t.Fatal(fmt.Errorf("paginate record set shouldn't empty"))
	}

	p2.Cursor = p.NextCursor()
	if err := my.Paginate(ctx, p2, users); err != nil {
		t.Fatal(err)
	}
	if len(*(users)) <= 0 {
		t.Fatal(fmt.Errorf("paginate record set shouldn't empty"))
	}
}

func TestMySQLUpsert(t *testing.T) {
	u := getFakeUser()
	if err := my.Upsert(ctx, u); err != nil {
		t.Fatal(err)
	}

	u = getFakeUser()
	if err := my.Upsert(ctx, u, idKey); err != nil {
		t.Fatal(err)
	}

	u = getFakeUser()
	if err := my.Upsert(ctx, u, nameKey); err != nil {
		t.Fatal(err)
	}

	users := []*User{getFakeUser(), getFakeUser()}
	if err := my.Upsert(ctx, &users); err != nil {
		t.Fatal(err)
	}

	uu := []User{*getFakeUser(), *getFakeUser()}
	if err := my.Upsert(ctx, &uu); err != nil {
		t.Fatal(err)
	}

	uuu := []User{*getFakeUser(), *getFakeUser()}
	if err := my.Upsert(ctx, &uuu, idKey); err != nil {
		t.Fatal(err)
	}

	uuu = []User{*getFakeUser(), *getFakeUser()}
	if err := my.Upsert(ctx, &uuu, nameKey); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLUpdate(t *testing.T) {
	if err := my.Table("User").Limit(1).
		Where("Name", "=", "Dr. Antoinette Zboncak").
		Update(ctx, map[string]interface{}{
			"Name": "sianloong",
		}); err != nil {
		t.Fatal(err)
	}

	if err := my.Table("User").Limit(1).
		Update(ctx, map[string]interface{}{
			"Emails": []string{"abc@gmail.com", "abc@hotmail.com", "abc@yahoo.com"},
		}); err != nil {
		t.Fatal(err)
	}

	// TODO: support struct
	// if err := my.Table("User").Limit(1).
	// 	Update(map[string]interface{}{
	// 		"Address": Address{"", "Line2", "", 63000},
	// 	}); err != nil {
	// 	t.Fatal(err)
	// }
}
func TestMySQLSoftDelete(t *testing.T) {
	u := getFakeUser()
	if err := my.Create(ctx, u); err != nil {
		t.Fatal(err)
	}
	if err := my.Delete(ctx, u); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLHardDelete(t *testing.T) {
	u := new(User)
	if err := my.First(ctx, u); err != nil {
		t.Fatal(err)
	}
	if err := my.Destroy(ctx, u); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLTable(t *testing.T) {
	uuu := []*User{getFakeUser(), getFakeUser()}
	if err := my.Table("TempUser").Create(ctx, &uuu); err != nil {
		t.Fatal(err)
	}

	users := new([]User)
	if err := my.Table("User").
		WhereLike("Name", "nick%").
		Get(ctx, users); err != nil {
		t.Fatal(err)
	}

	if err := my.Table("User").
		Where("Age", ">", 0).
		Get(ctx, users); err != nil {
		t.Fatal(err)
	}

	user := new(User)
	if err := my.Table("User").
		First(ctx, user); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLRunInTransaction(t *testing.T) {
	if err := my.RunInTransaction(func(txn *goloquent.DB) error {
		u := new(User)
		if err := txn.NewQuery().
			WLock().First(ctx, u); err != nil {
			return err
		}

		u.Name = "NewName"
		u.UpdatedDateTime = time.Now().UTC()
		return txn.Save(ctx, u)
	}); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLScan(t *testing.T) {
	var count, sum uint
	if err := my.Table("User").
		Select("COALESCE(COUNT(*),0), COALESCE(SUM(Age),0)").
		Scan(ctx, &count, &sum); err != nil {
		t.Fatal(err)
	}
	log.Println("Count :", count, ", Sum :", sum)
}

func TestMySQLClose(t *testing.T) {
	defer my.Close()
}
