package sql

import (
	"bytes"
	"database/sql"
	"reflect"
	"testing"
	"time"
)

//if u want to run the go test,please use isFakeTest = false & to set right NewPg(...) feature
var isFakeTest = true

func conn() (pg SQL, fakeTest bool) {
	fakeTest = isFakeTest
	if fakeTest == true {
		pg = nil
		return
	}
	pg = NewPg("postgres1", "password1", "localhost:5566", "testDb", "disable")
	return
}

func TestNewPg(t *testing.T) {
	if pg, isFakeConn := conn(); !isFakeConn {
		if pg.Conn() == nil {
			t.Error("test postgres connection failed")
		}
	}
}

func TestPg_initialize(t *testing.T) {
	if pg, isFakeConn := conn(); !isFakeConn {
		pg.initialize()
		if pg.Conn() == nil {
			t.Error("test postgres connection failed")
		}
	}
}

func TestPg_retry(t *testing.T) {
	db, isFakeConn := conn()
	if isFakeConn {
		return
	}

	type fields struct {
		db         *sql.DB
		table      *PgTable
		query      *PgQuery
		dsn        bytes.Buffer
		retryTimes int
		meta       *meta
	}
	type args struct {
		f func() SQL
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int
	}{
		{
			name: "test retry",
			fields: fields{
				db:         db.Conn(),
				table:      nil,
				query:      nil,
				dsn:        bytes.Buffer{},
				retryTimes: 0,
				meta:       nil,
			},
			args: args{
				f: func() SQL {
					return db
				},
			},
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pg{
				db:    tt.fields.db,
				table: tt.fields.table,
				query: tt.fields.query,
				dsn:   tt.fields.dsn,
				//retryTimes: tt.fields.retryTimes,
				meta: tt.fields.meta,
			}
			if p.retry(tt.args.f, 1, 0); !reflect.DeepEqual(p.retryTimes, tt.want) {
				t.Errorf("retry() = %v, want %v", p.retryTimes, tt.want)
			}
		})
	}
}

func TestPg_Conn(t *testing.T) {
	connect, isFakeConn := conn()
	if isFakeConn {
		return
	}
	db := connect.Conn()
	type fields struct {
		db         *sql.DB
		table      *PgTable
		query      *PgQuery
		dsn        bytes.Buffer
		retryTimes int
		meta       *meta
	}
	tests := []struct {
		name   string
		fields fields
		want   *sql.DB
	}{
		{
			name: "test Conn",
			fields: fields{
				db: db,
			},
			want: &sql.DB{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pg{
				db: tt.fields.db,
			}
			if got := p.Conn(); reflect.TypeOf(got) != reflect.TypeOf(tt.want) {
				t.Errorf("Conn() = %v, want %v", got, tt.want)
			}
		})
	}
}

type TestTable struct {
	Id        int        `json:"id" pri:"true"`
	Name      string     `json:"name"`
	Desc      string     `json:"desc"`
	Address   string     `json:"address"`
	CreatedAt time.Time  `json:"created_date"`
	ChangedAt time.Time  `json:"changed_date"`
	DeletedAt *time.Time `json:"deleted_date"`
	IsFirst   bool       `json:"is_first"`
}

func TestPgTable_Find(t *testing.T) {
	db, isFakeConn := conn()
	if isFakeConn {
		return
	}
	var app []TestTable
	err := db.Table("app").
		//Select("id,name,description,icon").
		Where("id=?", 62).
		WhereOr("id=?", 1).
		Where("type=?", "normal").Sort("id", "desc").
		Offset(0).Limit(5).Find(&app)
	if err != nil {
		t.Error(err)
	}
	var app2 TestTable
	err = db.Table("app").
		//Select("id,name,description,icon").
		WhereOr("id=?", 63).
		Where("type=?", "normal").Sort("id", "desc").
		Offset(0).Limit(1).Find(&app2)
	if err != nil {
		t.Error(err)
	}
}

func TestPgTable_Count(t *testing.T) {
	db, isFakeConn := conn()
	if isFakeConn {
		return
	}
	var count int64
	err := db.Table("app").
		//Select("id,name,description,icon").
		Where("id=?", 62).
		WhereOr("id=?", 1).
		Where("type=?", "normal").Sort("id", "desc").
		Offset(0).Limit(5).Count(&count)
	if err != nil {
		t.Error(err)
	}
}

func TestPgTable_Sum(t *testing.T) {
	db, isFakeConn := conn()
	if isFakeConn {
		return
	}
	var sum int64
	err := db.Table("app").
		Select("id").
		Where("id=?", 62).
		WhereOr("id=?", 1).
		Where("type=?", "normal").
		Sum(&sum)
	if err != nil {
		t.Error(err)
	}
}

func TestPgTable_Save(t *testing.T) {
	db, isFakeConn := conn()
	if isFakeConn {
		return
	}
	app := []TestTable{
		{
			Name:      "aaa",
			Id:        0,
			Desc:      "aaa",
			Address:   "bbb",
			CreatedAt: time.Now(),
			ChangedAt: time.Now(),
		},
		{
			Name:      "ddd",
			Id:        0,
			Desc:      "eee",
			Address:   "ggg",
			CreatedAt: time.Now(),
			ChangedAt: time.Now(),
		},
	}
	err := db.Table("app").Save(&app)
	if err != nil {
		t.Error(err)
	}
}

func TestPgTable_Delete(t *testing.T) {
	db, isFakeConn := conn()
	if isFakeConn {
		return
	}
	err := db.Table("app").Where("id=?", 6).Delete()
	if err != nil {
		t.Error(err)
	}
}

func TestPgTable_Update(t *testing.T) {
	db, isFakeConn := conn()
	if isFakeConn {
		return
	}
	app := TestTable{
		Name:      "aaa",
		Id:        0,
		Desc:      "aaa",
		Address:   "bbb",
		CreatedAt: time.Now(),
		ChangedAt: time.Now(),
		IsFirst:   true,
	}
	err := db.Table("app").Where("id=?", 62).Update(&app)
	if err != nil {
		t.Error(err)
	}
}
