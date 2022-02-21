package sql

import (
	"bytes"
	"database/sql"
)

type SQL interface {
	initialize() SQL
	Conn() *sql.DB
	Table(tableName string) Table
	clear()
}

type Table interface {
	Select(fields string) Table
	Where(where string, argc ...interface{}) Table
	WhereOr(where string, argc ...interface{}) Table
	Sort(filed string, sortBy string) Query
	Offset(offset int64) Query
	Limit(limit int64) Query
	Group(group string) Query
	Find(dest interface{}) error
	Count(count *int64) error
	Sum(sum *int64) error
	Avg(avg *int64) error
	Update(dest interface{}) error
	Save(dest interface{}) error
	Delete() error
	SetInc(field string) error // feature:field value + 1
	SetDec(field string) error // feature:field value - 1
	parseSQL(op interface{}) bytes.Buffer
}

type Query interface {
	Sort(filed string, sortBy string) Query
	Offset(offset int64) Query
	Limit(limit int64) Query
	Group(group string) Query
	Find(dest interface{}) error
	Count(count *int64) error
	Sum(sum *int64) error
	Avg(avg *int64) error
}
