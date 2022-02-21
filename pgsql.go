package sql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type Pg struct {
	db    *sql.DB
	table *PgTable
	query *PgQuery
	dsn   bytes.Buffer
	*meta
}

type meta struct {
	dbName        string
	tableName     string
	fields        string
	where         []*storage
	sort          bytes.Buffer
	limit         int64
	offset        int64
	group         string
	retryTimes    int
	storageCursor int
	filler        []interface{}
	ctx           context.Context
}

type storage struct {
	storageType storageType
	bucket      string
	argc        []interface{}
}

type storageType int

const (
	storageTypeWhereAnd storageType = 1
	storageTypeWhereOr  storageType = 2
	storageTypeSaveData storageType = 3
)

type opType int

const (
	opTypeQuery   opType = 1
	opTypeCount   opType = 2
	opTypeSum     opType = 3
	opTypeAvg     opType = 4
	opTypeCreate  opType = 5
	opTypeDelete  opType = 6
	opTypeSave    opType = 7
	opTypeSaveInt opType = 8
	opTypeSaveDec opType = 9
)

//NewPg is used to create postgres connection
//dsn1: "postgres://pqgotest:password@localhost/pqgotest?sslmode=verify-full"
//dsn2: "port=5433 user=postgres password=123456 dbname=ficow sslmode=disable"
//dsn1 is used here
func NewPg(user, pass, server, dbName, sslMode string) SQL {
	pg := &Pg{meta: &meta{}}
	pg.dsn.WriteString(`postgres://`)
	pg.dsn.WriteString(user)
	pg.dsn.WriteString(":")
	pg.dsn.WriteString(pass)
	pg.dsn.WriteString("@")
	pg.dsn.WriteString(server)
	pg.dsn.WriteString("/")
	pg.dsn.WriteString(dbName)
	pg.dsn.WriteString("?sslmode=")
	pg.dsn.WriteString(sslMode)
	return pg.initialize()
}

func (p *Pg) initialize() SQL {
	var err error
	if p.db, err = sql.Open("postgres", p.dsn.String()); err != nil {
		fmt.Printf("postgres:\t%s\n", err.Error())
		p.retry(p.initialize, 3, 5)
	}

	if err = p.db.Ping(); err != nil {
		fmt.Printf("postgres:\t%s\n", err.Error())
		p.retry(p.initialize, 3, 5)
	}
	//if u want to reset "maxLifeTime","maxOpenConns","maxIdleConns",please use it like this: p.Db().SetConnMaxLifetime(...)
	p.db.SetConnMaxLifetime(time.Minute * 3)
	p.db.SetMaxOpenConns(10)
	p.db.SetMaxIdleConns(10)
	p.table = &PgTable{Pg: p}
	p.query = &PgQuery{Pg: p}
	return p
}

func (p *Pg) retry(f func() SQL, times, sleep int) SQL {
	if p.retryTimes < times {
		p.retryTimes++
		fmt.Printf("Retry\t(%d/3) times after 5 seconds\n", p.retryTimes)
		time.Sleep(time.Duration(sleep) * time.Second)
		f()
	} else {
		fmt.Println("can't connect to postgres")
		os.Exit(0)
	}
	return p
}

func (p *Pg) Conn() *sql.DB {
	return p.db
}

func (p *Pg) Table(tableName string) Table {
	p.tableName = tableName
	p.fields = "*"
	p.ctx = context.Background()
	return p.table
}

func (p *Pg) clear() {
	p.meta = &meta{}
}

type PgTable struct {
	*Pg
}

func (p *PgTable) Select(fields string) Table {
	p.fields = fields
	return p.table
}

func (p *PgTable) Where(where string, argc ...interface{}) Table {
	p.where = append(p.where, &storage{
		storageType: storageTypeWhereAnd,
		bucket:      where,
		argc:        argc,
	})
	return p
}

func (p *PgTable) WhereOr(where string, argc ...interface{}) Table {
	p.where = append(p.where, &storage{
		storageType: storageTypeWhereOr,
		bucket:      where,
		argc:        argc,
	})
	return p
}

func (p *PgTable) Sort(filed string, sortBy string) Query {
	p.sort.Reset()
	p.sort.WriteString(" ORDER BY ")
	p.sort.WriteString(filed)
	p.sort.WriteString(" ")
	p.sort.WriteString(sortBy)
	return p.query
}

func (p *PgTable) Offset(offset int64) Query {
	p.offset = offset
	return p.query
}

func (p *PgTable) Limit(limit int64) Query {
	p.limit = limit
	return p.query
}

func (p *PgTable) Group(group string) Query {
	p.group = group
	return p.query
}

func (p *PgTable) Find(dest interface{}) error {
	sql := p.parseSQL(opTypeQuery)
	defer p.clear()
	stmt, err := p.db.PrepareContext(p.ctx, sql.String())
	if err != nil {
		return fmt.Errorf("find:prepare sql error:%w", err)
	}
	row, err := stmt.QueryContext(p.ctx, p.filler...)
	if err != nil {
		return fmt.Errorf("find:query context:%w", err)
	}
	columns, err := row.Columns()
	if err != nil {
		return err
	}
	isSlice, err := p.checkIsSlice(dest)
	if err != nil {
		return fmt.Errorf("find:%w", err)
	}
	receiver := make([]interface{}, len(columns))
	var destFieldLen int
	var metaElem interface{}
	if isSlice {
		destType := reflect.Indirect(reflect.ValueOf(dest)).Type()
		valueElem := reflect.New(destType.Elem())
		metaElem = reflect.New(destType.Elem()).Interface()
		destFieldLen = valueElem.Elem().NumField()
	} else {
		destFieldLen = reflect.TypeOf(dest).Elem().NumField()
		metaElem = dest
	}
	var receiverMap = make(map[string]int)
	for i, column := range columns {
		var tmp string
		receiver[i] = &tmp
		receiverMap[column] = i
	}
	for i := 0; i < destFieldLen; i++ {
		obj := reflect.TypeOf(metaElem).Elem().Field(i)
		tag := obj.Tag.Get("json")
		if tag == "" {
			tag = obj.Name
		}
		receiverIdx, findOK := receiverMap[tag]
		if !findOK {
			continue
		}
		fieldType := reflect.ValueOf(metaElem).Elem().Field(i).Type().String()
		switch fieldType {
		case "int", "uint", "int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64":
			var intMeta int64
			receiver[receiverIdx] = &intMeta
		case "float32", "float64":
			var floatMeta float64
			receiver[receiverIdx] = &floatMeta
		case "time.Time":
			var timeMeta = time.Time{}
			receiver[receiverIdx] = &timeMeta
		case "*time.Time":
			var timeMeta = &time.Time{}
			receiver[receiverIdx] = &timeMeta
		case "*string":
			var strMeta string
			tmp := &strMeta
			receiver[receiverIdx] = &tmp
		case "bool":
			var boolMeta bool
			receiver[receiverIdx] = &boolMeta
		default:
			var strMeta string
			receiver[receiverIdx] = &strMeta
		}
	}
	var out = reflect.Indirect(reflect.ValueOf(dest))
	for row.Next() {
		err = row.Scan(receiver...)
		if err != nil {
			return fmt.Errorf("find:scan record error:%w", err)
		}
		packJson := map[string]interface{}{}
		for key, idx := range receiverMap {
			packJson[key] = receiver[idx]
		}
		jsonByte, err := json.Marshal(packJson)
		if err != nil {
			return fmt.Errorf("find:json marshal error:%w", err)
		}
		if !isSlice {
			if err := json.Unmarshal(jsonByte, dest); err != nil {
				return fmt.Errorf("find:json unmarshal error:%w", err)
			}
			return nil
		}
		if err := json.Unmarshal(jsonByte, metaElem); err != nil {
			return fmt.Errorf("find:json unmarshal error:%w", err)
		}
		out = reflect.Append(out, reflect.ValueOf(metaElem).Elem())
	}
	reflect.ValueOf(dest).Elem().Set(reflect.ValueOf(out.Interface()))
	return nil
}

func (p *PgTable) Count(count *int64) error {
	if p.fields == "*" {
		p.fields = "COUNT(*)"
	}
	sql := p.parseSQL(opTypeCount)
	defer p.clear()
	stmt, err := p.db.PrepareContext(p.ctx, sql.String())
	if err != nil {
		return fmt.Errorf("count:prepare sql error:%w", err)
	}
	if err = stmt.QueryRowContext(p.ctx, p.filler...).Scan(count); err != nil {
		return fmt.Errorf("count:query row context error:%w", err)
	}
	return nil
}

func (p *PgTable) Sum(sum *int64) error {
	if p.fields == "*" {
		return fmt.Errorf("sum:please use 'Select(fieldName)' to set the sum field")
	}
	sql := p.parseSQL(opTypeSum)
	defer p.clear()
	stmt, err := p.db.PrepareContext(p.ctx, sql.String())
	if err != nil {
		return fmt.Errorf("sum:prepare sql error:%w", err)
	}
	if err = stmt.QueryRowContext(p.ctx, p.filler...).Scan(sum); err != nil {
		return fmt.Errorf("sum:query row context error:%w", err)
	}
	return nil
}

func (p *PgTable) Avg(avg *int64) error {
	if p.fields == "*" {
		return fmt.Errorf("avg:please use 'Select(fieldName)' to set the avg field")
	}
	sql := p.parseSQL(opTypeSum)
	defer p.clear()
	stmt, err := p.db.PrepareContext(p.ctx, sql.String())
	if err != nil {
		return fmt.Errorf("avg:prepare sql error:%w", err)
	}
	if err = stmt.QueryRowContext(p.ctx, p.filler...).Scan(avg); err != nil {
		return fmt.Errorf("avg:query row context error:%w", err)
	}
	return nil
}

func (p *PgTable) Update(dest interface{}) error {
	sql := p.parseSQL(opTypeSave)
	defer p.clear()
	isMap, err := p.checkUpdateType(dest)
	if err != nil {
		fmt.Errorf("update:%w", err)
	}
	var fieldList, valueList []string
	var updateNum = p.storageCursor

	if isMap {
		m := reflect.ValueOf(dest).Elem()
		ranger := m.MapRange()
		for ranger.Next() {
			updateNum++
			key := ranger.Key().Interface()
			value := ranger.Value().Interface()
			if realKey, ok := key.(string); ok {
				fieldList = append(fieldList, fmt.Sprintf("\"%s\"", realKey))
			}
			valueList = append(valueList, fmt.Sprintf("$%d", updateNum))
			p.filler = append(p.filler, value)
		}
	} else {
		var curRowsNum int
		rowsNum := reflect.TypeOf(dest).Elem().NumField()
		for curRowsNum < rowsNum {
			updateNum++
			obj := reflect.TypeOf(dest).Elem().Field(curRowsNum)
			jsonName := obj.Tag.Get("json")
			if jsonName == "" {
				jsonName = obj.Name
			}
			if isPri := obj.Tag.Get("pri"); isPri != "" {
				updateNum--
			} else {
				fieldList = append(fieldList, jsonName)
				p.filler = append(p.filler, reflect.ValueOf(dest).Elem().Field(curRowsNum).Interface())
				valueList = append(valueList, fmt.Sprintf("$%d", updateNum))
			}
			curRowsNum++
		}
	}

	sqlStr := strings.ReplaceAll(sql.String(), "$FIELDS", fmt.Sprintf("(%s)", strings.Join(fieldList, ",")))
	sqlStr = strings.ReplaceAll(sqlStr, "$VALUES", fmt.Sprintf("(%s)", strings.Join(valueList, ",")))
	stmt, err := p.db.PrepareContext(p.ctx, sqlStr)
	if err != nil {
		return fmt.Errorf("update:prepare sql error:%w", err)
	}
	if _, err = stmt.ExecContext(p.ctx, p.filler...); err != nil {
		return fmt.Errorf("update:exec context:%w", err)
	}

	return nil
}

func (p *PgTable) Save(dest interface{}) error {
	sql := p.parseSQL(opTypeCreate)
	defer p.clear()
	isSlice, err := p.checkIsSlice(dest)
	if err != nil {
		fmt.Errorf("save:%w", err)
	}
	var metaElem interface{}
	var rowsNum int
	var columnsNum = 1
	if isSlice {
		columnsNum = reflect.ValueOf(dest).Elem().Len()
		destType := reflect.Indirect(reflect.ValueOf(dest)).Type()
		valueElem := reflect.New(destType.Elem())
		metaElem = valueElem.Interface()
		rowsNum = valueElem.Elem().NumField()
	} else {
		rowsNum = reflect.TypeOf(dest).Elem().NumField()
		metaElem = dest
	}
	var curColumnsNum, insertNum int
	var fieldList, valueList []string
	var insertArgs []interface{}
	for curColumnsNum < columnsNum {
		var curRowsNum int
		var curValueList []string
		for curRowsNum < rowsNum {
			insertNum++
			if isSlice {
				sliceElem := reflect.ValueOf(dest).Elem().Index(curColumnsNum).Addr().Interface()
				metaElem = sliceElem
			}
			obj := reflect.TypeOf(metaElem).Elem().Field(curRowsNum)
			jsonName := obj.Tag.Get("json")
			if jsonName == "" {
				jsonName = obj.Name
			}
			if curColumnsNum == 0 {
				fieldList = append(fieldList, jsonName)
			}
			if isPri := obj.Tag.Get("pri"); isPri != "" {
				curValueList = append(curValueList, "DEFAULT")
				insertNum--
			} else {
				insertArgs = append(insertArgs, reflect.ValueOf(metaElem).Elem().Field(curRowsNum).Interface())
				curValueList = append(curValueList, fmt.Sprintf("$%d", insertNum))
			}
			curRowsNum++
		}
		valueList = append(valueList, strings.Join(curValueList, ","))
		curColumnsNum++
	}

	sqlStr := strings.ReplaceAll(sql.String(), "$FIELDS", fmt.Sprintf("(%s)", strings.Join(fieldList, ",")))
	sqlStr = strings.ReplaceAll(sqlStr, "$VALUES", fmt.Sprintf("(%s)", strings.Join(valueList, "),(")))
	stmt, err := p.db.PrepareContext(p.ctx, sqlStr)
	if err != nil {
		return fmt.Errorf("save:prepare sql error:%w", err)
	}
	if _, err = stmt.ExecContext(p.ctx, insertArgs...); err != nil {
		return fmt.Errorf("save:exec context:%w", err)
	}
	return nil
}

func (p *PgTable) Delete() error {
	sql := p.parseSQL(opTypeDelete)
	defer p.clear()
	if p.where == nil {
		return fmt.Errorf("delete:must have deletion condition")
	}
	stmt, err := p.db.PrepareContext(p.ctx, sql.String())
	if err != nil {
		return fmt.Errorf("delete:prepare sql error:%w", err)
	}
	if _, err = stmt.ExecContext(p.ctx, p.filler...); err != nil {
		return fmt.Errorf("delete:exec context:%w", err)
	}

	return nil
}

func (p *PgTable) SetInc(field string) error {
	sql := p.parseSQL(opTypeSaveInt)
	defer p.clear()
	sqlStr := strings.ReplaceAll(sql.String(), "$FIELDS", field)
	stmt, err := p.db.PrepareContext(p.ctx, sqlStr)
	if err != nil {
		return fmt.Errorf("save inc:prepare sql error:%w", err)
	}
	if _, err = stmt.ExecContext(p.ctx, p.meta.filler...); err != nil {
		return fmt.Errorf("save inc:exec context:%w", err)
	}
	return nil
}

func (p *PgTable) SetDec(field string) error {
	sql := p.parseSQL(opTypeSaveDec)
	defer p.clear()
	sqlStr := strings.ReplaceAll(sql.String(), "$FIELDS", field)
	stmt, err := p.db.PrepareContext(p.ctx, sqlStr)
	if err != nil {
		return fmt.Errorf("save dec:prepare sql error:%w", err)
	}
	if _, err = stmt.ExecContext(p.ctx, p.meta.filler...); err != nil {
		return fmt.Errorf("save dec:exec context:%w", err)
	}
	return nil
}

func (p *PgTable) parseSQL(op interface{}) (cond bytes.Buffer) {
	tableName := p.parseTableName()
	switch op.(opType) {
	case opTypeQuery:
		cond.WriteString("SELECT ")
		cond.WriteString(p.fields)
		cond.WriteString(" FROM ")
		cond.Write(tableName.Bytes())
		where := p.parseWhere()
		if where.Len() != 0 {
			cond.WriteString(" WHERE ")
			cond.Write(where.Bytes())
		}
		if p.group != "" {
			cond.WriteString(" GROUP BY ")
			cond.WriteString(p.group)
		}
		if p.sort.Len() != 0 {
			cond.WriteString(p.sort.String())
		}
		if p.offset > 0 {
			cond.WriteString(" OFFSET ")
			cond.WriteString(strconv.FormatInt(p.offset, 10))
		}
		if p.limit > 0 {
			cond.WriteString(" LIMIT ")
			cond.WriteString(strconv.FormatInt(p.limit, 10))
		}
	case opTypeCount:
		cond.WriteString("SELECT ")
		cond.WriteString(p.fields)
		cond.WriteString(" FROM ")
		cond.Write(tableName.Bytes())
		where := p.parseWhere()
		if where.Len() != 0 {
			cond.WriteString(" WHERE ")
			cond.Write(where.Bytes())
		}
	case opTypeSum:
		cond.WriteString("SELECT SUM(")
		cond.WriteString(p.fields)
		cond.WriteString(") FROM ")
		cond.Write(tableName.Bytes())
		where := p.parseWhere()
		if where.Len() != 0 {
			cond.WriteString(" WHERE ")
			cond.Write(where.Bytes())
		}
	case opTypeAvg:
		cond.WriteString("SELECT AVG(")
		cond.WriteString(p.fields)
		cond.WriteString(") FROM ")
		cond.Write(tableName.Bytes())
		where := p.parseWhere()
		if where.Len() != 0 {
			cond.WriteString(" WHERE ")
			cond.Write(where.Bytes())
		}
	case opTypeCreate:
		cond.WriteString("INSERT INTO ")
		cond.Write(tableName.Bytes())
		cond.WriteString("$FIELDS VALUES $VALUES")
	case opTypeSave:
		cond.WriteString("UPDATE ")
		cond.Write(tableName.Bytes())
		cond.WriteString(" SET $FIELDS = $VALUES")
		where := p.parseWhere()
		if where.Len() != 0 {
			cond.WriteString(" WHERE ")
			cond.Write(where.Bytes())
		}
	case opTypeDelete:
		cond.WriteString("DELETE FROM ")
		cond.Write(tableName.Bytes())
		where := p.parseWhere()
		if where.Len() != 0 {
			cond.WriteString(" WHERE ")
			cond.Write(where.Bytes())
		}
	case opTypeSaveInt:
		cond.WriteString("UPDATE ")
		cond.Write(tableName.Bytes())
		cond.WriteString(" SET $FIELDS = $FIELDS + 1")
		where := p.parseWhere()
		if where.Len() != 0 {
			cond.WriteString(" WHERE ")
			cond.Write(where.Bytes())
		}
	case opTypeSaveDec:
		cond.WriteString("UPDATE ")
		cond.Write(tableName.Bytes())
		cond.WriteString(" SET $FIELDS = $FIELDS - 1")
		where := p.parseWhere()
		if where.Len() != 0 {
			cond.WriteString(" WHERE ")
			cond.Write(where.Bytes())
		}
	}
	return
}

func (p *PgTable) parseTableName() (cond bytes.Buffer) {
	// only mysql can use it
	//if p.meta.dbName != "" {
	//	cond.WriteString("`")
	//	cond.WriteString(p.dbName)
	//	cond.WriteString("`.")
	//}
	if p.meta.tableName != "" {
		cond.WriteString(`"`)
		cond.WriteString(p.tableName)
		cond.WriteString(`"`)
	}
	return
}

func (p *PgTable) parseWhere() (cond bytes.Buffer) {
	if p.meta.where != nil {
		for _, row := range p.meta.where {
			p.storageCursor++
			if row.storageType == storageTypeWhereOr && cond.Len() != 0 {
				cond.WriteString(" OR ")
			}
			if row.storageType == storageTypeWhereAnd && cond.Len() != 0 {
				cond.WriteString(" AND ")
			}
			tag := fmt.Sprintf("$%d", p.storageCursor)
			cond.WriteString(strings.ReplaceAll(row.bucket, "?", tag))
			p.filler = append(p.filler, row.argc...)
		}
	}
	return
}

func (p *PgTable) checkIsSlice(dest interface{}) (isSlice bool, err error) {
	switch reflect.TypeOf(dest).Kind() {
	case reflect.Ptr:
		switch reflect.ValueOf(dest).Elem().Kind() {
		case reflect.Slice:
			isSlice = true
		case reflect.Struct:
			isSlice = false
		//case reflect.Map:
		//	isSlice = false
		default:
			err = fmt.Errorf("checkIsSlice:dest must be a slice/struct pointer")
			return
		}
	default:
		err = fmt.Errorf("checkIsSlice:dest must be a slice/struct pointer")
		return
	}
	return
}

func (p *PgTable) checkUpdateType(dest interface{}) (isMap bool, err error) {
	switch reflect.TypeOf(dest).Kind() {
	case reflect.Ptr:
		switch reflect.ValueOf(dest).Elem().Kind() {
		case reflect.Struct:
			isMap = false
		case reflect.Map:
			isMap = true
		default:
			err = fmt.Errorf("checkUpdateType:dest must be a map/struct pointer")
			return
		}
	default:
		err = fmt.Errorf("checkUpdateType:dest must be a map/struct pointer")
		return
	}
	return
}

type PgQuery struct {
	*Pg
}

func (p *PgQuery) Sort(filed string, sortBy string) Query {
	return p.table.Sort(filed, sortBy)
}

func (p *PgQuery) Offset(offset int64) Query {
	return p.table.Offset(offset)
}

func (p *PgQuery) Limit(limit int64) Query {
	return p.table.Limit(limit)
}

func (p *PgQuery) Group(group string) Query {
	p.group = group
	return p.query
}

func (p *PgQuery) Find(dest interface{}) error {
	return p.table.Find(dest)
}

func (p *PgQuery) Count(count *int64) error {
	return p.table.Count(count)
}

func (p *PgQuery) Sum(sum *int64) error {
	return p.table.Sum(sum)
}

func (p *PgQuery) Avg(avg *int64) error {
	return p.table.Avg(avg)
}
