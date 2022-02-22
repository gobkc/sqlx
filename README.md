# sqlx

Golang's lightweight ORM Library


Overview
---

- Part-Featured ORM
- SQL Builder, Create/Delete/Find/Save/Count/Sum/Max/Avg/SetInc/SetDev with SQL Expr
- Developer Not Friendly,Because I'm Rubbish

Contributing
---

You can commit PR to this repository

Quick start
---
````
package main

import "github.com/gobkc/sqlx"

func main() {
    db := sqlx.NewPg("postgres1", "password1", "localhost:5566", "testDb", "disable")
    err := db.Table("app").Where("id=?", 62).Update(&map[string]interface{}{"name":"123"})
	if err != nil {
		fmt.Println(err)
	}
}
````
Please refer to [pgsql_test.go](https://github.com/gobkc/sqlx/blob/main/pgsql_test.go) document for more example

License
---

© Gobkc, 2022~time.Now

Released under the Apache License

